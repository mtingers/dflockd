package httpapi

import (
	"context"
	"crypto/subtle"
	"crypto/tls"
	_ "embed"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mtingers/dflockd/internal/config"
	"github.com/mtingers/dflockd/internal/server"
)

// openAPISpec is the hand-authored OpenAPI 3.1 contract document for the
// HTTP API. Mirrored to docs/openapi.json for tooling that prefers a
// file reference. The drift test (openapi_test.go) enforces both copies
// stay in sync with each other and with the registered routes.
//
//go:embed openapi.json
var openAPISpec []byte

// httpServer wraps a *http.Server plus the bridge it delegates to.
type httpServer struct {
	bridge *Bridge
	cfg    *config.Config
	log    *slog.Logger
	srv    *http.Server
}

// Run starts the HTTP API listener on the configured host+port and blocks
// until ctx is cancelled, at which point it gracefully drains sessions
// and closes the listener. Returns nil on clean shutdown.
//
// Run is the single public entry point used by cmd/dflockd/main.go.
func Run(ctx context.Context, srv *server.Server, cfg *config.Config, log *slog.Logger) error {
	if cfg.HTTPPort == 0 {
		return nil // disabled
	}
	host := cfg.HTTPHost
	if host == "" {
		host = cfg.Host
	}
	addr := net.JoinHostPort(host, strconv.Itoa(cfg.HTTPPort))

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("http listen: %w", err)
	}

	// Reuse the TCP server's TLS cert/key if configured so operators only
	// manage one set of certs.
	if cfg.TLSCert != "" && cfg.TLSKey != "" {
		cert, err := tls.LoadX509KeyPair(cfg.TLSCert, cfg.TLSKey)
		if err != nil {
			listener.Close()
			return fmt.Errorf("http tls: %w", err)
		}
		tlsCfg := &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}
		listener = tls.NewListener(listener, tlsCfg)
		log.Info("http TLS enabled")
	}

	bridge := NewBridge(ctx, srv, cfg, log, cfg.HTTPSessionIdleTimeout, cfg.HTTPMaxSessions)
	var bridgeShutdownOnce sync.Once
	bridgeDone := make(chan struct{})
	startBridgeShutdown := func() {
		bridgeShutdownOnce.Do(func() {
			go func() {
				bridge.Shutdown()
				close(bridgeDone)
			}()
		})
	}

	hs := &httpServer{
		bridge: bridge,
		cfg:    cfg,
		log:    log,
	}

	mux := http.NewServeMux()
	hs.registerRoutes(mux)

	hs.srv = &http.Server{
		Handler: hs.withAuth(jsonRouteErrors(mux)),
		// Leave ReadTimeout/WriteTimeout at 0 (unlimited) so long-poll
		// acquires with large --default-lease-ttl values aren't cut off.
		// ReadHeaderTimeout still protects against slowloris.
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
		ErrorLog:          nil, // slog is our path; don't double-log
	}
	hs.srv.RegisterOnShutdown(startBridgeShutdown)

	log.Info("http listening", "addr", addr)

	// Run the HTTP server in its own goroutine so we can coordinate
	// shutdown via ctx.
	serveErr := make(chan error, 1)
	go func() {
		if err := hs.srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
			return
		}
		serveErr <- nil
	}()

	select {
	case <-ctx.Done():
		startBridgeShutdown()
		shutdownTimeout := cfg.ShutdownTimeout
		if shutdownTimeout <= 0 {
			shutdownTimeout = 30 * time.Second
		}
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		_ = hs.srv.Shutdown(shutdownCtx)
		<-bridgeDone
		<-serveErr
		return nil
	case err := <-serveErr:
		startBridgeShutdown()
		<-bridgeDone
		return err
	}
}

// withAuth wraps the mux with an auth check. If --auth-token is set, every
// request must carry `Authorization: Bearer <token>`. The `/v1/openapi.json`
// endpoint is exempt so the spec can be fetched by any tool.
func (h *httpServer) withAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h.cfg.AuthToken == "" {
			next.ServeHTTP(w, r)
			return
		}
		// Exempt: the OpenAPI spec describes auth; it shouldn't itself
		// require auth.
		if r.URL.Path == "/v1/openapi.json" {
			next.ServeHTTP(w, r)
			return
		}
		got := extractBearerToken(r.Header.Get("Authorization"))
		if got == "" || subtle.ConstantTimeCompare([]byte(got), []byte(h.cfg.AuthToken)) != 1 {
			writeError(w, http.StatusUnauthorized, "unauthorized", "")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func extractBearerToken(header string) string {
	const prefix = "Bearer "
	if strings.HasPrefix(header, prefix) {
		return strings.TrimSpace(header[len(prefix):])
	}
	return ""
}

func jsonRouteErrors(mux *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, pattern := mux.Handler(r); pattern != "" {
			mux.ServeHTTP(w, r)
			return
		}

		if allowed := allowedMethodsForPath(mux, r); len(allowed) > 0 {
			w.Header().Set("Allow", strings.Join(allowed, ", "))
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "")
			return
		}

		writeError(w, http.StatusNotFound, "not_found", "")
	})
}

func allowedMethodsForPath(mux *http.ServeMux, r *http.Request) []string {
	seen := make(map[string]bool)
	var allowed []string
	for _, route := range registeredRoutes {
		for _, method := range route.Methods {
			allowed = appendAllowedMethodForPath(allowed, seen, mux, r, method)
			if method == http.MethodGet {
				allowed = appendAllowedMethodForPath(allowed, seen, mux, r, http.MethodHead)
			}
		}
	}
	return allowed
}

func appendAllowedMethodForPath(allowed []string, seen map[string]bool, mux *http.ServeMux, r *http.Request, method string) []string {
	if seen[method] {
		return allowed
	}
	clone := new(http.Request)
	*clone = *r
	clone.Method = method
	if _, pattern := mux.Handler(clone); pattern == "" {
		return allowed
	}
	seen[method] = true
	return append(allowed, method)
}

// ---------------------------------------------------------------------------
// Route registration
// ---------------------------------------------------------------------------

// RegisteredPath is a compile-time-visible list of every path the HTTP
// server handles, with its method(s). The OpenAPI drift test (phase 4)
// walks this list and asserts 1:1 correspondence with paths documented in
// openapi.json.
type RegisteredPath struct {
	// Pattern is the human-readable path template using {placeholder}
	// syntax. e.g. "/v1/locks/{key}".
	Pattern string
	// Methods is the set of HTTP methods this pattern handles.
	Methods []string
}

// registeredRoutes is the canonical list. Keep in sync with
// registerRoutes — the OpenAPI drift test enforces this. Kept as a
// package-level var so we don't reallocate on every call.
var registeredRoutes = []RegisteredPath{
	{Pattern: "/v1/sessions", Methods: []string{"POST"}},
	{Pattern: "/v1/sessions/{id}", Methods: []string{"DELETE"}},
	{Pattern: "/v1/sessions/{id}/ping", Methods: []string{"POST"}},
	{Pattern: "/v1/stats", Methods: []string{"GET"}},
	{Pattern: "/v1/locks/{key}", Methods: []string{"POST"}},
	{Pattern: "/v1/locks/{key}/release", Methods: []string{"POST"}},
	{Pattern: "/v1/locks/{key}/renew", Methods: []string{"POST"}},
	{Pattern: "/v1/locks/{key}/enqueue", Methods: []string{"POST"}},
	{Pattern: "/v1/locks/{key}/wait", Methods: []string{"POST"}},
	{Pattern: "/v1/semaphores/{key}", Methods: []string{"POST"}},
	{Pattern: "/v1/semaphores/{key}/release", Methods: []string{"POST"}},
	{Pattern: "/v1/semaphores/{key}/renew", Methods: []string{"POST"}},
	{Pattern: "/v1/semaphores/{key}/enqueue", Methods: []string{"POST"}},
	{Pattern: "/v1/semaphores/{key}/wait", Methods: []string{"POST"}},
	{Pattern: "/v1/signals/{channel}", Methods: []string{"POST"}},
	{Pattern: "/v1/signals", Methods: []string{"GET"}}, // SSE stream
	{Pattern: "/v1/openapi.json", Methods: []string{"GET"}},
}

// Routes exposes the registered route list for tests.
func Routes() []RegisteredPath { return registeredRoutes }

// registerRoutes wires up the ServeMux using Go 1.22+ method+pattern
// syntax. Path parameters come from r.PathValue; each `{var}` matches a
// single URL path segment, so a lock literally named "release" routes
// to `POST /v1/locks/release` (acquire with key="release") while
// `POST /v1/locks/release/release` correctly dispatches to the release
// action on that key. Previously a manual last-segment split ambiguated
// these cases.
//
// Keys with literal `/` must be percent-encoded by the caller (`%2F`);
// ServeMux decodes path values so the handler sees the slash. However,
// `{var}` only matches a single segment, so `POST /v1/locks/foo/bar`
// (unencoded) falls through to 404 instead of misrouting — a
// deliberate trade-off for predictable behavior.
func (h *httpServer) registerRoutes(mux *http.ServeMux) {
	// Sessions
	mux.HandleFunc("POST /v1/sessions", h.handleCreateSession)
	mux.HandleFunc("DELETE /v1/sessions/{id}", withSessionID(h.handleDeleteSession))
	mux.HandleFunc("POST /v1/sessions/{id}/ping", withSessionID(h.handlePingSession))

	// Introspection
	mux.HandleFunc("GET /v1/stats", h.handleStats)
	mux.HandleFunc("GET /v1/openapi.json", h.handleOpenAPI)

	// Locks
	mux.HandleFunc("POST /v1/locks/{key}", withKey(h.handleAcquireLock))
	mux.HandleFunc("POST /v1/locks/{key}/release", withKey(h.handleReleaseLock))
	mux.HandleFunc("POST /v1/locks/{key}/renew", withKey(h.handleRenewLock))
	mux.HandleFunc("POST /v1/locks/{key}/enqueue", withKey(h.handleEnqueueLock))
	mux.HandleFunc("POST /v1/locks/{key}/wait", withKey(h.handleWaitLock))

	// Semaphores
	mux.HandleFunc("POST /v1/semaphores/{key}", withKey(h.handleAcquireSem))
	mux.HandleFunc("POST /v1/semaphores/{key}/release", withKey(h.handleReleaseSem))
	mux.HandleFunc("POST /v1/semaphores/{key}/renew", withKey(h.handleRenewSem))
	mux.HandleFunc("POST /v1/semaphores/{key}/enqueue", withKey(h.handleEnqueueSem))
	mux.HandleFunc("POST /v1/semaphores/{key}/wait", withKey(h.handleWaitSem))

	// Signals
	mux.HandleFunc("POST /v1/signals/{channel}", withChannel(h.handlePublishSignal))
	mux.HandleFunc("GET /v1/signals", h.handleSSE)
}

// withKey extracts and validates the `{key}` path param, then invokes fn.
// On validation failure, writes 400 bad_request and returns without
// invoking fn.
func withKey(fn func(http.ResponseWriter, *http.Request, string)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		if err := validateRESTKey(key); err != nil {
			writeError(w, http.StatusBadRequest, "bad_request", err.Error())
			return
		}
		fn(w, r, key)
	}
}

// withChannel is the signal-channel equivalent of withKey (validation
// rules are identical; the parameter name differs for clarity in URLs).
func withChannel(fn func(http.ResponseWriter, *http.Request, string)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		channel := r.PathValue("channel")
		if err := validateRESTKey(channel); err != nil {
			writeError(w, http.StatusBadRequest, "bad_request", err.Error())
			return
		}
		fn(w, r, channel)
	}
}

// withSessionID extracts and validates the `{id}` path param
// (32-char lowercase hex) before invoking fn.
func withSessionID(fn func(http.ResponseWriter, *http.Request, string)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if !isValidSessionID(id) {
			writeError(w, http.StatusBadRequest, "bad_request", "invalid session id")
			return
		}
		fn(w, r, id)
	}
}

// isValidSessionID matches the format produced by mintSessionID: 32 hex chars.
func isValidSessionID(id string) bool {
	if len(id) != 32 {
		return false
	}
	for _, c := range id {
		if !(c >= '0' && c <= '9') && !(c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// OpenAPI handler stub (phase 4 will replace with embedded spec).
// ---------------------------------------------------------------------------

func (h *httpServer) handleOpenAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write(openAPISpec)
}
