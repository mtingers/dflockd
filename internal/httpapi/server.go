// Package httpapi implements the REST surface for dflockd.
//
// Architecture: HTTP requests bind a session to a connID, then call
// LockManager methods directly. There is no virtual TCP transport,
// no protocol parsing, and no SSE multiplexer — those layers exist
// to support pub/sub which this server intentionally does not provide.
package httpapi

import (
	"context"
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
	"sync/atomic"
	"time"

	"github.com/mtingers/dflockd/internal/config"
	"github.com/mtingers/dflockd/internal/server"
)

// openAPISpec is the embedded OpenAPI 3.1 contract. The canonical
// source is internal/httpapi/openapi.json; `make openapi-sync` copies
// it to docs/openapi.json for the docs site, and the drift test in
// openapi_test.go fails if the two copies diverge.
//
//go:embed openapi.json
var openAPISpec []byte

// httpServer wires the HTTP listener to the SessionStore + LockManager.
type httpServer struct {
	sessions *SessionStore
	cfg      *config.Config
	log      *slog.Logger
	srv      *http.Server
	metrics  *metricsRegistry
	limiter  *httpRateLimiter
	draining atomic.Bool
}

// Run starts the HTTP API on the configured host:port and blocks
// until ctx is cancelled. Returns nil on clean shutdown. When
// HTTPPort==0 returns immediately (HTTP API disabled).
func Run(ctx context.Context, srv *server.Server, cfg *config.Config, log *slog.Logger) error {
	if cfg.HTTPPort == 0 {
		return nil
	}
	listener, err := buildHTTPListener(cfg, log)
	if err != nil {
		return err
	}
	hs, startShutdown := buildHTTPServer(ctx, srv, cfg, log)
	defer hs.limiter.Stop()
	log.Info("http listening", "addr", listener.Addr())
	return hs.serveUntilDone(ctx, listener, startShutdown, cfg.ShutdownTimeout)
}

// buildHTTPListener resolves the bind address and wraps the listener
// in TLS when configured.
func buildHTTPListener(cfg *config.Config, log *slog.Logger) (net.Listener, error) {
	addr := net.JoinHostPort(httpHostFor(cfg), strconv.Itoa(cfg.HTTPPort))
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("http listen: %w", err)
	}
	return wrapTLSIfConfigured(listener, cfg, log)
}

func httpHostFor(cfg *config.Config) string {
	if cfg.HTTPHost != "" {
		return cfg.HTTPHost
	}
	return cfg.Host
}

func wrapTLSIfConfigured(listener net.Listener, cfg *config.Config, log *slog.Logger) (net.Listener, error) {
	if cfg.TLSCert == "" || cfg.TLSKey == "" {
		return listener, nil
	}
	cert, err := tls.LoadX509KeyPair(cfg.TLSCert, cfg.TLSKey)
	if err != nil {
		listener.Close()
		return nil, fmt.Errorf("http tls: %w", err)
	}
	log.Info("http TLS enabled")
	return tls.NewListener(listener, tlsConfig(cert)), nil
}

func tlsConfig(cert tls.Certificate) *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
}

// buildHTTPServer wires the SessionStore, middleware chain, and
// http.Server together. Returns the server plus a once-only
// session-shutdown trigger.
func buildHTTPServer(ctx context.Context, srv *server.Server, cfg *config.Config, log *slog.Logger) (*httpServer, func()) {
	sessions := NewSessionStore(ctx, srv, cfg.HTTPSessionIdleTimeout, cfg.HTTPMaxSessions, cfg.HTTPMaxSessionsPerIP)
	hs := &httpServer{
		sessions: sessions,
		cfg:      cfg,
		log:      log,
		metrics:  newMetricsRegistry(),
		limiter:  newHTTPRateLimiter(cfg.HTTPRateLimitPerIP, cfg.HTTPRateLimitBurst),
	}
	hs.srv = newStdHTTPServer(hs, cfg)
	startShutdown := newOnceShutdown(sessions)
	hs.srv.RegisterOnShutdown(startShutdown)
	return hs, startShutdown
}

// newStdHTTPServer constructs the *http.Server with our middleware
// chain and ConnState hook installed.
func newStdHTTPServer(hs *httpServer, cfg *config.Config) *http.Server {
	mux := http.NewServeMux()
	hs.registerRoutes(mux)
	connLimiter := newHTTPConnLimiter(cfg.HTTPMaxConnectionsPerIP)
	return &http.Server{
		Handler:           hs.withCORS(hs.withMetrics(mux, hs.withRateLimit(hs.withAuth(jsonRouteErrors(mux))))),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
		ConnState:         connLimiter.ConnState,
	}
}

// newOnceShutdown returns a function that triggers session shutdown
// at most once.
func newOnceShutdown(sessions *SessionStore) func() {
	var once sync.Once
	return func() {
		once.Do(func() { go sessions.Shutdown() })
	}
}

// serveUntilDone runs the http.Server.Serve loop and blocks until ctx
// is cancelled or Serve returns. On context cancellation it triggers
// graceful shutdown bounded by shutdownTimeout.
func (hs *httpServer) serveUntilDone(ctx context.Context, listener net.Listener, startShutdown func(), shutdownTimeout time.Duration) error {
	serveErr := make(chan error, 1)
	go func() { serveErr <- runServe(hs.srv, listener) }()
	select {
	case <-ctx.Done():
		hs.draining.Store(true)
		hs.gracefulShutdown(startShutdown, shutdownTimeout)
		<-serveErr
		return nil
	case err := <-serveErr:
		startShutdown()
		return err
	}
}

// runServe wraps Serve so http.ErrServerClosed becomes a nil return.
func runServe(srv *http.Server, listener net.Listener) error {
	if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (hs *httpServer) gracefulShutdown(startShutdown func(), timeout time.Duration) {
	startShutdown()
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_ = hs.srv.Shutdown(shutdownCtx)
}

// jsonRouteErrors wraps mux to ensure every 404/405 produces a JSON
// errorBody rather than the default plain-text http.Error output.
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

// allowedMethodsForPath returns the HTTP methods registered for r.URL.Path.
// Used by the 405 path to populate the Allow header.
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

// RegisteredPath documents one route for tests/introspection.
type RegisteredPath struct {
	Pattern string
	Methods []string
}

// registeredRoutes is the canonical list of routes. Keep in sync with
// registerRoutes — Routes() exposes it for tests.
var registeredRoutes = []RegisteredPath{
	{Pattern: "/health", Methods: []string{"GET"}},
	{Pattern: "/ready", Methods: []string{"GET"}},
	{Pattern: "/metrics", Methods: []string{"GET"}},
	{Pattern: "/v1/openapi.json", Methods: []string{"GET"}},
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
}

// Routes returns the registered route list for tests.
func Routes() []RegisteredPath { return registeredRoutes }

// registerRoutes wires the mux. Uses Go 1.22+ "METHOD /path" syntax;
// {var} captures one path segment.
func (h *httpServer) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /health", h.handleHealth)
	mux.HandleFunc("GET /ready", h.handleReady)
	mux.HandleFunc("GET /metrics", h.handleMetrics)

	mux.HandleFunc("POST /v1/sessions", h.handleCreateSession)
	mux.HandleFunc("DELETE /v1/sessions/{id}", withSessionID(h.handleDeleteSession))
	mux.HandleFunc("POST /v1/sessions/{id}/ping", withSessionID(h.handlePingSession))

	mux.HandleFunc("GET /v1/stats", h.handleStats)
	mux.HandleFunc("GET /v1/openapi.json", h.handleOpenAPI)

	mux.HandleFunc("POST /v1/locks/{key}", withKey(h.handleAcquireLock))
	mux.HandleFunc("POST /v1/locks/{key}/release", withKey(h.handleReleaseLock))
	mux.HandleFunc("POST /v1/locks/{key}/renew", withKey(h.handleRenewLock))
	mux.HandleFunc("POST /v1/locks/{key}/enqueue", withKey(h.handleEnqueueLock))
	mux.HandleFunc("POST /v1/locks/{key}/wait", withKey(h.handleWaitLock))

	mux.HandleFunc("POST /v1/semaphores/{key}", withKey(h.handleAcquireSem))
	mux.HandleFunc("POST /v1/semaphores/{key}/release", withKey(h.handleReleaseSem))
	mux.HandleFunc("POST /v1/semaphores/{key}/renew", withKey(h.handleRenewSem))
	mux.HandleFunc("POST /v1/semaphores/{key}/enqueue", withKey(h.handleEnqueueSem))
	mux.HandleFunc("POST /v1/semaphores/{key}/wait", withKey(h.handleWaitSem))
}

// withKey extracts and validates the {key} path param.
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

// withSessionID extracts and validates the {id} path param against
// the format produced by mintSessionID (32 lowercase hex chars).
func withSessionID(fn func(http.ResponseWriter, *http.Request, string)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if !IsValidSessionID(id) {
			writeError(w, http.StatusBadRequest, "bad_request", "invalid session id")
			return
		}
		fn(w, r, id)
	}
}
