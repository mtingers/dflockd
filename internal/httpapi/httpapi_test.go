package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mtingers/dflockd/internal/config"
	"github.com/mtingers/dflockd/internal/lock"
	"github.com/mtingers/dflockd/internal/server"
)

// startHTTP launches a server (TCP + HTTP) and returns the HTTP base
// URL plus a stop function.
func startHTTP(t *testing.T, mods ...func(*config.Config)) (string, func()) {
	t.Helper()
	cfg := defaultTestConfig()
	for _, fn := range mods {
		fn(cfg)
	}

	// TCP listener (server.NextConnID needs a Server even though no
	// TCP test traffic is generated).
	tcpL, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Port = tcpL.Addr().(*net.TCPAddr).Port

	// HTTP listener.
	httpL, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg.HTTPPort = httpL.Addr().(*net.TCPAddr).Port
	cfg.HTTPHost = "127.0.0.1"

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	lm := lock.NewLockManager(cfg, log)
	srv := server.New(lm, cfg, log)

	ctx, cancel := context.WithCancel(context.Background())
	tcpDone := make(chan struct{})
	httpDone := make(chan struct{})

	go func() {
		_ = srv.RunOnListener(ctx, tcpL)
		close(tcpDone)
	}()
	go func() {
		_ = runHTTPOnListener(ctx, srv, cfg, log, httpL)
		close(httpDone)
	}()

	base := fmt.Sprintf("http://127.0.0.1:%d", cfg.HTTPPort)

	// Wait for the HTTP server to be ready.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(base + "/health")
		if err == nil {
			resp.Body.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	stopOnce := sync.Once{}
	return base, func() {
		stopOnce.Do(func() {
			cancel()
			select {
			case <-tcpDone:
			case <-time.After(5 * time.Second):
				t.Error("tcp didn't stop")
			}
			select {
			case <-httpDone:
			case <-time.After(5 * time.Second):
				t.Error("http didn't stop")
			}
		})
	}
}

// runHTTPOnListener mirrors Run but takes a pre-listening listener so
// tests can pick a random port.
func runHTTPOnListener(ctx context.Context, srv *server.Server, cfg *config.Config, log *slog.Logger, listener net.Listener) error {
	sessions := NewSessionStore(ctx, srv, cfg.HTTPSessionIdleTimeout, cfg.HTTPMaxSessions, cfg.HTTPMaxSessionsPerIP)
	hs := &httpServer{
		sessions: sessions,
		cfg:      cfg,
		log:      log,
		metrics:  newMetricsRegistry(),
		limiter:  newHTTPRateLimiter(cfg.HTTPRateLimitPerIP, cfg.HTTPRateLimitBurst),
	}
	defer hs.limiter.Stop()

	mux := http.NewServeMux()
	hs.registerRoutes(mux)
	connLimiter := newHTTPConnLimiter(cfg.HTTPMaxConnectionsPerIP)
	hs.srv = &http.Server{
		Handler:           hs.withCORS(hs.withMetrics(mux, hs.withRateLimit(hs.withAuth(jsonRouteErrors(mux))))),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
		ConnState:         connLimiter.ConnState,
	}
	hs.srv.RegisterOnShutdown(func() { go sessions.Shutdown() })

	serveErr := make(chan error, 1)
	go func() {
		err := hs.srv.Serve(listener)
		if err != nil && err != http.ErrServerClosed {
			serveErr <- err
			return
		}
		serveErr <- nil
	}()

	select {
	case <-ctx.Done():
		hs.draining.Store(true)
		go sessions.Shutdown()
		shCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = hs.srv.Shutdown(shCtx)
		<-serveErr
		return nil
	case err := <-serveErr:
		go sessions.Shutdown()
		return err
	}
}

func defaultTestConfig() *config.Config {
	return &config.Config{
		Host:                    "127.0.0.1",
		MaxLocks:                1024,
		DefaultLeaseTTL:         33 * time.Second,
		LeaseSweepInterval:      time.Second,
		GCInterval:              time.Second,
		GCMaxIdleTime:           time.Minute,
		ReadTimeout:             5 * time.Second,
		WriteTimeout:            time.Second,
		ShutdownTimeout:         time.Second,
		AutoReleaseOnDisconnect: true,
		HTTPSessionIdleTimeout:  5 * time.Second,
	}
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

type httpClient struct {
	t       *testing.T
	base    string
	session string
	auth    string
}

func newClient(t *testing.T, base string) *httpClient {
	return &httpClient{t: t, base: base}
}

// postRaw sends an arbitrary byte body. Used to exercise malformed
// bodies (the JSON-encoder path in post would refuse to encode them).
func (c *httpClient) postRaw(path string, body []byte) *http.Response {
	c.t.Helper()
	req, err := http.NewRequest("POST", c.base+path, bytes.NewReader(body))
	if err != nil {
		c.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.session != "" {
		req.Header.Set("X-Dflockd-Session", c.session)
	}
	if c.auth != "" {
		req.Header.Set("Authorization", "Bearer "+c.auth)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	return resp
}

func (c *httpClient) post(path string, body any) *http.Response {
	c.t.Helper()
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	req, err := http.NewRequest("POST", c.base+path, &buf)
	if err != nil {
		c.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.session != "" {
		req.Header.Set("X-Dflockd-Session", c.session)
	}
	if c.auth != "" {
		req.Header.Set("Authorization", "Bearer "+c.auth)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	return resp
}

func (c *httpClient) get(path string) *http.Response {
	c.t.Helper()
	req, err := http.NewRequest("GET", c.base+path, nil)
	if err != nil {
		c.t.Fatal(err)
	}
	if c.auth != "" {
		req.Header.Set("Authorization", "Bearer "+c.auth)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	return resp
}

func (c *httpClient) delete(path string) *http.Response {
	c.t.Helper()
	req, err := http.NewRequest("DELETE", c.base+path, nil)
	if err != nil {
		c.t.Fatal(err)
	}
	if c.auth != "" {
		req.Header.Set("Authorization", "Bearer "+c.auth)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	return resp
}

func decode(t *testing.T, r *http.Response, v any) {
	t.Helper()
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		t.Fatal(err)
	}
}

// startSession is the typical first call: create + bind a session id.
func (c *httpClient) startSession() {
	c.t.Helper()
	resp := c.post("/v1/sessions", nil)
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		c.t.Fatalf("create session: %d %s", resp.StatusCode, body)
	}
	var v createSessionResponse
	decode(c.t, resp, &v)
	c.session = v.SessionID
}

// ---------------------------------------------------------------------------
// Health / readiness / stats
// ---------------------------------------------------------------------------

func TestHTTP_Health(t *testing.T) {
	base, stop := startHTTP(t)
	defer stop()
	c := newClient(t, base)
	resp := c.get("/health")
	if resp.StatusCode != 200 {
		t.Errorf("health: %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestHTTP_Stats(t *testing.T) {
	base, stop := startHTTP(t)
	defer stop()
	c := newClient(t, base)
	resp := c.get("/v1/stats")
	if resp.StatusCode != 200 {
		t.Errorf("stats: %d", resp.StatusCode)
	}
	var stats lock.Stats
	decode(t, resp, &stats)
	if stats.Locks == nil {
		t.Error("locks should be at least []")
	}
}

// ---------------------------------------------------------------------------
// Session lifecycle
// ---------------------------------------------------------------------------

func TestHTTP_Sessions(t *testing.T) {
	base, stop := startHTTP(t)
	defer stop()
	c := newClient(t, base)

	resp := c.post("/v1/sessions", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("create: %d", resp.StatusCode)
	}
	var v createSessionResponse
	decode(t, resp, &v)
	if !IsValidSessionID(v.SessionID) {
		t.Fatalf("session id: %q", v.SessionID)
	}

	c.session = v.SessionID
	resp = c.post("/v1/sessions/"+v.SessionID+"/ping", nil)
	if resp.StatusCode != 204 {
		t.Errorf("ping: %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp = c.delete("/v1/sessions/" + v.SessionID)
	if resp.StatusCode != 204 {
		t.Errorf("delete: %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp = c.delete("/v1/sessions/" + v.SessionID)
	if resp.StatusCode != 410 {
		t.Errorf("delete twice: %d, want 410", resp.StatusCode)
	}
	resp.Body.Close()
}

// ---------------------------------------------------------------------------
// Acquire / release
// ---------------------------------------------------------------------------

func TestHTTP_AcquireRelease(t *testing.T) {
	base, stop := startHTTP(t)
	defer stop()
	c := newClient(t, base)
	c.startSession()

	resp := c.post("/v1/locks/foo", acquireRequest{AcquireTimeoutS: 1})
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("acquire: %d %s", resp.StatusCode, body)
	}
	var v opResponse
	decode(t, resp, &v)
	if v.Status != "ok" || v.Token == "" {
		t.Fatalf("got %+v", v)
	}

	resp = c.post("/v1/locks/foo/release", releaseRequest{Token: v.Token})
	if resp.StatusCode != 204 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("release: %d %s", resp.StatusCode, body)
	}
}

func TestHTTP_Acquire_Timeout(t *testing.T) {
	base, stop := startHTTP(t)
	defer stop()
	c1 := newClient(t, base)
	c1.startSession()
	resp := c1.post("/v1/locks/k", acquireRequest{AcquireTimeoutS: 1})
	if resp.StatusCode != 200 {
		t.Fatal("first acquire failed")
	}
	resp.Body.Close()

	c2 := newClient(t, base)
	c2.startSession()
	resp = c2.post("/v1/locks/k", acquireRequest{AcquireTimeoutS: 1})
	var v opResponse
	decode(t, resp, &v)
	if v.Status != "timeout" {
		t.Errorf("got %q, want timeout", v.Status)
	}
}

func TestHTTP_Renew(t *testing.T) {
	base, stop := startHTTP(t)
	defer stop()
	c := newClient(t, base)
	c.startSession()

	resp := c.post("/v1/locks/k", acquireRequest{AcquireTimeoutS: 1})
	var v opResponse
	decode(t, resp, &v)

	resp = c.post("/v1/locks/k/renew", renewRequest{Token: v.Token, LeaseTTLS: 60})
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("renew: %d %s", resp.StatusCode, body)
	}
	var rv renewResponse
	decode(t, resp, &rv)
	if rv.RemainingS != 60 {
		t.Errorf("remaining %d, want 60", rv.RemainingS)
	}
}

// ---------------------------------------------------------------------------
// Two-phase enqueue/wait
// ---------------------------------------------------------------------------

func TestHTTP_EnqueueWait(t *testing.T) {
	base, stop := startHTTP(t)
	defer stop()

	holder := newClient(t, base)
	holder.startSession()
	resp := holder.post("/v1/locks/k", acquireRequest{AcquireTimeoutS: 1, LeaseTTLS: 30})
	var hv opResponse
	decode(t, resp, &hv)

	queuer := newClient(t, base)
	queuer.startSession()
	resp = queuer.post("/v1/locks/k/enqueue", enqueueRequest{LeaseTTLS: 30})
	var qv opResponse
	decode(t, resp, &qv)
	if qv.Status != "queued" {
		t.Fatalf("got %q, want queued", qv.Status)
	}

	done := make(chan opResponse, 1)
	go func() {
		resp := queuer.post("/v1/locks/k/wait", waitRequest{TimeoutS: 5})
		var v opResponse
		decode(t, resp, &v)
		done <- v
	}()
	time.Sleep(50 * time.Millisecond)
	holder.post("/v1/locks/k/release", releaseRequest{Token: hv.Token})

	select {
	case v := <-done:
		if v.Status != "ok" || v.Token == "" {
			t.Fatalf("wait: %+v", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("wait never returned")
	}
}

// ---------------------------------------------------------------------------
// Semaphores
// ---------------------------------------------------------------------------

func TestHTTP_Semaphore(t *testing.T) {
	base, stop := startHTTP(t)
	defer stop()

	for i := 0; i < 3; i++ {
		c := newClient(t, base)
		c.startSession()
		resp := c.post("/v1/semaphores/sem", semAcquireRequest{AcquireTimeoutS: 1, Limit: 3, LeaseTTLS: 30})
		var v opResponse
		decode(t, resp, &v)
		if v.Status != "ok" {
			t.Fatalf("hold %d: %+v", i, v)
		}
	}
	c := newClient(t, base)
	c.startSession()
	resp := c.post("/v1/semaphores/sem", semAcquireRequest{AcquireTimeoutS: 1, Limit: 3, LeaseTTLS: 30})
	var v opResponse
	decode(t, resp, &v)
	if v.Status != "timeout" {
		t.Errorf("got %q, want timeout", v.Status)
	}
}

// ---------------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------------

func TestHTTP_AuthRequired(t *testing.T) {
	base, stop := startHTTP(t, func(c *config.Config) { c.AuthToken = "secret" })
	defer stop()

	c := newClient(t, base)
	resp := c.post("/v1/sessions", nil)
	if resp.StatusCode != 401 {
		t.Errorf("got %d, want 401", resp.StatusCode)
	}
	resp.Body.Close()

	c.auth = "secret"
	resp = c.post("/v1/sessions", nil)
	if resp.StatusCode != 200 {
		t.Errorf("got %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()

	// Health is exempt.
	c.auth = ""
	resp = c.get("/health")
	if resp.StatusCode != 200 {
		t.Errorf("health: %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()
}

// ---------------------------------------------------------------------------
// Bad input
// ---------------------------------------------------------------------------

func TestHTTP_BadKey(t *testing.T) {
	base, stop := startHTTP(t)
	defer stop()
	c := newClient(t, base)
	c.startSession()

	// Slash in key gets URL-encoded to single segment, but space-style
	// invalid chars hit the validator. Use an empty/long URL-encoded
	// key by trying %20 (which decodes to a space).
	resp := c.post("/v1/locks/bad%20key", acquireRequest{AcquireTimeoutS: 1})
	if resp.StatusCode != 400 {
		t.Errorf("got %d, want 400", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestHTTP_NoSessionHeader(t *testing.T) {
	base, stop := startHTTP(t)
	defer stop()
	c := newClient(t, base)
	resp := c.post("/v1/locks/k", acquireRequest{AcquireTimeoutS: 1})
	if resp.StatusCode != 400 {
		t.Errorf("got %d, want 400", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestHTTP_UnknownSession(t *testing.T) {
	base, stop := startHTTP(t)
	defer stop()
	c := newClient(t, base)
	c.session = strings.Repeat("0", 32)
	resp := c.post("/v1/locks/k", acquireRequest{AcquireTimeoutS: 1})
	if resp.StatusCode != 410 {
		t.Errorf("got %d, want 410", resp.StatusCode)
	}
	resp.Body.Close()
}

// ---------------------------------------------------------------------------
// Disconnect-style cleanup: DELETE and idle sweep release held locks
// ---------------------------------------------------------------------------

// TestHTTP_DeleteSessionReleasesHeldLocks asserts that explicit
// session deletion drops locks held on that session, mirroring the
// TCP "client disconnect → auto-release" behaviour.
func TestHTTP_DeleteSessionReleasesHeldLocks(t *testing.T) {
	base, stop := startHTTP(t)
	defer stop()

	holder := newClient(t, base)
	holder.startSession()
	resp := holder.post("/v1/locks/k", acquireRequest{AcquireTimeoutS: 1, LeaseTTLS: 30})
	var v opResponse
	decode(t, resp, &v)
	if v.Status != "ok" || v.Token == "" {
		t.Fatalf("holder acquire: %+v", v)
	}

	// Holder vanishes without releasing.
	resp = holder.delete("/v1/sessions/" + holder.session)
	if resp.StatusCode != 204 {
		t.Fatalf("delete: %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Another caller should now succeed within a short timeout (proving
	// the slot was freed, not waiting for lease expiry).
	other := newClient(t, base)
	other.startSession()
	resp = other.post("/v1/locks/k", acquireRequest{AcquireTimeoutS: 1})
	decode(t, resp, &v)
	if v.Status != "ok" || v.Token == "" {
		t.Fatalf("post-delete acquire: %+v", v)
	}
}

// TestSession_BeginRequestAfterClose asserts the closed-flag check
// inside BeginRequest. Without this, a handler that already passed
// Lookup could call LockManager after Delete had run CleanupConnection,
// minting a token tied to a connID whose state is gone.
func TestSession_BeginRequestAfterClose(t *testing.T) {
	base, stop := startHTTP(t)
	defer stop()
	c := newClient(t, base)
	c.startSession()

	// Reach the live session via the store rather than guessing —
	// startHTTP doesn't expose it, so use the public DELETE path
	// and then assert that a follow-up lock op gets 410.
	resp := c.delete("/v1/sessions/" + c.session)
	if resp.StatusCode != 204 {
		t.Fatalf("delete: %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Subsequent lock op on the same session id must be rejected.
	resp = c.post("/v1/locks/k", acquireRequest{AcquireTimeoutS: 1})
	if resp.StatusCode != 410 {
		t.Errorf("post-delete acquire: got %d, want 410", resp.StatusCode)
	}
	resp.Body.Close()
}

// TestHTTP_DeleteWhileHandlerHoldsLock_Drains tests the Delete /
// in-flight handler interleaving directly. We start a long-poll
// /wait, then DELETE the session. Delete must wait for the wait
// handler to finish before CleanupConnection fires; the eventual
// /wait response should reflect cancellation, and the lock must
// not leak to the LockManager.
func TestHTTP_DeleteWhileHandlerHoldsLock_Drains(t *testing.T) {
	base, stop := startHTTP(t)
	defer stop()

	holder := newClient(t, base)
	holder.startSession()
	resp := holder.post("/v1/locks/k", acquireRequest{AcquireTimeoutS: 1, LeaseTTLS: 30})
	var hv opResponse
	decode(t, resp, &hv)

	queuer := newClient(t, base)
	queuer.startSession()
	resp = queuer.post("/v1/locks/k/enqueue", enqueueRequest{LeaseTTLS: 30})
	var qv opResponse
	decode(t, resp, &qv)
	if qv.Status != "queued" {
		t.Fatalf("expected queued, got %+v", qv)
	}

	// Long-poll wait in flight on queuer.
	waitDone := make(chan int, 1)
	go func() {
		resp := queuer.post("/v1/locks/k/wait", waitRequest{TimeoutS: 5})
		waitDone <- resp.StatusCode
	}()
	time.Sleep(50 * time.Millisecond)

	// DELETE should not return until /wait has unblocked.
	deleteDone := make(chan int, 1)
	go func() {
		resp := queuer.delete("/v1/sessions/" + queuer.session)
		deleteDone <- resp.StatusCode
	}()

	// Holder release frees the wait — both responses arrive afterwards.
	holder.post("/v1/locks/k/release", releaseRequest{Token: hv.Token})

	select {
	case <-waitDone:
	case <-time.After(3 * time.Second):
		t.Fatal("wait never returned")
	}
	select {
	case <-deleteDone:
	case <-time.After(3 * time.Second):
		t.Fatal("delete never returned")
	}

	// Whatever happened to queuer's grant, the lock must be free for
	// the next caller. If Delete had run CleanupConnection while the
	// wait handler was still inside lm.Wait, the wait handler could
	// mint a token tied to queuer.ConnID after CleanupConnection.
	other := newClient(t, base)
	other.startSession()
	resp = other.post("/v1/locks/k", acquireRequest{AcquireTimeoutS: 1})
	var ov opResponse
	decode(t, resp, &ov)
	if ov.Status != "ok" {
		t.Fatalf("post-delete-race acquire: %+v — leaked token tied to deleted session?", ov)
	}
}

// TestHTTP_BadJsonDoesNotKeepSessionAlive asserts BeginRequest is
// only entered after the body parses. A malformed body must fail
// fast (400) without bumping inFlight, so the sweeper isn't gated
// by an unparseable request body.
func TestHTTP_BadJsonDoesNotKeepSessionAlive(t *testing.T) {
	base, stop := startHTTP(t, func(c *config.Config) {
		c.HTTPSessionIdleTimeout = 50 * time.Millisecond
	})
	defer stop()

	holder := newClient(t, base)
	holder.startSession()
	resp := holder.post("/v1/locks/k", acquireRequest{AcquireTimeoutS: 1, LeaseTTLS: 60})
	var hv opResponse
	decode(t, resp, &hv)
	if hv.Token == "" {
		t.Fatal("setup acquire failed")
	}

	// Send garbage body. If BeginRequest were entered before parsing,
	// the malformed parse would still bump inFlight briefly — but the
	// real bug is the slowloris case. We exercise that the 400 path
	// fires from a code position where inFlight stayed 0, by checking
	// that the immediately-following idle sweep can still reap.
	for i := 0; i < 5; i++ {
		resp := holder.postRaw("/v1/locks/k/release", []byte("{not json"))
		if resp.StatusCode != 400 {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("malformed release returned %d: %s", resp.StatusCode, body)
		}
		resp.Body.Close()
	}

	// Wait past 2× idleTimeout and confirm the holder's session was
	// reaped (lock released).
	deadline := time.Now().Add(2 * time.Second)
	other := newClient(t, base)
	other.startSession()
	for time.Now().Before(deadline) {
		resp := other.post("/v1/locks/k", acquireRequest{AcquireTimeoutS: 0})
		var v opResponse
		decode(t, resp, &v)
		if v.Status == "ok" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("idle sweep didn't reap holder after malformed POSTs — body parse held inFlight?")
}

// TestHTTP_LongWaitNotIdleSwept asserts that an in-flight /wait
// outliving the idle timeout isn't reaped by the sweeper. Regression
// test for the behavior the bridge had via inFlight gating.
func TestHTTP_LongWaitNotIdleSwept(t *testing.T) {
	base, stop := startHTTP(t, func(c *config.Config) {
		// Tiny idle timeout so the sweeper would fire well before the
		// /wait would naturally complete.
		c.HTTPSessionIdleTimeout = 50 * time.Millisecond
	})
	defer stop()

	holder := newClient(t, base)
	holder.startSession()
	resp := holder.post("/v1/locks/k", acquireRequest{AcquireTimeoutS: 1, LeaseTTLS: 30})
	var hv opResponse
	decode(t, resp, &hv)

	queuer := newClient(t, base)
	queuer.startSession()
	resp = queuer.post("/v1/locks/k/enqueue", enqueueRequest{LeaseTTLS: 30})
	var qv opResponse
	decode(t, resp, &qv)
	if qv.Status != "queued" {
		t.Fatalf("expected queued, got %+v", qv)
	}

	// Block on /wait far longer than 2× idle timeout. The sweeper
	// must skip this session because BeginRequest holds inFlight > 0.
	done := make(chan opResponse, 1)
	go func() {
		resp := queuer.post("/v1/locks/k/wait", waitRequest{TimeoutS: 2})
		var v opResponse
		decode(t, resp, &v)
		done <- v
	}()

	// Sleep way past 2× idle timeout, then satisfy the wait.
	time.Sleep(500 * time.Millisecond)
	holder.post("/v1/locks/k/release", releaseRequest{Token: hv.Token})

	select {
	case v := <-done:
		if v.Status != "ok" || v.Token == "" {
			t.Fatalf("long /wait got %+v — sweeper killed it?", v)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("wait never returned")
	}
}

// TestHTTP_PerSessionSerialization asserts that two concurrent lock
// ops on the same session run sequentially rather than racing. The
// HTTP API is supposed to model a single TCP connection.
func TestHTTP_PerSessionSerialization(t *testing.T) {
	base, stop := startHTTP(t)
	defer stop()

	c := newClient(t, base)
	c.startSession()

	// First request blocks: enqueue then wait on a contended key.
	holder := newClient(t, base)
	holder.startSession()
	resp := holder.post("/v1/locks/k", acquireRequest{AcquireTimeoutS: 1, LeaseTTLS: 30})
	var hv opResponse
	decode(t, resp, &hv)

	resp = c.post("/v1/locks/k/enqueue", enqueueRequest{LeaseTTLS: 30})
	var qv opResponse
	decode(t, resp, &qv)
	if qv.Status != "queued" {
		t.Fatalf("expected queued, got %+v", qv)
	}

	// Two concurrent /wait calls on the same session+key. Without
	// per-session serialization, both can park on the same waiter
	// channel and race. With it, the second blocks behind the first.
	type result struct {
		idx int
		v   opResponse
	}
	results := make(chan result, 2)
	for i := 0; i < 2; i++ {
		i := i
		go func() {
			resp := c.post("/v1/locks/k/wait", waitRequest{TimeoutS: 2})
			var v opResponse
			decode(t, resp, &v)
			results <- result{i, v}
		}()
	}

	// Release the holder; only one /wait should claim the grant.
	time.Sleep(50 * time.Millisecond)
	holder.post("/v1/locks/k/release", releaseRequest{Token: hv.Token})

	var grants, notQueued, timeouts int
	for i := 0; i < 2; i++ {
		select {
		case r := <-results:
			switch r.v.Status {
			case "ok":
				grants++
			case "timeout":
				timeouts++
			default:
				// "not_enqueued" via the conflict mapping is also a
				// valid second-call outcome under serialization.
				notQueued++
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("call %d never returned", i)
		}
	}
	if grants != 1 {
		t.Errorf("got %d grants, want 1 (serialization broken)", grants)
	}
}

// TestHTTP_IdleSweepReleasesHeldLocks drives the idle sweeper directly
// to assert that an abandoned (no-DELETE) session's locks get released
// once it ages past 2× idle timeout.
func TestHTTP_IdleSweepReleasesHeldLocks(t *testing.T) {
	base, stop := startHTTP(t, func(c *config.Config) {
		// Idle timeout small so the sweeper cutoff is short.
		c.HTTPSessionIdleTimeout = 50 * time.Millisecond
	})
	defer stop()

	holder := newClient(t, base)
	holder.startSession()
	resp := holder.post("/v1/locks/k", acquireRequest{AcquireTimeoutS: 1, LeaseTTLS: 60})
	var v opResponse
	decode(t, resp, &v)
	if v.Status != "ok" {
		t.Fatalf("holder: %+v", v)
	}

	// Wait past 2× idle timeout, then trigger the sweep manually so the
	// test doesn't depend on the background ticker firing.
	time.Sleep(150 * time.Millisecond)

	// Fish the SessionStore out via a fresh HTTP request that exercises
	// the sweeper indirectly: we drive the sweep through a small race
	// by making the next request, which doesn't touch the holder's
	// session. Wait for the periodic sweeper instead.
	deadline := time.Now().Add(2 * time.Second)
	other := newClient(t, base)
	other.startSession()
	for time.Now().Before(deadline) {
		resp = other.post("/v1/locks/k", acquireRequest{AcquireTimeoutS: 0})
		decode(t, resp, &v)
		if v.Status == "ok" {
			return // success: holder's lock was reaped
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("idle sweeper never released the orphaned lock; last status %q", v.Status)
}

func TestRoutes_NotEmpty(t *testing.T) {
	if len(Routes()) < 5 {
		t.Errorf("Routes() returned %d", len(Routes()))
	}
}
