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
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mtingers/dflockd/internal/config"
	"github.com/mtingers/dflockd/internal/lock"
	"github.com/mtingers/dflockd/internal/protocol"
	"github.com/mtingers/dflockd/internal/server"
)

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

func testConfig() *config.Config {
	cfg := &config.Config{
		Host:                    "127.0.0.1",
		Port:                    0,
		DefaultLeaseTTL:         33 * time.Second,
		LeaseSweepInterval:      100 * time.Millisecond,
		GCInterval:              100 * time.Millisecond,
		GCMaxIdleTime:           60 * time.Second,
		MaxLocks:                1024,
		ReadTimeout:             5 * time.Second,
		WriteTimeout:            5 * time.Second,
		AutoReleaseOnDisconnect: true,
		HTTPSessionIdleTimeout:  1 * time.Second,
		HTTPSSEPingInterval:     200 * time.Millisecond,
	}
	if err := cfg.Validate(); err != nil {
		panic(err)
	}
	return cfg
}

// testHarness wires up the moving parts for HTTP-bridge integration
// tests. It intentionally does NOT hold a *testing.T: storing the
// initial test's T across subtest boundaries breaks Go's failure
// attribution and can cause t.Fatalf calls to fire on the wrong test.
// Callers pass their current t to each helper.
//
// Two serving paths are available:
//   - h.handler: the full withAuth-wrapped ServeMux. Most tests dispatch
//     directly through this via httptest.NewRecorder, avoiding an OS
//     socket per request.
//   - h.http: a real httptest.NewServer. Only SSE tests and the
//     explicit TCP-listener integration test use this.
type testHarness struct {
	srv     *server.Server
	lm      *lock.LockManager
	bridge  *Bridge
	handler http.Handler
	http    *httptest.Server
	cancel  context.CancelFunc
}

// newHarness wires up a server + bridge + httptest.Server. The TCP listener
// is not started — we don't need it for HTTP-only tests.
func newHarness(t *testing.T, cfg *config.Config) *testHarness {
	t.Helper()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	lm := lock.NewLockManager(cfg, log)
	srv := server.New(lm, cfg, log)

	ctx, cancel := context.WithCancel(context.Background())

	// Run lease-expiry and GC loops so lease expiry tests behave realistically.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); lm.LeaseExpiryLoop(ctx) }()
	go func() { defer wg.Done(); lm.GCLoop(ctx) }()

	bridge := NewBridge(ctx, srv, cfg, log, cfg.HTTPSessionIdleTimeout, cfg.HTTPMaxSessions)

	hs := &httpServer{
		bridge:  bridge,
		cfg:     cfg,
		log:     log,
		metrics: newMetricsRegistry(),
		limiter: newHTTPRateLimiter(cfg.HTTPRateLimitPerIP, cfg.HTTPRateLimitBurst),
	}
	mux := http.NewServeMux()
	hs.registerRoutes(mux)
	handler := hs.withCORS(hs.withMetrics(mux, hs.withRateLimit(hs.withAuth(jsonRouteErrors(mux)))))
	ts := httptest.NewServer(handler)

	h := &testHarness{
		srv:     srv,
		lm:      lm,
		bridge:  bridge,
		handler: handler,
		http:    ts,
		cancel: func() {
			ts.Close()
			bridge.Shutdown()
			cancel()
			wg.Wait()
		},
	}
	t.Cleanup(h.cancel)
	return h
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

// do dispatches the request directly through the in-process handler via
// httptest.ResponseRecorder. No TCP socket, no OS scheduling roundtrip.
// Use this for everything except SSE (which needs a real listener for
// streaming semantics).
func (h *testHarness) do(t *testing.T, method, path string, sessionID string, body any) *http.Response {
	t.Helper()
	return h.doWithContext(t, context.Background(), method, path, sessionID, body)
}

func (h *testHarness) doWithContext(t *testing.T, ctx context.Context, method, path string, sessionID string, body any) *http.Response {
	t.Helper()
	var reqBody io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		reqBody = bytes.NewReader(buf)
	}
	req := httptest.NewRequest(method, path, reqBody).WithContext(ctx)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if sessionID != "" {
		req.Header.Set("X-Dflockd-Session", sessionID)
	}
	if h.bridge.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+h.bridge.authToken)
	}
	rec := httptest.NewRecorder()
	h.handler.ServeHTTP(rec, req)
	return rec.Result()
}

func decodeBody(t *testing.T, resp *http.Response, v any) {
	t.Helper()
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil && err != io.EOF {
		t.Fatalf("decode body: %v", err)
	}
}

func (h *testHarness) createSession(t *testing.T) string {
	t.Helper()
	resp := h.do(t, "POST", "/v1/sessions", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("create session: status %d", resp.StatusCode)
	}
	var body createSessionResponse
	decodeBody(t, resp, &body)
	return body.SessionID
}

// ---------------------------------------------------------------------------
// Phase 1: Session lifecycle
// ---------------------------------------------------------------------------

func TestCreateSession_ReturnsValidID(t *testing.T) {
	h := newHarness(t, testConfig())
	resp := h.do(t, "POST", "/v1/sessions", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	var body createSessionResponse
	decodeBody(t, resp, &body)
	if !isValidSessionID(body.SessionID) {
		t.Fatalf("invalid session id: %q", body.SessionID)
	}
	if body.IdleTimeoutS <= 0 {
		t.Fatalf("expected positive idle timeout, got %d", body.IdleTimeoutS)
	}
	if got := h.bridge.SessionCount(); got != 1 {
		t.Fatalf("session count: got %d, want 1", got)
	}
}

func TestDeleteSession_Removes(t *testing.T) {
	h := newHarness(t, testConfig())
	id := h.createSession(t)
	resp := h.do(t, "DELETE", "/v1/sessions/"+id, "", nil)
	if resp.StatusCode != 204 {
		t.Fatalf("delete: status %d", resp.StatusCode)
	}
	if got := h.bridge.SessionCount(); got != 0 {
		t.Fatalf("session count after delete: got %d, want 0", got)
	}
	// Deleting again returns 410.
	resp = h.do(t, "DELETE", "/v1/sessions/"+id, "", nil)
	if resp.StatusCode != 410 {
		t.Fatalf("redelete: status %d, want 410", resp.StatusCode)
	}
}

func TestPingSession_OK(t *testing.T) {
	h := newHarness(t, testConfig())
	id := h.createSession(t)
	resp := h.do(t, "POST", "/v1/sessions/"+id+"/ping", "", nil)
	if resp.StatusCode != 204 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("ping: status %d body %q", resp.StatusCode, string(b))
	}
}

func TestPingSession_UnknownReturns410(t *testing.T) {
	h := newHarness(t, testConfig())
	resp := h.do(t, "POST", "/v1/sessions/00000000000000000000000000000000/ping", "", nil)
	if resp.StatusCode != 410 {
		t.Fatalf("status: %d want 410", resp.StatusCode)
	}
}

func TestHealthAndReadyBypassAuth(t *testing.T) {
	cfg := testConfig()
	cfg.AuthToken = "secret"
	h := newHarness(t, cfg)

	for _, path := range []string{"/health", "/ready"} {
		req := httptest.NewRequest("GET", path, nil)
		rec := httptest.NewRecorder()
		h.handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s status %d, want 200", path, rec.Code)
		}
	}
}

func TestReadyReportsDraining(t *testing.T) {
	cfg := testConfig()
	h := newHarness(t, cfg)
	hs := &httpServer{
		bridge:  h.bridge,
		cfg:     cfg,
		log:     h.bridge.log,
		metrics: newMetricsRegistry(),
	}
	hs.draining.Store(true)
	mux := http.NewServeMux()
	hs.registerRoutes(mux)
	handler := hs.withAuth(jsonRouteErrors(mux))

	req := httptest.NewRequest("GET", "/ready", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("ready draining status %d, want 503", rec.Code)
	}
}

func TestMetricsEndpointEmitsPrometheus(t *testing.T) {
	h := newHarness(t, testConfig())
	_ = h.do(t, "GET", "/health", "", nil)
	resp := h.do(t, "GET", "/metrics", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("metrics: status %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	text := string(body)
	for _, want := range []string{
		"dflockd_http_requests_total",
		`path="/health"`,
		"dflockd_connections",
		"dflockd_http_sessions",
		"dflockd_ready",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("metrics missing %q in:\n%s", want, text)
		}
	}
}

func TestHTTPRateLimiterEvictsIdleBuckets(t *testing.T) {
	l := newHTTPRateLimiter(10, 10)
	defer l.Stop()

	base := time.Now()
	if !l.allow("192.0.2.10", base) {
		t.Fatal("first allow should succeed")
	}
	if !l.allow("192.0.2.11", base) {
		t.Fatal("first allow should succeed")
	}

	bucketCount := func() int {
		l.mu.Lock()
		defer l.mu.Unlock()
		return len(l.buckets)
	}

	if got := bucketCount(); got != 2 {
		t.Fatalf("buckets after allows: got %d want 2", got)
	}

	// Sweep with cutoff before any activity — nothing should be evicted.
	l.sweep(base)
	if got := bucketCount(); got != 2 {
		t.Fatalf("after no-op sweep: got %d want 2", got)
	}

	// Sweep with `now` past the eviction threshold — both buckets removed.
	l.sweep(base.Add(rateBucketIdleEviction + time.Second))
	if got := bucketCount(); got != 0 {
		t.Fatalf("after eviction sweep: got %d want 0", got)
	}

	// Recreating a bucket after eviction must behave identically to the
	// first allow (full burst minus one), so a freshly-resurrected IP isn't
	// punished for the sweeper having run.
	if !l.allow("192.0.2.10", base.Add(rateBucketIdleEviction+2*time.Second)) {
		t.Fatal("post-eviction allow should succeed with full burst")
	}
}

func TestHTTPRateLimiterStopIsIdempotentAndNilSafe(t *testing.T) {
	(*httpRateLimiter)(nil).Stop() // must not panic on nil receiver
	l := newHTTPRateLimiter(1, 1)
	l.Stop()
	l.Stop() // second Stop must be a no-op, not a panic
}

func TestHTTPRateLimitPerIP(t *testing.T) {
	cfg := testConfig()
	cfg.HTTPRateLimitPerIP = 1
	cfg.HTTPRateLimitBurst = 1
	h := newHarness(t, cfg)

	resp := h.do(t, "GET", "/metrics", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("first metrics status %d", resp.StatusCode)
	}
	resp = h.do(t, "GET", "/metrics", "", nil)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("second metrics status %d, want 429", resp.StatusCode)
	}
	resp = h.do(t, "GET", "/health", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health should bypass rate limit, got %d", resp.StatusCode)
	}
}

type testNetAddr string

func (a testNetAddr) Network() string { return "tcp" }
func (a testNetAddr) String() string  { return string(a) }

type testConn struct {
	remote net.Addr
	closed bool
}

func (c *testConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (c *testConn) Write([]byte) (int, error)        { return 0, net.ErrClosed }
func (c *testConn) Close() error                     { c.closed = true; return nil }
func (c *testConn) LocalAddr() net.Addr              { return testNetAddr("127.0.0.1:0") }
func (c *testConn) RemoteAddr() net.Addr             { return c.remote }
func (c *testConn) SetDeadline(time.Time) error      { return nil }
func (c *testConn) SetReadDeadline(time.Time) error  { return nil }
func (c *testConn) SetWriteDeadline(time.Time) error { return nil }

func TestHTTPMaxConnectionsPerIP(t *testing.T) {
	limiter := newHTTPConnLimiter(1)
	c1 := &testConn{remote: testNetAddr("192.0.2.10:1000")}
	c2 := &testConn{remote: testNetAddr("192.0.2.10:1001")}
	c3 := &testConn{remote: testNetAddr("192.0.2.10:1002")}

	limiter.ConnState(c1, http.StateNew)
	if c1.closed {
		t.Fatal("first connection unexpectedly closed")
	}

	limiter.ConnState(c2, http.StateNew)
	if !c2.closed {
		t.Fatal("second same-IP connection was not closed")
	}

	limiter.ConnState(c1, http.StateClosed)
	limiter.ConnState(c3, http.StateNew)
	if c3.closed {
		t.Fatal("connection after close unexpectedly rejected")
	}
}

func TestCORSPreflight(t *testing.T) {
	cfg := testConfig()
	cfg.HTTPCORSAllowedOrigins = []string{"https://app.example"}
	h := newHarness(t, cfg)

	req := httptest.NewRequest(http.MethodOptions, "/v1/sessions", nil)
	req.Header.Set("Origin", "https://app.example")
	req.Header.Set("Access-Control-Request-Method", "POST")
	rec := httptest.NewRecorder()
	h.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("preflight status %d, want 204", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://app.example" {
		t.Fatalf("allow-origin %q", got)
	}
}

func TestMaxSessionsPerIP(t *testing.T) {
	cfg := testConfig()
	cfg.HTTPMaxSessionsPerIP = 1
	h := newHarness(t, cfg)

	resp := h.do(t, "POST", "/v1/sessions", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("first session status %d", resp.StatusCode)
	}
	resp = h.do(t, "POST", "/v1/sessions", "", nil)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("second session status %d, want 503", resp.StatusCode)
	}
	var body errorBody
	decodeBody(t, resp, &body)
	if body.Error != "max_sessions_per_ip" {
		t.Fatalf("error %q, want max_sessions_per_ip", body.Error)
	}
}

func TestCreateSessionPrunesDeadSessionsBeforeMaxSessions(t *testing.T) {
	cfg := testConfig()
	cfg.HTTPMaxSessions = 1
	h := newHarness(t, cfg)

	id := h.createSession(t)
	s, err := h.bridge.LookupSession(id)
	if err != nil {
		t.Fatal(err)
	}
	s.close()

	if _, err := h.bridge.LookupSession(id); err != ErrSessionGone {
		t.Fatalf("lookup closed session: got %v want %v", err, ErrSessionGone)
	}
	if got := h.bridge.SessionCount(); got != 0 {
		t.Fatalf("session count after lookup pruned dead session: got %d want 0", got)
	}

	resp := h.do(t, "POST", "/v1/sessions", "", nil)
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("create after dead session: status %d body %s", resp.StatusCode, string(body))
	}
	resp.Body.Close()
}

func TestSessionSweeperDoesNotCloseActiveCommand(t *testing.T) {
	cfg := testConfig()
	cfg.HTTPSessionIdleTimeout = 100 * time.Millisecond
	h := newHarness(t, cfg)

	idB := h.createSession(t)

	token, err := h.lm.Acquire(context.Background(), lock.LockPrefix+"active-wait", time.Second, 30*time.Second, 999, 1)
	if err != nil {
		t.Fatal(err)
	}
	if token == "" {
		t.Fatal("direct acquire timed out")
	}

	type result struct {
		status int
		body   acquireResponse
	}
	done := make(chan result, 1)
	go func() {
		r := h.do(t, "POST", "/v1/locks/active-wait", idB, acquireRequest{AcquireTimeoutS: 2, LeaseTTLS: 30})
		var ack acquireResponse
		decodeBody(t, r, &ack)
		done <- result{status: r.StatusCode, body: ack}
	}()

	// Let the sweeper pass the hard cutoff while B is still waiting.
	time.Sleep(350 * time.Millisecond)
	if !h.lm.Release(lock.LockPrefix+"active-wait", token) {
		t.Fatal("direct release failed")
	}

	select {
	case got := <-done:
		if got.status != 200 || got.body.Status != "ok" || got.body.Token == "" {
			t.Fatalf("B acquire result: status=%d body=%+v, want 200 ok", got.status, got.body)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("B acquire did not return")
	}
}

// ---------------------------------------------------------------------------
// Phase 1: Acquire / release
// ---------------------------------------------------------------------------

func TestAcquireRelease_RoundTrip(t *testing.T) {
	h := newHarness(t, testConfig())
	id := h.createSession(t)

	resp := h.do(t, "POST", "/v1/locks/my-job", id, acquireRequest{AcquireTimeoutS: 5})
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("acquire: %d %s", resp.StatusCode, string(b))
	}
	var ack acquireResponse
	decodeBody(t, resp, &ack)
	if ack.Status != "ok" || ack.Token == "" {
		t.Fatalf("acquire ack: %+v", ack)
	}

	resp = h.do(t, "POST", "/v1/locks/my-job/release", id, releaseRequest{Token: ack.Token})
	if resp.StatusCode != 204 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("release: %d %s", resp.StatusCode, string(b))
	}
}

func TestAcquireTimeout_Returns200Timeout(t *testing.T) {
	h := newHarness(t, testConfig())

	// Session A holds the lock.
	idA := h.createSession(t)
	resp := h.do(t, "POST", "/v1/locks/contended", idA, acquireRequest{AcquireTimeoutS: 2, LeaseTTLS: 30})
	if resp.StatusCode != 200 {
		t.Fatalf("A acquire: %d", resp.StatusCode)
	}

	// Session B can't get it, times out after 1s.
	idB := h.createSession(t)
	start := time.Now()
	resp = h.do(t, "POST", "/v1/locks/contended", idB, acquireRequest{AcquireTimeoutS: 1})
	elapsed := time.Since(start)
	if resp.StatusCode != 200 {
		t.Fatalf("B acquire: %d", resp.StatusCode)
	}
	var ack acquireResponse
	decodeBody(t, resp, &ack)
	if ack.Status != "timeout" {
		t.Fatalf("B ack: %+v, want timeout", ack)
	}
	if elapsed < 900*time.Millisecond {
		t.Fatalf("elapsed too short: %v", elapsed)
	}
}

func TestHTTPRejectsNegativeLeaseTTL(t *testing.T) {
	h := newHarness(t, testConfig())
	id := h.createSession(t)

	cases := []struct {
		name string
		path string
		body any
	}{
		{
			name: "lock acquire",
			path: "/v1/locks/neg-lease",
			body: acquireRequest{AcquireTimeoutS: 1, LeaseTTLS: -1},
		},
		{
			name: "lock enqueue",
			path: "/v1/locks/neg-lease/enqueue",
			body: enqueueRequest{LeaseTTLS: -1},
		},
		{
			name: "lock renew",
			path: "/v1/locks/neg-lease/renew",
			body: renewRequest{Token: "abcdef", LeaseTTLS: -1},
		},
		{
			name: "semaphore acquire",
			path: "/v1/semaphores/neg-lease",
			body: semAcquireRequest{AcquireTimeoutS: 1, Limit: 2, LeaseTTLS: -1},
		},
		{
			name: "semaphore enqueue",
			path: "/v1/semaphores/neg-lease/enqueue",
			body: semEnqueueRequest{Limit: 2, LeaseTTLS: -1},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := h.do(t, "POST", tc.path, id, tc.body)
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("status: got %d want 400 body=%s", resp.StatusCode, string(body))
			}
		})
	}
}

func TestHTTPRejectsTooLargeSeconds(t *testing.T) {
	tooLarge := maxProtocolSeconds + 1
	if int64(int(tooLarge)) != tooLarge {
		t.Skip("int cannot represent protocol overflow boundary on this platform")
	}
	v := int(tooLarge)

	h := newHarness(t, testConfig())
	id := h.createSession(t)

	cases := []struct {
		name string
		path string
		body any
	}{
		{
			name: "lock acquire timeout",
			path: "/v1/locks/too-large",
			body: acquireRequest{AcquireTimeoutS: v},
		},
		{
			name: "lock acquire lease",
			path: "/v1/locks/too-large",
			body: acquireRequest{AcquireTimeoutS: 0, LeaseTTLS: v},
		},
		{
			name: "lock enqueue lease",
			path: "/v1/locks/too-large/enqueue",
			body: enqueueRequest{LeaseTTLS: v},
		},
		{
			name: "lock wait timeout",
			path: "/v1/locks/too-large/wait",
			body: waitRequest{TimeoutS: v},
		},
		{
			name: "lock renew lease",
			path: "/v1/locks/too-large/renew",
			body: renewRequest{Token: "abcdef", LeaseTTLS: v},
		},
		{
			name: "semaphore acquire timeout",
			path: "/v1/semaphores/too-large",
			body: semAcquireRequest{AcquireTimeoutS: v, Limit: 1},
		},
		{
			name: "semaphore enqueue lease",
			path: "/v1/semaphores/too-large/enqueue",
			body: semEnqueueRequest{Limit: 1, LeaseTTLS: v},
		},
		{
			name: "semaphore wait timeout",
			path: "/v1/semaphores/too-large/wait",
			body: waitRequest{TimeoutS: v},
		},
		{
			name: "semaphore renew lease",
			path: "/v1/semaphores/too-large/renew",
			body: renewRequest{Token: "abcdef", LeaseTTLS: v},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := h.do(t, "POST", tc.path, id, tc.body)
			if resp.StatusCode != http.StatusBadRequest {
				b, _ := io.ReadAll(resp.Body)
				t.Fatalf("status: got %d want 400 body=%s", resp.StatusCode, string(b))
			}
			var body errorBody
			decodeBody(t, resp, &body)
			if body.Error != "bad_request" || !strings.Contains(body.Detail, "too large") {
				t.Fatalf("body: %+v", body)
			}
		})
	}
}

func TestHTTPRejectsProtocolNewlinesInArguments(t *testing.T) {
	h := newHarness(t, testConfig())
	id := h.createSession(t)

	cases := []struct {
		name string
		path string
		body any
	}{
		{
			name: "release token",
			path: "/v1/locks/inject/release",
			body: releaseRequest{Token: "abc\nping\n_\n"},
		},
		{
			name: "renew token",
			path: "/v1/locks/inject/renew",
			body: renewRequest{Token: "abc\nping\n_\n"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := h.do(t, "POST", tc.path, id, tc.body)
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("status: got %d want 400 body=%s", resp.StatusCode, string(body))
			}
		})
	}

	req, err := http.NewRequest("GET", h.http.URL+"/v1/signals?pattern=events.%3E&group=bad%0Agroup", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := h.http.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("SSE status: got %d want 400 body=%s", resp.StatusCode, string(body))
	}
}

func TestHTTPRejectsProtocolLineOverflow(t *testing.T) {
	h := newHarness(t, testConfig())
	id := h.createSession(t)
	long := strings.Repeat("x", protocol.MaxLineBytes+1)

	cases := []struct {
		name string
		path string
		body any
	}{
		{
			name: "lock key",
			path: "/v1/locks/" + long,
			body: acquireRequest{AcquireTimeoutS: 0},
		},
		{
			name: "release token",
			path: "/v1/locks/overflow/release",
			body: releaseRequest{Token: long},
		},
		{
			name: "renew token",
			path: "/v1/locks/overflow/renew",
			body: renewRequest{Token: long},
		},
		{
			name: "renew argument",
			path: "/v1/locks/overflow/renew",
			body: renewRequest{Token: strings.Repeat("x", protocol.MaxLineBytes), LeaseTTLS: 1},
		},
		{
			name: "signal channel",
			path: "/v1/signals/" + long,
			body: signalRequest{Payload: "payload"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := h.do(t, "POST", tc.path, id, tc.body)
			if resp.StatusCode != http.StatusBadRequest {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("status: got %d want 400 body=%s", resp.StatusCode, string(body))
			}
			var body errorBody
			decodeBody(t, resp, &body)
			if body.Error != "bad_request" || !strings.Contains(body.Detail, "too long") {
				t.Fatalf("body: %+v", body)
			}
		})
	}

	req, err := http.NewRequest("GET", h.http.URL+"/v1/signals?pattern=events.%3E&group="+long, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := h.http.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("SSE status: got %d want 400 body=%s", resp.StatusCode, string(body))
	}
}

func TestHTTPRejectsInvalidSignalPayloads(t *testing.T) {
	h := newHarness(t, testConfig())

	cases := []struct {
		name    string
		channel string
		payload string
		want    string
	}{
		{
			name:    "whitespace only",
			channel: "events.blank",
			payload: "   \t",
			want:    "empty",
		},
		{
			name:    "too large for pushed TCP frame",
			channel: "events.large",
			payload: strings.Repeat("x", protocol.MaxSignalPayloadBytes("events.large")+1),
			want:    "too large",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := h.do(t, "POST", "/v1/signals/"+tc.channel, "", signalRequest{Payload: tc.payload})
			if resp.StatusCode != http.StatusBadRequest {
				b, _ := io.ReadAll(resp.Body)
				t.Fatalf("status: got %d want 400 body=%s", resp.StatusCode, string(b))
			}
			var body errorBody
			decodeBody(t, resp, &body)
			if body.Error != "bad_request" || !strings.Contains(body.Detail, tc.want) {
				t.Fatalf("body: %+v", body)
			}
		})
	}
}

// TestRouting_ReservedActionNameAsKey covers the URL dispatch bug where
// a lock literally named "release" (or one of the other reserved action
// words) was ambiguous — `POST /v1/locks/release` could mean "acquire
// key=release" or "release action on an unspecified key." With Go 1.22+
// ServeMux path patterns, each `{key}` matches exactly one segment, so
// the disambiguation is mechanical: acquire the key, then release it on
// the sub-path.
func TestRouting_ReservedActionNameAsKey(t *testing.T) {
	h := newHarness(t, testConfig())
	id := h.createSession(t)

	for _, key := range []string{"release", "renew", "enqueue", "wait"} {
		t.Run(key, func(t *testing.T) {
			resp := h.do(t, "POST", "/v1/locks/"+key, id, acquireRequest{AcquireTimeoutS: 1, LeaseTTLS: 10})
			if resp.StatusCode != 200 {
				b, _ := io.ReadAll(resp.Body)
				t.Fatalf("acquire: %d %s", resp.StatusCode, string(b))
			}
			var ack acquireResponse
			decodeBody(t, resp, &ack)
			if ack.Status != "ok" || ack.Token == "" {
				t.Fatalf("acquire ack: %+v", ack)
			}
			// POST /v1/locks/release/release — release the lock
			// literally named "release" (or "renew" etc).
			resp = h.do(t, "POST", "/v1/locks/"+key+"/release", id, releaseRequest{Token: ack.Token})
			if resp.StatusCode != 204 {
				b, _ := io.ReadAll(resp.Body)
				t.Fatalf("release: %d %s", resp.StatusCode, string(b))
			}
		})
	}
}

func TestRouting_EncodedSlashKeyRoundTrip(t *testing.T) {
	h := newHarness(t, testConfig())
	id := h.createSession(t)

	resp := h.do(t, "POST", "/v1/locks/foo%2Frelease", id, acquireRequest{AcquireTimeoutS: 1, LeaseTTLS: 10})
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("acquire: %d %s", resp.StatusCode, string(b))
	}
	var ack acquireResponse
	decodeBody(t, resp, &ack)
	if ack.Status != "ok" || ack.Token == "" {
		t.Fatalf("acquire ack: %+v", ack)
	}

	resp = h.do(t, "POST", "/v1/locks/foo%2Frelease/release", id, releaseRequest{Token: ack.Token})
	if resp.StatusCode != 204 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("release: %d %s", resp.StatusCode, string(b))
	}
}

func TestDeleteSession_ReleasesHeldLocks(t *testing.T) {
	h := newHarness(t, testConfig())
	idA := h.createSession(t)

	resp := h.do(t, "POST", "/v1/locks/cleanup-test", idA, acquireRequest{AcquireTimeoutS: 1, LeaseTTLS: 60})
	if resp.StatusCode != 200 {
		t.Fatalf("A acquire: %d", resp.StatusCode)
	}

	// Delete session A. Should trigger protocol-level CleanupConnection,
	// releasing the lock.
	resp = h.do(t, "DELETE", "/v1/sessions/"+idA, "", nil)
	if resp.StatusCode != 204 {
		t.Fatalf("delete: %d", resp.StatusCode)
	}

	// Session B should now grab the lock immediately.
	idB := h.createSession(t)
	resp = h.do(t, "POST", "/v1/locks/cleanup-test", idB, acquireRequest{AcquireTimeoutS: 1})
	if resp.StatusCode != 200 {
		t.Fatalf("B acquire: %d", resp.StatusCode)
	}
	var ack acquireResponse
	decodeBody(t, resp, &ack)
	if ack.Status != "ok" {
		t.Fatalf("B ack: %+v", ack)
	}
}

// ---------------------------------------------------------------------------
// Phase 1: Renew
// ---------------------------------------------------------------------------

func TestRenew_ExtendsLease(t *testing.T) {
	h := newHarness(t, testConfig())
	id := h.createSession(t)

	resp := h.do(t, "POST", "/v1/locks/renew-test", id, acquireRequest{AcquireTimeoutS: 1, LeaseTTLS: 10})
	var ack acquireResponse
	decodeBody(t, resp, &ack)
	if ack.Token == "" {
		t.Fatalf("acquire: %+v", ack)
	}

	resp = h.do(t, "POST", "/v1/locks/renew-test/renew", id, renewRequest{Token: ack.Token, LeaseTTLS: 20})
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("renew: %d %s", resp.StatusCode, string(b))
	}
	var rn renewResponse
	decodeBody(t, resp, &rn)
	if rn.RemainingS != 20 {
		t.Fatalf("remaining: got %d want 20", rn.RemainingS)
	}
}

func TestRenew_UnknownTokenReturns404(t *testing.T) {
	h := newHarness(t, testConfig())
	id := h.createSession(t)
	resp := h.do(t, "POST", "/v1/locks/some-key/renew", id, renewRequest{Token: "nonexistent"})
	if resp.StatusCode != 404 {
		t.Fatalf("status: %d want 404", resp.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// Phase 1: Semaphore
// ---------------------------------------------------------------------------

func TestSemaphore_AcquireRelease(t *testing.T) {
	h := newHarness(t, testConfig())
	id := h.createSession(t)

	resp := h.do(t, "POST", "/v1/semaphores/worker-pool", id, semAcquireRequest{
		AcquireTimeoutS: 1, Limit: 3, LeaseTTLS: 30,
	})
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("sem acquire: %d %s", resp.StatusCode, string(b))
	}
	var ack acquireResponse
	decodeBody(t, resp, &ack)
	if ack.Token == "" {
		t.Fatalf("sem ack: %+v", ack)
	}

	resp = h.do(t, "POST", "/v1/semaphores/worker-pool/release", id, releaseRequest{Token: ack.Token})
	if resp.StatusCode != 204 {
		t.Fatalf("sem release: %d", resp.StatusCode)
	}
}

func TestSemaphore_LimitMismatchReturns409(t *testing.T) {
	h := newHarness(t, testConfig())
	idA := h.createSession(t)
	idB := h.createSession(t)

	resp := h.do(t, "POST", "/v1/semaphores/sem-limit", idA, semAcquireRequest{
		AcquireTimeoutS: 1, Limit: 3, LeaseTTLS: 30,
	})
	if resp.StatusCode != 200 {
		t.Fatalf("A: %d", resp.StatusCode)
	}

	resp = h.do(t, "POST", "/v1/semaphores/sem-limit", idB, semAcquireRequest{
		AcquireTimeoutS: 1, Limit: 5, LeaseTTLS: 30,
	})
	if resp.StatusCode != 409 {
		t.Fatalf("B: %d want 409", resp.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// Phase 1: Stats
// ---------------------------------------------------------------------------

func TestStats_ReflectsHeldLock(t *testing.T) {
	h := newHarness(t, testConfig())
	id := h.createSession(t)

	h.do(t, "POST", "/v1/locks/stats-test", id, acquireRequest{AcquireTimeoutS: 1, LeaseTTLS: 30})

	resp := h.do(t, "GET", "/v1/stats", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("stats: %d", resp.StatusCode)
	}
	var body map[string]any
	decodeBody(t, resp, &body)
	locks, _ := body["locks"].([]any)
	found := false
	for _, l := range locks {
		lm := l.(map[string]any)
		if lm["key"] == "stats-test" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("stats missing stats-test: %+v", body)
	}
}

// ---------------------------------------------------------------------------
// Phase 1: Auth
// ---------------------------------------------------------------------------

func TestAuth_FlowWithToken(t *testing.T) {
	cfg := testConfig()
	cfg.AuthToken = "super-secret"
	h := newHarness(t, cfg)
	// The harness's do() helper automatically attaches Bearer.
	id := h.createSession(t)
	resp := h.do(t, "POST", "/v1/locks/auth-test", id, acquireRequest{AcquireTimeoutS: 1})
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("acquire: %d %s", resp.StatusCode, string(b))
	}
}

func TestAuth_MissingTokenReturns401(t *testing.T) {
	cfg := testConfig()
	cfg.AuthToken = "super-secret"
	h := newHarness(t, cfg)

	req, _ := http.NewRequest("POST", h.http.URL+"/v1/sessions", nil)
	resp, err := h.http.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 401 {
		t.Fatalf("status: %d want 401", resp.StatusCode)
	}
}

func TestAuth_WrongTokenReturns401(t *testing.T) {
	cfg := testConfig()
	cfg.AuthToken = "super-secret"
	h := newHarness(t, cfg)

	req, _ := http.NewRequest("POST", h.http.URL+"/v1/sessions", nil)
	req.Header.Set("Authorization", "Bearer wrong")
	resp, err := h.http.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 401 {
		t.Fatalf("status: %d want 401", resp.StatusCode)
	}
}

func TestAuth_OpenAPIEndpointIsExempt(t *testing.T) {
	cfg := testConfig()
	cfg.AuthToken = "super-secret"
	h := newHarness(t, cfg)

	req, _ := http.NewRequest("GET", h.http.URL+"/v1/openapi.json", nil)
	resp, err := h.http.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("status: %d want 200 (openapi should be exempt)", resp.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// Phase 1: Bad requests
// ---------------------------------------------------------------------------

func TestMissingSessionHeaderReturns400(t *testing.T) {
	h := newHarness(t, testConfig())
	resp := h.do(t, "POST", "/v1/locks/foo", "", acquireRequest{AcquireTimeoutS: 1})
	if resp.StatusCode != 400 {
		t.Fatalf("status: %d want 400", resp.StatusCode)
	}
}

func TestBadJSONReturns400(t *testing.T) {
	h := newHarness(t, testConfig())
	id := h.createSession(t)
	req, _ := http.NewRequest("POST", h.http.URL+"/v1/locks/foo", strings.NewReader("not json"))
	req.Header.Set("X-Dflockd-Session", id)
	req.Header.Set("Content-Type", "application/json")
	resp, err := h.http.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 400 {
		t.Fatalf("status: %d want 400", resp.StatusCode)
	}
}

func TestRouteErrorsAreJSON(t *testing.T) {
	h := newHarness(t, testConfig())

	resp := h.do(t, "GET", "/v1/locks/foo", "", nil)
	if resp.StatusCode != http.StatusMethodNotAllowed {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("method status: got %d want 405 body=%s", resp.StatusCode, string(body))
	}
	if got := resp.Header.Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
		t.Fatalf("method content-type: got %q want application/json", got)
	}
	if got := resp.Header.Get("Allow"); got != "POST" {
		t.Fatalf("allow: got %q want POST", got)
	}
	var body errorBody
	decodeBody(t, resp, &body)
	if body.Error != "method_not_allowed" {
		t.Fatalf("method error: got %q want method_not_allowed", body.Error)
	}

	resp = h.do(t, "GET", "/v1/does-not-exist", "", nil)
	if resp.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("not found status: got %d want 404 body=%s", resp.StatusCode, string(body))
	}
	if got := resp.Header.Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
		t.Fatalf("not found content-type: got %q want application/json", got)
	}
	decodeBody(t, resp, &body)
	if body.Error != "not_found" {
		t.Fatalf("not found error: got %q want not_found", body.Error)
	}
}

func TestTrailingJSONReturns400(t *testing.T) {
	h := newHarness(t, testConfig())
	id := h.createSession(t)

	req := httptest.NewRequest("POST", "/v1/locks/foo", strings.NewReader(`{"acquire_timeout_s":0} {"extra":true}`))
	req.Header.Set("X-Dflockd-Session", id)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.handler.ServeHTTP(rec, req)
	resp := rec.Result()
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d want 400 body=%s", resp.StatusCode, string(body))
	}
}

func TestLockEnqueueEmptyBodyUsesDefaultTTL(t *testing.T) {
	h := newHarness(t, testConfig())
	id := h.createSession(t)

	resp := h.do(t, "POST", "/v1/locks/enqueue-empty-body/enqueue", id, nil)
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("enqueue: got %d want 200 body=%s", resp.StatusCode, string(body))
	}
	var ack enqueueResponse
	decodeBody(t, resp, &ack)
	if ack.Status != "acquired" || ack.Token == "" || ack.LeaseTTLS != int(testConfig().DefaultLeaseTTL.Seconds()) {
		t.Fatalf("enqueue ack: %+v", ack)
	}
}

func TestInvalidSessionIDFormat_Returns400(t *testing.T) {
	h := newHarness(t, testConfig())
	resp := h.do(t, "DELETE", "/v1/sessions/not-hex-32", "", nil)
	if resp.StatusCode != 400 {
		t.Fatalf("status: %d want 400", resp.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// Phase 1: Idle sweeper
// ---------------------------------------------------------------------------

func TestIdleSweeper_ClosesAbandonedSession(t *testing.T) {
	cfg := testConfig()
	cfg.HTTPSessionIdleTimeout = 200 * time.Millisecond
	h := newHarness(t, cfg)
	id := h.createSession(t)
	if h.bridge.SessionCount() != 1 {
		t.Fatalf("expected 1 session")
	}
	// Wait > 2×idleTimeout + sweep interval.
	time.Sleep(1 * time.Second)
	if got := h.bridge.SessionCount(); got != 0 {
		t.Fatalf("session count after sweep: got %d, want 0", got)
	}
	// Subsequent request should return 410.
	resp := h.do(t, "POST", "/v1/locks/foo", id, acquireRequest{AcquireTimeoutS: 1})
	if resp.StatusCode != 410 {
		t.Fatalf("status after sweep: %d want 410", resp.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// Phase 1: Zero HTTP baseline
// ---------------------------------------------------------------------------

func TestHTTPPortZero_DoesNotStartHTTP(t *testing.T) {
	cfg := testConfig()
	cfg.HTTPPort = 0

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	lm := lock.NewLockManager(cfg, log)
	srv := server.New(lm, cfg, log)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := Run(ctx, srv, cfg, log); err != nil {
		t.Fatalf("Run with HTTPPort=0 returned err: %v", err)
	}
}

// TestRunAndShutdown spins up a full HTTP listener and verifies it binds
// and shuts down cleanly.
func TestRunAndShutdown(t *testing.T) {
	cfg := testConfig()

	// Find an open port.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg.HTTPPort = l.Addr().(*net.TCPAddr).Port
	l.Close()
	cfg.HTTPHost = "127.0.0.1"

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	lm := lock.NewLockManager(cfg, log)
	srv := server.New(lm, cfg, log)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, srv, cfg, log)
	}()

	// Poll the port until available, or timeout.
	addr := fmt.Sprintf("127.0.0.1:%d", cfg.HTTPPort)
	deadline := time.Now().Add(3 * time.Second)
	var ok bool
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			c.Close()
			ok = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !ok {
		t.Fatalf("http server never became reachable on %s", addr)
	}

	// Make a real HTTP call.
	resp, err := http.Get("http://" + addr + "/v1/stats")
	if err != nil {
		t.Fatalf("GET /v1/stats: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("stats status: %d", resp.StatusCode)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestRunShutdownDoesNotWaitForSSETimeout(t *testing.T) {
	cfg := testConfig()
	cfg.ShutdownTimeout = 2 * time.Second

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg.HTTPPort = l.Addr().(*net.TCPAddr).Port
	l.Close()
	cfg.HTTPHost = "127.0.0.1"

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	lm := lock.NewLockManager(cfg, log)
	srv := server.New(lm, cfg, log)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, srv, cfg, log)
	}()

	addr := fmt.Sprintf("127.0.0.1:%d", cfg.HTTPPort)
	deadline := time.Now().Add(3 * time.Second)
	for {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			c.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("http server never became reachable on %s", addr)
		}
		time.Sleep(50 * time.Millisecond)
	}

	resp, err := http.Get("http://" + addr + "/v1/signals?pattern=shutdown.%3E")
	if err != nil {
		cancel()
		t.Fatalf("open SSE: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		cancel()
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("SSE status: got %d body=%s", resp.StatusCode, string(body))
	}

	start := time.Now()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Run waited for ShutdownTimeout with an active SSE stream")
	}
	if elapsed := time.Since(start); elapsed >= cfg.ShutdownTimeout {
		t.Fatalf("shutdown took %s, want less than %s", elapsed, cfg.ShutdownTimeout)
	}
}
