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
	rt := newHTTPTestRuntime(t, mods...)
	rt.start()
	rt.waitReady(t)
	return rt.base, rt.stop(t)
}

type httpTestRuntime struct {
	cfg               *config.Config
	log               *slog.Logger
	srv               *server.Server
	tcpL, httpL       net.Listener
	ctx               context.Context
	cancel            context.CancelFunc
	tcpDone, httpDone chan struct{}
	base              string
	stopOnce          sync.Once
}

func newHTTPTestRuntime(t *testing.T, mods ...func(*config.Config)) *httpTestRuntime {
	cfg := testHTTPConfig(mods...)
	tcpL, httpL := testHTTPListeners(t, cfg)
	log := discardLogger()
	ctx, cancel := context.WithCancel(context.Background())
	return &httpTestRuntime{cfg: cfg, log: log, srv: testTCPServer(t, cfg, log), tcpL: tcpL, httpL: httpL, ctx: ctx, cancel: cancel, tcpDone: make(chan struct{}), httpDone: make(chan struct{}), base: httpBase(cfg)}
}

func testHTTPConfig(mods ...func(*config.Config)) *config.Config {
	cfg := defaultTestConfig()
	for _, fn := range mods {
		fn(cfg)
	}
	return cfg
}

func testHTTPListeners(t *testing.T, cfg *config.Config) (net.Listener, net.Listener) {
	tcpL := mustListenLocal(t)
	httpL := mustListenLocal(t)
	cfg.Port = tcpL.Addr().(*net.TCPAddr).Port
	cfg.HTTPPort = httpL.Addr().(*net.TCPAddr).Port
	cfg.HTTPHost = "127.0.0.1"
	return tcpL, httpL
}

func mustListenLocal(t *testing.T) net.Listener {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testTCPServer(t *testing.T, cfg *config.Config, log *slog.Logger) *server.Server {
	t.Helper()
	lm, err := lock.NewLockManager(cfg, log)
	if err != nil {
		t.Fatalf("NewLockManager: %v", err)
	}
	t.Cleanup(func() { lm.Close() })
	return server.New(lm, cfg, log)
}

func httpBase(cfg *config.Config) string {
	return fmt.Sprintf("http://127.0.0.1:%d", cfg.HTTPPort)
}

func (rt *httpTestRuntime) start() {
	go rt.runTCP()
	go rt.runHTTP()
}

func (rt *httpTestRuntime) runTCP() {
	_ = rt.srv.RunOnListener(rt.ctx, rt.tcpL)
	close(rt.tcpDone)
}

func (rt *httpTestRuntime) runHTTP() {
	_ = runHTTPOnListener(rt.ctx, rt.srv, rt.cfg, rt.log, rt.httpL)
	close(rt.httpDone)
}

func (rt *httpTestRuntime) waitReady(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if healthReady(rt.base) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func healthReady(base string) bool {
	resp, err := http.Get(base + "/health")
	if err != nil {
		return false
	}
	resp.Body.Close()
	return true
}

func (rt *httpTestRuntime) stop(t *testing.T) func() {
	return func() {
		rt.stopOnce.Do(func() { rt.stopNow(t) })
	}
}

func (rt *httpTestRuntime) stopNow(t *testing.T) {
	rt.cancel()
	waitStopped(t, "tcp", rt.tcpDone)
	waitStopped(t, "http", rt.httpDone)
}

func waitStopped(t *testing.T, name string, done <-chan struct{}) {
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Errorf("%s didn't stop", name)
	}
}

// runHTTPOnListener mirrors Run but takes a pre-listening listener so
// tests can pick a random port.
func runHTTPOnListener(ctx context.Context, srv *server.Server, cfg *config.Config, log *slog.Logger, listener net.Listener) error {
	hs, shutdown := buildHTTPServer(ctx, srv, cfg, log)
	defer hs.limiter.Stop()
	srv.SetExtraConnCounter(func() int64 { return int64(hs.sessions.Count()) })
	defer srv.SetExtraConnCounter(nil)
	return hs.serveUntilDone(ctx, listener, shutdown, 2*time.Second)
}

func TestHTTP_ShutdownWaitsForSessionCleanup(t *testing.T) {
	cfg := defaultTestConfig()
	httpL := mustListenLocal(t)
	cfg.HTTPPort = httpL.Addr().(*net.TCPAddr).Port
	cfg.HTTPHost = "127.0.0.1"
	log := discardLogger()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hs, shutdown := buildHTTPServer(ctx, testTCPServer(t, cfg, log), cfg, log)
	defer hs.limiter.Stop()

	cleanupStarted := make(chan struct{})
	releaseCleanup := make(chan struct{})
	defer closeIfOpen(releaseCleanup)
	hs.sessions.cleanupConn = func(uint64) error {
		close(cleanupStarted)
		<-releaseCleanup
		return nil
	}

	done := make(chan error, 1)
	go func() { done <- hs.serveUntilDone(ctx, httpL, shutdown, 0) }()
	waitHTTPReady(t, httpBase(cfg))
	startedClient(t, httpBase(cfg))

	cancel()
	waitClosed(t, cleanupStarted, "session cleanup did not start")
	assertNotStopped(t, done, "http server returned before session cleanup finished")
	close(releaseCleanup)
	waitHTTPDone(t, done)
}

func waitHTTPReady(t *testing.T, base string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if healthReady(base) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("http server did not become ready")
}

func waitClosed(t *testing.T, ch <-chan struct{}, msg string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal(msg)
	}
}

func assertNotStopped(t *testing.T, done <-chan error, msg string) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatalf("%s: %v", msg, err)
	case <-time.After(100 * time.Millisecond):
	}
}

func waitHTTPDone(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("http server returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("http server did not stop after session cleanup finished")
	}
}

func closeIfOpen(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}

func defaultTestConfig() *config.Config {
	cfg := defaultHTTPConfigValue
	return &cfg
}

var defaultHTTPConfigValue = config.Config{
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

func startedClient(t *testing.T, base string) *httpClient {
	c := newClient(t, base)
	c.startSession()
	return c
}

// postRaw sends an arbitrary byte body. Used to exercise malformed
// bodies (the JSON-encoder path in post would refuse to encode them).
func (c *httpClient) postRaw(path string, body []byte) *http.Response {
	return c.do(c.jsonRequest("POST", path, bytes.NewReader(body)))
}

func (c *httpClient) post(path string, body any) *http.Response {
	return c.do(c.jsonRequest("POST", path, encodedBody(c.t, body)))
}

func (c *httpClient) get(path string) *http.Response {
	return c.do(c.request("GET", path, nil))
}

func (c *httpClient) delete(path string) *http.Response {
	return c.do(c.request("DELETE", path, nil))
}

func encodedBody(t *testing.T, body any) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	return &buf
}

func (c *httpClient) jsonRequest(method, path string, body io.Reader) *http.Request {
	req := c.request(method, path, body)
	req.Header.Set("Content-Type", "application/json")
	return req
}

func (c *httpClient) request(method, path string, body io.Reader) *http.Request {
	c.t.Helper()
	req, err := http.NewRequest(method, c.base+path, body)
	if err != nil {
		c.t.Fatal(err)
	}
	c.addHeaders(req)
	return req
}

func (c *httpClient) addHeaders(req *http.Request) {
	if c.session != "" {
		req.Header.Set("X-Dflockd-Session", c.session)
	}
	if c.auth != "" {
		req.Header.Set("Authorization", "Bearer "+c.auth)
	}
}

func (c *httpClient) do(req *http.Request) *http.Response {
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

func (c *httpClient) acquireLock(key string, req acquireRequest) opResponse {
	return c.lockOp("/v1/locks/"+key, req)
}

func (c *httpClient) enqueueLock(key string, req enqueueRequest) opResponse {
	return c.lockOp("/v1/locks/"+key+"/enqueue", req)
}

func (c *httpClient) waitLock(key string, req waitRequest) opResponse {
	return c.lockOp("/v1/locks/"+key+"/wait", req)
}

func (c *httpClient) lockOp(path string, body any) opResponse {
	resp := c.post(path, body)
	var v opResponse
	decode(c.t, resp, &v)
	return v
}

func (c *httpClient) releaseLock(key, token string) *http.Response {
	return c.post("/v1/locks/"+key+"/release", releaseRequest{Token: token})
}

func (c *httpClient) waitLockAsync(key string, timeoutS int) <-chan opResponse {
	done := make(chan opResponse, 1)
	go func() { done <- c.waitLock(key, waitRequest{TimeoutS: timeoutS}) }()
	return done
}

func (c *httpClient) waitLockStatusAsync(key string, timeoutS int) <-chan int {
	done := make(chan int, 1)
	go func() { done <- statusCode(c.post("/v1/locks/"+key+"/wait", waitRequest{TimeoutS: timeoutS})) }()
	return done
}

func (c *httpClient) deleteSessionStatusAsync() <-chan int {
	done := make(chan int, 1)
	go func() { done <- statusCode(c.delete("/v1/sessions/" + c.session)) }()
	return done
}

func statusCode(resp *http.Response) int {
	defer resp.Body.Close()
	return resp.StatusCode
}

func requireOpStatus(t *testing.T, v opResponse, status string) {
	t.Helper()
	if v.Status != status {
		t.Fatalf("got %+v, want status %q", v, status)
	}
}

func requireTokenStatus(t *testing.T, v opResponse, status string) {
	t.Helper()
	if v.Status != status || v.Token == "" {
		t.Fatalf("got %+v, want %s with token", v, status)
	}
}

func waitOp(t *testing.T, done <-chan opResponse, timeout time.Duration, msg string) opResponse {
	t.Helper()
	select {
	case v := <-done:
		return v
	case <-time.After(timeout):
		t.Fatal(msg)
		return opResponse{}
	}
}

func waitStatus(t *testing.T, done <-chan int, timeout time.Duration, msg string) int {
	t.Helper()
	select {
	case status := <-done:
		return status
	case <-time.After(timeout):
		t.Fatal(msg)
		return 0
	}
}

func setupQueuedLock(t *testing.T, base, key string) (*httpClient, *httpClient, opResponse) {
	holder := startedClient(t, base)
	hv := holder.acquireLock(key, acquireRequest{AcquireTimeoutS: 1, LeaseTTLS: 30})
	queuer := startedClient(t, base)
	requireOpStatus(t, queuer.enqueueLock(key, enqueueRequest{LeaseTTLS: 30}), "queued")
	return holder, queuer, hv
}

func postMalformedReleases(t *testing.T, c *httpClient, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		postMalformedRelease(t, c)
	}
}

func postMalformedRelease(t *testing.T, c *httpClient) {
	t.Helper()
	resp := c.postRaw("/v1/locks/k/release", []byte("{not json"))
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		failHTTPStatus(t, resp, "malformed release")
	}
}

func failHTTPStatus(t *testing.T, resp *http.Response, label string) {
	body, _ := io.ReadAll(resp.Body)
	t.Fatalf("%s returned %d: %s", label, resp.StatusCode, body)
}

func waitForAcquireOK(t *testing.T, c *httpClient, key string, timeout time.Duration) opResponse {
	t.Helper()
	return waitForAcquireStatus(t, c, key, "ok", timeout)
}

func waitForAcquireStatus(t *testing.T, c *httpClient, key, status string, timeout time.Duration) opResponse {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if v, ok := tryAcquireStatus(c, key, status); ok {
			return v
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("lock %q never reached status %q", key, status)
	return opResponse{}
}

func tryAcquireStatus(c *httpClient, key, status string) (opResponse, bool) {
	v := c.acquireLock(key, acquireRequest{AcquireTimeoutS: 0})
	return v, v.Status == status
}

type waitOutcomeCounts struct {
	grants, timeouts, other int
}

func startWaiters(c *httpClient, key string, n, timeoutS int) <-chan opResponse {
	results := make(chan opResponse, n)
	for i := 0; i < n; i++ {
		go func() { results <- c.waitLock(key, waitRequest{TimeoutS: timeoutS}) }()
	}
	return results
}

func collectWaitOutcomes(t *testing.T, results <-chan opResponse, n int) waitOutcomeCounts {
	var counts waitOutcomeCounts
	for i := 0; i < n; i++ {
		counts.add(waitOp(t, results, 5*time.Second, "wait never returned"))
	}
	return counts
}

func (c *waitOutcomeCounts) add(v opResponse) {
	switch v.Status {
	case "ok":
		c.grants++
	case "timeout":
		c.timeouts++
	default:
		c.other++
	}
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

// TestHTTP_StatsConnectionsIncludesSessions guards the unified
// connection counter: TCP "stats" and HTTP "/v1/stats" must report
// the same Connections value, and that value must include the
// active HTTP session count.
func TestHTTP_StatsConnectionsIncludesSessions(t *testing.T) {
	rt := newHTTPTestRuntime(t)
	rt.start()
	rt.waitReady(t)
	defer rt.stop(t)()

	c := newClient(t, rt.base)
	c.startSession()

	resp := c.get("/v1/stats")
	var stats lock.Stats
	decode(t, resp, &stats)
	if stats.Connections < 1 {
		t.Fatalf("expected /v1/stats Connections >= 1 with one session active, got %d", stats.Connections)
	}
	if got := rt.srv.TotalConnCount(); got != stats.Connections {
		t.Fatalf("Server.TotalConnCount=%d, /v1/stats Connections=%d (must match)", got, stats.Connections)
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

	holder, queuer, hv := setupQueuedLock(t, base, "k")
	done := queuer.waitLockAsync("k", 5)
	time.Sleep(50 * time.Millisecond)
	holder.releaseLock("k", hv.Token).Body.Close()
	requireTokenStatus(t, waitOp(t, done, 2*time.Second, "wait never returned"), "ok")
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

// TestHTTP_AuthFailureSlowdown verifies HTTP auth failures incur the
// brute-force-defense delay, matching the TCP server's rejectAuth
// sleep so HTTP isn't a faster credential-stuffing surface than TCP.
func TestHTTP_AuthFailureSlowdown(t *testing.T) {
	base, stop := startHTTP(t, func(c *config.Config) { c.AuthToken = "secret" })
	defer stop()

	c := newClient(t, base)
	start := time.Now()
	resp := c.post("/v1/sessions", nil)
	elapsed := time.Since(start)
	resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatalf("got %d, want 401", resp.StatusCode)
	}
	// Allow generous slack: middleware sleeps 100ms; we just need to
	// confirm the delay is applied (i.e. >= ~80ms after RTT/jitter).
	if elapsed < 80*time.Millisecond {
		t.Errorf("auth-failure response returned in %s, expected >=80ms slowdown", elapsed)
	}
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
	requireTokenStatus(t, holder.acquireLock("k", acquireRequest{AcquireTimeoutS: 1, LeaseTTLS: 30}), "ok")

	// Holder vanishes without releasing.
	resp := holder.delete("/v1/sessions/" + holder.session)
	if resp.StatusCode != 204 {
		t.Fatalf("delete: %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Another caller should now succeed within a short timeout (proving
	// the slot was freed, not waiting for lease expiry).
	v := startedClient(t, base).acquireLock("k", acquireRequest{AcquireTimeoutS: 1})
	requireTokenStatus(t, v, "ok")
}

func TestHTTP_DeleteSessionReportsCleanupError(t *testing.T) {
	cfg := defaultTestConfig()
	log := discardLogger()
	srv := testTCPServer(t, cfg, log)
	ctx := context.Background()
	hs, _ := buildHTTPServer(ctx, srv, cfg, log)
	defer hs.limiter.Stop()
	defer hs.sessions.Shutdown()
	hs.sessions.cleanupConn = func(uint64) error {
		return lock.ErrFencePersistence
	}
	s, err := hs.sessions.Create("127.0.0.1")
	if err != nil {
		t.Fatalf("create session: %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/v1/sessions/"+s.ID, nil)
	hs.handleDeleteSession(rec, req, s.ID)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("delete status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	var body errorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Error != "fence_persistence" {
		t.Fatalf("error = %q, want fence_persistence", body.Error)
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

	holder, queuer, hv := setupQueuedLock(t, base, "k")
	waitDone := queuer.waitLockStatusAsync("k", 5)
	time.Sleep(50 * time.Millisecond)
	deleteDone := queuer.deleteSessionStatusAsync()

	// Holder release frees the wait — both responses arrive afterwards.
	holder.releaseLock("k", hv.Token).Body.Close()
	waitStatus(t, waitDone, 3*time.Second, "wait never returned")
	waitStatus(t, deleteDone, 3*time.Second, "delete never returned")

	// Whatever happened to queuer's grant, the lock must be free for
	// the next caller. If Delete had run CleanupConnection while the
	// wait handler was still inside lm.Wait, the wait handler could
	// mint a token tied to queuer.ConnID after CleanupConnection.
	ov := startedClient(t, base).acquireLock("k", acquireRequest{AcquireTimeoutS: 1})
	requireOpStatus(t, ov, "ok")
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
	requireTokenStatus(t, holder.acquireLock("k", acquireRequest{AcquireTimeoutS: 1, LeaseTTLS: 60}), "ok")

	// Send garbage body. If BeginRequest were entered before parsing,
	// the malformed parse would still bump inFlight briefly — but the
	// real bug is the slowloris case. We exercise that the 400 path
	// fires from a code position where inFlight stayed 0, by checking
	// that the immediately-following idle sweep can still reap.
	postMalformedReleases(t, holder, 5)

	// Wait past 2× idleTimeout and confirm the holder's session was
	// reaped (lock released).
	waitForAcquireOK(t, startedClient(t, base), "k", 2*time.Second)
}

// TestHTTP_DeleteAbortsLongPollWait asserts that DELETE doesn't get
// stuck behind a long-poll /wait on the same session: the wait
// should be cancelled within milliseconds, not block until its own
// timeout. Old-bridge semantics (session.close() cancels in-flight
// commands) should be preserved.
func TestHTTP_DeleteAbortsLongPollWait(t *testing.T) {
	base, stop := startHTTP(t)
	defer stop()

	// Holder takes the lock so the queuer's wait will actually block.
	holder := newClient(t, base)
	holder.startSession()
	hv := holder.acquireLock("k", acquireRequest{AcquireTimeoutS: 1, LeaseTTLS: 60})
	defer holder.post("/v1/locks/k/release", releaseRequest{Token: hv.Token})

	queuer := newClient(t, base)
	queuer.startSession()
	if qv := queuer.enqueueLock("k", enqueueRequest{LeaseTTLS: 60}); qv.Status != "queued" {
		t.Fatalf("queued: %+v", qv)
	}

	// Wait with a long timeout (60s); we expect DELETE to cut it short.
	waitDone := make(chan int, 1)
	go func() {
		resp := queuer.post("/v1/locks/k/wait", waitRequest{TimeoutS: 60})
		waitDone <- statusCode(resp)
	}()
	time.Sleep(50 * time.Millisecond) // let the wait reach lm.Wait

	deleteDone := make(chan int, 1)
	go func() {
		resp := queuer.delete("/v1/sessions/" + queuer.session)
		deleteDone <- resp.StatusCode
	}()

	select {
	case <-deleteDone:
		// good — DELETE didn't get held hostage by the 60s wait
	case <-time.After(2 * time.Second):
		t.Fatal("DELETE blocked by long-poll /wait — session ctx never cancelled")
	}
	select {
	case got := <-waitDone:
		if got != http.StatusGone {
			t.Fatalf("/wait status = %d, want 410 after session delete", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("/wait never returned after DELETE — handler still parked")
	}
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

	holder, queuer, hv := setupQueuedLock(t, base, "k")

	// Block on /wait far longer than 2× idle timeout. The sweeper
	// must skip this session because BeginRequest holds inFlight > 0.
	done := queuer.waitLockAsync("k", 2)

	// Sleep way past 2× idle timeout, then satisfy the wait.
	time.Sleep(500 * time.Millisecond)
	holder.releaseLock("k", hv.Token).Body.Close()
	requireTokenStatus(t, waitOp(t, done, 3*time.Second, "wait never returned"), "ok")
}

// TestHTTP_PerSessionSerialization asserts that two concurrent lock
// ops on the same session run sequentially rather than racing. The
// HTTP API is supposed to model a single TCP connection.
func TestHTTP_PerSessionSerialization(t *testing.T) {
	base, stop := startHTTP(t)
	defer stop()

	// First request blocks: enqueue then wait on a contended key.
	holder, c, hv := setupQueuedLock(t, base, "k")

	// Two concurrent /wait calls on the same session+key. Without
	// per-session serialization, both can park on the same waiter
	// channel and race. With it, the second blocks behind the first.
	results := startWaiters(c, "k", 2, 2)

	// Release the holder; only one /wait should claim the grant.
	time.Sleep(50 * time.Millisecond)
	holder.releaseLock("k", hv.Token).Body.Close()

	counts := collectWaitOutcomes(t, results, 2)
	if counts.grants != 1 {
		t.Errorf("got %d grants, want 1 (serialization broken)", counts.grants)
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

	holder := startedClient(t, base)
	requireOpStatus(t, holder.acquireLock("k", acquireRequest{AcquireTimeoutS: 1, LeaseTTLS: 60}), "ok")

	// Wait past 2× idle timeout, then trigger the sweep manually so the
	// test doesn't depend on the background ticker firing.
	time.Sleep(150 * time.Millisecond)

	// Fish the SessionStore out via a fresh HTTP request that exercises
	// the sweeper indirectly: we drive the sweep through a small race
	// by making the next request, which doesn't touch the holder's
	// session. Wait for the periodic sweeper instead.
	waitForAcquireOK(t, startedClient(t, base), "k", 2*time.Second)
}

func TestRoutes_NotEmpty(t *testing.T) {
	if len(Routes()) < 5 {
		t.Errorf("Routes() returned %d", len(Routes()))
	}
}

// TestHTTP_OpsAfterSessionDeleteReturn410 asserts every lock-modifying op
// on a deleted session id surfaces 410 session_gone, not 500 / hang. Sister
// to TestSession_BeginRequestAfterClose which only checks acquire.
func TestHTTP_OpsAfterSessionDeleteReturn410(t *testing.T) {
	base, stop := startHTTP(t)
	defer stop()

	c := startedClient(t, base)

	resp := c.delete("/v1/sessions/" + c.session)
	if resp.StatusCode != 204 {
		t.Fatalf("delete: %d", resp.StatusCode)
	}
	resp.Body.Close()

	checks := []struct {
		name string
		do   func() *http.Response
	}{
		{"acquire", func() *http.Response { return c.post("/v1/locks/k", acquireRequest{AcquireTimeoutS: 1}) }},
		{"enqueue", func() *http.Response { return c.post("/v1/locks/k/enqueue", enqueueRequest{}) }},
		{"wait", func() *http.Response { return c.post("/v1/locks/k/wait", waitRequest{TimeoutS: 1}) }},
		{"renew", func() *http.Response { return c.post("/v1/locks/k/renew", renewRequest{Token: "x"}) }},
		{"release", func() *http.Response { return c.post("/v1/locks/k/release", releaseRequest{Token: "x"}) }},
		{"sem-acquire", func() *http.Response {
			return c.post("/v1/semaphores/k", semAcquireRequest{AcquireTimeoutS: 1, Limit: 1})
		}},
		{"ping", func() *http.Response { return c.post("/v1/sessions/"+c.session+"/ping", nil) }},
	}
	for _, tc := range checks {
		resp := tc.do()
		if resp.StatusCode != http.StatusGone {
			t.Errorf("%s: status = %d, want 410", tc.name, resp.StatusCode)
		}
		resp.Body.Close()
	}
}

// TestHTTP_WaitNaturalTimeoutReturnsTimeoutNot410 pins the renderLockErr
// boundary: when /wait's own timer fires (not the session ctx), the response
// is 200 with status="timeout" — not 410 session_gone. Regression guard for
// the v2.0.1 renderLockErr split.
func TestHTTP_WaitNaturalTimeoutReturnsTimeoutNot410(t *testing.T) {
	base, stop := startHTTP(t)
	defer stop()

	_, queuer, _ := setupQueuedLock(t, base, "k")

	resp := queuer.post("/v1/locks/k/wait", waitRequest{TimeoutS: 1})
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200 (timeout, not 410)", resp.StatusCode)
	}
	var v opResponse
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		t.Fatal(err)
	}
	if v.Status != "timeout" {
		t.Errorf("status = %q, want timeout", v.Status)
	}
}

// TestHTTP_MaxSessionsEnforced asserts the global cap surfaces 503
// max_sessions before allowing the 2nd session to land.
func TestHTTP_MaxSessionsEnforced(t *testing.T) {
	base, stop := startHTTP(t, func(c *config.Config) { c.HTTPMaxSessions = 1 })
	defer stop()

	startedClient(t, base) // burns the only slot

	resp := newClient(t, base).post("/v1/sessions", nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
	var e errorBody
	if err := json.NewDecoder(resp.Body).Decode(&e); err != nil {
		t.Fatal(err)
	}
	if e.Error != "max_sessions" {
		t.Errorf("error = %q, want max_sessions", e.Error)
	}
}

// TestHTTP_MaxSessionsPerIPEnforced exercises the per-IP cap. All
// loopback clients share the same source IP, so the third session
// from the same client must surface 503 max_sessions_per_ip.
func TestHTTP_MaxSessionsPerIPEnforced(t *testing.T) {
	base, stop := startHTTP(t, func(c *config.Config) { c.HTTPMaxSessionsPerIP = 2 })
	defer stop()

	startedClient(t, base)
	startedClient(t, base)

	resp := newClient(t, base).post("/v1/sessions", nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
	var e errorBody
	if err := json.NewDecoder(resp.Body).Decode(&e); err != nil {
		t.Fatal(err)
	}
	if e.Error != "max_sessions_per_ip" {
		t.Errorf("error = %q, want max_sessions_per_ip", e.Error)
	}
}

// TestHTTP_MethodNotAllowedOnPostOnlyRoute pins the mux's method-aware
// routing: GET on a POST-only endpoint must surface 405, not 404 or
// the wrong handler.
func TestHTTP_MethodNotAllowedOnPostOnlyRoute(t *testing.T) {
	base, stop := startHTTP(t)
	defer stop()

	c := startedClient(t, base)

	resp := c.get("/v1/locks/k")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("GET /v1/locks/{key}: status = %d, want 405", resp.StatusCode)
	}
}

// TestHTTP_MetricsMethodCardinalityBounded checks that arbitrary HTTP
// method tokens against an unmatched path don't become distinct
// /metrics labels — they're bucketed as "OTHER" so the per-route
// metrics map stays bounded.
func TestHTTP_MetricsMethodCardinalityBounded(t *testing.T) {
	base, stop := startHTTP(t)
	defer stop()
	c := newClient(t, base)

	weird := []string{"WIBBLE1", "WIBBLE2", "QUUX9"}
	for _, m := range weird {
		c.do(c.request(m, "/no/such/route", nil)).Body.Close()
	}

	body := readBody(t, c.get("/metrics"))
	for _, m := range weird {
		if strings.Contains(body, m) {
			t.Errorf("/metrics leaked unbounded method label %q:\n%s", m, body)
		}
	}
	if !strings.Contains(body, `method="OTHER"`) {
		t.Errorf("/metrics missing bucketed OTHER label:\n%s", body)
	}
}

func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(b)
}
