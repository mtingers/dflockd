package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mtingers/dflockd/internal/lock"
	"github.com/mtingers/dflockd/internal/server"
)

// httpFakeCluster is a controllable server.Cluster for exercising the
// HTTP API's cluster-mode routing without a real Raft cluster.
type httpFakeCluster struct {
	mu            sync.Mutex
	leader        bool
	leaderAddr    string
	acquireResult lock.ApplyResult
	enqueueResult lock.ApplyResult
	releaseResult lock.ApplyResult
	renewResult   lock.ApplyResult
	cleanupCalls  int
}

var _ server.Cluster = (*httpFakeCluster)(nil)

func (f *httpFakeCluster) setLeader(v bool) { f.mu.Lock(); f.leader = v; f.mu.Unlock() }

func (f *httpFakeCluster) IsLeader() bool { f.mu.Lock(); defer f.mu.Unlock(); return f.leader }

func (f *httpFakeCluster) LeaderClientAddr() (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.leaderAddr, f.leaderAddr != ""
}

func (f *httpFakeCluster) StatusJSON() json.RawMessage {
	return json.RawMessage(`{"node_id":"fake","role":"leader","term":3}`)
}

func (f *httpFakeCluster) ProposeAcquire(_ context.Context, _ string, _ int, _ string, _ uint64, _ time.Duration, _ [8]byte) (lock.ApplyResult, error) {
	return f.acquireResult, nil
}
func (f *httpFakeCluster) ProposeEnqueue(_ context.Context, _ string, _ int, _ string, _ uint64, _ time.Duration, _ [8]byte) (lock.ApplyResult, error) {
	return f.enqueueResult, nil
}
func (f *httpFakeCluster) ProposeRelease(_ context.Context, _, _ string) (lock.ApplyResult, error) {
	return f.releaseResult, nil
}
func (f *httpFakeCluster) ProposeRenew(_ context.Context, _, _ string, _ time.Duration) (lock.ApplyResult, error) {
	return f.renewResult, nil
}
func (f *httpFakeCluster) ProposeCleanupConn(_ context.Context, _ string, _ uint64) (lock.ApplyResult, error) {
	f.mu.Lock()
	f.cleanupCalls++
	f.mu.Unlock()
	return lock.ApplyResult{Status: lock.StatusOK}, nil
}

func newClusterHTTPTest(t *testing.T, fc *httpFakeCluster) *httpServer {
	t.Helper()
	cfg := defaultTestConfig()
	log := discardLogger()
	lm, err := lock.NewLockManager(cfg, log)
	if err != nil {
		t.Fatalf("NewLockManager: %v", err)
	}
	t.Cleanup(func() { lm.Close() })
	srv := server.New(lm, cfg, log)
	srv.SetCluster(fc)
	hs, _ := buildHTTPServer(context.Background(), srv, cfg, log)
	t.Cleanup(func() { hs.limiter.Stop(); hs.sessions.Shutdown() })
	return hs
}

func TestHTTPCluster_AcquireOnLeaderReturnsToken(t *testing.T) {
	fc := &httpFakeCluster{leader: true, acquireResult: lock.ApplyResult{Status: lock.StatusOK, Token: "tok-x", LeaseSec: 30}}
	hs := newClusterHTTPTest(t, fc)
	s, _ := hs.sessions.Create("127.0.0.1")
	rec := httptest.NewRecorder()
	hs.runAcquireCluster(rec, httptest.NewRequest(http.MethodPost, "/v1/locks/k", nil), s, "k", lock.LockPrefix, 1, 1, 30)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body)
	}
	var op opResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &op); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if op.Status != "ok" || op.Token != "tok-x" || op.LeaseTTLS != 30 {
		t.Fatalf("op = %+v", op)
	}
}

func TestHTTPCluster_FollowerReturns503NotLeader(t *testing.T) {
	fc := &httpFakeCluster{leader: false, leaderAddr: "10.0.0.9:6388"}
	hs := newClusterHTTPTest(t, fc)
	s, _ := hs.sessions.Create("127.0.0.1")
	rec := httptest.NewRecorder()
	hs.runAcquireCluster(rec, httptest.NewRequest(http.MethodPost, "/v1/locks/k", nil), s, "k", lock.LockPrefix, 1, 1, 30)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("follower acquire status = %d, want 503", rec.Code)
	}
	if got := rec.Header().Get("X-Dflockd-Leader"); got != "10.0.0.9:6388" {
		t.Fatalf("X-Dflockd-Leader = %q", got)
	}
	var e errorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &e); err != nil || e.Error != "not_leader" {
		t.Fatalf("body = %s (err %v)", rec.Body, err)
	}
}

func TestHTTPCluster_QueuedAcquireTimesOut(t *testing.T) {
	fc := &httpFakeCluster{leader: true, acquireResult: lock.ApplyResult{Status: lock.StatusQueued}}
	hs := newClusterHTTPTest(t, fc)
	s, _ := hs.sessions.Create("127.0.0.1")
	rec := httptest.NewRecorder()
	// timeoutS=0 → the wait timer fires immediately → "timeout".
	hs.runAcquireCluster(rec, httptest.NewRequest(http.MethodPost, "/v1/locks/k", nil), s, "k", lock.LockPrefix, 1, 0, 30)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var op opResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &op)
	if op.Status != "timeout" {
		t.Fatalf("op = %+v, want status=timeout", op)
	}
}

func TestHTTPCluster_ReleaseAndRenew(t *testing.T) {
	fc := &httpFakeCluster{
		leader:        true,
		releaseResult: lock.ApplyResult{Status: lock.StatusOK},
		renewResult:   lock.ApplyResult{Status: lock.StatusOK, LeaseSec: 42},
	}
	hs := newClusterHTTPTest(t, fc)

	rec := httptest.NewRecorder()
	hs.runReleaseCluster(rec, httptest.NewRequest(http.MethodPost, "/v1/locks/k/release", nil), "lock:k", "tok-x")
	if rec.Code != http.StatusNoContent {
		t.Fatalf("release status = %d, want 204", rec.Code)
	}

	rec = httptest.NewRecorder()
	hs.runRenewCluster(rec, httptest.NewRequest(http.MethodPost, "/v1/locks/k/renew", nil), "lock:k", "tok-x", 42)
	if rec.Code != http.StatusOK {
		t.Fatalf("renew status = %d, want 200", rec.Code)
	}
	var rr renewResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &rr)
	if rr.RemainingS != 42 {
		t.Fatalf("renew remaining = %d, want 42", rr.RemainingS)
	}

	// On a follower, release/renew also redirect.
	fc.setLeader(false)
	rec = httptest.NewRecorder()
	hs.runReleaseCluster(rec, httptest.NewRequest(http.MethodPost, "/v1/locks/k/release", nil), "lock:k", "tok-x")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("follower release status = %d, want 503", rec.Code)
	}
}

func TestHTTPCluster_NotHeldRelease(t *testing.T) {
	fc := &httpFakeCluster{leader: true, releaseResult: lock.ApplyResult{Status: lock.StatusNotHeld}}
	hs := newClusterHTTPTest(t, fc)
	rec := httptest.NewRecorder()
	hs.runReleaseCluster(rec, httptest.NewRequest(http.MethodPost, "/v1/locks/k/release", nil), "lock:k", "ghost")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("release of unheld status = %d, want 404", rec.Code)
	}
}

func TestHTTPCluster_StatsHasClusterBlock(t *testing.T) {
	fc := &httpFakeCluster{leader: true}
	hs := newClusterHTTPTest(t, fc)
	rec := httptest.NewRecorder()
	hs.handleStats(rec, httptest.NewRequest(http.MethodGet, "/v1/stats", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("stats status = %d", rec.Code)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil {
		t.Fatalf("stats decode: %v\n%s", err, rec.Body)
	}
	if _, ok := m["cluster"]; !ok {
		t.Fatalf("stats missing cluster block: %s", rec.Body)
	}
}

func TestHTTPCluster_MetricsHasRaftGauges(t *testing.T) {
	fc := &httpFakeCluster{leader: true}
	hs := newClusterHTTPTest(t, fc)
	rec := httptest.NewRecorder()
	hs.handleMetrics(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		`dflockd_raft_state{state="leader"} 1`,
		`dflockd_raft_state{state="follower"} 0`,
		"dflockd_raft_is_leader 1",
		"dflockd_raft_term 3",
		"dflockd_raft_commit_index",
		"dflockd_raft_voters",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q\n%s", want, body)
		}
	}
}

func TestHTTPCluster_SessionDeleteProposesCleanup(t *testing.T) {
	fc := &httpFakeCluster{leader: true}
	hs := newClusterHTTPTest(t, fc)
	s, err := hs.sessions.Create("127.0.0.1")
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	rec := httptest.NewRecorder()
	hs.handleDeleteSession(rec, httptest.NewRequest(http.MethodDelete, "/v1/sessions/"+s.ID, nil), s.ID)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d, want 204", rec.Code)
	}
	fc.mu.Lock()
	got := fc.cleanupCalls
	fc.mu.Unlock()
	if got != 1 {
		t.Fatalf("ProposeCleanupConn called %d times, want 1", got)
	}
}
