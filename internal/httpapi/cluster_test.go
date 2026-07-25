package httpapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mtingers/dflockd/internal/config"
	"github.com/mtingers/dflockd/internal/lock"
	"github.com/mtingers/dflockd/internal/raft"
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
	cleanupRef    string
	cleanupConnID uint64
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
func (f *httpFakeCluster) ProposeCleanupConn(_ context.Context, ref string, connID uint64) (lock.ApplyResult, error) {
	f.mu.Lock()
	f.cleanupCalls++
	f.cleanupRef = ref
	f.cleanupConnID = connID
	f.mu.Unlock()
	return lock.ApplyResult{Status: lock.StatusOK}, nil
}

func (f *httpFakeCluster) Barrier(_ context.Context) error { return nil }

func (f *httpFakeCluster) AddVoter(_ context.Context, _ raft.NodeID, _, _ string) error {
	return nil
}

func (f *httpFakeCluster) RemoveServer(_ context.Context, _ raft.NodeID) error { return nil }

func (f *httpFakeCluster) MetricsSnapshot() raft.ClusterMetrics {
	return raft.ClusterMetrics{}
}

func newClusterHTTPTest(t *testing.T, fc server.Cluster) *httpServer {
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

func TestHTTPCluster_EnqueueAcquiredFastPath(t *testing.T) {
	fc := &httpFakeCluster{leader: true, enqueueResult: lock.ApplyResult{Status: lock.StatusAcquired, Token: "et", LeaseSec: 33}}
	hs := newClusterHTTPTest(t, fc)
	s, _ := hs.sessions.Create("127.0.0.1")
	rec := httptest.NewRecorder()
	hs.runEnqueueCluster(rec, httptest.NewRequest(http.MethodPost, "/v1/locks/k/enqueue", nil), s, "k", lock.LockPrefix, 1, 33)
	if rec.Code != http.StatusOK {
		t.Fatalf("enqueue status = %d, want 200", rec.Code)
	}
	var op opResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &op)
	if op.Status != "acquired" || op.Token != "et" || op.LeaseTTLS != 33 {
		t.Fatalf("op = %+v", op)
	}

	// Queued enqueue → "queued", and a follower enqueue → 503.
	fc.enqueueResult = lock.ApplyResult{Status: lock.StatusQueued}
	rec = httptest.NewRecorder()
	hs.runEnqueueCluster(rec, httptest.NewRequest(http.MethodPost, "/v1/locks/k/enqueue", nil), s, "k2", lock.LockPrefix, 1, 33)
	_ = json.Unmarshal(rec.Body.Bytes(), &op)
	if op.Status != "queued" {
		t.Fatalf("queued enqueue op = %+v", op)
	}
	fc.setLeader(false)
	rec = httptest.NewRecorder()
	hs.runEnqueueCluster(rec, httptest.NewRequest(http.MethodPost, "/v1/locks/k/enqueue", nil), s, "k", lock.LockPrefix, 1, 33)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("follower enqueue status = %d, want 503", rec.Code)
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
	s, err := hs.sessions.CreateWithStableRef("127.0.0.1", "worker-delete")
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
	ref := fc.cleanupRef
	connID := fc.cleanupConnID
	fc.mu.Unlock()
	if got != 1 {
		t.Fatalf("ProposeCleanupConn called %d times, want 1", got)
	}
	if ref != "worker-delete" || connID == 0 {
		t.Fatalf("cleanup identity = (%q, %d), want stable ref + non-zero connID", ref, connID)
	}
	if !hs.sessions.Server().BindStableRef(s.ConnID, "reused-after-delete") {
		t.Fatal("stable-ref binding was not cleared after session deletion")
	}
	hs.sessions.Server().ClearStableRef(s.ConnID)
}

type applyHTTPCluster struct {
	*httpFakeCluster
	lm         *lock.LockManager
	acquireRef string
	acquireCID uint64
	enqueueRef string
	enqueueCID uint64
}

func (f *applyHTTPCluster) ProposeAcquire(_ context.Context, key string, limit int, ref string, connID uint64, leaseTTL time.Duration, salt [8]byte) (lock.ApplyResult, error) {
	f.mu.Lock()
	f.acquireRef = ref
	f.acquireCID = connID
	f.mu.Unlock()
	result, grants, err := f.lm.ApplyAcquire(time.Now(), key, limit, ref, connID, leaseTTL, salt)
	f.lm.RouteGrants(grants)
	return result, err
}

func (f *applyHTTPCluster) ProposeEnqueue(_ context.Context, key string, limit int, ref string, connID uint64, leaseTTL time.Duration, salt [8]byte) (lock.ApplyResult, error) {
	f.mu.Lock()
	f.enqueueRef = ref
	f.enqueueCID = connID
	f.mu.Unlock()
	result, grants, err := f.lm.ApplyEnqueue(time.Now(), key, limit, ref, connID, leaseTTL, salt)
	f.lm.RouteGrants(grants)
	return result, err
}

func (f *applyHTTPCluster) ProposeCleanupConn(_ context.Context, ref string, connID uint64) (lock.ApplyResult, error) {
	result, grants, err := f.lm.ApplyCleanupConn(time.Now(), ref, connID)
	f.lm.RouteGrants(grants)
	return result, err
}

func TestHTTPCluster_StableRefReattachesAcrossLeaderChange(t *testing.T) {
	cfg := defaultTestConfig()
	cfg.OrphanTTL = time.Minute
	log := discardLogger()
	lm, err := lock.NewLockManager(cfg, log)
	if err != nil {
		t.Fatalf("NewLockManager: %v", err)
	}
	defer lm.Close()

	clusterA := &applyHTTPCluster{httpFakeCluster: &httpFakeCluster{leader: true}, lm: lm}
	clusterB := &applyHTTPCluster{httpFakeCluster: &httpFakeCluster{leader: false}, lm: lm}
	hsA := newHTTPServerForSharedFSM(t, cfg, log, lm, clusterA)
	hsB := newHTTPServerForSharedFSM(t, cfg, log, lm, clusterB)
	defer shutdownTestHTTPServer(hsA)
	defer shutdownTestHTTPServer(hsB)

	sessionA := createStableSession(t, hsA, "worker-failover")
	first := acquireViaClusterHTTP(t, hsA, sessionA, "failover-key")
	clusterA.mu.Lock()
	refA, cidA := clusterA.acquireRef, clusterA.acquireCID
	clusterA.mu.Unlock()
	blockerA := createStableSession(t, hsA, "queue-blocker")
	acquireViaClusterHTTP(t, hsA, blockerA, "queued-key")
	waiterA := createStableSession(t, hsA, "queued-worker")
	if queued := enqueueViaClusterHTTP(t, hsA, waiterA, "queued-key"); queued.Status != "queued" {
		t.Fatalf("initial enqueue status = %q, want queued", queued.Status)
	}

	clusterA.setLeader(false)
	clusterB.setLeader(true)
	sessionB := createStableSession(t, hsB, "worker-failover")
	second := acquireViaClusterHTTP(t, hsB, sessionB, "failover-key")
	waiterB := createStableSession(t, hsB, "queued-worker")
	if queued := enqueueViaClusterHTTP(t, hsB, waiterB, "queued-key"); queued.Status != "queued" {
		t.Fatalf("re-attached enqueue status = %q, want queued", queued.Status)
	}

	if first.Token == "" || second.Token != first.Token {
		t.Fatalf("re-attached token = %q, want original %q", second.Token, first.Token)
	}
	clusterB.mu.Lock()
	refB, cidB := clusterB.acquireRef, clusterB.acquireCID
	clusterB.mu.Unlock()
	if refA != "worker-failover" || refB != refA {
		t.Fatalf("acquire refs = (%q, %q), want stable ref on both leaders", refA, refB)
	}
	if cidA>>32 == cidB>>32 {
		t.Fatalf("server epochs unexpectedly match: cidA=%d cidB=%d", cidA, cidB)
	}
	clusterB.mu.Lock()
	enqueueRef, enqueueCID := clusterB.enqueueRef, clusterB.enqueueCID
	clusterB.mu.Unlock()
	if enqueueRef != "queued-worker" || !lm.HasActiveWaiterForTest("lock:queued-key", enqueueRef, enqueueCID) {
		t.Fatalf("queued waiter was not rebound: ref=%q cid=%d", enqueueRef, enqueueCID)
	}
	if got := lm.CountWaitersForTest("lock:queued-key"); got != 1 {
		t.Fatalf("queued-key waiters = %d, want 1 re-attached slot", got)
	}
}

func newHTTPServerForSharedFSM(t *testing.T, cfg *config.Config, log *slog.Logger, lm *lock.LockManager, cluster server.Cluster) *httpServer {
	t.Helper()
	srv := server.New(lm, cfg, log)
	srv.SetCluster(cluster)
	hs, _ := buildHTTPServer(context.Background(), srv, cfg, log)
	return hs
}

func shutdownTestHTTPServer(hs *httpServer) {
	hs.limiter.Stop()
	hs.sessions.Shutdown()
}

func createStableSession(t *testing.T, hs *httpServer, ref string) *Session {
	t.Helper()
	body, err := json.Marshal(createSessionRequest{StableRef: &ref})
	if err != nil {
		t.Fatalf("marshal session request: %v", err)
	}
	rec := httptest.NewRecorder()
	hs.handleCreateSession(rec, httptest.NewRequest(http.MethodPost, "/v1/sessions", strings.NewReader(string(body))))
	if rec.Code != http.StatusOK {
		t.Fatalf("create stable session: status=%d body=%s", rec.Code, rec.Body)
	}
	var response createSessionResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode session response: %v", err)
	}
	session, err := hs.sessions.Lookup(response.SessionID)
	if err != nil {
		t.Fatalf("lookup session: %v", err)
	}
	return session
}

func acquireViaClusterHTTP(t *testing.T, hs *httpServer, session *Session, key string) opResponse {
	t.Helper()
	rec := httptest.NewRecorder()
	hs.runAcquireCluster(rec, httptest.NewRequest(http.MethodPost, "/v1/locks/"+key, nil), session, key, lock.LockPrefix, 1, 0, 30)
	if rec.Code != http.StatusOK {
		t.Fatalf("acquire: status=%d body=%s", rec.Code, rec.Body)
	}
	var response opResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode acquire response: %v", err)
	}
	return response
}

func enqueueViaClusterHTTP(t *testing.T, hs *httpServer, session *Session, key string) opResponse {
	t.Helper()
	rec := httptest.NewRecorder()
	hs.runEnqueueCluster(rec, httptest.NewRequest(http.MethodPost, "/v1/locks/"+key+"/enqueue", nil), session, key, lock.LockPrefix, 1, 30)
	if rec.Code != http.StatusOK {
		t.Fatalf("enqueue: status=%d body=%s", rec.Code, rec.Body)
	}
	var response opResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode enqueue response: %v", err)
	}
	return response
}
