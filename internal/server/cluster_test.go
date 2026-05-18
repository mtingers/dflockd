package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mtingers/dflockd/internal/config"
	"github.com/mtingers/dflockd/internal/lock"
	"github.com/mtingers/dflockd/internal/protocol"
	"github.com/mtingers/dflockd/internal/raft"
)

// fakeCluster is a controllable Cluster used to drive the cluster-mode
// handler branches. Each Propose method either succeeds with a canned
// ApplyResult or returns a canned error, and a synchronous "promote a
// queued waiter" hook routes a grant through the LockManager's
// listener registry.
type fakeCluster struct {
	mu            sync.Mutex
	leader        bool
	leaderAddr    string
	cleanupCalls  int
	proposeErr    error
	acquireResult lock.ApplyResult
	releaseResult lock.ApplyResult
	renewResult   lock.ApplyResult
	enqueueResult lock.ApplyResult
	lm            *lock.LockManager // for delivering follow-on grants
	promotion     *promotion
}

type promotion struct {
	token    string
	leaseSec int
	delay    time.Duration
}

func (f *fakeCluster) IsLeader() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.leader
}

func (f *fakeCluster) LeaderClientAddr() (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.leaderAddr, f.leaderAddr != ""
}

func (f *fakeCluster) StatusJSON() json.RawMessage {
	return json.RawMessage(`{"node_id":"fake","role":"leader","term":7}`)
}

func (f *fakeCluster) ProposeAcquire(ctx context.Context, key string, limit int, ref string, connID uint64, leaseTTL time.Duration, salt [8]byte) (lock.ApplyResult, error) {
	if f.proposeErr != nil {
		return lock.ApplyResult{}, f.proposeErr
	}
	if f.promotion != nil {
		f.schedulePromotion(ref)
	}
	return f.acquireResult, nil
}

// schedulePromotion routes a follow-on grant for `ref` (the ref the
// server actually registered its listener under) after the configured
// delay.
func (f *fakeCluster) schedulePromotion(ref string) {
	p := f.promotion
	go func() {
		if p.delay > 0 {
			time.Sleep(p.delay)
		}
		f.lm.RouteGrants([]lock.Grant{{Ref: ref, Token: p.token, LeaseSec: p.leaseSec}})
	}()
}

func (f *fakeCluster) ProposeEnqueue(ctx context.Context, key string, limit int, ref string, connID uint64, leaseTTL time.Duration, salt [8]byte) (lock.ApplyResult, error) {
	if f.proposeErr != nil {
		return lock.ApplyResult{}, f.proposeErr
	}
	if f.promotion != nil {
		f.schedulePromotion(ref)
	}
	return f.enqueueResult, nil
}

func (f *fakeCluster) ProposeRelease(ctx context.Context, key, token string) (lock.ApplyResult, error) {
	if f.proposeErr != nil {
		return lock.ApplyResult{}, f.proposeErr
	}
	return f.releaseResult, nil
}

func (f *fakeCluster) ProposeRenew(ctx context.Context, key, token string, leaseTTL time.Duration) (lock.ApplyResult, error) {
	if f.proposeErr != nil {
		return lock.ApplyResult{}, f.proposeErr
	}
	return f.renewResult, nil
}

func (f *fakeCluster) ProposeCleanupConn(ctx context.Context, ref string, connID uint64) (lock.ApplyResult, error) {
	f.mu.Lock()
	f.cleanupCalls++
	f.mu.Unlock()
	return lock.ApplyResult{Status: lock.StatusOK}, nil
}

func (f *fakeCluster) Barrier(ctx context.Context) error {
	if f.proposeErr != nil {
		return f.proposeErr
	}
	return nil
}

func (f *fakeCluster) AddVoter(ctx context.Context, id raft.NodeID, raftAddr, clientAddr string) error {
	if f.proposeErr != nil {
		return f.proposeErr
	}
	return nil
}

func (f *fakeCluster) RemoveServer(ctx context.Context, id raft.NodeID) error {
	if f.proposeErr != nil {
		return f.proposeErr
	}
	return nil
}

func (f *fakeCluster) MetricsSnapshot() raft.ClusterMetrics {
	return raft.ClusterMetrics{}
}

// --- harness ---

func newTestServer(t *testing.T) (*Server, *lock.LockManager) {
	t.Helper()
	cfg := &config.Config{
		MaxLocks:        128,
		DefaultLeaseTTL: 30 * time.Second,
		GCMaxIdleTime:   60 * time.Second,
		ReadTimeout:     2 * time.Second,
	}
	lm, err := lock.NewLockManager(cfg, slog.Default())
	if err != nil {
		t.Fatalf("NewLockManager: %v", err)
	}
	t.Cleanup(func() { _ = lm.Close() })
	return New(lm, cfg, slog.Default()), lm
}

func reqAcquire(key string, lease time.Duration, timeout time.Duration) *protocol.Request {
	return &protocol.Request{Cmd: protocol.CmdAcquire, Key: key, LeaseTTL: lease, AcquireTimeout: timeout}
}

// --- tests ---

func TestSetClusterToggles(t *testing.T) {
	s, _ := newTestServer(t)
	if s.clusterOrNil() != nil {
		t.Fatalf("initial cluster should be nil")
	}
	fc := &fakeCluster{leader: true}
	s.SetCluster(fc)
	if s.clusterOrNil() == nil {
		t.Fatalf("after SetCluster: nil")
	}
	s.SetCluster(nil)
	if s.clusterOrNil() != nil {
		t.Fatalf("after SetCluster(nil): not nil")
	}
}

func TestClusterAcquireNotLeaderReturnsRedirect(t *testing.T) {
	s, _ := newTestServer(t)
	fc := &fakeCluster{leader: false, leaderAddr: "h:9999"}
	s.SetCluster(fc)
	ack := s.handleAcquire(context.Background(), reqAcquire("k", 10*time.Second, 1*time.Second), 1)
	if ack.Status != protocol.StatusErrorNotLeader {
		t.Fatalf("status = %q, want error_not_leader", ack.Status)
	}
	if ack.Extra != "h:9999" {
		t.Fatalf("extra = %q, want leader addr", ack.Extra)
	}
}

func TestClusterAcquireLeaderImmediate(t *testing.T) {
	s, _ := newTestServer(t)
	fc := &fakeCluster{
		leader:        true,
		acquireResult: lock.ApplyResult{Status: lock.StatusOK, Token: "abc-token", LeaseSec: 30},
	}
	s.SetCluster(fc)
	ack := s.handleAcquire(context.Background(), reqAcquire("k", 30*time.Second, 1*time.Second), 1)
	if ack.Status != protocol.StatusOK || ack.Token != "abc-token" || ack.LeaseTTL != 30 {
		t.Fatalf("ack = %+v", ack)
	}
}

func TestClusterAcquireQueuedWaitsForGrant(t *testing.T) {
	s, lm := newTestServer(t)
	fc := &fakeCluster{
		leader:        true,
		acquireResult: lock.ApplyResult{Status: lock.StatusQueued},
		lm:            lm,
		promotion:     &promotion{token: "tk-after", leaseSec: 25, delay: 20 * time.Millisecond},
	}
	s.SetCluster(fc)
	ack := s.handleAcquire(context.Background(), reqAcquire("k", 30*time.Second, 2*time.Second), 1)
	if ack.Status != protocol.StatusOK || ack.Token != "tk-after" || ack.LeaseTTL != 25 {
		t.Fatalf("queued+promotion ack = %+v", ack)
	}
}

// A two-phase Enqueue that comes back queued must register a grant
// listener that survives until the matching Wait — a promotion landing
// in between must not be lost.
func TestClusterEnqueueThenWaitGetsPromotion(t *testing.T) {
	s, lm := newTestServer(t)
	fc := &fakeCluster{
		leader:        true,
		enqueueResult: lock.ApplyResult{Status: lock.StatusQueued},
		lm:            lm,
		promotion:     &promotion{token: "tk-promoted", leaseSec: 40, delay: 15 * time.Millisecond},
	}
	s.SetCluster(fc)
	const connID = 7
	if ack := s.handleEnqueue(&protocol.Request{Cmd: protocol.CmdEnqueue, Key: "k", LeaseTTL: 40 * time.Second}, connID); ack.Status != protocol.StatusQueued {
		t.Fatalf("enqueue ack = %+v, want queued", ack)
	}
	// The promotion fires ~15ms after the enqueue (before Wait below).
	time.Sleep(30 * time.Millisecond)
	ack := s.handleWait(context.Background(), &protocol.Request{Cmd: protocol.CmdWait, Key: "k", AcquireTimeout: time.Second}, connID)
	if ack.Status != protocol.StatusOK || ack.Token != "tk-promoted" || ack.LeaseTTL != 40 {
		t.Fatalf("wait after enqueue+promotion ack = %+v", ack)
	}
}

func TestClusterAcquireQueuedTimesOut(t *testing.T) {
	s, _ := newTestServer(t)
	fc := &fakeCluster{
		leader:        true,
		acquireResult: lock.ApplyResult{Status: lock.StatusQueued},
	}
	s.SetCluster(fc)
	ack := s.handleAcquire(context.Background(), reqAcquire("k", 30*time.Second, 30*time.Millisecond), 1)
	if ack.Status != protocol.StatusTimeout {
		t.Fatalf("queued no-grant ack = %+v, want timeout", ack)
	}
}

func TestClusterReleaseRouting(t *testing.T) {
	s, _ := newTestServer(t)
	fc := &fakeCluster{leader: true, releaseResult: lock.ApplyResult{Status: lock.StatusOK}}
	s.SetCluster(fc)
	ack := s.handleRelease(&protocol.Request{Cmd: protocol.CmdRelease, Key: "k", Token: "t"})
	if ack.Status != protocol.StatusOK {
		t.Fatalf("release ack = %+v", ack)
	}
}

func TestClusterReleaseNotLeader(t *testing.T) {
	s, _ := newTestServer(t)
	fc := &fakeCluster{leader: false, leaderAddr: "elsewhere:1"}
	s.SetCluster(fc)
	ack := s.handleRelease(&protocol.Request{Cmd: protocol.CmdRelease, Key: "k", Token: "t"})
	if ack.Status != protocol.StatusErrorNotLeader || ack.Extra != "elsewhere:1" {
		t.Fatalf("release on follower = %+v", ack)
	}
}

func TestClusterRenewRouting(t *testing.T) {
	s, _ := newTestServer(t)
	fc := &fakeCluster{leader: true, renewResult: lock.ApplyResult{Status: lock.StatusOK, LeaseSec: 60}}
	s.SetCluster(fc)
	ack := s.handleRenew(&protocol.Request{Cmd: protocol.CmdRenew, Key: "k", Token: "t", LeaseTTL: 60 * time.Second})
	if ack.Status != protocol.StatusOK || ack.Extra != "60" {
		t.Fatalf("renew ack = %+v", ack)
	}
}

func TestClusterEnqueueAcquired(t *testing.T) {
	s, _ := newTestServer(t)
	fc := &fakeCluster{leader: true, enqueueResult: lock.ApplyResult{Status: lock.StatusAcquired, Token: "et", LeaseSec: 33}}
	s.SetCluster(fc)
	ack := s.handleEnqueue(&protocol.Request{Cmd: protocol.CmdEnqueue, Key: "k", LeaseTTL: 33 * time.Second}, 1)
	if ack.Status != protocol.StatusAcquired || ack.Token != "et" || ack.LeaseTTL != 33 {
		t.Fatalf("enqueue ack = %+v", ack)
	}
}

func TestClusterEnqueueQueued(t *testing.T) {
	s, _ := newTestServer(t)
	fc := &fakeCluster{leader: true, enqueueResult: lock.ApplyResult{Status: lock.StatusQueued}}
	s.SetCluster(fc)
	ack := s.handleEnqueue(&protocol.Request{Cmd: protocol.CmdEnqueue, Key: "k", LeaseTTL: 30 * time.Second}, 1)
	if ack.Status != protocol.StatusQueued {
		t.Fatalf("enqueue queued ack = %+v", ack)
	}
}

func TestClusterCleanupOnConnTeardown(t *testing.T) {
	// Wire a fake conn close path: ServeConn is heavy; instead exercise
	// teardownConn directly with a discardable net.Conn.
	s, _ := newTestServer(t)
	fc := &fakeCluster{leader: true}
	s.SetCluster(fc)
	cliConn, srvConn := net.Pipe()
	defer cliConn.Close()
	defer srvConn.Close()
	s.teardownConn(srvConn, "peer", 7, func() {})
	if got := atomicCleanupCount(fc); got != 1 {
		t.Fatalf("cleanupCalls = %d, want 1", got)
	}
}

func TestClusterCleanupSkippedOnFollower(t *testing.T) {
	s, _ := newTestServer(t)
	fc := &fakeCluster{leader: false}
	s.SetCluster(fc)
	cliConn, srvConn := net.Pipe()
	defer cliConn.Close()
	defer srvConn.Close()
	s.teardownConn(srvConn, "peer", 9, func() {})
	if got := atomicCleanupCount(fc); got != 0 {
		t.Fatalf("cleanupCalls = %d, want 0 (follower)", got)
	}
}

func atomicCleanupCount(f *fakeCluster) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cleanupCalls
}

func TestFormatNotLeaderEmitsExtra(t *testing.T) {
	out := protocol.FormatResponse(&protocol.Ack{Status: protocol.StatusErrorNotLeader, Extra: "host:6388"}, 0)
	if !strings.Contains(string(out), "error_not_leader host:6388") {
		t.Fatalf("not_leader frame = %q", out)
	}
	out = protocol.FormatResponse(&protocol.Ack{Status: protocol.StatusErrorNotLeader}, 0)
	if string(out) != "error_not_leader\n" {
		t.Fatalf("empty-addr frame = %q", out)
	}
}

func TestStatsIncludesClusterBlockWhenClustered(t *testing.T) {
	s, _ := newTestServer(t)
	// Single-node: no "cluster" key.
	js, ok := s.statsJSON()
	if !ok {
		t.Fatalf("statsJSON failed")
	}
	if strings.Contains(js, `"cluster"`) {
		t.Fatalf("single-node stats unexpectedly has a cluster block: %s", js)
	}
	// Clustered: a "cluster" object appears, lock fields still present.
	s.SetCluster(&fakeCluster{leader: true})
	js, ok = s.statsJSON()
	if !ok {
		t.Fatalf("statsJSON (clustered) failed")
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal([]byte(js), &m); err != nil {
		t.Fatalf("stats JSON unmarshal: %v\n%s", err, js)
	}
	if _, has := m["cluster"]; !has {
		t.Fatalf("clustered stats missing the cluster block: %s", js)
	}
	if !strings.Contains(string(m["cluster"]), `"role":"leader"`) {
		t.Fatalf("cluster block = %s", m["cluster"])
	}
}

func TestStartBackgroundLoopsSkippedClustered(t *testing.T) {
	// Indirect check: when cluster is set, startBackgroundLoops must
	// not add to the WaitGroup. Drive it directly with a tiny context.
	s, _ := newTestServer(t)
	s.SetCluster(&fakeCluster{leader: true})
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // immediately
	s.startBackgroundLoops(ctx, &wg)
	wg.Wait() // must return without blocking
}

// Defensive: ensure the Cluster interface stays stable so test fakes
// keep compiling.
var _ Cluster = (*fakeCluster)(nil)

// Lint-noise placeholder: keep the atomic import live.
var _ atomic.Int32
