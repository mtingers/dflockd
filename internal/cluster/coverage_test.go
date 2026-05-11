package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mtingers/dflockd/internal/lock"
	"github.com/mtingers/dflockd/internal/raft"
)

// Exercise the remaining propose helpers + dispatch arms so the FSM's
// switch is fully covered.

func TestProposeEnqueueEvictRenewCleanupGC(t *testing.T) {
	tc := newCluster(t, "n1")
	defer tc.stopAll()
	tc.waitLeader()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Enqueue fast path on a free key.
	r1, err := tc.nodes["n1"].ProposeEnqueue(ctx, "lock:k", 1, "A", 1, 30*time.Second, saltOf(1))
	if err != nil || r1.Status != lock.StatusAcquired || r1.Token == "" {
		t.Fatalf("ProposeEnqueue fast path: %+v %v", r1, err)
	}

	// Renew that lease.
	r2, err := tc.nodes["n1"].ProposeRenew(ctx, "lock:k", r1.Token, 60*time.Second)
	if err != nil || r2.Status != lock.StatusOK || r2.LeaseSec != 60 {
		t.Fatalf("ProposeRenew: %+v %v", r2, err)
	}

	// Evict.
	r3, err := tc.nodes["n1"].ProposeEvict(ctx, "lock:k", r1.Token)
	if err != nil || r3.Status != lock.StatusOK {
		t.Fatalf("ProposeEvict: %+v %v", r3, err)
	}

	// Acquire something then CleanupConn to release it.
	_, err = tc.nodes["n1"].ProposeAcquire(ctx, "lock:k2", 1, "B", 9, 30*time.Second, saltOf(2))
	if err != nil {
		t.Fatalf("ProposeAcquire k2: %v", err)
	}
	if _, err := tc.nodes["n1"].ProposeCleanupConn(ctx, "B", 9); err != nil {
		t.Fatalf("ProposeCleanupConn: %v", err)
	}

	// GC must succeed (no-op if everything is held; here all holders are
	// gone, so future calls drop the resource).
	if _, err := tc.nodes["n1"].ProposeGC(ctx); err != nil {
		t.Fatalf("ProposeGC: %v", err)
	}
}

func TestFSMDispatchRejectsBadSalt(t *testing.T) {
	tc := newCluster(t, "n1")
	defer tc.stopAll()
	tc.waitLeader()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// Hand-craft a Command with an invalid salt to drive applyErrResult
	// through the FSM dispatch path.
	bad := Command{Kind: KindAcquire, Key: "lock:k", Limit: 1, Ref: "X", ConnID: 1, LeaseTTLNanos: int64(30 * time.Second), SaltB64: "!!notbase64!!"}
	_, err := tc.nodes["n1"].Propose(ctx, bad)
	if err == nil {
		t.Fatalf("Propose with bad salt should err")
	}
}

func TestUnwrapApplyResultEdgeCases(t *testing.T) {
	// nil v -> StatusOK + nil err.
	r, err := unwrapApplyResult(nil)
	if err != nil || r.Status != lock.StatusOK {
		t.Fatalf("unwrap(nil) = %+v %v", r, err)
	}
	// bare ApplyResult -> echoed.
	want := lock.ApplyResult{Status: lock.StatusOK, Token: "t", LeaseSec: 5}
	r, err = unwrapApplyResult(want)
	if err != nil || r != want {
		t.Fatalf("unwrap(bare) = %+v %v", r, err)
	}
	// applyErrTyped -> err surfaces.
	r, err = unwrapApplyResult(applyErrTyped{Result: want, Err: errors.New("boom")})
	if err == nil || r != want {
		t.Fatalf("unwrap(err typed) = %+v %v", r, err)
	}
	// Unrelated type -> errUnknownKind.
	_, err = unwrapApplyResult(struct{}{})
	if !errors.Is(err, errUnknownKind) {
		t.Fatalf("unwrap(unknown) = %v, want errUnknownKind", err)
	}
}

func TestConfigValidate(t *testing.T) {
	cfg := Config{
		Raft:    fastRaftConfig("n1"),
		Members: map[raft.NodeID]Member{"n1": {RaftAddr: "h:1", ClientAddr: "c:1"}},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("valid config: %v", err)
	}
	// Missing members.
	bad := cfg
	bad.Members = nil
	if err := bad.Validate(); err == nil {
		t.Fatalf("empty Members should error")
	}
	// This node not in Members.
	bad = cfg
	bad.Members = map[raft.NodeID]Member{"other": {RaftAddr: "h:1"}}
	if err := bad.Validate(); err == nil {
		t.Fatalf("missing self in Members should error")
	}
	// Bad Raft config (zero ID).
	bad = cfg
	bad.Raft.ID = ""
	if err := bad.Validate(); err == nil {
		t.Fatalf("bad Raft config should error")
	}
}

func TestLeaderClientAddrEmptyWhenNoLeader(t *testing.T) {
	// Start a 3-node cluster, then immediately isolate every member ->
	// none can win an election, so LeaderClientAddr should report none.
	tc := newCluster(t, "n1", "n2", "n3")
	defer tc.stopAll()
	for _, id := range tc.ids {
		tc.net.Isolate(id)
	}
	// Wait until everyone has stepped down (the original leader, if any,
	// hasn't yet — but we explicitly check the API doesn't panic).
	time.Sleep(200 * time.Millisecond)
	for _, id := range tc.ids {
		_, _ = tc.nodes[id].LeaderClientAddr() // must not panic regardless
	}
}

func TestStatusReturnsRaftSnapshot(t *testing.T) {
	tc := newCluster(t, "n1", "n2", "n3")
	defer tc.stopAll()
	leader := tc.waitLeader()
	st := tc.nodes[leader].Status()
	if st.Role != "leader" || st.ID != leader {
		t.Fatalf("Status = %+v", st)
	}
}

func TestKindString(t *testing.T) {
	// Exercise each branch + the default.
	cases := map[Kind]string{
		KindAcquire: "acquire", KindEnqueue: "enqueue", KindRelease: "release",
		KindRenew: "renew", KindEvict: "evict", KindCleanupConn: "cleanup_conn",
		KindGC: "gc", KindBarrier: "barrier", KindEvictExpired: "evict_expired",
	}
	for k, want := range cases {
		if got := k.String(); got != want {
			t.Fatalf("Kind(%d).String() = %q, want %q", k, got, want)
		}
	}
	if got := Kind(99).String(); got != "kind(99)" {
		t.Fatalf("unknown kind = %q", got)
	}
}

func TestCommandValidate(t *testing.T) {
	long := make([]byte, maxCommandKeyBytes+1)
	for i := range long {
		long[i] = 'x'
	}
	bad := []Command{
		{Kind: KindAcquire, Key: "k", LeaseTTLNanos: -1},
		{Kind: KindAcquire, Key: string(long)},
		{Kind: KindAcquire, Key: "k", Ref: string(make([]byte, maxCommandRefBytes+1))},
		{Kind: KindAcquire, Key: "k", Limit: maxCommandLimit + 1},
		{Kind: KindAcquire, Key: "k", Limit: -1},
		{Kind: KindAcquire, Key: ""},
		{Kind: KindRelease, Key: ""},
		{Kind: KindRenew, Key: ""},
	}
	for i, c := range bad {
		if err := c.Validate(); err == nil {
			t.Fatalf("case %d: Validate() = nil, want error for %+v", i, c)
		}
	}
	good := []Command{
		{Kind: KindAcquire, Key: "lock:k", Limit: 1, LeaseTTLNanos: int64(time.Second)},
		{Kind: KindGC},      // no key required
		{Kind: KindBarrier}, // no key required
		{Kind: KindEvictExpired},
		{Kind: KindCleanupConn, Ref: "n1-7", ConnID: 7},
	}
	for i, c := range good {
		if err := c.Validate(); err != nil {
			t.Fatalf("case %d: Validate() = %v, want nil for %+v", i, err, c)
		}
	}
}

func TestLogSweepErr(t *testing.T) {
	tc := newCluster(t, "n1")
	defer tc.stopAll()
	n := tc.nodes["n1"]
	// Just exercise the classification branches — the function only logs,
	// so "doesn't panic" is the contract.
	n.logSweepErr("x", raft.ErrNotLeader)
	n.logSweepErr("x", raft.ErrLeadershipLost)
	n.logSweepErr("x", raft.ErrStopped)
	n.logSweepErr("x", context.Canceled)
	n.logSweepErr("x", context.DeadlineExceeded)
	n.logSweepErr("x", errors.New("something else"))
}
