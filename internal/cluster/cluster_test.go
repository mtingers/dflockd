package cluster

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mtingers/dflockd/internal/config"
	"github.com/mtingers/dflockd/internal/lock"
	"github.com/mtingers/dflockd/internal/raft"
)

type unboundMemTransport struct{ *raft.MemTransport }

func (*unboundMemTransport) PeerIdentityBound() bool { return false }

// --- harness ---

type testCluster struct {
	t     *testing.T
	net   *raft.MemNetwork
	ids   []raft.NodeID
	nodes map[raft.NodeID]*Node
	lms   map[raft.NodeID]*lock.LockManager
	trs   map[raft.NodeID]*raft.MemTransport
	sweep time.Duration // 0 → node default
}

func newCluster(t *testing.T, ids ...raft.NodeID) *testCluster {
	return newClusterWithSweep(t, 0, ids...)
}

func TestDynamicMembershipRequiresIdentityBoundTransport(t *testing.T) {
	id := raft.NodeID("n1")
	members := map[raft.NodeID]Member{
		id: {RaftAddr: "raft-n1", ClientAddr: "client-n1:0"},
	}
	cfg := Config{Raft: fastRaftConfig(id), Members: members}
	lm := newClusterLM(t)
	base := raft.NewMemNetwork().Transport(id)
	defer base.Close()
	node, err := NewNode(cfg, lm, raft.NewMemStorage(), &unboundMemTransport{base}, slog.Default())
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if err := node.AddVoter(context.Background(), "n2", "raft-n2", "client-n2:0"); !errors.Is(err, ErrMembershipIdentityRequired) {
		t.Fatalf("AddVoter error = %v", err)
	}
	if err := node.RemoveServer(context.Background(), id); !errors.Is(err, ErrMembershipIdentityRequired) {
		t.Fatalf("RemoveServer error = %v", err)
	}
	if got := node.raft.Status().LastLogIndex; got != 0 {
		t.Fatalf("membership rejection appended log index %d", got)
	}
}

func newClusterWithSweep(t *testing.T, sweep time.Duration, ids ...raft.NodeID) *testCluster {
	return newClusterWithClock(t, sweep, nil, ids...)
}

func newClusterWithClock(t *testing.T, sweep time.Duration, now func() time.Time, ids ...raft.NodeID) *testCluster {
	t.Helper()
	tc := &testCluster{
		t: t, net: raft.NewMemNetwork(), ids: ids, sweep: sweep,
		nodes: map[raft.NodeID]*Node{},
		lms:   map[raft.NodeID]*lock.LockManager{},
		trs:   map[raft.NodeID]*raft.MemTransport{},
	}
	members := map[raft.NodeID]Member{}
	for _, id := range ids {
		members[id] = Member{RaftAddr: "raft-" + string(id), ClientAddr: "client-" + string(id) + ":0"}
	}
	for _, id := range ids {
		tc.startNode(id, members, now)
	}
	return tc
}

func (tc *testCluster) startNode(id raft.NodeID, members map[raft.NodeID]Member, now func() time.Time) {
	tc.t.Helper()
	rcfg := fastRaftConfig(id)
	cfg := Config{
		Raft: rcfg, Members: members, AdvertiseAddr: members[id].ClientAddr,
		SweepInterval: tc.sweep, Now: now,
	}
	lm := newClusterLM(tc.t)
	tr := tc.net.Transport(id)
	st := raft.NewMemStorage()
	n, err := NewNode(cfg, lm, st, tr, slog.Default())
	if err != nil {
		tc.t.Fatalf("NewNode(%s): %v", id, err)
	}
	n.Start()
	tc.nodes[id], tc.lms[id], tc.trs[id] = n, lm, tr
}

func (tc *testCluster) stopAll() {
	for _, n := range tc.nodes {
		_ = n.Close()
	}
	for _, tr := range tc.trs {
		_ = tr.Close()
	}
}

func fastRaftConfig(id raft.NodeID) raft.Config {
	c := raft.DefaultConfig()
	c.ID = id
	c.HeartbeatInterval = 5 * time.Millisecond
	c.ElectionTimeoutMin = 30 * time.Millisecond
	c.ElectionTimeoutMax = 60 * time.Millisecond
	return c
}

func newClusterLM(t *testing.T) *lock.LockManager {
	t.Helper()
	cfg := &config.Config{
		MaxLocks:        128,
		DefaultLeaseTTL: 30 * time.Second,
		GCMaxIdleTime:   60 * time.Second,
	}
	lm, err := lock.NewLockManager(cfg, slog.Default())
	if err != nil {
		t.Fatalf("NewLockManager: %v", err)
	}
	t.Cleanup(func() { _ = lm.Close() })
	return lm
}

func pollUntil(t *testing.T, timeout time.Duration, f func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if f() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return false
}

func (tc *testCluster) waitLeader() raft.NodeID {
	tc.t.Helper()
	var leader raft.NodeID
	ok := pollUntil(tc.t, 3*time.Second, func() bool {
		var found raft.NodeID
		count := 0
		for _, id := range tc.ids {
			if tc.nodes[id].IsLeader() {
				found, count = id, count+1
			}
		}
		if count != 1 {
			return false
		}
		for _, id := range tc.ids {
			if tc.nodes[id].LeaderID() != found {
				return false
			}
		}
		leader = found
		return true
	})
	if !ok {
		tc.t.Fatalf("no stable leader")
	}
	return leader
}

// holderTokens returns the lock infos whose stripped key matches `key`.
// (The raw stats list carries the internal "lock:"/"sem:" prefix.)
func holderTokens(lm *lock.LockManager, key string) []lock.LockInfo {
	stats := lm.Stats(0)
	var out []lock.LockInfo
	for _, li := range stats.Locks {
		if lock.StripKeyPrefix(li.Key) == key {
			out = append(out, li)
		}
	}
	return out
}

func saltOf(b byte) [8]byte {
	var s [8]byte
	for i := range s {
		s[i] = b
	}
	return s
}

// --- tests ---

func TestSingleNodeClusterProposeApplies(t *testing.T) {
	tc := newCluster(t, "n1")
	defer tc.stopAll()
	tc.waitLeader()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	res, err := tc.nodes["n1"].ProposeAcquire(ctx, "lock:k", 1, "A", 1, 30*time.Second, saltOf(1))
	if err != nil {
		t.Fatalf("ProposeAcquire: %v", err)
	}
	if res.Status != lock.StatusOK || res.Token == "" {
		t.Fatalf("acquire result = %+v", res)
	}
	if h := holderTokens(tc.lms["n1"], "k"); len(h) != 1 {
		t.Fatalf("expected 1 holder, got %d", len(h))
	}
}

func TestThreeNodeProposeReplicatesToAllFSMs(t *testing.T) {
	tc := newCluster(t, "n1", "n2", "n3")
	defer tc.stopAll()
	leader := tc.waitLeader()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	res, err := tc.nodes[leader].ProposeAcquire(ctx, "lock:k", 1, "A", 1, 30*time.Second, saltOf(7))
	if err != nil {
		t.Fatalf("ProposeAcquire: %v", err)
	}
	if res.Status != lock.StatusOK {
		t.Fatalf("status = %d", res.Status)
	}
	// All three FSMs must converge to hold the lock at the same token.
	want := res.Token
	for _, id := range tc.ids {
		if !pollUntil(t, 2*time.Second, func() bool {
			h := holderTokens(tc.lms[id], "k")
			return len(h) == 1
		}) {
			t.Fatalf("%s FSM didn't see the holder", id)
		}
	}
	// Token consistency: check by ApplyRenew via a Propose on the leader.
	rn, err := tc.nodes[leader].ProposeRenew(ctx, "lock:k", want, 60*time.Second)
	if err != nil {
		t.Fatalf("ProposeRenew: %v", err)
	}
	if rn.Status != lock.StatusOK {
		t.Fatalf("renew status = %d (token %q)", rn.Status, want)
	}
}

func TestProposeReleasePromotesAndReplicates(t *testing.T) {
	tc := newCluster(t, "n1", "n2", "n3")
	defer tc.stopAll()
	leader := tc.waitLeader()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	rA, err := tc.nodes[leader].ProposeAcquire(ctx, "lock:k", 1, "A", 1, 30*time.Second, saltOf(1))
	if err != nil {
		t.Fatalf("acquire A: %v", err)
	}
	rB, err := tc.nodes[leader].ProposeAcquire(ctx, "lock:k", 1, "B", 2, 30*time.Second, saltOf(2))
	if err != nil {
		t.Fatalf("acquire B: %v", err)
	}
	if rB.Status != lock.StatusQueued {
		t.Fatalf("acquire B status = %d, want queued", rB.Status)
	}
	// Release A; B's promotion should be visible on every replica.
	_, err = tc.nodes[leader].ProposeRelease(ctx, "lock:k", rA.Token)
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	for _, id := range tc.ids {
		if !pollUntil(t, 2*time.Second, func() bool {
			return len(holderTokens(tc.lms[id], "k")) == 1
		}) {
			t.Fatalf("%s FSM holder count not 1 after release+promote", id)
		}
	}
}

func TestProposeOnFollowerErrsNotLeader(t *testing.T) {
	tc := newCluster(t, "n1", "n2", "n3")
	defer tc.stopAll()
	leader := tc.waitLeader()
	var follower raft.NodeID
	for _, id := range tc.ids {
		if id != leader {
			follower = id
			break
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_, err := tc.nodes[follower].ProposeAcquire(ctx, "lock:k", 1, "A", 1, 30*time.Second, saltOf(1))
	if err == nil {
		t.Fatalf("Propose on follower should err")
	}
}

func TestCommandEncodeDecodeRoundTrip(t *testing.T) {
	salt := saltOf(0x42)
	c := Command{
		Kind: KindAcquire, NowNanos: 1234, Key: "lock:k", Limit: 1, Ref: "R",
		ConnID: 7, LeaseTTLNanos: 1_000_000_000, SaltB64: EncodeSalt(salt),
	}
	data, err := c.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got != c {
		t.Fatalf("round-trip diverged:\n got  %+v\n want %+v", got, c)
	}
}

func TestCommandRejectsUnknownKind(t *testing.T) {
	if _, err := (Command{}).Encode(); err == nil {
		t.Fatalf("encoding KindUnknown should fail")
	}
	bogus := []byte(`{"k":0,"t":0}`)
	if _, err := Decode(bogus); err == nil {
		t.Fatalf("decoding KindUnknown should fail")
	}
}

func TestSaltCodecRoundTrip(t *testing.T) {
	s := saltOf(0xAB)
	got, err := DecodeSalt(EncodeSalt(s))
	if err != nil || got != s {
		t.Fatalf("salt round-trip: got %x %v want %x", got, err, s)
	}
	if _, err := DecodeSalt("bogus"); err == nil {
		t.Fatalf("DecodeSalt should fail on garbage")
	}
}

func TestLeaderFailoverContinuesPropose(t *testing.T) {
	tc := newCluster(t, "n1", "n2", "n3")
	defer tc.stopAll()
	leader := tc.waitLeader()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// Acquire on the old leader so there's some state to inherit.
	_, err := tc.nodes[leader].ProposeAcquire(ctx, "lock:pre", 1, "A", 1, 30*time.Second, saltOf(1))
	if err != nil {
		t.Fatalf("pre-failover acquire: %v", err)
	}
	tc.net.Crash(leader, true)
	// A new leader must emerge among the remaining two.
	var newLeader raft.NodeID
	ok := pollUntil(t, 3*time.Second, func() bool {
		for _, id := range tc.ids {
			if id == leader {
				continue
			}
			if tc.nodes[id].IsLeader() {
				newLeader = id
				return true
			}
		}
		return false
	})
	if !ok {
		t.Fatalf("no new leader after crash")
	}
	// Proposing on the new leader must continue to work.
	if _, err := tc.nodes[newLeader].ProposeAcquire(ctx, "lock:post", 1, "B", 2, 30*time.Second, saltOf(2)); err != nil {
		t.Fatalf("post-failover propose: %v", err)
	}
	// Both replicas of new-leader's surviving partition should agree.
	for _, id := range tc.ids {
		if id == leader {
			continue
		}
		if !pollUntil(t, 2*time.Second, func() bool {
			return len(holderTokens(tc.lms[id], "post")) == 1
		}) {
			t.Fatalf("post-failover replica %s missing the new holder", id)
		}
	}
}

func TestBarrierAppliesAsNoOp(t *testing.T) {
	tc := newCluster(t, "n1", "n2", "n3")
	defer tc.stopAll()
	leader := tc.waitLeader()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := tc.nodes[leader].Barrier(ctx); err != nil {
		t.Fatalf("Barrier: %v", err)
	}
}

// The leader-driven sweep loop must reclaim a holder whose lease has
// expired, with no further client activity on that key.
func TestSweepLoopEvictsExpiredHolder(t *testing.T) {
	tc := newClusterWithSweep(t, 10*time.Millisecond, "n1", "n2", "n3")
	defer tc.stopAll()
	leader := tc.waitLeader()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// A 1ms lease: expired almost immediately; only the sweep loop can
	// reclaim it (nothing else touches "lock:ephemeral").
	if r, err := tc.nodes[leader].ProposeAcquire(ctx, "lock:ephemeral", 1, "A", 1, time.Millisecond, saltOf(1)); err != nil || r.Status != lock.StatusOK {
		t.Fatalf("ProposeAcquire = %+v, %v", r, err)
	}
	if got := holderTokens(tc.lms[leader], "ephemeral"); len(got) != 1 {
		t.Fatalf("right after acquire: %d holders, want 1", len(got))
	}
	// Within a few sweep ticks the leader proposes KindEvictExpired and
	// the holder vanishes on every replica.
	for _, id := range tc.ids {
		id := id
		if !pollUntil(t, time.Second, func() bool { return len(holderTokens(tc.lms[id], "ephemeral")) == 0 }) {
			t.Fatalf("node %s: expired holder not reclaimed by the sweep loop", id)
		}
	}
}

func TestInjectedClockDrivesProposalsAndSweeps(t *testing.T) {
	var nowNanos atomic.Int64
	start := time.Now()
	nowNanos.Store(start.UnixNano())
	now := func() time.Time { return time.Unix(0, nowNanos.Load()) }
	tc := newClusterWithClock(t, 10*time.Millisecond, now, "n1")
	defer tc.stopAll()
	leader := tc.waitLeader()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if r, err := tc.nodes[leader].ProposeAcquire(
		ctx, "lock:clock", 1, "A", 1, time.Hour, saltOf(1),
	); err != nil || r.Status != lock.StatusOK {
		t.Fatalf("ProposeAcquire = %+v, %v", r, err)
	}
	if got := holderTokens(tc.lms[leader], "clock"); len(got) != 1 {
		t.Fatalf("holders before jump = %d, want 1", len(got))
	}
	nowNanos.Store(start.Add(2 * time.Hour).UnixNano())
	if !pollUntil(t, time.Second, func() bool {
		return len(holderTokens(tc.lms[leader], "clock")) == 0
	}) {
		t.Fatal("injected clock jump did not trigger lease eviction")
	}
}

func TestIdleSweepSkipsRaftProposals(t *testing.T) {
	tc := newCluster(t, "n1")
	defer tc.stopAll()
	leader := tc.waitLeader()
	n := tc.nodes[leader]
	before := n.MetricsSnapshot().Raft.Proposals
	for i := 0; i < 5; i++ {
		n.runOneSweep(time.Second, true)
	}
	after := n.MetricsSnapshot().Raft.Proposals
	if after != before {
		t.Fatalf("idle sweeps added %d proposals, want 0", after-before)
	}
}

func TestSweepEvictsExpiredHolderAfterLeaderFailover(t *testing.T) {
	tc := newClusterWithSweep(t, 10*time.Millisecond, "n1", "n2", "n3")
	defer tc.stopAll()
	oldLeader := tc.waitLeader()
	defer tc.net.Crash(oldLeader, false)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if r, err := tc.nodes[oldLeader].ProposeAcquire(
		ctx, "lock:failover-expiry", 1, "A", 1, 300*time.Millisecond, saltOf(1),
	); err != nil || r.Status != lock.StatusOK {
		t.Fatalf("ProposeAcquire = %+v, %v", r, err)
	}
	for _, id := range tc.ids {
		if !pollUntil(t, time.Second, func() bool {
			return len(holderTokens(tc.lms[id], "failover-expiry")) == 1
		}) {
			t.Fatalf("node %s did not apply holder before failover", id)
		}
	}
	tc.net.Crash(oldLeader, true)

	var newLeader raft.NodeID
	if !pollUntil(t, 2*time.Second, func() bool {
		for _, id := range tc.ids {
			if id != oldLeader && tc.nodes[id].IsLeader() {
				newLeader = id
				return true
			}
		}
		return false
	}) {
		t.Fatal("no replacement leader")
	}
	for _, id := range tc.ids {
		if id == oldLeader {
			continue
		}
		if !pollUntil(t, time.Second, func() bool {
			return len(holderTokens(tc.lms[id], "failover-expiry")) == 0
		}) {
			t.Fatalf("node %s: new leader %s did not sweep expired holder", id, newLeader)
		}
	}
}

func TestLeaderClientAddrFromMembers(t *testing.T) {
	tc := newCluster(t, "n1", "n2", "n3")
	defer tc.stopAll()
	leader := tc.waitLeader()
	for _, id := range tc.ids {
		addr, ok := tc.nodes[id].LeaderClientAddr()
		if !ok {
			t.Fatalf("%s reports no leader address", id)
		}
		want := "client-" + string(leader) + ":0"
		if addr != want {
			t.Fatalf("%s leader addr = %q, want %q", id, addr, want)
		}
	}
}

func TestFailedAddVoterDoesNotPublishMember(t *testing.T) {
	tc := newCluster(t, "n1", "n2", "n3")
	defer tc.stopAll()
	leader := tc.waitLeader()
	original, ok := tc.nodes[leader].member(leader)
	if !ok {
		t.Fatalf("leader %s missing from member map", leader)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := tc.nodes[leader].AddVoter(ctx, leader, "bogus-raft", "bogus-client")
	if err == nil {
		t.Fatal("adding the existing leader unexpectedly succeeded")
	}
	if got, ok := tc.nodes[leader].member(leader); !ok || got != original {
		t.Fatalf("failed add changed leader member: got %+v, ok=%v; want %+v", got, ok, original)
	}

	var follower raft.NodeID
	for _, id := range tc.ids {
		if id != leader {
			follower = id
			break
		}
	}
	err = tc.nodes[follower].AddVoter(ctx, "ghost", "ghost-raft", "ghost-client")
	if err == nil {
		t.Fatal("follower AddVoter unexpectedly succeeded")
	}
	if ghost, ok := tc.nodes[follower].member("ghost"); ok {
		t.Fatalf("failed follower add published ghost member: %+v", ghost)
	}
}
