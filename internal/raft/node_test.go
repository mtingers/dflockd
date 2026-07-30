package raft

import (
	"testing"
	"time"
)

// --- test cluster harness (in-process, over a MemNetwork) ---

type testCluster struct {
	t     *testing.T
	net   *MemNetwork
	ids   []NodeID
	nodes map[NodeID]*Node
	store map[NodeID]*MemStorage
	tr    map[NodeID]*MemTransport
	fsm   map[NodeID]*recordingFSM
	cfg   Config // template; ID is filled per node
}

func newTestCluster(t *testing.T, ids ...NodeID) *testCluster {
	t.Helper()
	tc := &testCluster{t: t, net: NewMemNetwork(), ids: ids, nodes: map[NodeID]*Node{}, store: map[NodeID]*MemStorage{}, tr: map[NodeID]*MemTransport{}, fsm: map[NodeID]*recordingFSM{}, cfg: fastConfig()}
	conf := configFor(ids)
	for _, id := range ids {
		tc.startNode(id, conf)
	}
	return tc
}

func fastConfig() Config {
	c := DefaultConfig()
	c.HeartbeatInterval = 5 * time.Millisecond
	c.ElectionTimeoutMin = 30 * time.Millisecond
	c.ElectionTimeoutMax = 60 * time.Millisecond
	return c
}

func configFor(ids []NodeID) Configuration {
	v := map[NodeID]string{}
	for _, id := range ids {
		v[id] = string(id)
	}
	return Configuration{Voters: v}
}

func (tc *testCluster) startNode(id NodeID, conf Configuration) {
	tc.t.Helper()
	cfg := tc.cfg
	cfg.ID = id
	st := NewMemStorage()
	fsm := tc.makeFSM(id)
	tr := tc.net.Transport(id)
	n, err := NewNode(cfg, fsm, st, tr, conf, nil)
	if err != nil {
		tc.t.Fatalf("NewNode(%s): %v", id, err)
	}
	n.Start()
	tc.nodes[id], tc.store[id], tc.tr[id], tc.fsm[id] = n, st, tr, fsm
}

// makeFSM returns this node's FSM. The default is a recordingFSM (each
// Apply records the entry data) so tests can inspect what each follower
// saw. Tests that don't care can ignore tc.fsm[id].
func (tc *testCluster) makeFSM(id NodeID) *recordingFSM {
	return newRecordingFSM()
}

func (tc *testCluster) stopAll() {
	for _, n := range tc.nodes {
		_ = n.Close()
	}
	for _, tr := range tc.tr {
		_ = tr.Close()
	}
}

// restart closes a node and recreates it on the same (state-retaining)
// MemStorage — models a crash + reboot. A fresh recordingFSM is given;
// its state is reconstructed via FSM.Restore or replayed entries.
func (tc *testCluster) restart(id NodeID) {
	tc.t.Helper()
	_ = tc.nodes[id].Close()
	_ = tc.tr[id].Close()
	cfg := tc.cfg
	cfg.ID = id
	fsm := newRecordingFSM()
	tr := tc.net.Transport(id)
	n, err := NewNode(cfg, fsm, tc.store[id], tr, configFor(tc.ids), nil)
	if err != nil {
		tc.t.Fatalf("restart NewNode(%s): %v", id, err)
	}
	n.Start()
	tc.nodes[id], tc.tr[id], tc.fsm[id] = n, tr, fsm
}

// waitLeader polls until exactly one node reports itself leader and every
// reachable node agrees on that leader; returns its id. Fails on timeout.
func (tc *testCluster) waitLeader(reachable ...NodeID) NodeID {
	tc.t.Helper()
	if len(reachable) == 0 {
		reachable = tc.ids
	}
	id, ok := pollUntil(tc.t, 3*time.Second, func() (NodeID, bool) {
		return tc.findStableLeader(reachable)
	})
	if !ok {
		tc.t.Fatalf("no stable leader among %v within timeout", reachable)
	}
	return id
}

func (tc *testCluster) findStableLeader(reachable []NodeID) (NodeID, bool) {
	var leader NodeID
	leaders := 0
	for _, id := range reachable {
		if tc.nodes[id].Status().Role == "leader" {
			leader, leaders = id, leaders+1
		}
	}
	if leaders != 1 {
		return "", false
	}
	for _, id := range reachable {
		if tc.nodes[id].Status().LeaderID != leader {
			return "", false
		}
	}
	return leader, true
}

func pollUntil[T any](t *testing.T, timeout time.Duration, f func() (T, bool)) (T, bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if v, ok := f(); ok {
			return v, true
		}
		time.Sleep(2 * time.Millisecond)
	}
	var zero T
	return zero, false
}

func (tc *testCluster) term(id NodeID) Term { return tc.nodes[id].Status().Term }

// --- tests ---

func TestSingleNodeElectsItself(t *testing.T) {
	tc := newTestCluster(t, "n1")
	defer tc.stopAll()
	if got := tc.waitLeader(); got != "n1" {
		t.Fatalf("leader = %s, want n1", got)
	}
}

func TestThreeNodeElectsOneLeader(t *testing.T) {
	tc := newTestCluster(t, "n1", "n2", "n3")
	defer tc.stopAll()
	leader := tc.waitLeader()
	// All three should now be at the same term.
	lt := tc.term(leader)
	for _, id := range tc.ids {
		if got := tc.term(id); got != lt {
			t.Fatalf("term(%s) = %d, want %d (== leader's)", id, got, lt)
		}
	}
}

func TestFiveNodeElectsOneLeader(t *testing.T) {
	tc := newTestCluster(t, "n1", "n2", "n3", "n4", "n5")
	defer tc.stopAll()
	tc.waitLeader()
}

func TestLeaderFailoverReElects(t *testing.T) {
	tc := newTestCluster(t, "n1", "n2", "n3")
	defer tc.stopAll()
	leader1 := tc.waitLeader()

	tc.net.Crash(leader1, true) // simulate the leader going away
	rest := otherIDs(tc.ids, leader1)
	leader2, ok := pollUntil(t, 3*time.Second, func() (NodeID, bool) {
		return tc.findStableLeader(rest)
	})
	if !ok {
		t.Fatalf("no new leader among %v after leader %s crashed", rest, leader1)
	}
	if leader2 == leader1 {
		t.Fatalf("new leader should not be the crashed one")
	}
	if tc.term(leader2) <= tc.term(leader1) {
		t.Fatalf("new leader's term %d should exceed old leader's %d", tc.term(leader2), tc.term(leader1))
	}

	// Old leader returns -> it must step down to follower of the new term.
	tc.net.Crash(leader1, false)
	if _, ok := pollUntil(t, 3*time.Second, func() (struct{}, bool) {
		s := tc.nodes[leader1].Status()
		return struct{}{}, s.Role == "follower" && s.LeaderID == leader2
	}); !ok {
		t.Fatalf("rejoined node %s did not become follower of %s", leader1, leader2)
	}
}

func TestPreVoteDoesNotInflateTermWhilePartitioned(t *testing.T) {
	tc := newTestCluster(t, "n1", "n2", "n3")
	defer tc.stopAll()
	tc.waitLeader()

	// Isolate n3: with PreVote it should stay a (pre-)candidate without
	// ever incrementing its term, because it can't win a pre-vote.
	tc.net.Isolate("n3")
	startTerm := tc.term("n3")
	time.Sleep(300 * time.Millisecond) // many election timeouts
	if got := tc.term("n3"); got != startTerm {
		t.Fatalf("isolated n3 term grew from %d to %d (PreVote should prevent this)", startTerm, got)
	}

	// Reconnect: n3 must rejoin as a follower without disrupting the leader.
	tc.net.Heal()
	if _, ok := pollUntil(t, 3*time.Second, func() (struct{}, bool) {
		return struct{}{}, tc.nodes["n3"].Status().Role == "follower"
	}); !ok {
		t.Fatalf("n3 did not rejoin as follower")
	}
}

func TestElectionRestrictionRejectsStaleLog(t *testing.T) {
	// Build a node by hand with a short log + a higher term, and another
	// "voter" with a longer log; the short-log node must not be granted a
	// vote.
	net := NewMemNetwork()
	conf := configFor([]NodeID{"a", "b"})

	storA := NewMemStorage()
	mustAppend(t, storA, []Entry{mkEntry(1, 1, "x")}) // a has only 1 entry
	a := mustNewNode(t, fastConfigID("a"), storA, net.Transport("a"), conf)
	a.Start()
	defer a.Close()

	storB := NewMemStorage()
	mustAppend(t, storB, []Entry{mkEntry(1, 1, "x"), mkEntry(2, 1, "y"), mkEntry(3, 1, "z")}) // b is longer
	b := mustNewNode(t, fastConfigID("b"), storB, net.Transport("b"), conf)
	b.Start()
	defer b.Close()

	// b has a longer log so it should win; a (shorter log) should never
	// be the leader.
	leader := waitLeaderOf(t, net, map[NodeID]*Node{"a": a, "b": b}, []NodeID{"a", "b"})
	if leader != "b" {
		t.Fatalf("leader = %s, want b (a has a stale log and must not win)", leader)
	}
}

func TestRecoversTermAndVoteAcrossRestart(t *testing.T) {
	tc := newTestCluster(t, "n1", "n2", "n3")
	defer tc.stopAll()
	leader := tc.waitLeader()
	termBefore := tc.term(leader)

	tc.restart(leader)
	// After restart it must not be at a lower term than before (it
	// persisted currentTerm), and a stable leader should re-emerge.
	if tb, ok := pollUntil(t, 3*time.Second, func() (Term, bool) {
		s := tc.nodes[leader].Status()
		return s.Term, s.Term >= termBefore
	}); !ok {
		t.Fatalf("restarted node's term %d regressed below %d", tb, termBefore)
	}
	tc.waitLeader()
}

func TestRPCWithLowerTermIsRejected(t *testing.T) {
	tc := newTestCluster(t, "n1", "n2", "n3")
	defer tc.stopAll()
	leader := tc.waitLeader()
	curTerm := tc.term(leader)
	sender := otherIDs(tc.ids, leader)[0]

	// A RequestVote stamped with a strictly lower term must be rejected
	// and the reply must carry the leader's real term (so the stale
	// sender learns it is behind). The leader itself must not step down.
	resp := tc.nodes[leader].handleRPC(sender, &RequestVoteReq{Term: 0, CandidateID: sender, LastLogIndex: 0, LastLogTerm: 0})
	rv, ok := resp.(*RequestVoteResp)
	if !ok || rv.VoteGranted || rv.Term != curTerm {
		t.Fatalf("stale RequestVote handled wrong: %+v (leader term %d)", resp, curTerm)
	}
	if tc.nodes[leader].Status().Role != "leader" {
		t.Fatalf("leader should not have stepped down for a stale RPC")
	}
}

func TestRPCWithHigherTermStepsLeaderDown(t *testing.T) {
	tc := newTestCluster(t, "n1", "n2", "n3")
	defer tc.stopAll()
	leader := tc.waitLeader()
	curTerm := tc.term(leader)
	sender := otherIDs(tc.ids, leader)[0]

	// An authenticated current voter at a strictly higher term must step the
	// leader down to follower and advance the term.
	tc.nodes[leader].handleRPC(sender, &AppendEntriesReq{Term: curTerm + 5, LeaderID: sender})
	if _, ok := pollUntil(t, 1*time.Second, func() (struct{}, bool) {
		s := tc.nodes[leader].Status()
		return struct{}{}, s.Role != "leader" && s.Term >= curTerm+5
	}); !ok {
		t.Fatalf("leader did not step down for higher-term RPC: %+v", tc.nodes[leader].Status())
	}
}

// --- small helpers used by the hand-built tests ---

func otherIDs(all []NodeID, excl NodeID) []NodeID {
	var out []NodeID
	for _, id := range all {
		if id != excl {
			out = append(out, id)
		}
	}
	return out
}

func fastConfigID(id NodeID) Config { c := fastConfig(); c.ID = id; return c }

func mustNewNode(t *testing.T, cfg Config, st Storage, tr Transport, conf Configuration) *Node {
	t.Helper()
	n, err := NewNode(cfg, NewNoopFSM(), st, tr, conf, nil)
	if err != nil {
		t.Fatalf("NewNode(%s): %v", cfg.ID, err)
	}
	return n
}

func mustAppend(t *testing.T, s Storage, es []Entry) {
	t.Helper()
	if err := s.Append(es); err != nil {
		t.Fatalf("Append: %v", err)
	}
}

func waitLeaderOf(t *testing.T, _ *MemNetwork, nodes map[NodeID]*Node, ids []NodeID) NodeID {
	t.Helper()
	id, ok := pollUntil(t, 3*time.Second, func() (NodeID, bool) {
		var leader NodeID
		count := 0
		for _, id := range ids {
			if nodes[id].Status().Role == "leader" {
				leader, count = id, count+1
			}
		}
		if count != 1 {
			return "", false
		}
		for _, id := range ids {
			if nodes[id].Status().LeaderID != leader {
				return "", false
			}
		}
		return leader, true
	})
	if !ok {
		t.Fatalf("no stable leader among %v", ids)
	}
	return id
}
