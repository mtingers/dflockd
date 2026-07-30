package cluster

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/mtingers/dflockd/internal/lock"
	"github.com/mtingers/dflockd/internal/raft"
)

// TestDynamicJoinColdNodeCatchesUpViaSnapshot proves a node added with
// AddVoter and started with empty storage catches up via
// InstallSnapshot. PR-3 gap 2's primary integration test.
//
// Sequence:
//  1. Bring up a 3-node cluster on a MemNetwork with a low snapshot
//     threshold so a handful of commits triggers a snapshot.
//  2. Propose enough commits to force a snapshot on the leader.
//  3. AddVoter("n4") on the leader.
//  4. Build n4 with empty MemStorage; join the network; start it.
//  5. Wait for n4's commitIndex to catch up to the leader's.
func TestDynamicJoinColdNodeCatchesUpViaSnapshot(t *testing.T) {
	if testing.Short() {
		t.Skip("dynamic-join: spins up 4 nodes + drives sustained writes")
	}
	tc := newDynJoinCluster(t, "n1", "n2", "n3")
	defer tc.stopAll()
	leader := waitDynJoinLeader(t, tc, 2*time.Second)
	driveCommitsForSnapshot(t, tc, leader, 20)
	addDynJoiner(t, tc, leader, "n4")
	if !pollUntilCaughtUp(t, tc, "n4", leader, 5*time.Second) {
		t.Fatalf("n4 did not catch up: leader=%d n4=%d",
			tc.nodes[leader].raft.Status().CommitIndex,
			tc.nodes["n4"].raft.Status().CommitIndex)
	}
	// Assert the catch-up actually traversed InstallSnapshot. A
	// cold-state joiner whose log start > 0 must have received a
	// snapshot — plain AppendEntries from index 1 would leave the
	// joiner's log starting at 1, not at the leader's snapshot index.
	leaderSnap := tc.nodes[leader].raft.Status().LastSnapshotIndex
	joinerSnap := tc.nodes["n4"].raft.Status().LastSnapshotIndex
	if leaderSnap == 0 {
		t.Fatalf("leader never snapshotted (threshold=5, commits=20+); test setup broken")
	}
	if joinerSnap == 0 {
		t.Fatalf("joiner caught up via AppendEntries only — InstallSnapshot path NOT exercised")
	}
	for id, node := range tc.nodes {
		member, ok := node.member("n4")
		if !ok || member.ClientAddr != "client-n4:0" {
			t.Fatalf("%s metadata for n4 = %+v, ok=%v", id, member, ok)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	if err := tc.nodes[leader].RemoveServer(ctx, leader); err != nil {
		cancel()
		t.Fatalf("remove leader: %v", err)
	}
	cancel()
	if !pollUntil(t, time.Second, func() bool { return !tc.nodes[leader].IsLeader() }) {
		t.Fatal("self-removed node did not step down")
	}
	nextLeader := waitDynJoinLeader(t, tc, 3*time.Second)
	if nextLeader == leader {
		t.Fatal("self-removed node remained leader")
	}
	wantAddr := "client-" + string(nextLeader) + ":0"
	for id, node := range tc.nodes {
		if id == leader {
			continue
		}
		if !pollUntil(t, 2*time.Second, func() bool {
			addr, ok := node.LeaderClientAddr()
			return ok && addr == wantAddr
		}) {
			addr, ok := node.LeaderClientAddr()
			t.Fatalf("%s redirect after failover = (%q, %v), want %q", id, addr, ok, wantAddr)
		}
		if _, present := node.raft.Status().Configuration.ClientAddrs[leader]; present {
			t.Fatalf("%s retained removed member client metadata", id)
		}
	}
}

// newDynJoinCluster is the cluster_test.go newCluster with a low
// SnapshotThresholdEntries so the InstallSnapshot path actually fires
// inside a test's lifetime.
func newDynJoinCluster(t *testing.T, ids ...raft.NodeID) *testCluster {
	t.Helper()
	tc := &testCluster{
		t: t, net: raft.NewMemNetwork(), ids: ids,
		nodes: map[raft.NodeID]*Node{},
		lms:   map[raft.NodeID]*lock.LockManager{},
		trs:   map[raft.NodeID]*raft.MemTransport{},
	}
	members := map[raft.NodeID]Member{}
	for _, id := range ids {
		members[id] = Member{RaftAddr: "raft-" + string(id), ClientAddr: "client-" + string(id) + ":0"}
	}
	for _, id := range ids {
		startDynJoinNode(tc, id, members, 5)
	}
	return tc
}

func startDynJoinNode(tc *testCluster, id raft.NodeID, members map[raft.NodeID]Member, snapThreshold uint64) {
	tc.t.Helper()
	rcfg := fastRaftConfig(id)
	rcfg.SnapshotThresholdEntries = snapThreshold
	cfg := Config{Raft: rcfg, Members: members, AdvertiseAddr: members[id].ClientAddr}
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

func waitDynJoinLeader(t *testing.T, tc *testCluster, timeout time.Duration) raft.NodeID {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for id, n := range tc.nodes {
			if n.IsLeader() {
				return id
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("no leader within %s", timeout)
	return ""
}

func driveCommitsForSnapshot(t *testing.T, tc *testCluster, leader raft.NodeID, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, _ = tc.nodes[leader].Propose(ctx, Command{Kind: KindGC})
		cancel()
	}
}

func addDynJoiner(t *testing.T, tc *testCluster, leader raft.NodeID, newID raft.NodeID) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := tc.nodes[leader].AddVoter(ctx, newID, "raft-"+string(newID), "client-"+string(newID)+":0"); err != nil {
		t.Fatalf("AddVoter(%s): %v", newID, err)
	}
	members := map[raft.NodeID]Member{}
	for _, mid := range tc.ids {
		members[mid] = Member{RaftAddr: "raft-" + string(mid), ClientAddr: "client-" + string(mid) + ":0"}
	}
	members[newID] = Member{RaftAddr: "raft-" + string(newID), ClientAddr: "client-" + string(newID) + ":0"}
	tc.ids = append(tc.ids, newID)
	startDynJoinNode(tc, newID, members, 5)
}

func pollUntilCaughtUp(t *testing.T, tc *testCluster, joiner, leader raft.NodeID, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		lc := tc.nodes[leader].raft.Status().CommitIndex
		jc := tc.nodes[joiner].raft.Status().CommitIndex
		if jc >= lc && jc > 0 {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}
