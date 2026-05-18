package raft

import (
	"context"
	"testing"
	"time"
)

// TestCounters_WiredOnPropose verifies that a successful Propose
// increments Counters.Proposals on the leader. RED until Node.Propose
// (or the apply loop) calls counters.IncProposals.
func TestCounters_WiredOnPropose(t *testing.T) {
	tc := newTestCluster(t, "a", "b", "c")
	defer tc.stopAll()
	leader := tc.waitLeader()
	n := tc.nodes[leader]

	before := n.Counters().Snapshot()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	fut, err := n.Propose(ctx, []byte("hello"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if _, err := fut.Wait(ctx); err != nil {
		t.Fatalf("Wait: %v", err)
	}

	after := n.Counters().Snapshot()
	if after.Proposals != before.Proposals+1 {
		t.Errorf("Counters.Proposals: want %d, got %d", before.Proposals+1, after.Proposals)
	}
	if after.Applies < before.Applies+1 {
		t.Errorf("Counters.Applies: want ≥ %d, got %d", before.Applies+1, after.Applies)
	}
}

// TestCounters_WiredOnLeaderChange verifies that becoming leader bumps
// LeaderChanges on the new leader. RED until Node's "I became leader"
// path calls counters.IncLeaderChange.
func TestCounters_WiredOnLeaderChange(t *testing.T) {
	tc := newTestCluster(t, "a", "b", "c")
	defer tc.stopAll()
	leader := tc.waitLeader()

	got := tc.nodes[leader].Counters().Snapshot().LeaderChanges
	if got < 1 {
		t.Errorf("LeaderChanges on the elected leader: want ≥ 1, got %d", got)
	}
}
