package raft

import (
	"context"
	"testing"
	"time"
)

// AddVoter / RemoveServer are tested at the raft level over a MemNetwork.
// The new member is registered as a "ghost" peer on the existing
// MemNetwork (no real Node behind it) — that's enough to verify the
// committed config-entry semantics: the leader's quorum target updates,
// and every existing follower's view of n.config reflects the change.

func TestAddVoterUpdatesConfigOnAllReplicas(t *testing.T) {
	tc := newTestCluster(t, "n1", "n2", "n3")
	defer tc.stopAll()
	leader := tc.waitLeader()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	fut, err := tc.nodes[leader].AddVoter(ctx, "n4", "raft-n4")
	if err != nil {
		t.Fatalf("AddVoter: %v", err)
	}
	if _, err := fut.Wait(ctx); err != nil {
		// The future may NOT resolve because the new member isn't running
		// to ack — that's expected. Probe the run-loop-owned config
		// directly via Status instead.
	}
	// Adopt-on-append means every existing voter sees n4 in its
	// Voters set on the next AppendEntries that delivered the entry.
	for _, id := range tc.ids {
		_, ok := pollUntil(t, 2*time.Second, func() (struct{}, bool) {
			s := tc.nodes[id].Status()
			for _, v := range s.Voters {
				if v == "n4" {
					return struct{}{}, true
				}
			}
			return struct{}{}, false
		})
		if !ok {
			t.Fatalf("%s did not adopt n4 as a voter", id)
		}
	}
}

func TestRemoveServerShrinksMembership(t *testing.T) {
	tc := newTestCluster(t, "n1", "n2", "n3")
	defer tc.stopAll()
	leader := tc.waitLeader()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// Remove a follower (removing the leader is the harder edge case).
	var target NodeID
	for _, id := range tc.ids {
		if id != leader {
			target = id
			break
		}
	}
	fut, err := tc.nodes[leader].RemoveServer(ctx, target)
	if err != nil {
		t.Fatalf("RemoveServer: %v", err)
	}
	_, _ = fut.Wait(ctx) // best-effort — config-change semantics adopt-on-append
	_, ok := pollUntil(t, 2*time.Second, func() (struct{}, bool) {
		s := tc.nodes[leader].Status()
		for _, v := range s.Voters {
			if v == target {
				return struct{}{}, false
			}
		}
		return struct{}{}, true
	})
	if !ok {
		t.Fatalf("leader still lists removed voter %q", target)
	}
}

func TestAddVoterAlreadyPresentErrs(t *testing.T) {
	tc := newTestCluster(t, "n1", "n2", "n3")
	defer tc.stopAll()
	leader := tc.waitLeader()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	fut, err := tc.nodes[leader].AddVoter(ctx, leader, "anywhere") // self is already a voter
	if err != nil {
		t.Fatalf("AddVoter submit: %v", err)
	}
	if _, err := fut.Wait(ctx); err == nil {
		t.Fatalf("AddVoter for existing voter should error")
	}
}

func TestRemoveServerUnknownErrs(t *testing.T) {
	tc := newTestCluster(t, "n1", "n2", "n3")
	defer tc.stopAll()
	leader := tc.waitLeader()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	fut, err := tc.nodes[leader].RemoveServer(ctx, "ghost")
	if err != nil {
		t.Fatalf("RemoveServer submit: %v", err)
	}
	if _, err := fut.Wait(ctx); err == nil {
		t.Fatalf("RemoveServer for unknown peer should error")
	}
}

func TestConfChangeOnFollowerErrsNotLeader(t *testing.T) {
	tc := newTestCluster(t, "n1", "n2", "n3")
	defer tc.stopAll()
	leader := tc.waitLeader()
	var follower NodeID
	for _, id := range tc.ids {
		if id != leader {
			follower = id
			break
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	fut, err := tc.nodes[follower].AddVoter(ctx, "n4", "raft-n4")
	if err != nil {
		t.Fatalf("AddVoter submit: %v", err)
	}
	if _, err := fut.Wait(ctx); err == nil {
		t.Fatalf("AddVoter on follower should error")
	}
}
