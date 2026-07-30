package raft

import (
	"testing"
	"time"
)

func clientAddrConfig(ids []NodeID) Configuration {
	cfg := configFor(ids)
	cfg.ClientAddrs = make(map[NodeID]string, len(ids))
	for _, id := range ids {
		cfg.ClientAddrs[id] = string(id) + ":client"
	}
	return cfg
}

// Followers call LeaderClientAddr on every redirected request. Resolving it
// through Status() would be a run-loop round trip with no timeout, so a loop
// busy with (say) an inline snapshot read would stall every redirect behind
// it. The published leadership state must answer without the loop.
func TestLeaderClientAddrDoesNotWaitOnRunLoop(t *testing.T) {
	ids := []NodeID{"a"}
	cfg := fastConfigID("a")
	n, err := NewNode(cfg, NewNoopFSM(), NewMemStorage(), NewMemNetwork().Transport("a"), clientAddrConfig(ids), nil)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if err := n.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer n.Close()

	if _, ok := pollUntil(t, 3*time.Second, func() (bool, bool) {
		addr, ok := n.LeaderClientAddr()
		return true, ok && addr == "a:client"
	}); !ok {
		t.Fatal("leader never published its client address")
	}

	// Occupy the run loop, exactly as a long inline operation would.
	release := make(chan struct{})
	occupied := make(chan struct{})
	n.controlc <- func() {
		close(occupied)
		<-release
	}
	<-occupied
	defer close(release)

	done := make(chan string, 1)
	go func() {
		addr, _ := n.LeaderClientAddr()
		done <- addr
	}()
	select {
	case addr := <-done:
		if addr != "a:client" {
			t.Fatalf("LeaderClientAddr = %q, want %q", addr, "a:client")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("LeaderClientAddr blocked while the run loop was busy: the redirect " +
			"path must read published leadership state, not round-trip the run loop")
	}
}

// The published address has to track configuration changes, since that is the
// whole reason it can be read without the run loop.
func TestLeaderClientAddrFollowsConfigurationChanges(t *testing.T) {
	ids := []NodeID{"a", "b"}
	n := mustNewNode(t, fastConfigID("a"), NewMemStorage(), NewMemNetwork().Transport("a"), clientAddrConfig(ids))
	defer n.Close()

	// Follower that believes "b" leads reports b's replicated client address.
	n.becomeFollower(2, "b")
	if addr, ok := n.LeaderClientAddr(); !ok || addr != "b:client" {
		t.Fatalf("LeaderClientAddr = %q, %v; want b:client", addr, ok)
	}

	// A configuration that re-homes b must be reflected immediately.
	moved := clientAddrConfig(ids)
	moved.ClientAddrs["b"] = "b-moved:client"
	n.adoptConfig(moved, 7)
	if addr, ok := n.LeaderClientAddr(); !ok || addr != "b-moved:client" {
		t.Fatalf("after adoptConfig LeaderClientAddr = %q, %v; want b-moved:client", addr, ok)
	}

	// No known leader means no redirect target.
	n.becomeFollower(3, "")
	if addr, ok := n.LeaderClientAddr(); ok {
		t.Fatalf("LeaderClientAddr = %q with no leader, want ok=false", addr)
	}
}

// A configuration carrying no client metadata must report ok=false rather than
// an empty address, so the cluster layer falls back to its static startup map.
func TestLeaderClientAddrAbsentWithoutReplicatedMetadata(t *testing.T) {
	n := mustNewNode(t, fastConfigID("a"), NewMemStorage(), NewMemNetwork().Transport("a"),
		configFor([]NodeID{"a", "b"}))
	defer n.Close()

	n.becomeFollower(2, "b")
	if addr, ok := n.LeaderClientAddr(); ok {
		t.Fatalf("LeaderClientAddr = %q, want ok=false without replicated metadata", addr)
	}
}
