package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mtingers/dflockd/internal/raft"
)

// TestClusterBarrier_LeaderOK exercises the happy path: leader, propose
// succeeds.
func TestClusterBarrier_LeaderOK(t *testing.T) {
	fc := &fakeCluster{leader: true}
	srv, _ := newTestServer(t)
	srv.SetCluster(fc)
	if err := srv.ClusterBarrier(ctx2s(t)); err != nil {
		t.Fatalf("ClusterBarrier on leader: %v", err)
	}
}

// TestClusterBarrier_FollowerReturnsNotLeader verifies the follower
// gate fires before the underlying Barrier call.
func TestClusterBarrier_FollowerReturnsNotLeader(t *testing.T) {
	fc := &fakeCluster{leader: false}
	srv, _ := newTestServer(t)
	srv.SetCluster(fc)
	err := srv.ClusterBarrier(ctx2s(t))
	if !errors.Is(err, ErrNotClusterLeader) {
		t.Fatalf("err = %v, want ErrNotClusterLeader", err)
	}
}

// TestClusterBarrier_SingleNodeReturnsError verifies that calling
// ClusterBarrier on a single-node server (no cluster set) returns the
// "not in cluster mode" error, not nil.
func TestClusterBarrier_SingleNodeReturnsError(t *testing.T) {
	srv, _ := newTestServer(t)
	if err := srv.ClusterBarrier(ctx2s(t)); err == nil {
		t.Fatalf("ClusterBarrier on single-node: want error, got nil")
	}
}

// TestClusterAddVoter_LeaderOK verifies the happy path delegates through.
func TestClusterAddVoter_LeaderOK(t *testing.T) {
	fc := &fakeCluster{leader: true}
	srv, _ := newTestServer(t)
	srv.SetCluster(fc)
	if err := srv.ClusterAddVoter(ctx2s(t), "d", "1.2.3.4:7001", "1.2.3.4:6388"); err != nil {
		t.Fatalf("ClusterAddVoter: %v", err)
	}
}

// TestClusterAddVoter_FollowerReturnsNotLeader verifies the follower
// gate.
func TestClusterAddVoter_FollowerReturnsNotLeader(t *testing.T) {
	fc := &fakeCluster{leader: false}
	srv, _ := newTestServer(t)
	srv.SetCluster(fc)
	err := srv.ClusterAddVoter(ctx2s(t), "d", "1.2.3.4:7001", "1.2.3.4:6388")
	if !errors.Is(err, ErrNotClusterLeader) {
		t.Fatalf("err = %v, want ErrNotClusterLeader", err)
	}
}

// TestClusterRemoveVoter_LeaderOK verifies the happy path delegates.
func TestClusterRemoveVoter_LeaderOK(t *testing.T) {
	fc := &fakeCluster{leader: true}
	srv, _ := newTestServer(t)
	srv.SetCluster(fc)
	if err := srv.ClusterRemoveVoter(ctx2s(t), "d"); err != nil {
		t.Fatalf("ClusterRemoveVoter: %v", err)
	}
}

// TestClusterRemoveVoter_FollowerReturnsNotLeader verifies the follower
// gate.
func TestClusterRemoveVoter_FollowerReturnsNotLeader(t *testing.T) {
	fc := &fakeCluster{leader: false}
	srv, _ := newTestServer(t)
	srv.SetCluster(fc)
	err := srv.ClusterRemoveVoter(ctx2s(t), "d")
	if !errors.Is(err, ErrNotClusterLeader) {
		t.Fatalf("err = %v, want ErrNotClusterLeader", err)
	}
}

// TestClusterMetricsSnapshot_SingleNodeReturnsZero verifies that a
// server without a cluster wired in returns a zero MetricsSnapshot
// (rather than panicking).
func TestClusterMetricsSnapshot_SingleNodeReturnsZero(t *testing.T) {
	srv, _ := newTestServer(t)
	got := srv.ClusterMetricsSnapshot()
	if got != (raft.ClusterMetrics{}) {
		t.Fatalf("MetricsSnapshot on single-node: want zero, got %+v", got)
	}
}

// TestClusterMetricsSnapshot_DelegatesToCluster verifies the call
// returns whatever the underlying Cluster.MetricsSnapshot returns.
func TestClusterMetricsSnapshot_DelegatesToCluster(t *testing.T) {
	fc := &fakeCluster{leader: true}
	srv, _ := newTestServer(t)
	srv.SetCluster(fc)
	// fake's MetricsSnapshot returns zero — verify that's what we get
	// back (delegation check), not a panic from a nil snapshot or
	// something more exciting.
	if got := srv.ClusterMetricsSnapshot(); got != (raft.ClusterMetrics{}) {
		t.Fatalf("MetricsSnapshot = %+v, want zero", got)
	}
}

// ctx2s returns a 2-second-deadline context; the cancel is registered
// with t.Cleanup so each test releases its timer when it finishes.
func ctx2s(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)
	return ctx
}
