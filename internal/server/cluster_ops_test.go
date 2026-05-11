package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mtingers/dflockd/internal/lock"
)

func TestClusterOps_LeaderAndFollower(t *testing.T) {
	s, _ := newTestServer(t)
	if s.IsClusterMode() || s.IsClusterLeader() {
		t.Fatalf("single-node server should not report cluster mode")
	}
	fc := &fakeCluster{leader: true, leaderAddr: "h:1", acquireResult: lock.ApplyResult{Status: lock.StatusOK, Token: "tk", LeaseSec: 30}}
	s.SetCluster(fc)
	if !s.IsClusterMode() || !s.IsClusterLeader() {
		t.Fatalf("clustered+leader server should report both")
	}
	if s.ClusterLeaderAddr() != "h:1" {
		t.Fatalf("ClusterLeaderAddr = %q", s.ClusterLeaderAddr())
	}
	if string(s.ClusterStatusJSON()) == "" {
		t.Fatalf("ClusterStatusJSON empty")
	}

	ctx := context.Background()
	res, err := s.ClusterAcquire(ctx, "lock:k", 1, 1, 30*time.Second, time.Second)
	if err != nil || res.Status != lock.StatusOK || res.Token != "tk" {
		t.Fatalf("ClusterAcquire = %+v, %v", res, err)
	}

	// On a follower every mutating op returns ErrNotClusterLeader.
	fc.mu.Lock()
	fc.leader = false
	fc.mu.Unlock()
	for _, call := range []func() error{
		func() error { _, e := s.ClusterAcquire(ctx, "lock:k", 1, 1, time.Second, time.Second); return e },
		func() error { _, e := s.ClusterEnqueue(ctx, "lock:k", 1, 1, time.Second); return e },
		func() error { _, e := s.ClusterWait(ctx, 1, time.Second); return e },
		func() error { _, e := s.ClusterRelease(ctx, "lock:k", "tk"); return e },
		func() error { _, e := s.ClusterRenew(ctx, "lock:k", "tk", time.Second); return e },
	} {
		if err := call(); !errors.Is(err, ErrNotClusterLeader) {
			t.Fatalf("follower op = %v, want ErrNotClusterLeader", err)
		}
	}
}

func TestClusterOps_QueuedAcquireTimesOut(t *testing.T) {
	s, _ := newTestServer(t)
	s.SetCluster(&fakeCluster{leader: true, acquireResult: lock.ApplyResult{Status: lock.StatusQueued}})
	// timeout 0 → the wait timer fires immediately → still StatusQueued.
	res, err := s.ClusterAcquire(context.Background(), "lock:k", 1, 1, 30*time.Second, 0)
	if err != nil || res.Status != lock.StatusQueued {
		t.Fatalf("queued+timeout = %+v, %v", res, err)
	}
}

func TestClusterOps_EnqueueStashesGrantListener(t *testing.T) {
	s, _ := newTestServer(t)
	fc := &fakeCluster{leader: true, enqueueResult: lock.ApplyResult{Status: lock.StatusQueued}}
	s.SetCluster(fc)
	if _, err := s.ClusterEnqueue(context.Background(), "lock:k", 1, 5, time.Second); err != nil {
		t.Fatalf("ClusterEnqueue: %v", err)
	}
	if _, ok := s.pendingGrants.Load(uint64(5)); !ok {
		t.Fatalf("queued enqueue should have stashed a grant listener for connID 5")
	}
	// Wait consumes it.
	if _, err := s.ClusterWait(context.Background(), 5, 0); err != nil {
		t.Fatalf("ClusterWait: %v", err)
	}
	if _, ok := s.pendingGrants.Load(uint64(5)); ok {
		t.Fatalf("ClusterWait should have consumed the stashed listener")
	}
}

func TestClusterOps_CleanupConnID(t *testing.T) {
	s, _ := newTestServer(t)
	// Single-node → delegates to the LockManager (no error).
	if err := s.CleanupConnID(7); err != nil {
		t.Fatalf("single-node CleanupConnID: %v", err)
	}
	fc := &fakeCluster{leader: true}
	s.SetCluster(fc)
	if err := s.CleanupConnID(9); err != nil {
		t.Fatalf("leader CleanupConnID: %v", err)
	}
	if c := atomicCleanupCount(fc); c != 1 {
		t.Fatalf("ProposeCleanupConn called %d times, want 1", c)
	}
	// Follower → dropped (no propose, no error).
	fc.mu.Lock()
	fc.leader = false
	fc.mu.Unlock()
	if err := s.CleanupConnID(9); err != nil {
		t.Fatalf("follower CleanupConnID: %v", err)
	}
	if c := atomicCleanupCount(fc); c != 1 {
		t.Fatalf("follower CleanupConnID should not propose; calls=%d", c)
	}
}
