package lock

import (
	"bytes"
	"log/slog"
	"testing"
	"time"

	"github.com/mtingers/dflockd/internal/config"
)

// newOrphanTestLM is newApplyTestLM with OrphanTTL set so stable-ref
// re-attach is active.
func newOrphanTestLM(t *testing.T, orphanTTL time.Duration) *LockManager {
	t.Helper()
	cfg := &config.Config{
		MaxLocks:        128,
		MaxWaiters:      0,
		DefaultLeaseTTL: 30 * time.Second,
		GCMaxIdleTime:   60 * time.Second,
		OrphanTTL:       orphanTTL,
	}
	lm, err := NewLockManager(cfg, slog.Default())
	if err != nil {
		t.Fatalf("NewLockManager: %v", err)
	}
	t.Cleanup(func() { _ = lm.Close() })
	return lm
}

// TestCleanupConnOrphansStableRefWaiter: when OrphanTTL > 0 and the
// waiter has a stable ref, CleanupConn must mark it abandoned, not
// remove it.
func TestCleanupConnOrphansStableRefWaiter(t *testing.T) {
	lm := newOrphanTestLM(t, 30*time.Second)
	// Holder: ref-A holds slot.
	if _, _, err := lm.ApplyAcquire(at(100), "lock:k", 1, "ref-A", 1, 30*time.Second, saltOf(1)); err != nil {
		t.Fatalf("acquire A: %v", err)
	}
	// Waiter: ref-B queues with stable ref.
	res, _, err := lm.ApplyEnqueue(at(101), "lock:k", 1, "ref-B", 2, 30*time.Second, saltOf(2))
	if err != nil || res.Status != StatusQueued {
		t.Fatalf("enqueue B: %+v %v", res, err)
	}
	// CleanupConn for B (the connID-2 connection went away).
	if _, _, err := lm.ApplyCleanupConn(at(102), "", 2); err != nil {
		t.Fatalf("cleanup B: %v", err)
	}
	// The waiter must still be in the FSM, marked abandoned.
	if !lm.HasOrphanedWaiterForTest("lock:k", "ref-B") {
		t.Fatalf("ref-B's waiter should be orphaned, not removed")
	}
}

// TestEnqueueReAdoptsOrphanedWaiter: after CleanupConn orphans a
// stable-ref waiter, a re-Enqueue with the same (key, ref) must
// re-adopt that waiter (same slot, new connID, original salt preserved).
// The salt check distinguishes "re-adopt" from "create fresh" — the
// caller passes a different salt on the second call, and a re-adopt
// must preserve the first one (so when promoted, the resulting token
// is deterministic across replicas independent of what the
// reconnecting client's RNG happens to spit out).
func TestEnqueueReAdoptsOrphanedWaiter(t *testing.T) {
	lm := newOrphanTestLM(t, 30*time.Second)
	_, _, _ = lm.ApplyAcquire(at(100), "lock:k", 1, "ref-A", 1, 30*time.Second, saltOf(1))
	originalSalt := saltOf(2)
	_, _, _ = lm.ApplyEnqueue(at(101), "lock:k", 1, "ref-B", 2, 30*time.Second, originalSalt)
	_, _, _ = lm.ApplyCleanupConn(at(102), "", 2)
	// Re-enqueue with same ref but different connID AND different salt.
	res, _, err := lm.ApplyEnqueue(at(103), "lock:k", 1, "ref-B", 99, 30*time.Second, saltOf(77))
	if err != nil {
		t.Fatalf("re-enqueue: %v", err)
	}
	if res.Status != StatusQueued {
		t.Fatalf("re-enqueue status = %v, want StatusQueued (re-adopted)", res.Status)
	}
	if !lm.HasActiveWaiterForTest("lock:k", "ref-B", 99) {
		t.Fatalf("waiter not re-adopted with new connID 99")
	}
	if got := lm.WaiterSaltForTest("lock:k", "ref-B"); got != originalSalt {
		t.Fatalf("waiter salt = %v, want original %v (re-adopt should preserve)", got, originalSalt)
	}
	// Crucially: there's only ONE waiter for ref-B (no duplicate slot).
	if n := lm.CountWaitersForTest("lock:k"); n != 1 {
		t.Fatalf("waiter count = %d, want 1 (re-adopted, not duplicated)", n)
	}
}

// TestCleanupConnWithoutOrphanTTLRemovesWaiter: with OrphanTTL=0 (the
// default), CleanupConn must behave as it did pre-PR-4 — remove the
// waiter regardless of ref.
func TestCleanupConnWithoutOrphanTTLRemovesWaiter(t *testing.T) {
	lm := newApplyTestLM(t) // OrphanTTL = 0
	_, _, _ = lm.ApplyAcquire(at(100), "lock:k", 1, "ref-A", 1, 30*time.Second, saltOf(1))
	_, _, _ = lm.ApplyEnqueue(at(101), "lock:k", 1, "ref-B", 2, 30*time.Second, saltOf(2))
	_, _, _ = lm.ApplyCleanupConn(at(102), "", 2)
	if n := lm.CountWaitersForTest("lock:k"); n != 0 {
		t.Fatalf("waiter count = %d, want 0 (OrphanTTL=0; old behavior)", n)
	}
}

// TestSnapshotRoundTripPreservesOrphanState verifies the v2 snapshot
// codec carries `abandonedAtNanos` through Snapshot → Restore. This is
// the FSM-determinism property the cluster relies on for replicas to
// converge on the same orphan state.
func TestSnapshotRoundTripPreservesOrphanState(t *testing.T) {
	src := newOrphanTestLM(t, 30*time.Second)
	_, _, _ = src.ApplyAcquire(at(100), "lock:k", 1, "ref-A", 1, 30*time.Second, saltOf(1))
	_, _, _ = src.ApplyEnqueue(at(101), "lock:k", 1, "ref-B", 2, 30*time.Second, saltOf(2))
	_, _, _ = src.ApplyCleanupConn(at(102), "", 2)
	if !src.HasOrphanedWaiterForTest("lock:k", "ref-B") {
		t.Fatalf("setup: ref-B not orphaned in src")
	}
	var buf bytes.Buffer
	if err := src.Snapshot(&buf); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	dst := newOrphanTestLM(t, 30*time.Second)
	if err := dst.Restore(&buf); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if !dst.HasOrphanedWaiterForTest("lock:k", "ref-B") {
		t.Fatalf("after Restore: ref-B should still be orphaned (abandonedAtNanos lost in codec)")
	}
	res, _, err := dst.ApplyEnqueue(at(103), "lock:k", 1, "ref-B", 99, 30*time.Second, saltOf(77))
	if err != nil || res.Status != StatusQueued {
		t.Fatalf("re-adopt after Restore = %+v, %v; want queued", res, err)
	}
	if n := dst.CountWaitersForTest("lock:k"); n != 1 {
		t.Fatalf("waiters after restored re-adopt = %d, want 1", n)
	}
}

// TestEvictExpiredRemovesOrphanPastTTL: an orphaned waiter past
// OrphanTTL must be removed by the next EvictExpired sweep.
func TestEvictExpiredRemovesOrphanPastTTL(t *testing.T) {
	lm := newOrphanTestLM(t, 10*time.Second)
	_, _, _ = lm.ApplyAcquire(at(100), "lock:k", 1, "ref-A", 1, 30*time.Second, saltOf(1))
	_, _, _ = lm.ApplyEnqueue(at(101), "lock:k", 1, "ref-B", 2, 30*time.Second, saltOf(2))
	_, _, _ = lm.ApplyCleanupConn(at(102), "", 2)
	if n := lm.CountWaitersForTest("lock:k"); n != 1 {
		t.Fatalf("waiter count after cleanup = %d, want 1 (orphaned)", n)
	}
	// Just past OrphanTTL.
	_, _, _ = lm.ApplyEvictExpired(at(115)) // 102 + 10 + 3 = past TTL
	if n := lm.CountWaitersForTest("lock:k"); n != 0 {
		t.Fatalf("waiter count after evict = %d, want 0 (orphan past TTL)", n)
	}
}
