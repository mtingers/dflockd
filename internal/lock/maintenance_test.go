package lock

import (
	"testing"
	"time"
)

func TestEvictionDueTracksLeaseBoundary(t *testing.T) {
	lm := newApplyTestLM(t)
	holder, _, err := lm.ApplyAcquire(at(100), "lock:k", 1, "holder", 1, 10*time.Second, saltOf(1))
	if err != nil {
		t.Fatalf("ApplyAcquire: %v", err)
	}
	if lm.EvictionDue(at(109)) {
		t.Fatal("eviction reported before lease deadline")
	}
	if !lm.EvictionDue(at(110)) {
		t.Fatal("eviction not reported at lease deadline")
	}
	if _, _, err := lm.ApplyRelease(at(110), "lock:k", holder.Token); err != nil {
		t.Fatalf("ApplyRelease: %v", err)
	}
	if lm.EvictionDue(at(111)) {
		t.Fatal("eviction remained due after holder removal")
	}
}

func TestEvictionDueTracksOrphanHolderAndWaiter(t *testing.T) {
	t.Run("holder", func(t *testing.T) {
		lm := newOrphanTestLM(t, 10*time.Second)
		_, _, _ = lm.ApplyAcquire(at(100), "lock:k", 1, "holder", 1, time.Hour, saltOf(1))
		_, _, _ = lm.ApplyCleanupConn(at(102), "holder", 1)
		assertOrphanEvictionBoundary(t, lm)
	})
	t.Run("waiter", func(t *testing.T) {
		lm := newOrphanTestLM(t, 10*time.Second)
		_, _, _ = lm.ApplyAcquire(at(100), "lock:k", 1, "holder", 1, time.Hour, saltOf(1))
		_, _, _ = lm.ApplyAcquire(at(101), "lock:k", 1, "waiter", 2, time.Hour, saltOf(2))
		_, _, _ = lm.ApplyCleanupConn(at(102), "waiter", 2)
		assertOrphanEvictionBoundary(t, lm)
	})
}

func assertOrphanEvictionBoundary(t *testing.T, lm *LockManager) {
	t.Helper()
	if lm.EvictionDue(at(112)) {
		t.Fatal("orphan eviction reported at strict TTL boundary")
	}
	if !lm.EvictionDue(at(113)) {
		t.Fatal("orphan eviction not reported after TTL")
	}
	if _, _, err := lm.ApplyEvictExpired(at(113)); err != nil {
		t.Fatalf("ApplyEvictExpired: %v", err)
	}
	if lm.EvictionDue(at(113)) {
		t.Fatal("orphan eviction remained due after sweep")
	}
}

func TestGCDueTracksIdleBoundary(t *testing.T) {
	lm := newApplyTestLM(t)
	holder, _, _ := lm.ApplyAcquire(at(100), "lock:k", 1, "holder", 1, time.Minute, saltOf(1))
	_, _, _ = lm.ApplyRelease(at(101), "lock:k", holder.Token)
	if lm.GCDue(at(161)) {
		t.Fatal("GC reported at strict idle boundary")
	}
	if !lm.GCDue(at(162)) {
		t.Fatal("GC not reported after idle threshold")
	}
	lm.ApplyGC(at(162))
	if lm.GCDue(at(163)) {
		t.Fatal("GC remained due after idle resource removal")
	}
}
