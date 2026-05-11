package lock

import (
	"bytes"
	"log/slog"
	"testing"
	"time"

	"github.com/mtingers/dflockd/internal/config"
)

func newApplyTestLM(t *testing.T) *LockManager {
	t.Helper()
	cfg := &config.Config{
		MaxLocks:        128,
		MaxWaiters:      0,
		DefaultLeaseTTL: 30 * time.Second,
		GCMaxIdleTime:   60 * time.Second,
	}
	lm, err := NewLockManager(cfg, slog.Default())
	if err != nil {
		t.Fatalf("NewLockManager: %v", err)
	}
	t.Cleanup(func() { _ = lm.Close() })
	return lm
}

func at(secs int) time.Time { return time.Unix(int64(secs), 0) }

func saltOf(b byte) [8]byte {
	var s [8]byte
	for i := range s {
		s[i] = b
	}
	return s
}

// --- ApplyAcquire ---

func TestApplyAcquireGrantsFreeSlot(t *testing.T) {
	lm := newApplyTestLM(t)
	res, grants, err := lm.ApplyAcquire(at(100), "lock:k", 1, "ref-A", 1, 30*time.Second, saltOf(1))
	if err != nil {
		t.Fatalf("ApplyAcquire: %v", err)
	}
	if res.Status != StatusOK || res.Token == "" || res.LeaseSec != 30 {
		t.Fatalf("first acquire = %+v", res)
	}
	if len(grants) != 0 {
		t.Fatalf("first acquire grants = %d, want 0", len(grants))
	}
	// Second acquire on same key from a different ref must queue.
	res2, _, err := lm.ApplyAcquire(at(101), "lock:k", 1, "ref-B", 2, 30*time.Second, saltOf(2))
	if err != nil {
		t.Fatalf("ApplyAcquire #2: %v", err)
	}
	if res2.Status != StatusQueued || res2.Token != "" {
		t.Fatalf("second acquire = %+v, want queued", res2)
	}
}

func TestApplyReleasePromotesNextWaiter(t *testing.T) {
	lm := newApplyTestLM(t)
	r1, _, _ := lm.ApplyAcquire(at(100), "lock:k", 1, "A", 1, 30*time.Second, saltOf(1))
	_, _, _ = lm.ApplyAcquire(at(101), "lock:k", 1, "B", 2, 30*time.Second, saltOf(2))
	// Release A; B should be promoted.
	rel, grants, err := lm.ApplyRelease(at(102), "lock:k", r1.Token)
	if err != nil {
		t.Fatalf("ApplyRelease: %v", err)
	}
	if rel.Status != StatusOK {
		t.Fatalf("release status = %d", rel.Status)
	}
	if len(grants) != 1 {
		t.Fatalf("expected 1 grant, got %d", len(grants))
	}
	g := grants[0]
	if g.Ref != "B" || g.Token == "" || g.Token == r1.Token {
		t.Fatalf("grant = %+v (token must be new, ref=B)", g)
	}
}

func TestApplyEvictRemovesHolder(t *testing.T) {
	lm := newApplyTestLM(t)
	r, _, _ := lm.ApplyAcquire(at(100), "lock:k", 1, "A", 1, 30*time.Second, saltOf(1))
	rel, _, err := lm.ApplyEvict(at(200), "lock:k", r.Token)
	if err != nil {
		t.Fatalf("ApplyEvict: %v", err)
	}
	if rel.Status != StatusOK {
		t.Fatalf("evict status = %d", rel.Status)
	}
	// Second evict on the same token is idempotent.
	rel2, _, _ := lm.ApplyEvict(at(201), "lock:k", r.Token)
	if rel2.Status != StatusOK {
		t.Fatalf("idempotent evict status = %d", rel2.Status)
	}
}

func TestApplyRenewExtendsLease(t *testing.T) {
	lm := newApplyTestLM(t)
	r, _, _ := lm.ApplyAcquire(at(100), "lock:k", 1, "A", 1, 30*time.Second, saltOf(1))
	renew, _, err := lm.ApplyRenew(at(110), "lock:k", r.Token, 60*time.Second)
	if err != nil {
		t.Fatalf("ApplyRenew: %v", err)
	}
	if renew.Status != StatusOK || renew.LeaseSec != 60 {
		t.Fatalf("renew = %+v", renew)
	}
}

func TestApplyRenewExpiredEvictsAndPromotes(t *testing.T) {
	lm := newApplyTestLM(t)
	r, _, _ := lm.ApplyAcquire(at(100), "lock:k", 1, "A", 1, 30*time.Second, saltOf(1))
	_, _, _ = lm.ApplyAcquire(at(101), "lock:k", 1, "B", 2, 30*time.Second, saltOf(2))
	// Renew well past A's lease (lease ends at 130; renew at 200).
	renew, grants, _ := lm.ApplyRenew(at(200), "lock:k", r.Token, 30*time.Second)
	if renew.Status != StatusErrLeaseExpired {
		t.Fatalf("renew expired status = %d", renew.Status)
	}
	if len(grants) != 1 || grants[0].Ref != "B" {
		t.Fatalf("expected promotion of B; got %+v", grants)
	}
}

func TestApplyEnqueueAcquiredFastPath(t *testing.T) {
	lm := newApplyTestLM(t)
	r, _, _ := lm.ApplyEnqueue(at(100), "lock:k", 1, "A", 1, 30*time.Second, saltOf(1))
	if r.Status != StatusAcquired || r.Token == "" {
		t.Fatalf("enqueue fast path = %+v", r)
	}
}

func TestApplyEnqueueAlreadyEnqueued(t *testing.T) {
	lm := newApplyTestLM(t)
	_, _, _ = lm.ApplyEnqueue(at(100), "lock:k", 1, "A", 1, 30*time.Second, saltOf(1))
	r, _, _ := lm.ApplyEnqueue(at(101), "lock:k", 1, "A2", 1, 30*time.Second, saltOf(2))
	if r.Status != StatusErrAlreadyEnqueued {
		t.Fatalf("duplicate enqueue for same conn = %+v", r)
	}
}

func TestApplyCleanupConnReleasesAndPromotes(t *testing.T) {
	lm := newApplyTestLM(t)
	_, _, _ = lm.ApplyAcquire(at(100), "lock:k", 1, "A", 1, 30*time.Second, saltOf(1))
	_, _, _ = lm.ApplyAcquire(at(101), "lock:k", 1, "B", 2, 30*time.Second, saltOf(2))
	_, grants, err := lm.ApplyCleanupConn(at(102), "A", 1)
	if err != nil {
		t.Fatalf("ApplyCleanupConn: %v", err)
	}
	if len(grants) != 1 || grants[0].Ref != "B" {
		t.Fatalf("cleanup should promote B; got %+v", grants)
	}
}

func TestApplyGCDropsIdleResource(t *testing.T) {
	lm := newApplyTestLM(t)
	// Acquire then release the only holder; resource is now idle.
	r, _, _ := lm.ApplyAcquire(at(100), "lock:k", 1, "A", 1, 30*time.Second, saltOf(1))
	_, _, _ = lm.ApplyRelease(at(101), "lock:k", r.Token)
	// Run GC well past the idle threshold.
	if got := lm.ApplyGC(at(1000)); got.Status != StatusOK {
		t.Fatalf("ApplyGC status = %d", got.Status)
	}
	if lm.resourceTotal.Load() != 0 {
		t.Fatalf("resourceTotal after GC = %d, want 0", lm.resourceTotal.Load())
	}
}

// --- Token monotonicity / determinism ---

func TestFSMTokensAreStrictlyMonotonic(t *testing.T) {
	lm := newApplyTestLM(t)
	prev := ""
	for i := 0; i < 16; i++ {
		// Each ref gets its own lock so there's never contention.
		key := "lock:k-" + string(rune('a'+i))
		r, _, _ := lm.ApplyAcquire(at(int(100+i)), key, 1, "r", uint64(i+1), 30*time.Second, saltOf(byte(i)))
		if r.Token <= prev {
			t.Fatalf("token regressed: %q <= %q (i=%d)", r.Token, prev, i)
		}
		prev = r.Token
	}
}

func TestApplyDeterministicReplay(t *testing.T) {
	// Two fresh managers, identical command sequence -> identical Snapshot.
	a := newApplyTestLM(t)
	b := newApplyTestLM(t)
	cmds := []func(*LockManager){
		func(lm *LockManager) {
			_, _, _ = lm.ApplyAcquire(at(100), "lock:x", 1, "A", 1, 30*time.Second, saltOf(1))
		},
		func(lm *LockManager) {
			_, _, _ = lm.ApplyAcquire(at(101), "lock:x", 1, "B", 2, 30*time.Second, saltOf(2))
		},
		func(lm *LockManager) {
			_, _, _ = lm.ApplyEnqueue(at(102), "lock:y", 1, "C", 3, 30*time.Second, saltOf(3))
		},
		func(lm *LockManager) {
			_, _, _ = lm.ApplyEnqueue(at(103), "lock:y", 1, "D", 4, 30*time.Second, saltOf(4))
		},
	}
	for _, c := range cmds {
		c(a)
		c(b)
	}
	if !sameSnapshot(t, a, b) {
		t.Fatalf("FSMs diverged after identical command sequence")
	}
}

func sameSnapshot(t *testing.T, a, b *LockManager) bool {
	t.Helper()
	var ba, bb bytes.Buffer
	if err := a.Snapshot(&ba); err != nil {
		t.Fatalf("a.Snapshot: %v", err)
	}
	if err := b.Snapshot(&bb); err != nil {
		t.Fatalf("b.Snapshot: %v", err)
	}
	return bytes.Equal(ba.Bytes(), bb.Bytes())
}

// --- Snapshot / Restore round-trip ---

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	a := newApplyTestLM(t)
	// Build varied state: 2 locks with held + queued; 1 sem with limit 2;
	// one fast-path Enqueue.
	_, _, _ = a.ApplyAcquire(at(100), "lock:k1", 1, "A", 1, 30*time.Second, saltOf(1))
	_, _, _ = a.ApplyAcquire(at(101), "lock:k1", 1, "B", 2, 30*time.Second, saltOf(2))
	_, _, _ = a.ApplyEnqueue(at(102), "lock:k2", 1, "C", 3, 60*time.Second, saltOf(3)) // fast-path
	_, _, _ = a.ApplyAcquire(at(103), "sem:s", 2, "D", 4, 30*time.Second, saltOf(4))
	_, _, _ = a.ApplyAcquire(at(104), "sem:s", 2, "E", 5, 30*time.Second, saltOf(5))

	var buf bytes.Buffer
	if err := a.Snapshot(&buf); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	b := newApplyTestLM(t)
	if err := b.Restore(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	// Round-trip equivalence: another snapshot of b should equal a's.
	var buf2 bytes.Buffer
	if err := b.Snapshot(&buf2); err != nil {
		t.Fatalf("Snapshot after restore: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), buf2.Bytes()) {
		t.Fatalf("snapshot round-trip diverged (len %d -> %d)", buf.Len(), buf2.Len())
	}
	// And b's fence counter should match a's.
	if b.fsmFenceCounter != a.fsmFenceCounter {
		t.Fatalf("fence counter %d -> %d", a.fsmFenceCounter, b.fsmFenceCounter)
	}
}

func TestRestoreReplacesAllPriorState(t *testing.T) {
	a := newApplyTestLM(t)
	_, _, _ = a.ApplyAcquire(at(100), "lock:original", 1, "A", 1, 30*time.Second, saltOf(1))

	b := newApplyTestLM(t)
	_, _, _ = b.ApplyAcquire(at(200), "lock:other", 1, "X", 9, 30*time.Second, saltOf(9))
	// Restore b from a's snapshot — b's prior state must vanish.
	var buf bytes.Buffer
	_ = a.Snapshot(&buf)
	if err := b.Restore(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	sh := b.shardFor("lock:other")
	sh.mu.Lock()
	_, stillThere := sh.resources["lock:other"]
	sh.mu.Unlock()
	if stillThere {
		t.Fatalf("Restore should have dropped b's prior 'lock:other' resource")
	}
}

// --- Grant routing via the listener registry ---

func TestRouteGrantsDeliversToRegisteredListener(t *testing.T) {
	lm := newApplyTestLM(t)
	r, _, _ := lm.ApplyAcquire(at(100), "lock:k", 1, "A", 1, 30*time.Second, saltOf(1))

	// Register a listener for B before its queued acquire so the eventual
	// promotion is observable.
	ch, cancel := lm.WatchGrants("B")
	defer cancel()
	_, _, _ = lm.ApplyAcquire(at(101), "lock:k", 1, "B", 2, 30*time.Second, saltOf(2))

	// Release A -> B promoted; route grants -> ch must see B's grant.
	_, grants, _ := lm.ApplyRelease(at(102), "lock:k", r.Token)
	lm.RouteGrants(grants)

	select {
	case g := <-ch:
		if g.Ref != "B" || g.Token == "" {
			t.Fatalf("grant routed wrongly: %+v", g)
		}
	default:
		t.Fatalf("no grant delivered to B's listener")
	}
}

func TestRouteGrantsToUnregisteredRefIsBenign(t *testing.T) {
	lm := newApplyTestLM(t)
	r, _, _ := lm.ApplyAcquire(at(100), "lock:k", 1, "A", 1, 30*time.Second, saltOf(1))
	_, _, _ = lm.ApplyAcquire(at(101), "lock:k", 1, "B", 2, 30*time.Second, saltOf(2)) // no listener for B
	_, grants, _ := lm.ApplyRelease(at(102), "lock:k", r.Token)
	lm.RouteGrants(grants) // must not panic; holder for B still exists in FSM
}
