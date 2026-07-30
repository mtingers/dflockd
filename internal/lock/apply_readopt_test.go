package lock

import (
	"testing"
	"time"
)

// The tests in this file exercise the HARD-CRASH failover re-attach
// path: a holder/waiter is left in the FSM with abandonedAtNanos == 0
// (no graceful CleanupConn ran, because the owning node was killed),
// and a reconnect with the same stable ref must re-adopt it. This is
// the scenario PR-4 shipped but did NOT actually solve — its finders
// only matched gracefully-orphaned entries (abandonedAtNanos != 0).
//
// "Hard crash" is simulated by NOT calling ApplyCleanupConn between the
// original acquire/enqueue and the reconnect: the orphan stamp is never
// set, exactly as on a new leader that inherited the FSM via snapshot.
//
// The reconnect therefore arrives on a connID minted by a DIFFERENT
// server process, which is how the FSM tells "the owner's node is gone"
// from "another live client is naming this ref" — cluster connIDs carry
// a fixed process tag in their high 24 bits.
// deadNodeConn / newNodeConn below make that explicit.

// deadNodeConn returns a connID as minted by the node that later
// crashed; newNodeConn returns one from the node the client reconnects
// to. Distinct epochs, as in any real failover.
func deadNodeConn(n uint64) uint64 { return uint64(0xDEAD01)<<40 | n }
func newNodeConn(n uint64) uint64  { return uint64(0xA11FE0)<<40 | n }

// TestEnqueueReAdoptsHardCrashedHolder: a client holds a lock (acquired
// via the two-phase Enqueue fast-path), its node is killed without a
// graceful cleanup, and it reconnects (new connID, same ref). The
// re-Enqueue must re-adopt the existing holder — same token, no second
// holder, and the dead connection's index entries evicted.
func TestEnqueueReAdoptsHardCrashedHolder(t *testing.T) {
	lm := newOrphanTestLM(t, 30*time.Second)
	res1, _, err := lm.ApplyEnqueue(at(100), "lock:k", 1, "ref-A", deadNodeConn(1), 30*time.Second, saltOf(1))
	if err != nil || res1.Status != StatusAcquired || res1.Token == "" {
		t.Fatalf("initial enqueue = %+v err=%v, want StatusAcquired+token", res1, err)
	}
	// HARD CRASH: connID 1 vanishes; no ApplyCleanupConn.
	res2, _, err := lm.ApplyEnqueue(at(105), "lock:k", 1, "ref-A", newNodeConn(2), 30*time.Second, saltOf(2))
	if err != nil {
		t.Fatalf("reconnect enqueue: %v", err)
	}
	if res2.Status != StatusAcquired {
		t.Fatalf("reconnect status = %v, want StatusAcquired (re-adopted hard-crashed holder)", res2.Status)
	}
	if res2.Token != res1.Token {
		t.Fatalf("reconnect token = %q, want original %q (re-adopt must keep the token)", res2.Token, res1.Token)
	}
	if n := lm.HolderCountForTest("lock:k"); n != 1 {
		t.Fatalf("holder count = %d, want 1 (re-adopted, not duplicated)", n)
	}
	if lm.ConnTrackedForTest(deadNodeConn(1)) {
		t.Fatalf("dead connID 1 still tracked after re-adopt; index not evicted")
	}
	if !lm.ConnTrackedForTest(newNodeConn(2)) {
		t.Fatalf("new connID 2 not tracked after re-adopt")
	}
}

// TestAcquireReAdoptsHardCrashedHolder: same as above but via the
// single-phase Acquire API — the PRIMARY blocking-lock path. Before
// PR-5 this path had no re-adopt at all, so a reconnect queued behind
// its own orphaned holder until the lease lapsed.
func TestAcquireReAdoptsHardCrashedHolder(t *testing.T) {
	lm := newOrphanTestLM(t, 30*time.Second)
	res1, _, err := lm.ApplyAcquire(at(100), "lock:k", 1, "ref-A", deadNodeConn(1), 30*time.Second, saltOf(1))
	if err != nil || res1.Status != StatusOK || res1.Token == "" {
		t.Fatalf("initial acquire = %+v err=%v, want StatusOK+token", res1, err)
	}
	// HARD CRASH: no cleanup.
	res2, _, err := lm.ApplyAcquire(at(105), "lock:k", 1, "ref-A", newNodeConn(2), 30*time.Second, saltOf(2))
	if err != nil {
		t.Fatalf("reconnect acquire: %v", err)
	}
	if res2.Status != StatusOK {
		t.Fatalf("reconnect status = %v, want StatusOK (re-adopted hard-crashed holder)", res2.Status)
	}
	if res2.Token != res1.Token {
		t.Fatalf("reconnect token = %q, want original %q", res2.Token, res1.Token)
	}
	if n := lm.HolderCountForTest("lock:k"); n != 1 {
		t.Fatalf("holder count = %d, want 1 (re-adopted, not queued behind itself)", n)
	}
	if lm.CountWaitersForTest("lock:k") != 0 {
		t.Fatalf("a waiter was created; reconnect should re-adopt the holder, not queue")
	}
	if lm.ConnTrackedForTest(deadNodeConn(1)) {
		t.Fatalf("dead connID 1 still tracked after re-adopt")
	}
}

// TestEnqueueReAdoptsHardCrashedWaiter: a queued (not holding) client's
// node is killed; reconnect re-adopts the waiter, preserving its FIFO
// slot and original salt, and evicts the dead connection's index.
func TestEnqueueReAdoptsHardCrashedWaiter(t *testing.T) {
	lm := newOrphanTestLM(t, 30*time.Second)
	_, _, _ = lm.ApplyAcquire(at(100), "lock:k", 1, "ref-A", deadNodeConn(1), 30*time.Second, saltOf(1))
	origSalt := saltOf(2)
	res, _, err := lm.ApplyEnqueue(at(101), "lock:k", 1, "ref-B", deadNodeConn(2), 30*time.Second, origSalt)
	if err != nil || res.Status != StatusQueued {
		t.Fatalf("enqueue B = %+v err=%v, want StatusQueued", res, err)
	}
	if !lm.ConnTrackedForTest(deadNodeConn(2)) {
		t.Fatalf("setup: queued connID 2 should be index-tracked")
	}
	// HARD CRASH of connID 2; reconnect as 3 with a DIFFERENT salt.
	res3, _, err := lm.ApplyEnqueue(at(105), "lock:k", 1, "ref-B", newNodeConn(3), 30*time.Second, saltOf(77))
	if err != nil {
		t.Fatalf("reconnect enqueue: %v", err)
	}
	if res3.Status != StatusQueued {
		t.Fatalf("reconnect status = %v, want StatusQueued (re-adopted hard-crashed waiter)", res3.Status)
	}
	if !lm.HasActiveWaiterForTest("lock:k", "ref-B", newNodeConn(3)) {
		t.Fatalf("waiter not re-adopted with new connID 3")
	}
	if got := lm.WaiterSaltForTest("lock:k", "ref-B"); got != origSalt {
		t.Fatalf("waiter salt = %v, want original %v (re-adopt preserves salt)", got, origSalt)
	}
	if n := lm.CountWaitersForTest("lock:k"); n != 1 {
		t.Fatalf("waiter count = %d, want 1 (re-adopted, not duplicated)", n)
	}
	if lm.ConnTrackedForTest(deadNodeConn(2)) {
		t.Fatalf("dead connID 2 still tracked after re-adopt")
	}
}

// TestAcquireReAdoptsHardCrashedWaiter: the single-phase Acquire waiter
// case. A reconnect re-adopts the queued slot rather than appending a
// second waiter for the same ref.
func TestAcquireReAdoptsHardCrashedWaiter(t *testing.T) {
	lm := newOrphanTestLM(t, 30*time.Second)
	_, _, _ = lm.ApplyAcquire(at(100), "lock:k", 1, "ref-A", deadNodeConn(1), 30*time.Second, saltOf(1))
	origSalt := saltOf(2)
	res, _, _ := lm.ApplyAcquire(at(101), "lock:k", 1, "ref-B", deadNodeConn(2), 30*time.Second, origSalt)
	if res.Status != StatusQueued {
		t.Fatalf("acquire B status = %v, want StatusQueued", res.Status)
	}
	// HARD CRASH of connID 2; reconnect as 3.
	res3, _, err := lm.ApplyAcquire(at(105), "lock:k", 1, "ref-B", newNodeConn(3), 30*time.Second, saltOf(77))
	if err != nil {
		t.Fatalf("reconnect acquire: %v", err)
	}
	if res3.Status != StatusQueued {
		t.Fatalf("reconnect status = %v, want StatusQueued (re-adopted waiter)", res3.Status)
	}
	if !lm.HasActiveWaiterForTest("lock:k", "ref-B", newNodeConn(3)) {
		t.Fatalf("waiter not re-adopted with new connID 3")
	}
	if got := lm.WaiterSaltForTest("lock:k", "ref-B"); got != origSalt {
		t.Fatalf("waiter salt = %v, want original %v", got, origSalt)
	}
	if n := lm.CountWaitersForTest("lock:k"); n != 1 {
		t.Fatalf("waiter count = %d, want 1 (re-adopted, not duplicated)", n)
	}
}

// TestAcquireReAdoptsHardCrashWithoutOrphanRetention verifies that
// OrphanTTL controls graceful-disconnect retention, not failover recovery.
// A holder left by a dead server epoch is reattached by its stable ref even
// under the default OrphanTTL=0 policy.
func TestAcquireReAdoptsHardCrashWithoutOrphanRetention(t *testing.T) {
	lm := newApplyTestLM(t) // OrphanTTL = 0
	res1, _, _ := lm.ApplyAcquire(at(100), "lock:k", 1, "ref-A", deadNodeConn(1), 30*time.Second, saltOf(1))
	res2, _, _ := lm.ApplyAcquire(at(101), "lock:k", 1, "ref-A", newNodeConn(2), 30*time.Second, saltOf(2))
	if res2.Status != StatusOK || res2.Token != res1.Token {
		t.Fatalf("re-adopt = %+v, want original token %q", res2, res1.Token)
	}
	if got := lm.HolderTokenForRefTest("lock:k", "ref-A"); got != res1.Token {
		t.Fatalf("holder token changed to %q", got)
	}
	if n := lm.CountWaitersForTest("lock:k"); n != 0 {
		t.Fatalf("waiter count = %d, want 0", n)
	}
}
