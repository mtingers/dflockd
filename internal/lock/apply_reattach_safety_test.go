package lock

import (
	"testing"
	"time"

	"github.com/mtingers/dflockd/internal/config"
)

// Re-attach by stable ref exists so a client that reconnects after a
// leader failover keeps its slot. It must not double as a way for one
// live client to take over another live client's slot: `ref` arrives
// from the wire, so matching on it alone would let anyone who names a
// ref be handed that ref's holder — and its fencing token.
//
// The FSM decides "is the previous owner really gone?" from data it
// already replicates: an orphan stamp (the graceful path), or a connID
// minted by a different server process (the failover path — connIDs
// carry a fixed process tag in their high 24 bits).

const (
	epochA = uint64(0x11_1111) << 40 // "leader A's process"
	epochB = uint64(0x22_2222) << 40 // "leader B's process"
)

func reattachCfg() *config.Config {
	return &config.Config{
		MaxLocks: 100, MaxWaiters: 100, OrphanTTL: time.Minute,
		GCMaxIdleTime: time.Hour, AutoReleaseOnDisconnect: true,
	}
}

func applyNow() time.Time { return time.Unix(1700000000, 0) }

// A second, still-live connection on the same server that names another
// client's ref must not receive that client's holder or token.
func TestReAttachRefusedForLiveHolderOnSameNode(t *testing.T) {
	lm := managerWithCfg(t, reattachCfg())
	now := applyNow()

	victim, _, err := lm.ApplyAcquire(now, "lock:k", 1, "worker-1", epochA|1, time.Minute, [8]byte{1})
	if err != nil || victim.Status != StatusOK {
		t.Fatalf("victim acquire: %v %+v", err, victim)
	}

	got, _, err := lm.ApplyAcquire(now, "lock:k", 1, "worker-1", epochA|2, time.Minute, [8]byte{2})
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	if got.Status != StatusQueued {
		t.Fatalf("status = %v (token %q), want queued — the lock is held by a live conn", got.Status, got.Token)
	}
	if got.Token != "" {
		t.Fatalf("second acquire leaked the victim's token %q", got.Token)
	}
}

func TestReAttachRefusedAcrossOld32BitCounterBoundary(t *testing.T) {
	lm := managerWithCfg(t, reattachCfg())
	now := applyNow()
	before := epochA | (1<<32 - 1)
	after := epochA | 1<<32

	victim, _, err := lm.ApplyAcquire(now, "lock:k", 1, "worker-1", before, time.Minute, [8]byte{1})
	if err != nil || victim.Status != StatusOK {
		t.Fatalf("victim acquire: %v %+v", err, victim)
	}
	got, _, err := lm.ApplyAcquire(now, "lock:k", 1, "worker-1", after, time.Minute, [8]byte{2})
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	if got.Status != StatusQueued || got.Token != "" {
		t.Fatalf("status = %v token = %q, want queued across old counter boundary", got.Status, got.Token)
	}
}

// The same guard applies to the two-phase Enqueue path.
func TestEnqueueReAttachRefusedForLiveHolderOnSameNode(t *testing.T) {
	lm := managerWithCfg(t, reattachCfg())
	now := applyNow()

	victim, _, err := lm.ApplyEnqueue(now, "lock:k", 1, "worker-1", epochA|1, time.Minute, [8]byte{1})
	if err != nil || victim.Status != StatusAcquired {
		t.Fatalf("victim enqueue: %v %+v", err, victim)
	}

	got, _, err := lm.ApplyEnqueue(now, "lock:k", 1, "worker-1", epochA|2, time.Minute, [8]byte{2})
	if err != nil {
		t.Fatalf("second enqueue: %v", err)
	}
	if got.Status != StatusQueued || got.Token != "" {
		t.Fatalf("status = %v token = %q, want queued with no token", got.Status, got.Token)
	}
}

// The failover case still works: the holder was minted by a different
// server process (dead leader), so a reconnect re-adopts it — token and
// queue position preserved — with no orphan stamp in sight.
func TestReAttachAllowedAcrossServerEpochs(t *testing.T) {
	lm := managerWithCfg(t, reattachCfg())
	now := applyNow()

	old, _, err := lm.ApplyAcquire(now, "lock:k", 1, "worker-1", epochA|7, time.Minute, [8]byte{1})
	if err != nil || old.Status != StatusOK {
		t.Fatalf("original acquire: %v %+v", err, old)
	}

	got, _, err := lm.ApplyAcquire(now, "lock:k", 1, "worker-1", epochB|1, time.Minute, [8]byte{2})
	if err != nil {
		t.Fatalf("re-attach: %v", err)
	}
	if got.Status != StatusOK {
		t.Fatalf("status = %v, want ok (re-attach across a failover)", got.Status)
	}
	if got.Token != old.Token {
		t.Fatalf("token = %q, want the original %q", got.Token, old.Token)
	}
}

// A gracefully-orphaned holder is re-adopted even by a fresh connection
// on the same server process — that's the reconnect-to-the-same-leader
// case, and the orphan stamp is the proof the old conn is gone.
func TestReAttachAllowedForOrphanedHolderOnSameNode(t *testing.T) {
	lm := managerWithCfg(t, reattachCfg())
	now := applyNow()

	old, _, err := lm.ApplyAcquire(now, "lock:k", 1, "worker-1", epochA|1, time.Minute, [8]byte{1})
	if err != nil || old.Status != StatusOK {
		t.Fatalf("original acquire: %v %+v", err, old)
	}
	if _, _, err := lm.ApplyCleanupConn(now, "worker-1", epochA|1); err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	got, _, err := lm.ApplyAcquire(now, "lock:k", 1, "worker-1", epochA|2, time.Minute, [8]byte{2})
	if err != nil {
		t.Fatalf("re-attach: %v", err)
	}
	if got.Status != StatusOK || got.Token != old.Token {
		t.Fatalf("status = %v token = %q, want ok with the original token %q", got.Status, got.Token, old.Token)
	}
}

// A live waiter's queue slot is likewise not transferable to another
// live connection naming the same ref.
func TestReAttachRefusedForLiveWaiterOnSameNode(t *testing.T) {
	lm := managerWithCfg(t, reattachCfg())
	now := applyNow()

	if _, _, err := lm.ApplyAcquire(now, "lock:k", 1, "holder", epochA|1, time.Minute, [8]byte{1}); err != nil {
		t.Fatalf("holder acquire: %v", err)
	}
	queued, _, err := lm.ApplyAcquire(now, "lock:k", 1, "worker-1", epochA|2, time.Minute, [8]byte{2})
	if err != nil || queued.Status != StatusQueued {
		t.Fatalf("waiter acquire: %v %+v", err, queued)
	}

	// A different live conn names the same ref: it must take its own
	// place in line rather than adopt the existing waiter.
	if _, _, err := lm.ApplyAcquire(now, "lock:k", 1, "worker-1", epochA|3, time.Minute, [8]byte{3}); err != nil {
		t.Fatalf("second waiter acquire: %v", err)
	}
	if got := lm.CountWaitersForTest("lock:k"); got != 2 {
		t.Fatalf("waiters = %d, want 2 (no re-adopt of a live waiter)", got)
	}
}
