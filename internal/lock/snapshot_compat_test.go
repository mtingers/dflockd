package lock

import (
	"bytes"
	"testing"
	"time"
)

// Older snapshot generations must stay readable: an in-place upgrade restores
// from whatever the previous release wrote. Each optional field is gated on the
// version that introduced it, so bumping the writer must not stop the reader
// consuming a prior format's bytes — a skipped field desynchronises the whole
// stream, not just that value.

type legacySnapshotHolder struct {
	token, ref string
	connID     uint64
	leaseNanos int64
	abandoned  int64
}

type legacySnapshotWaiter struct {
	ref       string
	salt      [8]byte
	connID    uint64
	leaseTTL  int64
	abandoned int64
}

// writeLegacySnapshot emits one resource in the byte layout of snapshot
// version ver (1, 2, or 3), plus a single enqueued-index entry for the waiter.
func writeLegacySnapshot(t *testing.T, ver byte, key string, h legacySnapshotHolder, w legacySnapshotWaiter) []byte {
	t.Helper()
	var b bytes.Buffer
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("encode v%d snapshot: %v", ver, err)
		}
	}
	b.WriteString(snapshotMagic)
	b.WriteByte(ver)
	must(writeU64(&b, 99)) // fsmFenceCounter

	must(writeU32(&b, 1)) // one resource
	must(writeString16(&b, key))
	must(writeU32(&b, 1))                 // limit
	must(writeI64(&b, 1_700_000_000_000)) // LastActivity

	must(writeU32(&b, 1)) // one holder
	must(writeString16(&b, h.token))
	must(writeString16(&b, h.ref))
	must(writeU64(&b, h.connID))
	must(writeI64(&b, h.leaseNanos))
	if ver >= snapshotVer2 {
		must(writeI64(&b, h.abandoned))
	}

	must(writeU32(&b, 1)) // one waiter
	must(writeString16(&b, w.ref))
	_, err := b.Write(w.salt[:])
	must(err)
	must(writeU64(&b, w.connID))
	must(writeI64(&b, w.leaseTTL))
	if ver >= snapshotVer2 {
		must(writeI64(&b, w.abandoned))
	}

	must(writeU32(&b, 1)) // one enqueued-index entry (queued waiter, no token)
	must(writeU64(&b, w.connID))
	must(writeString16(&b, key))
	must(writeString16(&b, ""))
	must(writeI64(&b, w.leaseTTL))
	if ver >= snapshotVer3 {
		_, err := b.Write(w.salt[:])
		must(err)
	}
	return b.Bytes()
}

func newEmptyManager() *LockManager {
	lm := &LockManager{}
	for i := range lm.shards {
		lm.shards[i].init()
	}
	return lm
}

func TestRestoreAcceptsEveryPublishedSnapshotVersion(t *testing.T) {
	const key = "lock:compat"
	holder := legacySnapshotHolder{
		token:      "tok0123456789abcdef0123456789abc",
		ref:        "holder-ref",
		connID:     7,
		leaseNanos: 1_700_000_030_000,
		abandoned:  1_700_000_010_000,
	}
	waiterIn := legacySnapshotWaiter{
		ref:       "waiter-ref",
		salt:      [8]byte{1, 2, 3, 4, 5, 6, 7, 8},
		connID:    9,
		leaseTTL:  int64(30 * time.Second),
		abandoned: 1_700_000_020_000,
	}

	for _, ver := range []byte{snapshotVer1, snapshotVer2, snapshotVer3} {
		t.Run(string(rune('0'+ver)), func(t *testing.T) {
			raw := writeLegacySnapshot(t, ver, key, holder, waiterIn)
			lm := newEmptyManager()
			if err := lm.Restore(bytes.NewReader(raw)); err != nil {
				t.Fatalf("Restore v%d: %v", ver, err)
			}
			sh := lm.shardFor(key)
			st := sh.resources[key]
			if st == nil {
				t.Fatalf("v%d: resource missing after restore", ver)
			}
			h := st.Holders[holder.token]
			if h == nil {
				t.Fatalf("v%d: holder missing after restore", ver)
			}
			if h.ref != holder.ref || h.connID != holder.connID {
				t.Fatalf("v%d: holder identity = %q/%d", ver, h.ref, h.connID)
			}
			// v1 predates the field, so it legitimately restores as zero.
			wantAbandoned := holder.abandoned
			if ver == snapshotVer1 {
				wantAbandoned = 0
			}
			if h.abandonedAtNanos != wantAbandoned {
				t.Fatalf("v%d: holder abandonedAtNanos = %d, want %d", ver, h.abandonedAtNanos, wantAbandoned)
			}
			if got := st.waiterCount(); got != 1 {
				t.Fatalf("v%d: waiter count = %d, want 1", ver, got)
			}
			if got := st.Waiters[st.WaiterHead].salt; got != waiterIn.salt {
				t.Fatalf("v%d: waiter salt = %v, want %v", ver, got, waiterIn.salt)
			}
			es := sh.connEnqueued[connKey{ConnID: waiterIn.connID, Key: key}]
			if es == nil || es.waiter == nil {
				t.Fatalf("v%d: enqueued index did not re-link its waiter", ver)
			}
			if es.waiter != st.Waiters[st.WaiterHead] {
				t.Fatalf("v%d: enqueued index linked the wrong waiter", ver)
			}
		})
	}
}

// The current writer's output must round-trip through the current reader with
// every field intact, including the v3 waiter salt in the enqueued index.
func TestSnapshotRoundTripPreservesEnqueuedWaiterIdentity(t *testing.T) {
	const key = "lock:roundtrip"
	src := newEmptyManager()
	st := src.newResourceState(1, time.Unix(0, 1_700_000_000_000))
	first := &waiter{ref: "same-ref", connID: 11, salt: [8]byte{9}, leaseTTL: time.Minute}
	second := &waiter{ref: "same-ref", connID: 11, salt: [8]byte{42}, leaseTTL: time.Minute}
	st.appendWaiter(first)
	st.appendWaiter(second)
	sh := src.shardFor(key)
	sh.resources[key] = st
	src.resourceTotal.Add(1)
	// The index points at the SECOND waiter; only the salt distinguishes them.
	sh.setEnqueued(connKey{ConnID: 11, Key: key}, &enqueuedState{waiter: second, leaseTTL: time.Minute})

	var buf bytes.Buffer
	if err := src.Snapshot(&buf); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	dst := newEmptyManager()
	if err := dst.Restore(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	dsh := dst.shardFor(key)
	es := dsh.connEnqueued[connKey{ConnID: 11, Key: key}]
	if es == nil || es.waiter == nil {
		t.Fatal("enqueued index lost its waiter")
	}
	if es.waiter.salt != second.salt {
		t.Fatalf("enqueued index re-linked salt %v, want %v (wrong waiter of two on one conn)",
			es.waiter.salt, second.salt)
	}
}

func TestRestoreRejectsUnknownSnapshotVersion(t *testing.T) {
	var b bytes.Buffer
	b.WriteString(snapshotMagic)
	b.WriteByte(snapshotVer3 + 1)
	if err := newEmptyManager().Restore(bytes.NewReader(b.Bytes())); err == nil {
		t.Fatal("Restore accepted a future snapshot version")
	}
}
