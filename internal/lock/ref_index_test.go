package lock

import (
	"strconv"
	"testing"
	"time"
)

func TestRefIndexDisabledWithoutOrphanTTL(t *testing.T) {
	lm := newApplyTestLM(t)
	now := applyNow()
	const key = "lock:k"
	_, _, _ = lm.ApplyAcquire(now, key, 1, "holder", epochA|1, time.Minute, [8]byte{1})
	_, _, _ = lm.ApplyAcquire(now, key, 1, "waiter", epochA|2, time.Minute, [8]byte{2})

	sh := lm.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	st := sh.resources[key]
	if st.indexRefs || st.refs != nil {
		t.Fatalf("ref index enabled with OrphanTTL=0: enabled=%v refs=%v", st.indexRefs, st.refs)
	}
}

func TestRefIndexPreservesDuplicateWaiterFIFO(t *testing.T) {
	lm := managerWithCfg(t, reattachCfg())
	now := applyNow()
	const key = "lock:k"
	if _, _, err := lm.ApplyAcquire(now, key, 1, "holder", epochA|1, time.Minute, [8]byte{1}); err != nil {
		t.Fatalf("holder acquire: %v", err)
	}
	if got, _, err := lm.ApplyAcquire(now, key, 1, "worker", epochA|2, time.Minute, [8]byte{2}); err != nil || got.Status != StatusQueued {
		t.Fatalf("first waiter = %+v, %v; want queued", got, err)
	}
	if got, _, err := lm.ApplyAcquire(now, key, 1, "worker", epochA|3, time.Minute, [8]byte{3}); err != nil || got.Status != StatusQueued {
		t.Fatalf("second waiter = %+v, %v; want queued", got, err)
	}

	got, _, err := lm.ApplyAcquire(now, key, 1, "worker", epochB|1, time.Minute, [8]byte{4})
	if err != nil || got.Status != StatusQueued {
		t.Fatalf("re-attach = %+v, %v; want queued", got, err)
	}

	sh := lm.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	state := sh.resources[key].refs["worker"]
	if state == nil || len(state.waiters) != 2 {
		t.Fatal("worker waiter index is missing")
	}
	if state.waiters[0].connID != epochB|1 || state.waiters[0].salt != ([8]byte{2}) {
		t.Fatalf("indexed head = conn %x salt %v; want re-attached first waiter", state.waiters[0].connID, state.waiters[0].salt)
	}
	if state.waiters[1].connID != epochA|3 || state.waiters[1].salt != ([8]byte{3}) {
		t.Fatalf("indexed tail = conn %x salt %v; want untouched second waiter", state.waiters[1].connID, state.waiters[1].salt)
	}
}

func TestRefIndexChoosesSmallestReclaimableHolderToken(t *testing.T) {
	lm := managerWithCfg(t, reattachCfg())
	now := applyNow()
	const key = "sem:k"
	first, _, err := lm.ApplyAcquire(now, key, 2, "worker", epochA|1, time.Minute, [8]byte{1})
	if err != nil || first.Status != StatusOK {
		t.Fatalf("first acquire = %+v, %v", first, err)
	}
	second, _, err := lm.ApplyAcquire(now, key, 2, "worker", epochA|2, time.Minute, [8]byte{2})
	if err != nil || second.Status != StatusOK {
		t.Fatalf("second acquire = %+v, %v", second, err)
	}
	if first.Token >= second.Token {
		t.Fatalf("tokens not monotonic: %q >= %q", first.Token, second.Token)
	}

	got, _, err := lm.ApplyAcquire(now, key, 2, "worker", epochB|1, time.Minute, [8]byte{3})
	if err != nil || got.Status != StatusOK {
		t.Fatalf("re-attach = %+v, %v", got, err)
	}
	if got.Token != first.Token {
		t.Fatalf("re-attached token = %q, want smallest %q", got.Token, first.Token)
	}
}

func TestRefIndexTracksPromotionAndRemoval(t *testing.T) {
	lm := managerWithCfg(t, reattachCfg())
	now := applyNow()
	const key = "lock:k"
	holder, _, _ := lm.ApplyAcquire(now, key, 1, "holder", epochA|1, time.Minute, [8]byte{1})
	_, _, _ = lm.ApplyAcquire(now, key, 1, "waiter", epochA|2, time.Minute, [8]byte{2})

	_, grants, err := lm.ApplyRelease(now, key, holder.Token)
	if err != nil || len(grants) != 1 {
		t.Fatalf("release grants = %+v, %v; want one", grants, err)
	}
	assertRefState(t, lm, key, "holder", false, false)
	assertRefState(t, lm, key, "waiter", true, false)

	if _, _, err := lm.ApplyRelease(now, key, grants[0].Token); err != nil {
		t.Fatalf("release promoted holder: %v", err)
	}
	assertRefState(t, lm, key, "waiter", false, false)
}

func assertRefState(t *testing.T, lm *LockManager, key, ref string, holder, waiter bool) {
	t.Helper()
	sh := lm.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	state := sh.resources[key].refs[ref]
	if state == nil {
		if holder || waiter {
			t.Fatalf("ref %q index missing", ref)
		}
		return
	}
	if got := len(state.holders) > 0; got != holder {
		t.Fatalf("ref %q holder indexed = %v, want %v", ref, got, holder)
	}
	if got := len(state.waiters) > 0; got != waiter {
		t.Fatalf("ref %q waiter indexed = %v, want %v", ref, got, waiter)
	}
}

func BenchmarkFindWaiterByRefDeepQueue(b *testing.B) {
	const queueDepth = 100_000
	st := &ResourceState{Holders: make(map[string]*holder), indexRefs: true}
	var target *waiter
	for i := 0; i < queueDepth; i++ {
		w := &waiter{ref: "ref-" + strconv.Itoa(i), connID: epochA | uint64(i+1)}
		st.appendWaiter(w)
		if i == queueDepth-1 {
			target = w
		}
	}
	ref := target.ref
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got := findWaiterByRef(st, ref, epochB|1); got != target {
			b.Fatalf("findWaiterByRef returned %p, want %p", got, target)
		}
	}
}
