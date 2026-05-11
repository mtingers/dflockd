package lock

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func FuzzDecodeFenceRecord(f *testing.F) {
	valid := encodeFenceRecord(fenceRecord{seq: 1, ceiling: 42})
	for _, seed := range [][]byte{
		valid[:],
		{},
		[]byte("dflfnc1\n"),
		[]byte("not a fence record"),
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		var buf [fenceRecordSize]byte
		copy(buf[:], data)
		rec, ok := decodeFenceRecord(buf)
		if !ok {
			return
		}
		roundTrip, ok := decodeFenceRecord(encodeFenceRecord(rec))
		if !ok {
			t.Fatal("valid decoded record failed to re-encode")
		}
		if roundTrip != rec {
			t.Fatalf("round trip = %+v, want %+v", roundTrip, rec)
		}
	})
}

func FuzzLockManagerSequentialOps(f *testing.F) {
	for _, seed := range [][]byte{
		{0, 1, 2, 3, 4, 5},
		{0, 0, 1, 1, 2, 2, 3, 3},
		{5, 4, 3, 2, 1, 0},
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 512 {
			t.Skip("operation stream too large for this fuzz target")
		}
		lm := newTestManager(t, true)
		held := make(map[fuzzHeldKey]string)
		for i, b := range data {
			key := fmt.Sprintf("fuzz:%d", b&7)
			connID := uint64((b>>3)&3) + 1
			switch b % 6 {
			case 0:
				fuzzAcquire(t, lm, held, key, connID)
			case 1:
				fuzzRelease(t, lm, held, key, connID, b)
			case 2:
				fuzzRenew(t, lm, held, key, connID, b)
			case 3:
				fuzzEnqueueWait(t, lm, held, key, connID)
			case 4:
				fuzzCleanupConn(t, lm, held, connID)
			case 5:
				assertFuzzStats(t, i, lm.Stats(0))
			}
		}
	})
}

type fuzzHeldKey struct {
	key    string
	connID uint64
}

func fuzzAcquire(t *testing.T, lm *LockManager, held map[fuzzHeldKey]string, key string, connID uint64) {
	t.Helper()
	tok, err := lm.Acquire(context.Background(), key, 0, time.Minute, connID, 1)
	if err != nil {
		t.Fatalf("Acquire(%q, %d): %v", key, connID, err)
	}
	if tok != "" {
		held[fuzzHeldKey{key: key, connID: connID}] = tok
	}
}

func fuzzRelease(t *testing.T, lm *LockManager, held map[fuzzHeldKey]string, key string, connID uint64, salt byte) {
	t.Helper()
	hk := fuzzHeldKey{key: key, connID: connID}
	tok := held[hk]
	if tok == "" {
		tok = fmt.Sprintf("bogus-%d", salt)
	}
	ok, err := lm.Release(key, tok)
	if err != nil {
		t.Fatalf("Release(%q): %v", key, err)
	}
	if ok {
		delete(held, hk)
	}
}

func fuzzRenew(t *testing.T, lm *LockManager, held map[fuzzHeldKey]string, key string, connID uint64, salt byte) {
	t.Helper()
	tok := held[fuzzHeldKey{key: key, connID: connID}]
	if tok == "" {
		tok = fmt.Sprintf("bogus-%d", salt)
	}
	if _, _, err := lm.Renew(key, tok, time.Minute); err != nil {
		t.Fatalf("Renew(%q): %v", key, err)
	}
}

func fuzzEnqueueWait(t *testing.T, lm *LockManager, held map[fuzzHeldKey]string, key string, connID uint64) {
	t.Helper()
	status, tok, _, err := lm.Enqueue(key, time.Minute, connID, 1)
	if err != nil {
		if err == ErrAlreadyEnqueued {
			return
		}
		t.Fatalf("Enqueue(%q, %d): %v", key, connID, err)
	}
	if tok != "" {
		held[fuzzHeldKey{key: key, connID: connID}] = tok
	}
	if status != "queued" {
		return
	}
	tok, _, err = lm.Wait(context.Background(), key, 0, connID)
	if err != nil {
		t.Fatalf("Wait(%q, %d): %v", key, connID, err)
	}
	if tok != "" {
		held[fuzzHeldKey{key: key, connID: connID}] = tok
	}
}

func fuzzCleanupConn(t *testing.T, lm *LockManager, held map[fuzzHeldKey]string, connID uint64) {
	t.Helper()
	if err := lm.CleanupConnection(connID); err != nil {
		t.Fatalf("CleanupConnection(%d): %v", connID, err)
	}
	for hk := range held {
		if hk.connID == connID {
			delete(held, hk)
		}
	}
}

func assertFuzzStats(t *testing.T, step int, st *Stats) {
	t.Helper()
	for _, li := range st.Locks {
		if li.Waiters < 0 || li.LeaseExpiresInS < 0 {
			t.Fatalf("step %d: invalid lock stats %+v", step, li)
		}
	}
	for _, si := range st.Semaphores {
		if si.Waiters < 0 || si.Holders < 0 || si.Limit <= 0 {
			t.Fatalf("step %d: invalid semaphore stats %+v", step, si)
		}
	}
}
