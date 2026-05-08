package lock

import (
	"encoding/binary"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// TestFenceAllocator_InMemory_Monotonic exercises the no-file path:
// values are strictly increasing and seeded from fallbackSeed.
func TestFenceAllocator_InMemory_Monotonic(t *testing.T) {
	fa, err := newFenceAllocator("", 100, 1<<20)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	var prev uint64
	for i := 0; i < 1000; i++ {
		n := mustNextFence(t, fa)
		if n <= prev {
			t.Fatalf("not monotonic: %d <= %d", n, prev)
		}
		prev = n
	}
	if prev <= 100 {
		t.Fatalf("seeded value %d not above seed 100", prev)
	}
}

// TestFenceAllocator_File_PersistsAhead asserts the ceiling on disk
// after the first grant is strictly greater than any issued value —
// the invariant that lets recovery seed safely.
func TestFenceAllocator_File_PersistsAhead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fence")
	fa, err := newFenceAllocator(path, 0, 8)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer fa.close()
	first := mustNextFence(t, fa)
	persisted := readPersistedCeiling(t, path)
	if persisted <= first {
		t.Fatalf("persisted %d should be > issued %d", persisted, first)
	}
}

// TestFenceAllocator_File_RecoveryNeverDuplicates simulates a crash
// (close without flushing the in-memory counter) and asserts the
// next allocator instance never re-issues a value the prior one did.
func TestFenceAllocator_File_RecoveryNeverDuplicates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fence")
	issued := allocateBatch(t, path, 200, 8)
	maxBefore := maxKey(issued)
	more := allocateBatch(t, path, 200, 8)
	for n := range more {
		if _, dup := issued[n]; dup {
			t.Fatalf("recovered instance re-issued %d", n)
		}
		if n <= maxBefore {
			t.Fatalf("recovered value %d <= prior max %d", n, maxBefore)
		}
	}
}

// TestFenceAllocator_File_ExtendsAcrossRange forces many range
// extensions (rangeSize=4, 100 grants -> 25 extends) and confirms
// no value is ever duplicated.
func TestFenceAllocator_File_ExtendsAcrossRange(t *testing.T) {
	fa, err := newFenceAllocator(filepath.Join(t.TempDir(), "fence"), 0, 4)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer fa.close()
	seen := make(map[uint64]struct{})
	for i := 0; i < 100; i++ {
		n := mustNextFence(t, fa)
		if _, dup := seen[n]; dup {
			t.Fatalf("duplicate %d at iteration %d", n, i)
		}
		seen[n] = struct{}{}
	}
}

// TestFenceAllocator_File_Concurrent stresses the slow-path mutex
// with many goroutines and a small range so extends contend.
func TestFenceAllocator_File_Concurrent(t *testing.T) {
	fa, err := newFenceAllocator(filepath.Join(t.TempDir(), "fence"), 0, 16)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer fa.close()
	got := concurrentAllocate(t, fa, 16, 200)
	if len(got) != 16*200 {
		t.Fatalf("expected %d distinct, got %d", 16*200, len(got))
	}
}

// TestFenceAllocator_File_MalformedRejected refuses to start when
// the state file exists but has the wrong size.
func TestFenceAllocator_File_MalformedRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fence")
	if err := os.WriteFile(path, []byte("nope"), 0o600); err != nil {
		t.Fatalf("seed bad file: %v", err)
	}
	if _, err := newFenceAllocator(path, 0, 8); err == nil {
		t.Fatal("expected error on malformed state file")
	}
}

// TestFenceAllocator_File_TornLatestRecordFallsBack exercises the
// journal format: if the newest slot is torn, recovery uses the
// previous valid slot rather than trusting corrupted bytes.
func TestFenceAllocator_File_TornLatestRecordFallsBack(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fence")
	writeRecordAt(t, path, fenceRecord{seq: 1, ceiling: 100})
	writeCorruptRecordAt(t, path, fenceRecordSize, fenceRecord{seq: 2, ceiling: 200})

	fa, err := newFenceAllocator(path, 0, 8)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer fa.close()
	n := mustNextFence(t, fa)
	if n != 101 {
		t.Fatalf("recovered from wrong ceiling: issued %d, want 101", n)
	}
}

func TestFenceAllocator_File_PartialLatestRecordFallsBack(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fence")
	writeRecordAt(t, path, fenceRecord{seq: 1, ceiling: 100})
	writePartialRecordAt(t, path, fenceRecordSize, fenceRecord{seq: 2, ceiling: 200}, 9)

	fa, err := newFenceAllocator(path, 0, 8)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer fa.close()
	n := mustNextFence(t, fa)
	if n != 101 {
		t.Fatalf("recovered from wrong ceiling: issued %d, want 101", n)
	}
}

func TestFenceAllocator_RecordReadIOErrorFailsClosed(t *testing.T) {
	boom := errors.New("boom")
	_, ok, err := readFenceRecord(readAtFunc(func([]byte, int64) (int, error) {
		return 0, boom
	}), 0)
	if ok {
		t.Fatal("record should not be valid")
	}
	if !errors.Is(err, ErrFencePersistence) {
		t.Fatalf("got %v, want ErrFencePersistence", err)
	}
}

// TestFenceAllocator_File_OverflowRejected covers corrupt-but-valid
// records near MaxUint64. Extending such a ceiling would wrap, so
// startup fails closed.
func TestFenceAllocator_File_OverflowRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fence")
	writePersistedCeiling(t, path, math.MaxUint64-1)
	_, err := newFenceAllocator(path, 0, 8)
	if !errors.Is(err, ErrFencePersistence) {
		t.Fatalf("got %v, want ErrFencePersistence", err)
	}
}

// TestFenceAllocator_File_FallbackSeedFloors covers the case where
// the persisted ceiling is below the fallback seed (e.g., backup
// restored onto a machine whose wall clock has moved on). The new
// instance must seed at the higher of the two.
func TestFenceAllocator_File_FallbackSeedFloors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fence")
	writePersistedCeiling(t, path, 50)
	const seed uint64 = 1_000_000
	fa, err := newFenceAllocator(path, seed, 8)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer fa.close()
	n := mustNextFence(t, fa)
	if n <= seed {
		t.Fatalf("issued %d should exceed fallback seed %d", n, seed)
	}
}

func TestFenceAllocator_File_LegacyUint64Migrates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fence")
	writeLegacyCeiling(t, path, 50)
	fa, err := newFenceAllocator(path, 0, 8)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer fa.close()
	if n := mustNextFence(t, fa); n != 51 {
		t.Fatalf("issued %d, want 51", n)
	}
	if size := fenceFileSize(t, path); size != fenceRecordSize {
		t.Fatalf("state file size = %d, want %d after migration", size, fenceRecordSize)
	}
}

func TestFenceAllocator_File_ExclusiveLock(t *testing.T) {
	if !fenceFileLocksSupported {
		t.Skip("platform has no fence file lock implementation")
	}
	path := filepath.Join(t.TempDir(), "fence")
	fa, err := newFenceAllocator(path, 0, 8)
	if err != nil {
		t.Fatalf("first new: %v", err)
	}
	defer fa.close()
	if _, err := newFenceAllocator(path, 0, 8); err == nil {
		t.Fatal("expected second allocator on same state file to fail")
	}
}

func TestFenceAllocator_Close_Idempotent(t *testing.T) {
	fa, err := newFenceAllocator(filepath.Join(t.TempDir(), "fence"), 0, 8)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := fa.close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := fa.close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func mustNextFence(t *testing.T, fa *fenceAllocator) uint64 {
	t.Helper()
	n, err := fa.next()
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	return n
}

func allocateBatch(t *testing.T, path string, count int, rangeSize uint64) map[uint64]struct{} {
	t.Helper()
	fa, err := newFenceAllocator(path, 0, rangeSize)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	out := make(map[uint64]struct{}, count)
	for i := 0; i < count; i++ {
		out[mustNextFence(t, fa)] = struct{}{}
	}
	if err := fa.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return out
}

func concurrentAllocate(t *testing.T, fa *fenceAllocator, workers, perWorker int) map[uint64]struct{} {
	t.Helper()
	out := make(map[uint64]struct{}, workers*perWorker)
	var mu sync.Mutex
	var wg sync.WaitGroup
	var errOnce sync.Once
	var gotErr error
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				n, err := fa.next()
				if err != nil {
					errOnce.Do(func() { gotErr = err })
					return
				}
				mu.Lock()
				out[n] = struct{}{}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if gotErr != nil {
		t.Fatalf("next: %v", gotErr)
	}
	return out
}

func readPersistedCeiling(t *testing.T, path string) uint64 {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open persisted: %v", err)
	}
	defer f.Close()
	rec, err := readFenceState(f, 0)
	if err != nil {
		t.Fatalf("read persisted: %v", err)
	}
	return rec.ceiling
}

func writePersistedCeiling(t *testing.T, path string, value uint64) {
	t.Helper()
	writeRecordAt(t, path, fenceRecord{seq: 1, ceiling: value})
}

func writeRecordAt(t *testing.T, path string, rec fenceRecord) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatalf("open persisted: %v", err)
	}
	defer f.Close()
	if err := writeFenceRecord(f, rec); err != nil {
		t.Fatalf("write persisted: %v", err)
	}
}

func writeCorruptRecordAt(t *testing.T, path string, offset int64, rec fenceRecord) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatalf("open persisted: %v", err)
	}
	defer f.Close()
	buf := encodeFenceRecord(rec)
	buf[16] ^= 0xff
	if _, err := f.WriteAt(buf[:], offset); err != nil {
		t.Fatalf("write corrupt: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("sync corrupt: %v", err)
	}
}

func writePartialRecordAt(t *testing.T, path string, offset int64, rec fenceRecord, n int) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatalf("open persisted: %v", err)
	}
	defer f.Close()
	buf := encodeFenceRecord(rec)
	if _, err := f.WriteAt(buf[:n], offset); err != nil {
		t.Fatalf("write partial: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("sync partial: %v", err)
	}
}

type readAtFunc func([]byte, int64) (int, error)

func (fn readAtFunc) ReadAt(p []byte, off int64) (int, error) {
	return fn(p, off)
}

func writeLegacyCeiling(t *testing.T, path string, value uint64) {
	t.Helper()
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], value)
	if err := os.WriteFile(path, buf[:], 0o600); err != nil {
		t.Fatalf("write legacy: %v", err)
	}
}

func fenceFileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat fence file: %v", err)
	}
	return info.Size()
}

func maxKey(m map[uint64]struct{}) uint64 {
	var max uint64
	for k := range m {
		if k > max {
			max = k
		}
	}
	return max
}
