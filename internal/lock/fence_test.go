package lock

import (
	"encoding/binary"
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
		n := fa.next()
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
	first := fa.next()
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
// extensions (rangeSize=4, 100 grants → 25 extends) and confirms
// no value is ever duplicated.
func TestFenceAllocator_File_ExtendsAcrossRange(t *testing.T) {
	fa, err := newFenceAllocator(filepath.Join(t.TempDir(), "fence"), 0, 4)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer fa.close()
	seen := make(map[uint64]struct{})
	for i := 0; i < 100; i++ {
		n := fa.next()
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
	got := concurrentAllocate(fa, 16, 200)
	if len(got) != 16*200 {
		t.Fatalf("expected %d distinct, got %d", 16*200, len(got))
	}
}

// TestFenceAllocator_File_MalformedRejected refuses to start when
// the state file exists but has the wrong size — better fail-closed
// than silently treat garbage as a recovered ceiling.
func TestFenceAllocator_File_MalformedRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fence")
	if err := os.WriteFile(path, []byte("nope"), 0o600); err != nil {
		t.Fatalf("seed bad file: %v", err)
	}
	if _, err := newFenceAllocator(path, 0, 8); err == nil {
		t.Fatal("expected error on malformed state file")
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
	n := fa.next()
	if n <= seed {
		t.Fatalf("issued %d should exceed fallback seed %d", n, seed)
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func allocateBatch(t *testing.T, path string, count int, rangeSize uint64) map[uint64]struct{} {
	t.Helper()
	fa, err := newFenceAllocator(path, 0, rangeSize)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	out := make(map[uint64]struct{}, count)
	for i := 0; i < count; i++ {
		out[fa.next()] = struct{}{}
	}
	if err := fa.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return out
}

func concurrentAllocate(fa *fenceAllocator, workers, perWorker int) map[uint64]struct{} {
	out := make(map[uint64]struct{}, workers*perWorker)
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				n := fa.next()
				mu.Lock()
				out[n] = struct{}{}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return out
}

func readPersistedCeiling(t *testing.T, path string) uint64 {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read persisted: %v", err)
	}
	if len(data) != 8 {
		t.Fatalf("persisted file is %d bytes, expected 8", len(data))
	}
	return binary.BigEndian.Uint64(data)
}

func writePersistedCeiling(t *testing.T, path string, value uint64) {
	t.Helper()
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], value)
	if err := os.WriteFile(path, buf[:], 0o600); err != nil {
		t.Fatalf("write persisted: %v", err)
	}
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
