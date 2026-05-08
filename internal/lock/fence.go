package lock

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
)

// DefaultFenceRangeSize is how far ahead the on-disk ceiling is
// pre-allocated past the in-memory counter. One fsync per range; on
// crash recovery, up to this many fence values may be skipped.
const DefaultFenceRangeSize uint64 = 1 << 20

const fenceFileSize = 8

// fenceAllocator hands out strictly-monotonic uint64 fence values.
// When backed by a state file, it pre-allocates ranges to disk
// (single fsync per range) so a new instance after a crash always
// seeds above the highest value the prior instance ever issued.
type fenceAllocator struct {
	counter   atomic.Uint64
	ceiling   atomic.Uint64
	mu        sync.Mutex
	f         *os.File
	rangeSize uint64
}

// newFenceAllocator builds an allocator. stateFile == "" disables
// persistence and seeds from fallbackSeed; otherwise the file is
// opened (or created) and the recovered ceiling is used, floored
// by fallbackSeed for defence-in-depth against stale backups.
func newFenceAllocator(stateFile string, fallbackSeed, rangeSize uint64) (*fenceAllocator, error) {
	if stateFile == "" {
		return newMemFenceAllocator(fallbackSeed), nil
	}
	return newPersistentFenceAllocator(stateFile, fallbackSeed, rangeSize)
}

// newMemFenceAllocator returns an allocator with persistence
// disabled: ceiling is pinned at uint64-max so the slow path is
// never taken.
func newMemFenceAllocator(seed uint64) *fenceAllocator {
	fa := &fenceAllocator{}
	fa.counter.Store(seed)
	fa.ceiling.Store(^uint64(0))
	return fa
}

func newPersistentFenceAllocator(path string, fallbackSeed, rangeSize uint64) (*fenceAllocator, error) {
	f, recovered, err := openFenceFile(path, fallbackSeed)
	if err != nil {
		return nil, err
	}
	return primePersistentAllocator(f, recovered, rangeSize)
}

// primePersistentAllocator builds the allocator and forces an
// initial range extend so first-call latency is paid up front
// rather than mid-grant.
func primePersistentAllocator(f *os.File, recovered, rangeSize uint64) (*fenceAllocator, error) {
	fa := &fenceAllocator{f: f, rangeSize: rangeSize}
	fa.counter.Store(recovered)
	fa.ceiling.Store(recovered)
	if err := fa.extendLocked(recovered); err != nil {
		f.Close()
		return nil, err
	}
	return fa, nil
}

// next returns the next fence value, taking the slow path only
// when the in-memory range is exhausted. Panics if persistence is
// configured and the disk write fails — at that point the fence
// guarantee is unrecoverable.
func (fa *fenceAllocator) next() uint64 {
	n := fa.counter.Add(1)
	if n < fa.ceiling.Load() {
		return n
	}
	return fa.nextSlow(n)
}

// nextSlow extends the on-disk ceiling and returns n. The double-
// check inside the mutex lets goroutines that piled up while
// another extended skip the disk write.
func (fa *fenceAllocator) nextSlow(n uint64) uint64 {
	fa.mu.Lock()
	defer fa.mu.Unlock()
	if n < fa.ceiling.Load() {
		return n
	}
	if err := fa.extendLocked(n); err != nil {
		panic("fence persistence failed: " + err.Error())
	}
	return n
}

// extendLocked bumps the ceiling by rangeSize, persisting the new
// value to disk before publishing it in memory. Caller holds mu.
func (fa *fenceAllocator) extendLocked(target uint64) error {
	newCeiling := target + fa.rangeSize
	if err := fa.persistCeiling(newCeiling); err != nil {
		return err
	}
	fa.ceiling.Store(newCeiling)
	return nil
}

// persistCeiling writes the new ceiling and fsyncs. A no-op when
// persistence is disabled.
func (fa *fenceAllocator) persistCeiling(ceiling uint64) error {
	if fa.f == nil {
		return nil
	}
	return writeCeiling(fa.f, ceiling)
}

// close releases the state file. Safe to call on an in-memory
// allocator (no-op).
func (fa *fenceAllocator) close() error {
	if fa.f == nil {
		return nil
	}
	return fa.f.Close()
}

// openFenceFile opens (or creates) the state file and recovers the
// stored ceiling. Returns max(persisted, fallbackSeed) so a
// restored-from-backup file can't seed below the wall clock.
func openFenceFile(path string, fallbackSeed uint64) (*os.File, uint64, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, 0, fmt.Errorf("open fence state %q: %w", path, err)
	}
	return readRecoveredCeiling(f, fallbackSeed)
}

// readRecoveredCeiling returns the ceiling persisted in f, or
// fallbackSeed if f is empty. Files of unexpected size are
// rejected — fail-closed beats silently corrupting fences.
func readRecoveredCeiling(f *os.File, fallbackSeed uint64) (*os.File, uint64, error) {
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	if info.Size() == 0 {
		return f, fallbackSeed, nil
	}
	return readExistingCeiling(f, info.Size(), fallbackSeed)
}

func readExistingCeiling(f *os.File, size int64, fallbackSeed uint64) (*os.File, uint64, error) {
	if size != fenceFileSize {
		f.Close()
		return nil, 0, fmt.Errorf("fence state file is %d bytes, expected %d", size, fenceFileSize)
	}
	persisted, err := readUint64(f)
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	return f, maxU64(persisted, fallbackSeed), nil
}

func readUint64(f *os.File) (uint64, error) {
	var buf [fenceFileSize]byte
	if _, err := f.ReadAt(buf[:], 0); err != nil {
		return 0, fmt.Errorf("read fence state: %w", err)
	}
	return binary.BigEndian.Uint64(buf[:]), nil
}

// writeCeiling rewrites the file with ceiling and fsyncs. Aligned
// 8-byte writes are atomic on POSIX, so no rename-tempfile dance.
func writeCeiling(f *os.File, ceiling uint64) error {
	var buf [fenceFileSize]byte
	binary.BigEndian.PutUint64(buf[:], ceiling)
	if _, err := f.WriteAt(buf[:], 0); err != nil {
		return fmt.Errorf("write fence state: %w", err)
	}
	return f.Sync()
}

func maxU64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
