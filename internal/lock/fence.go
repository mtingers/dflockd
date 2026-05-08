package lock

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc64"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// DefaultFenceRangeSize is how far ahead the on-disk ceiling is
// pre-allocated past the in-memory counter. One fsync per range; on
// crash recovery, up to this many fence values may be skipped.
const DefaultFenceRangeSize uint64 = 1 << 20

const (
	legacyFenceFileSize = 8
	fenceRecordSize     = 32
	fenceJournalSlots   = 2
	fenceJournalSize    = fenceRecordSize * fenceJournalSlots
)

var (
	ErrFencePersistence = errors.New("fence persistence failed")
	fenceRecordMagic    = [8]byte{'d', 'f', 'l', 'f', 'n', 'c', '1', '\n'}
	fenceCRCTable       = crc64.MakeTable(crc64.ISO)
)

// fenceAllocator hands out strictly-monotonic uint64 fence values.
// When backed by a state file, it pre-allocates ranges to disk
// (single fsync per range) so a new instance after a crash always
// seeds above the highest value the prior instance ever issued.
type fenceAllocator struct {
	counter   atomic.Uint64
	ceiling   atomic.Uint64
	closed    atomic.Bool
	mu        sync.Mutex
	f         *os.File
	recordSeq uint64
	rangeSize uint64
}

type fenceRecord struct {
	seq     uint64
	ceiling uint64
}

// newFenceAllocator builds an allocator. stateFile == "" disables
// persistence and seeds from fallbackSeed; otherwise the file is
// opened (or created) and the recovered ceiling is used, floored
// by fallbackSeed for defence-in-depth against stale backups.
func newFenceAllocator(stateFile string, fallbackSeed, rangeSize uint64) (*fenceAllocator, error) {
	if stateFile == "" {
		return newMemFenceAllocator(fallbackSeed), nil
	}
	if rangeSize == 0 {
		return nil, fmt.Errorf("%w: range size must be > 0", ErrFencePersistence)
	}
	return newPersistentFenceAllocator(stateFile, fallbackSeed, rangeSize)
}

// newMemFenceAllocator returns an allocator with persistence
// disabled: ceiling is pinned at uint64-max so the slow path is
// never taken.
func newMemFenceAllocator(seed uint64) *fenceAllocator {
	fa := &fenceAllocator{}
	fa.counter.Store(seed)
	fa.ceiling.Store(math.MaxUint64)
	return fa
}

func newPersistentFenceAllocator(path string, fallbackSeed, rangeSize uint64) (*fenceAllocator, error) {
	f, recovered, err := openFenceFile(path, fallbackSeed)
	if err != nil {
		return nil, err
	}
	return primePersistentAllocator(path, f, recovered, rangeSize)
}

// primePersistentAllocator builds the allocator and forces an
// initial range extend so first-call latency is paid up front
// rather than mid-grant.
func primePersistentAllocator(path string, f *os.File, recovered fenceRecord, rangeSize uint64) (*fenceAllocator, error) {
	fa := &fenceAllocator{f: f, recordSeq: recovered.seq, rangeSize: rangeSize}
	fa.counter.Store(recovered.ceiling)
	fa.ceiling.Store(recovered.ceiling)
	if err := fa.extendLocked(recovered.ceiling); err != nil {
		_ = fa.close()
		return nil, err
	}
	if err := syncParentDir(path); err != nil {
		_ = fa.close()
		return nil, fmt.Errorf("%w: sync fence state directory: %v", ErrFencePersistence, err)
	}
	return fa, nil
}

// next returns the next fence value. The hot path is two atomic
// loads and a CAS; the slow path persists a new ceiling under mu.
// The closed check lives in the slow path only — fast-path values
// after Close are still strictly below the already-persisted
// ceiling, so a subsequent instance seeded from disk strictly
// exceeds them.
func (fa *fenceAllocator) next() (uint64, error) {
	n, err := fa.bumpCounter()
	if err != nil {
		return 0, err
	}
	if n < fa.ceiling.Load() {
		return n, nil
	}
	if err := fa.nextSlow(n); err != nil {
		return 0, err
	}
	return n, nil
}

// bumpCounter is the standard CAS-loop increment, refusing the
// uint64 wrap explicitly. Add(1) is unsafe here: a wrap from
// MaxUint64 to 0 silently produces a fence value far below the
// in-memory ceiling, breaking lex monotonicity.
func (fa *fenceAllocator) bumpCounter() (uint64, error) {
	for {
		cur := fa.counter.Load()
		if cur == math.MaxUint64 {
			return 0, fmt.Errorf("%w: counter exhausted", ErrFencePersistence)
		}
		if fa.counter.CompareAndSwap(cur, cur+1) {
			return cur + 1, nil
		}
	}
}

// nextSlow extends the on-disk ceiling. The double-check inside the
// mutex lets goroutines that piled up while another extended skip
// the disk write.
func (fa *fenceAllocator) nextSlow(n uint64) error {
	fa.mu.Lock()
	defer fa.mu.Unlock()
	if fa.closed.Load() {
		return fmt.Errorf("%w: allocator is closed", ErrFencePersistence)
	}
	if n < fa.ceiling.Load() {
		return nil
	}
	return fa.extendLocked(n)
}

// extendLocked bumps the ceiling by rangeSize, persisting the new
// value to disk before publishing it in memory. Caller holds mu.
func (fa *fenceAllocator) extendLocked(target uint64) error {
	newCeiling, err := checkedCeiling(target, fa.rangeSize)
	if err != nil {
		return err
	}
	if err := fa.persistCeiling(newCeiling); err != nil {
		return err
	}
	fa.ceiling.Store(newCeiling)
	return nil
}

func checkedCeiling(target, rangeSize uint64) (uint64, error) {
	if rangeSize == 0 || target > math.MaxUint64-rangeSize {
		return 0, fmt.Errorf("%w: counter would overflow", ErrFencePersistence)
	}
	return target + rangeSize, nil
}

// persistCeiling writes the new ceiling and fsyncs. A no-op when
// persistence is disabled.
func (fa *fenceAllocator) persistCeiling(ceiling uint64) error {
	if fa.f == nil {
		return nil
	}
	return fa.writeRecord(ceiling)
}

func (fa *fenceAllocator) writeRecord(ceiling uint64) error {
	if fa.recordSeq == math.MaxUint64 {
		return fmt.Errorf("%w: journal sequence exhausted", ErrFencePersistence)
	}
	rec := fenceRecord{seq: fa.recordSeq + 1, ceiling: ceiling}
	if err := writeFenceRecord(fa.f, rec); err != nil {
		return err
	}
	fa.recordSeq = rec.seq
	return nil
}

// close releases the state file. Safe to call multiple times; safe
// on an allocator without persistence.
func (fa *fenceAllocator) close() error {
	fa.mu.Lock()
	defer fa.mu.Unlock()
	if fa.f == nil {
		return nil
	}
	f := fa.f
	fa.f = nil
	fa.closed.Store(true)
	unlockErr := unlockFenceFile(f)
	closeErr := f.Close()
	return errors.Join(unlockErr, closeErr)
}

// openFenceFile opens (or creates) the state file, takes an
// exclusive advisory lock, and recovers the stored ceiling. Returns
// max(persisted, fallbackSeed) so a restored-from-backup file can't
// seed below the wall clock.
func openFenceFile(path string, fallbackSeed uint64) (*os.File, fenceRecord, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fenceRecord{}, fmt.Errorf("open fence state %q: %w", path, err)
	}
	if err := lockFenceFile(f); err != nil {
		f.Close()
		return nil, fenceRecord{}, fmt.Errorf("%w: lock fence state %q: %v", ErrFencePersistence, path, err)
	}
	return readRecoveredCeiling(f, fallbackSeed)
}

// readRecoveredCeiling returns the highest valid journal record, a
// legacy raw uint64 ceiling, or fallbackSeed if f is empty. Files of
// unexpected shape are rejected fail-closed.
func readRecoveredCeiling(f *os.File, fallbackSeed uint64) (*os.File, fenceRecord, error) {
	rec, err := readFenceState(f, fallbackSeed)
	if err != nil {
		_ = unlockFenceFile(f)
		f.Close()
		return nil, fenceRecord{}, err
	}
	return f, rec, nil
}

func readFenceState(f *os.File, fallbackSeed uint64) (fenceRecord, error) {
	info, err := f.Stat()
	if err != nil {
		return fenceRecord{}, err
	}
	switch size := info.Size(); size {
	case 0:
		return fenceRecord{ceiling: fallbackSeed}, nil
	case legacyFenceFileSize:
		return readLegacyCeiling(f, fallbackSeed)
	case fenceRecordSize, fenceJournalSize:
		return readJournalCeiling(f, size, fallbackSeed)
	default:
		return fenceRecord{}, fmt.Errorf("%w: state file is %d bytes, expected %d, %d, or %d",
			ErrFencePersistence, size, legacyFenceFileSize, fenceRecordSize, fenceJournalSize)
	}
}

func readLegacyCeiling(f *os.File, fallbackSeed uint64) (fenceRecord, error) {
	var buf [legacyFenceFileSize]byte
	if _, err := f.ReadAt(buf[:], 0); err != nil {
		return fenceRecord{}, fmt.Errorf("%w: read legacy fence state: %v", ErrFencePersistence, err)
	}
	return fenceRecord{ceiling: maxU64(binary.BigEndian.Uint64(buf[:]), fallbackSeed)}, nil
}

func readJournalCeiling(f *os.File, size int64, fallbackSeed uint64) (fenceRecord, error) {
	records := validFenceRecords(f, size)
	if len(records) == 0 {
		return fenceRecord{}, fmt.Errorf("%w: no valid fence journal records", ErrFencePersistence)
	}
	rec := latestFenceRecord(records)
	rec.ceiling = maxU64(rec.ceiling, fallbackSeed)
	return rec, nil
}

func validFenceRecords(f *os.File, size int64) []fenceRecord {
	var out []fenceRecord
	for slot := int64(0); slot < fenceJournalSlots && slot*fenceRecordSize < size; slot++ {
		if rec, ok := readFenceRecord(f, slot*fenceRecordSize); ok {
			out = append(out, rec)
		}
	}
	return out
}

func latestFenceRecord(records []fenceRecord) fenceRecord {
	latest := records[0]
	for _, rec := range records[1:] {
		if rec.seq > latest.seq {
			latest = rec
		}
	}
	return latest
}

func readFenceRecord(f *os.File, offset int64) (fenceRecord, bool) {
	var buf [fenceRecordSize]byte
	if _, err := f.ReadAt(buf[:], offset); err != nil {
		return fenceRecord{}, false
	}
	return decodeFenceRecord(buf)
}

func decodeFenceRecord(buf [fenceRecordSize]byte) (fenceRecord, bool) {
	if string(buf[:8]) != string(fenceRecordMagic[:]) {
		return fenceRecord{}, false
	}
	got := binary.BigEndian.Uint64(buf[24:32])
	if got != fenceChecksum(buf[:24]) {
		return fenceRecord{}, false
	}
	return fenceRecord{
		seq:     binary.BigEndian.Uint64(buf[8:16]),
		ceiling: binary.BigEndian.Uint64(buf[16:24]),
	}, true
}

func writeFenceRecord(f *os.File, rec fenceRecord) error {
	buf := encodeFenceRecord(rec)
	offset := int64((rec.seq - 1) % fenceJournalSlots * fenceRecordSize)
	n, err := f.WriteAt(buf[:], offset)
	if err != nil {
		return fmt.Errorf("%w: write fence state: %v", ErrFencePersistence, err)
	}
	if n != len(buf) {
		return fmt.Errorf("%w: write fence state: %v", ErrFencePersistence, io.ErrShortWrite)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("%w: sync fence state: %v", ErrFencePersistence, err)
	}
	return nil
}

func encodeFenceRecord(rec fenceRecord) [fenceRecordSize]byte {
	var buf [fenceRecordSize]byte
	copy(buf[:8], fenceRecordMagic[:])
	binary.BigEndian.PutUint64(buf[8:16], rec.seq)
	binary.BigEndian.PutUint64(buf[16:24], rec.ceiling)
	binary.BigEndian.PutUint64(buf[24:32], fenceChecksum(buf[:24]))
	return buf
}

func fenceChecksum(data []byte) uint64 {
	return crc64.Checksum(data, fenceCRCTable)
}

func syncParentDir(path string) error {
	dir := filepath.Dir(path)
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func maxU64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
