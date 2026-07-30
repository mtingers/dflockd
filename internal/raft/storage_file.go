package raft

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// FileStorage is the durable Storage: a directory holding a write-ahead
// log file, a two-slot HardState journal, and a snapshots/ subdirectory.
// The directory is exclusive-locked (flock) for the storage's lifetime so
// two dflockd processes can't share one --raft-dir. The whole
// post-snapshot log is kept in memory (it is bounded by the snapshot
// threshold and dflockd's entries are small), with durable write-through.
//
// Like raft.Node's other collaborators, FileStorage assumes single-
// threaded access from the run loop.
type FileStorage struct {
	memLog
	dir      string
	lockFile *os.File // flock holder for the directory; kept open
	wal      *walFile
	hs       *hardStateFile
	snaps    snapshotStore
	hard     HardState

	snapshotMu sync.Mutex // serializes canonical snapshot-file writers
}

const (
	walFileName       = "wal"
	hardStateFileName = "hardstate"
	dirLockFileName   = ".lock"
	snapshotsSubdir   = "snapshots"
	storageDirPerm    = 0o700
)

var _ Storage = (*FileStorage)(nil)
var _ asyncSnapshotStorage = (*FileStorage)(nil)

type fileSnapshotPreparation struct {
	owner     *FileStorage
	meta      SnapshotMeta
	walPath   string
	nextIndex Index
}

func (*fileSnapshotPreparation) isPreparedSnapshot() {}

// OpenFileStorage opens (creating if absent) a durable Storage rooted at
// dir. It fails if the platform lacks exclusive file locking or if
// another process already holds dir.
func OpenFileStorage(dir string) (*FileStorage, error) {
	if !fileLocksSupported {
		return nil, fmt.Errorf("raft: exclusive file locking unsupported on this platform; --raft-dir requires a Unix-like OS")
	}
	if err := os.MkdirAll(dir, storageDirPerm); err != nil {
		return nil, fmt.Errorf("raft: mkdir %s: %w", dir, err)
	}
	_ = fsyncDir(dir) // best effort: make the new dir's own dirent durable
	lf, err := acquireDirLock(dir)
	if err != nil {
		return nil, err
	}
	return openFileStorageLocked(dir, lf)
}

func acquireDirLock(dir string) (*os.File, error) {
	lf, err := os.OpenFile(filepath.Join(dir, dirLockFileName), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("raft: open lock file in %s: %w", dir, err)
	}
	if err := lockFile(lf); err != nil {
		lf.Close()
		return nil, fmt.Errorf("raft: %s is locked by another process: %w", dir, err)
	}
	return lf, nil
}

func openFileStorageLocked(dir string, lf *os.File) (*FileStorage, error) {
	s := &FileStorage{dir: dir, lockFile: lf, snaps: snapshotStore{dir: filepath.Join(dir, snapshotsSubdir)}}
	if err := s.loadAll(); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

// loadAll restores the in-memory state from disk. HardState is loaded before
// the WAL so a malformed WAL suffix can be checked against the durable commit
// index before any bytes are truncated.
func (s *FileStorage) loadAll() error {
	if err := s.loadSnapshotMeta(); err != nil {
		return err
	}
	if err := s.loadHardState(); err != nil {
		return err
	}
	return s.loadWAL()
}

func (s *FileStorage) loadSnapshotMeta() error {
	meta, _, ok, err := s.snaps.loadLatest()
	if err != nil {
		return err
	}
	if ok {
		s.memLog.snap = meta
	}
	return nil
}

func (s *FileStorage) loadWAL() error {
	path := filepath.Join(s.dir, walFileName)
	replay, err := inspectWAL(path)
	if err != nil {
		return err
	}
	kept, err := s.validateWALReplay(replay)
	if err != nil {
		return err
	}
	w, err := openWAL(path, replay)
	if err != nil {
		return err
	}
	s.wal = w
	s.memLog.entries = kept
	return nil
}

// validateWALReplay proves both structural continuity and recovery safety.
// In particular, a malformed suffix is discardable only when snapshot+valid
// WAL still contain every index at or below HardState.CommitIndex.
func (s *FileStorage) validateWALReplay(replay walReplay) ([]Entry, error) {
	kept := entriesAfter(replay.entries, s.memLog.snap.LastIncludedIndex)
	if len(kept) > 0 {
		if err := checkContiguous(kept, s.memLog.firstIndex()); err != nil {
			return nil, fmt.Errorf("raft: WAL inconsistent with snapshot: %w", err)
		}
	}
	available := s.memLog.snap.LastIncludedIndex
	lastTerm := s.memLog.snap.LastIncludedTerm
	for _, entry := range kept {
		if entry.Term < lastTerm {
			return nil, fmt.Errorf("raft: WAL term regressed at index %d from %d to %d", entry.Index, lastTerm, entry.Term)
		}
		lastTerm = entry.Term
		available = entry.Index
	}
	if s.hard.CommitIndex > available {
		if replay.tailErr != nil {
			return nil, fmt.Errorf(
				"raft: WAL corruption removed committed index %d (available through %d): %w",
				s.hard.CommitIndex, available, replay.tailErr,
			)
		}
		return nil, fmt.Errorf(
			"raft: durable commit index %d exceeds available log/snapshot index %d",
			s.hard.CommitIndex, available,
		)
	}
	if s.hard.CurrentTerm < lastTerm {
		return nil, fmt.Errorf(
			"raft: durable term %d is behind log/snapshot term %d",
			s.hard.CurrentTerm, lastTerm,
		)
	}
	return kept, nil
}

func entriesAfter(entries []Entry, thru Index) []Entry {
	for i, e := range entries {
		if e.Index > thru {
			return entries[i:]
		}
	}
	return nil
}

func (s *FileStorage) loadHardState() error {
	hf, hs, err := openHardStateFile(filepath.Join(s.dir, hardStateFileName))
	if err != nil {
		return err
	}
	s.hs, s.hard = hf, hs
	return nil
}

// --- Storage interface ---
// These methods implement raft.Storage. See the interface in
// storage.go for contract documentation; per-method comments here
// only call out file-backed specifics.

// LoadHardState implements raft.Storage.
func (s *FileStorage) LoadHardState() (HardState, error) { return s.hard, nil }

// SaveHardState implements raft.Storage. Persists via the
// checksummed two-slot HardState file (fsync'd) before returning;
// only on success does the in-memory copy advance.
func (s *FileStorage) SaveHardState(hs HardState) error {
	if err := s.hs.save(hs); err != nil {
		return err
	}
	s.hard = hs
	return nil
}

// FirstIndex implements raft.Storage.
func (s *FileStorage) FirstIndex() Index { return s.firstIndex() }

// LastIndex implements raft.Storage.
func (s *FileStorage) LastIndex() Index { return s.lastIndex() }

// Term implements raft.Storage.
func (s *FileStorage) Term(i Index) (Term, error) { return s.term(i) }

// Entries implements raft.Storage.
func (s *FileStorage) Entries(lo, hi Index) ([]Entry, error) { return s.slice(lo, hi) }

// Append implements raft.Storage. Durably appends entries to the WAL
// (one fsync per call) and then to the in-memory shadow log; either
// both happen or neither does.
func (s *FileStorage) Append(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	if err := checkContiguous(entries, s.lastIndex()+1); err != nil {
		return err
	}
	if err := s.wal.appendEntries(entries); err != nil {
		return err
	}
	s.memLog.entries = append(s.memLog.entries, entries...)
	return nil
}

// TruncateSuffix implements raft.Storage. It builds the retained log without
// mutating the in-memory view, atomically rewrites the WAL, then publishes the
// new view. A failed rewrite therefore leaves both memory and disk unchanged.
func (s *FileStorage) TruncateSuffix(from Index) error {
	if from > s.lastIndex() {
		return nil
	}
	if from < s.firstIndex() {
		return fmt.Errorf("raft: truncateSuffix(%d) cuts into snapshot at %d", from, s.memLog.snap.LastIncludedIndex)
	}
	keep := from - s.firstIndex()
	retained := append([]Entry(nil), s.memLog.entries[:keep]...)
	if err := s.wal.rewrite(retained); err != nil {
		return err
	}
	s.memLog.entries = retained
	return nil
}

// SaveSnapshot implements raft.Storage. Writes the snapshot file
// atomically (tmp + rename + fsync-dir), updates the in-memory
// metadata, then rewrites the WAL so log entries at or before the
// snapshot index are dropped.
func (s *FileStorage) SaveSnapshot(meta SnapshotMeta, data io.Reader) error {
	fsm, err := readSnapshotData(data)
	if err != nil {
		return err
	}
	s.snapshotMu.Lock()
	err = s.snaps.save(meta, fsm)
	s.snapshotMu.Unlock()
	if err != nil {
		return err
	}
	s.memLog.applySnapshot(meta)
	return s.wal.rewrite(s.memLog.entries)
}

// prepareSnapshot writes the new snapshot generation and a compacted WAL
// candidate without touching the live memLog or WAL handle.
func (s *FileStorage) prepareSnapshot(meta SnapshotMeta, data []byte, tail []Entry) (preparedSnapshot, error) {
	if len(data) > maxSnapshotDataBytes {
		return nil, fmt.Errorf("raft: snapshot data exceeds %d bytes", maxSnapshotDataBytes)
	}
	if len(tail) > 0 {
		if err := checkContiguous(tail, meta.LastIncludedIndex+1); err != nil {
			return nil, fmt.Errorf("raft: prepare snapshot tail: %w", err)
		}
	}
	if err := s.writePreparedSnapshot(meta, data); err != nil {
		return nil, err
	}
	path, err := writePreparedWAL(s.dir, tail)
	if err != nil {
		return nil, err
	}
	next := meta.LastIncludedIndex + 1
	if len(tail) > 0 {
		next = tail[len(tail)-1].Index + 1
	}
	return &fileSnapshotPreparation{owner: s, meta: meta, walPath: path, nextIndex: next}, nil
}

func (s *FileStorage) writePreparedSnapshot(meta SnapshotMeta, data []byte) error {
	s.snapshotMu.Lock()
	defer s.snapshotMu.Unlock()
	current, _, ok, err := s.snaps.loadLatest()
	if err != nil {
		return err
	}
	if ok && current.LastIncludedIndex > meta.LastIncludedIndex {
		return errSnapshotSuperseded
	}
	if ok && current.LastIncludedIndex == meta.LastIncludedIndex {
		if current.LastIncludedTerm != meta.LastIncludedTerm {
			return fmt.Errorf("raft: snapshot index %d has conflicting terms %d and %d",
				meta.LastIncludedIndex, current.LastIncludedTerm, meta.LastIncludedTerm)
		}
		currentData, err := s.snaps.load(current)
		if err != nil {
			return err
		}
		if !configurationsEqual(current.Configuration, meta.Configuration) || !bytes.Equal(currentData, data) {
			return fmt.Errorf("raft: snapshot generation %d/%d has conflicting metadata or data",
				meta.LastIncludedIndex, meta.LastIncludedTerm)
		}
		return nil
	}
	_, err = s.snaps.write(meta, data)
	return err
}

func writePreparedWAL(dir string, entries []Entry) (path string, err error) {
	f, err := os.CreateTemp(dir, ".wal-snapshot-*")
	if err != nil {
		return "", fmt.Errorf("create prepared wal: %w", err)
	}
	path = f.Name()
	defer func() {
		if err != nil {
			_ = os.Remove(path)
		}
	}()
	err = f.Chmod(walFilePerm)
	if err == nil {
		err = writeAllThenSync(f, encodeWALRecords(nil, entries))
	}
	err = errOr(err, f.Close())
	if err != nil {
		return "", fmt.Errorf("write prepared wal: %w", err)
	}
	return path, nil
}

func (s *FileStorage) commitPreparedSnapshot(prepared preparedSnapshot, delta []Entry) error {
	p, ok := prepared.(*fileSnapshotPreparation)
	if !ok || p.owner != s {
		return fmt.Errorf("raft: invalid prepared snapshot")
	}
	if len(delta) > 0 {
		if err := checkContiguous(delta, p.nextIndex); err != nil {
			return fmt.Errorf("raft: commit snapshot delta: %w", err)
		}
	}
	if err := appendPreparedWAL(p.walPath, delta); err != nil {
		return err
	}
	if err := s.wal.replacePrepared(p.walPath); err != nil {
		return err
	}
	p.walPath = ""
	s.memLog.applySnapshot(p.meta)
	return s.snaps.deleteAllExcept(snapshotName(p.meta))
}

func appendPreparedWAL(path string, entries []Entry) (err error) {
	if len(entries) == 0 {
		return nil
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, walFilePerm)
	if err != nil {
		return fmt.Errorf("open prepared wal: %w", err)
	}
	defer func() { err = errOr(err, f.Close()) }()
	if err = writeAllThenSync(f, encodeWALRecords(nil, entries)); err != nil {
		return fmt.Errorf("append prepared wal: %w", err)
	}
	return nil
}

func (s *FileStorage) abortPreparedSnapshot(prepared preparedSnapshot) {
	p, ok := prepared.(*fileSnapshotPreparation)
	if ok && p.owner == s && p.walPath != "" {
		_ = os.Remove(p.walPath)
		p.walPath = ""
	}
}

// readSnapshotData reads the FSM bytes, refusing anything larger than
// maxSnapshotDataBytes (a corrupt InstallSnapshot payload, or an FSM
// that has outgrown what we can persist/transfer).
func readSnapshotData(r io.Reader) ([]byte, error) {
	fsm, err := io.ReadAll(io.LimitReader(r, int64(maxSnapshotDataBytes)+1))
	if err != nil {
		return nil, fmt.Errorf("raft: read snapshot data: %w", err)
	}
	if len(fsm) > maxSnapshotDataBytes {
		return nil, fmt.Errorf("raft: snapshot data exceeds %d bytes", maxSnapshotDataBytes)
	}
	return fsm, nil
}

// SnapshotMeta implements raft.Storage.
func (s *FileStorage) SnapshotMeta() (SnapshotMeta, bool) {
	if !s.hasSnapshot() {
		return SnapshotMeta{}, false
	}
	return s.memLog.snap, true
}

// OpenSnapshot implements raft.Storage. The whole snapshot payload is
// read into memory (dflockd's state is small); the returned reader is
// a bytes.Reader wrapped in NopCloser.
func (s *FileStorage) OpenSnapshot() (io.ReadCloser, error) {
	if !s.hasSnapshot() {
		return nil, ErrNoSnapshot
	}
	fsm, err := s.snaps.load(s.memLog.snap)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(fsm)), nil
}

// Close implements raft.Storage. Releases the WAL handle, the
// HardState handle, and the --raft-dir advisory lock. Idempotent —
// later calls return the first error seen.
func (s *FileStorage) Close() error {
	var err error
	if s.wal != nil {
		err = errOr(err, s.wal.close())
	}
	if s.hs != nil {
		err = errOr(err, s.hs.close())
	}
	if s.lockFile != nil {
		_ = unlockFile(s.lockFile)
		err = errOr(err, s.lockFile.Close())
		s.lockFile = nil
	}
	return err
}

func errOr(a, b error) error {
	if a != nil {
		return a
	}
	return b
}
