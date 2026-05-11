package raft

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
}

const (
	walFileName       = "wal"
	hardStateFileName = "hardstate"
	dirLockFileName   = ".lock"
	snapshotsSubdir   = "snapshots"
	storageDirPerm    = 0o700
)

var _ Storage = (*FileStorage)(nil)

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

// loadAll restores the in-memory state from disk: snapshot meta, then the
// WAL entries that postdate it, then the HardState.
func (s *FileStorage) loadAll() error {
	if err := s.loadSnapshotMeta(); err != nil {
		return err
	}
	if err := s.loadWAL(); err != nil {
		return err
	}
	return s.loadHardState()
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
	w, entries, err := openWAL(filepath.Join(s.dir, walFileName))
	if err != nil {
		return err
	}
	s.wal = w
	return s.adoptWALEntries(entries)
}

// adoptWALEntries keeps only the WAL entries past the snapshot point and
// verifies they form a contiguous run starting at snapshotIndex+1.
func (s *FileStorage) adoptWALEntries(entries []Entry) error {
	kept := entriesAfter(entries, s.memLog.snap.LastIncludedIndex)
	if len(kept) == 0 {
		return nil
	}
	if err := checkContiguous(kept, s.memLog.firstIndex()); err != nil {
		return fmt.Errorf("raft: WAL inconsistent with snapshot: %w", err)
	}
	s.memLog.entries = kept
	return nil
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

func (s *FileStorage) LoadHardState() (HardState, error) { return s.hard, nil }

func (s *FileStorage) SaveHardState(hs HardState) error {
	if err := s.hs.save(hs); err != nil {
		return err
	}
	s.hard = hs
	return nil
}

func (s *FileStorage) FirstIndex() Index          { return s.firstIndex() }
func (s *FileStorage) LastIndex() Index           { return s.lastIndex() }
func (s *FileStorage) Term(i Index) (Term, error) { return s.term(i) }

func (s *FileStorage) Entries(lo, hi Index) ([]Entry, error) { return s.slice(lo, hi) }

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

func (s *FileStorage) TruncateSuffix(from Index) error {
	if from > s.lastIndex() {
		return nil
	}
	if err := s.truncateSuffix(from); err != nil {
		return err
	}
	return s.wal.rewrite(s.memLog.entries)
}

func (s *FileStorage) SaveSnapshot(meta SnapshotMeta, data io.Reader) error {
	fsm, err := readSnapshotData(data)
	if err != nil {
		return err
	}
	if err := s.snaps.save(meta, fsm); err != nil {
		return err
	}
	s.memLog.applySnapshot(meta)
	return s.wal.rewrite(s.memLog.entries)
}

// readSnapshotData reads the FSM bytes, refusing anything larger than
// maxSnapshotFileBytes (a corrupt InstallSnapshot payload, or an FSM
// that has outgrown what we can persist/transfer).
func readSnapshotData(r io.Reader) ([]byte, error) {
	fsm, err := io.ReadAll(io.LimitReader(r, maxSnapshotFileBytes+1))
	if err != nil {
		return nil, fmt.Errorf("raft: read snapshot data: %w", err)
	}
	if len(fsm) > maxSnapshotFileBytes {
		return nil, fmt.Errorf("raft: snapshot data exceeds %d bytes", maxSnapshotFileBytes)
	}
	return fsm, nil
}

func (s *FileStorage) SnapshotMeta() (SnapshotMeta, bool) {
	if !s.hasSnapshot() {
		return SnapshotMeta{}, false
	}
	return s.memLog.snap, true
}

func (s *FileStorage) OpenSnapshot() (io.ReadCloser, error) {
	if !s.hasSnapshot() {
		return nil, ErrNoSnapshot
	}
	_, fsm, ok, err := s.snaps.loadLatest()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrNoSnapshot
	}
	return io.NopCloser(bytes.NewReader(fsm)), nil
}

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
