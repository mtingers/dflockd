package raft

import (
	"bytes"
	"fmt"
	"io"
)

// MemStorage is an in-memory Storage. It persists nothing — a "restart"
// of a node backed by MemStorage starts from scratch — so it is for tests
// and for nodes that explicitly want non-durable consensus. It is safe
// for the single-threaded use raft.Node makes of it; concurrent callers
// must add their own mutex.
type MemStorage struct {
	memLog
	hard     HardState
	snapData []byte
}

// NewMemStorage returns an empty in-memory Storage.
func NewMemStorage() *MemStorage { return &MemStorage{} }

var _ Storage = (*MemStorage)(nil)

// The following methods implement raft.Storage. See storage.go for
// contract documentation.

// LoadHardState implements raft.Storage.
func (m *MemStorage) LoadHardState() (HardState, error) { return m.hard, nil }

// SaveHardState implements raft.Storage. In-memory: no disk write.
func (m *MemStorage) SaveHardState(hs HardState) error {
	m.hard = hs
	return nil
}

// FirstIndex implements raft.Storage.
func (m *MemStorage) FirstIndex() Index { return m.firstIndex() }

// LastIndex implements raft.Storage.
func (m *MemStorage) LastIndex() Index { return m.lastIndex() }

// Term implements raft.Storage.
func (m *MemStorage) Term(i Index) (Term, error) { return m.term(i) }

// Entries implements raft.Storage.
func (m *MemStorage) Entries(lo, hi Index) ([]Entry, error) { return m.slice(lo, hi) }

// Append implements raft.Storage.
func (m *MemStorage) Append(entries []Entry) error { return m.append(entries) }

// TruncateSuffix implements raft.Storage.
func (m *MemStorage) TruncateSuffix(from Index) error { return m.truncateSuffix(from) }

// SaveSnapshot implements raft.Storage. Reads data fully into memory
// (tests only use small payloads) and updates the snapshot bookkeeping
// in-place.
func (m *MemStorage) SaveSnapshot(meta SnapshotMeta, data io.Reader) error {
	if _, err := encodeRPCConfig(meta.Configuration); err != nil {
		return fmt.Errorf("raft: snapshot config: %w", err)
	}
	raw, err := io.ReadAll(io.LimitReader(data, int64(maxSnapshotDataBytes)+1))
	if err != nil {
		return err
	}
	if len(raw) > maxSnapshotDataBytes {
		return fmt.Errorf("raft: snapshot data exceeds %d bytes", maxSnapshotDataBytes)
	}
	m.snapData = raw
	m.applySnapshot(meta)
	return nil
}

// SnapshotMeta implements raft.Storage.
func (m *MemStorage) SnapshotMeta() (SnapshotMeta, bool) {
	if !m.hasSnapshot() {
		return SnapshotMeta{}, false
	}
	return m.snap, true
}

// OpenSnapshot implements raft.Storage.
func (m *MemStorage) OpenSnapshot() (io.ReadCloser, error) {
	if !m.hasSnapshot() {
		return nil, ErrNoSnapshot
	}
	return io.NopCloser(bytes.NewReader(m.snapData)), nil
}

// Close implements raft.Storage. In-memory: no-op.
func (m *MemStorage) Close() error { return nil }
