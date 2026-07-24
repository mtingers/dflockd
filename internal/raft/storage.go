package raft

import (
	"errors"
	"fmt"
	"io"
)

// Storage persists the parts of a Raft node that must survive a crash —
// the HardState, the replicated log, and the latest FSM snapshot. Write
// methods (SaveHardState, Append, TruncateSuffix, SaveSnapshot) must
// flush durably (fsync) before returning; a returned error means nothing
// was persisted and the node treats it as a hard fault.
//
// Index conventions: the log is 1-based. With a persisted snapshot at
// index S, the stored log "starts" logically at S+1 — FirstIndex returns
// S+1, Term(S) returns the snapshot's term even though entry S is no
// longer stored, and an empty-after-compaction log reports
// LastIndex == S (so the next append index is S+1).
type Storage interface {
	// LoadHardState returns the persisted HardState, or the zero value if
	// none has been saved yet.
	LoadHardState() (HardState, error)
	// SaveHardState persists hs durably.
	SaveHardState(hs HardState) error

	// FirstIndex is the index one past the snapshot (1 if no snapshot).
	FirstIndex() Index
	// LastIndex is the index of the last stored entry, or the snapshot
	// index (or 0) if the log is empty.
	LastIndex() Index
	// Term returns the term of entry i. i==0 yields 0; i==snapshot index
	// yields the snapshot term; i<FirstIndex-1 yields ErrCompacted;
	// i>LastIndex yields ErrUnavailable.
	Term(i Index) (Term, error)
	// Entries returns log entries in [lo, hi). ErrCompacted if lo is at or
	// before the snapshot; ErrUnavailable if hi exceeds LastIndex+1.
	Entries(lo, hi Index) ([]Entry, error)
	// Append durably appends entries; they must be contiguous and start at
	// LastIndex()+1.
	Append(entries []Entry) error
	// TruncateSuffix durably drops entries [from, LastIndex]. It is a
	// no-op if from > LastIndex; it errors if from would cut into the
	// snapshot.
	TruncateSuffix(from Index) error

	// SaveSnapshot persists meta + the bytes read from data, then drops
	// any log entries at or before meta.LastIncludedIndex (keeping a
	// contiguous tail if one exists at that index/term).
	SaveSnapshot(meta SnapshotMeta, data io.Reader) error
	// SnapshotMeta returns the persisted snapshot's metadata; ok is false
	// if no snapshot has been saved.
	SnapshotMeta() (SnapshotMeta, bool)
	// OpenSnapshot opens the persisted snapshot's payload for reading.
	// ErrNoSnapshot if there is none.
	OpenSnapshot() (io.ReadCloser, error)

	// Close releases any held resources (file handles, the directory
	// lock). Idempotent.
	Close() error
}

// Storage sentinel errors.
var (
	// ErrCompacted means the requested index is at or before the latest
	// snapshot and is no longer available from the log.
	ErrCompacted = errors.New("raft: requested index is compacted")
	// ErrUnavailable means the requested index is beyond the last log entry.
	ErrUnavailable = errors.New("raft: requested index is unavailable")
	// ErrNoSnapshot means no snapshot has been persisted.
	ErrNoSnapshot = errors.New("raft: no snapshot")
	// ErrNonContiguous means an Append's entries are not contiguous or do
	// not start at LastIndex+1.
	ErrNonContiguous = errors.New("raft: non-contiguous append")

	// errSnapshotSuperseded is internal control flow for an asynchronous
	// local snapshot overtaken by a newer installed snapshot.
	errSnapshotSuperseded = errors.New("raft: snapshot superseded")
)

// preparedSnapshot is an opaque, storage-owned snapshot preparation.
// Implementations create it off the Raft loop and finalize it on the loop.
type preparedSnapshot interface{ isPreparedSnapshot() }

// asyncSnapshotStorage is an optional optimization for local snapshots.
// prepareSnapshot performs expensive durable I/O without mutating live log
// state. commitPreparedSnapshot atomically publishes the prepared generation
// after the Node supplies entries appended while preparation was in flight.
type asyncSnapshotStorage interface {
	prepareSnapshot(meta SnapshotMeta, data []byte, tail []Entry) (preparedSnapshot, error)
	commitPreparedSnapshot(preparedSnapshot, []Entry) error
	abortPreparedSnapshot(preparedSnapshot)
}

// ---------------------------------------------------------------------------
// memLog — the in-memory log + snapshot bookkeeping shared by MemStorage and
// FileStorage. Both keep the whole post-snapshot log in memory (dflockd's
// state and the inter-snapshot log are small); FileStorage layers durable
// write-through on top. memLog itself does no synchronisation; the enclosing
// Storage value is the synchronisation boundary (raft callers serialise).
// ---------------------------------------------------------------------------

type memLog struct {
	// entries are contiguous; entries[k].Index == snap.LastIncludedIndex+1+k.
	entries []Entry
	// snap is the latest snapshot meta. LastIncludedIndex==0 means none.
	snap SnapshotMeta
}

func (l *memLog) hasSnapshot() bool { return l.snap.LastIncludedIndex > 0 }

func (l *memLog) firstIndex() Index { return l.snap.LastIncludedIndex + 1 }

func (l *memLog) lastIndex() Index {
	if len(l.entries) == 0 {
		return l.snap.LastIncludedIndex
	}
	return l.entries[len(l.entries)-1].Index
}

func (l *memLog) term(i Index) (Term, error) {
	if i == 0 {
		return 0, nil
	}
	if i == l.snap.LastIncludedIndex {
		return l.snap.LastIncludedTerm, nil
	}
	if i < l.firstIndex() {
		return 0, ErrCompacted
	}
	if i > l.lastIndex() {
		return 0, ErrUnavailable
	}
	return l.entries[i-l.firstIndex()].Term, nil
}

func (l *memLog) slice(lo, hi Index) ([]Entry, error) {
	if err := l.checkRange(lo, hi); err != nil {
		return nil, err
	}
	if lo >= hi {
		return nil, nil
	}
	off := lo - l.firstIndex()
	out := make([]Entry, hi-lo)
	copy(out, l.entries[off:off+Index(len(out))])
	return out, nil
}

func (l *memLog) checkRange(lo, hi Index) error {
	if lo < l.firstIndex() {
		return ErrCompacted
	}
	if hi > l.lastIndex()+1 {
		return ErrUnavailable
	}
	return nil
}

func (l *memLog) append(es []Entry) error {
	if len(es) == 0 {
		return nil
	}
	if err := checkContiguous(es, l.lastIndex()+1); err != nil {
		return err
	}
	l.entries = append(l.entries, es...)
	return nil
}

func checkContiguous(es []Entry, wantFirst Index) error {
	if es[0].Index != wantFirst {
		return fmt.Errorf("%w: first index %d, want %d", ErrNonContiguous, es[0].Index, wantFirst)
	}
	for k := 1; k < len(es); k++ {
		if es[k].Index != es[k-1].Index+1 {
			return fmt.Errorf("%w: index %d follows %d", ErrNonContiguous, es[k].Index, es[k-1].Index)
		}
	}
	return nil
}

func (l *memLog) truncateSuffix(from Index) error {
	if from < l.firstIndex() {
		return fmt.Errorf("raft: truncateSuffix(%d) cuts into snapshot at %d", from, l.snap.LastIncludedIndex)
	}
	if from > l.lastIndex() {
		return nil
	}
	keep := from - l.firstIndex()
	l.entries = append(l.entries[:0:0], l.entries[:keep]...)
	return nil
}

// applySnapshot installs meta as the new snapshot point. If an entry at
// meta.LastIncludedIndex with meta.LastIncludedTerm is present, the tail
// after it is kept; otherwise the whole log is dropped (a forward jump
// from a leader's InstallSnapshot).
func (l *memLog) applySnapshot(meta SnapshotMeta) {
	tail := l.tailAfterSnapshot(meta)
	l.entries = tail
	l.snap = meta
}

func (l *memLog) tailAfterSnapshot(meta SnapshotMeta) []Entry {
	t, err := l.term(meta.LastIncludedIndex)
	if err != nil || t != meta.LastIncludedTerm || meta.LastIncludedIndex > l.lastIndex() {
		return nil
	}
	off := meta.LastIncludedIndex + 1 - l.firstIndex()
	return append([]Entry(nil), l.entries[off:]...)
}
