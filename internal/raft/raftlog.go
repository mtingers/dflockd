package raft

import (
	"bytes"
	"fmt"
)

// raftLog wraps a Storage with the log queries and mutations the run loop
// needs: the up-to-date check for elections, the consistency check and
// conflict-resolution for AppendEntries, and commit-index tracking. It is
// run-loop-owned (no synchronisation of its own). The "applied" index is
// tracked separately by the node because applying happens off the run
// loop.
type raftLog struct {
	storage   Storage
	committed Index // highest log index known committed
}

// newRaftLog builds a raftLog, recovering committed from persisted state.
// A snapshot can raise the floor, but a commit beyond available durable state
// is corruption and must never be silently clamped down.
func newRaftLog(s Storage) (*raftLog, error) {
	hs, err := s.LoadHardState()
	if err != nil {
		return nil, err
	}
	l := &raftLog{storage: s, committed: hs.CommitIndex}
	if err := l.recoverCommitted(); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *raftLog) recoverCommitted() error {
	if floor := l.firstIndex() - 1; l.committed < floor {
		l.committed = floor
	}
	if l.committed > l.lastIndex() {
		return fmt.Errorf(
			"raft: durable commit index %d exceeds available index %d",
			l.committed, l.lastIndex(),
		)
	}
	return nil
}

func (l *raftLog) firstIndex() Index { return l.storage.FirstIndex() }
func (l *raftLog) lastIndex() Index  { return l.storage.LastIndex() }

// term returns the term of entry i (0 for i==0; the snapshot term for
// i==snapshot index; an error if compacted-away or beyond the log).
func (l *raftLog) term(i Index) (Term, error) { return l.storage.Term(i) }

// termOrZero is term but folds the not-available cases to 0 (used where
// the caller has already bounded i, or treats "no such entry" as term 0).
func (l *raftLog) termOrZero(i Index) Term {
	t, err := l.term(i)
	if err != nil {
		return 0
	}
	return t
}

func (l *raftLog) lastTerm() Term { return l.termOrZero(l.lastIndex()) }

func (l *raftLog) entries(lo, hi Index) ([]Entry, error) { return l.storage.Entries(lo, hi) }

// entriesFrom returns log entries [i, lastIndex], capped at max entries.
func (l *raftLog) entriesFrom(i Index, max int) ([]Entry, error) {
	hi := l.lastIndex() + 1
	if max > 0 && Index(max) < hi-i {
		hi = i + Index(max)
	}
	return l.entries(i, hi)
}

// append durably appends es (which must be contiguous from lastIndex+1).
func (l *raftLog) append(es []Entry) error { return l.storage.Append(es) }

// commitTo raises committed to min(i, lastIndex). Lowering is impossible.
func (l *raftLog) commitTo(i Index) {
	if i <= l.committed {
		return
	}
	if last := l.lastIndex(); i > last {
		i = last
	}
	l.committed = i
}

// matchTerm reports whether the log has an entry at idx whose term is term
// (idx==0 matches term 0 — the empty-prefix sentinel).
func (l *raftLog) matchTerm(idx Index, term Term) bool {
	t, err := l.term(idx)
	return err == nil && t == term
}

// isUpToDate implements the election restriction (Raft §5.4.1): a
// candidate's log is at least as up-to-date as ours when its last term is
// higher, or equal with a last index at least as large.
func (l *raftLog) isUpToDate(candLastIndex Index, candLastTerm Term) bool {
	myLastTerm := l.lastTerm()
	if candLastTerm != myLastTerm {
		return candLastTerm > myLastTerm
	}
	return candLastIndex >= l.lastIndex()
}

// conflictHint returns the back-off hint for an AppendEntries that failed
// its consistency check at prevLogIndex: if we have no entry there, the
// hint is our log-end+1 with term 0; otherwise it's the first index of
// the term that occupies prevLogIndex (so the leader can skip the whole
// bad term in one round).
func (l *raftLog) conflictHint(prevLogIndex Index) (Index, Term) {
	if prevLogIndex > l.lastIndex() {
		return l.lastIndex() + 1, 0
	}
	t, err := l.term(prevLogIndex)
	if err != nil {
		return l.lastIndex() + 1, 0
	}
	return l.firstIndexOfTerm(prevLogIndex, t), t
}

func (l *raftLog) firstIndexOfTerm(at Index, term Term) Index {
	i := at
	for i > l.firstIndex() && l.termOrZero(i-1) == term {
		i--
	}
	return i
}

// appendFromLeader installs entries an AppendEntries delivered, given the
// match already verified at (prevLogIndex, prevLogTerm). It skips any
// leading entries that already match ours, truncates the first divergent
// one and everything after it, then appends the rest. Returns the index
// the follower now agrees with the leader through — prevLogIndex+len(entries)
// — which is what the leader records as that follower's matchIndex. (The
// follower may retain further uncommitted entries past that point; per the
// algorithm those are only truncated when a later RPC carries a
// conflicting entry at their index.) changedFrom is the first replaced or
// newly appended index, or zero when every carried entry already matched.
func (l *raftLog) appendFromLeader(prevLogIndex Index, entries []Entry) (through Index, changedFrom Index, err error) {
	through = prevLogIndex + Index(len(entries))
	keepFrom := l.firstDivergent(prevLogIndex, entries)
	if keepFrom == len(entries) {
		return through, 0, nil // every carried entry already present
	}
	if err := l.truncateAndAppend(entries[keepFrom:]); err != nil {
		return 0, 0, err
	}
	return through, entries[keepFrom].Index, nil
}

// firstDivergent returns the index into entries of the first entry that
// is not already present in our log with the same term (entries[i] sits
// at log index prevLogIndex+1+i).
func (l *raftLog) firstDivergent(prevLogIndex Index, entries []Entry) int {
	for i, e := range entries {
		if !l.matchTerm(prevLogIndex+1+Index(i), e.Term) {
			return i
		}
	}
	return len(entries)
}

func (l *raftLog) truncateAndAppend(tail []Entry) error {
	if err := l.storage.TruncateSuffix(tail[0].Index); err != nil {
		return err
	}
	return l.storage.Append(tail)
}

// installSnapshot persists the snapshot and resets the log to start past
// it (keeping a matching tail if one exists). Returns the new last index.
func (l *raftLog) installSnapshot(meta SnapshotMeta, data []byte) (Index, error) {
	if err := l.storage.SaveSnapshot(meta, bytes.NewReader(data)); err != nil {
		return 0, err
	}
	l.commitTo(meta.LastIncludedIndex)
	return l.lastIndex(), nil
}
