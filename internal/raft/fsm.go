package raft

import "io"

// FSM is the application state machine driven by the replicated log. The
// raft.Node feeds it committed entries in index order from a dedicated
// goroutine — implementations therefore do NOT need internal locking for
// the Apply path, but must guarantee that Snapshot returns a self-
// contained view that Persist can serialize without further locking
// against concurrent Apply calls (typically by copying out the state
// under whatever locks the FSM already maintains for its non-raft
// callers).
//
// Apply is the only path that may mutate FSM state.
type FSM interface {
	// Apply ingests one committed entry. The returned value is delivered
	// to the proposer's Future (if any). Apply must be a pure function of
	// (current state, entry) — no time.Now, no rand, no map-iteration-
	// order-dependent output — so every node converges to the same state.
	Apply(e Entry) any
	// Snapshot returns a point-in-time view that Persist can encode
	// concurrently with subsequent Apply calls.
	Snapshot() (FSMSnapshot, error)
	// Restore loads state from r, replacing whatever the FSM currently
	// holds. r contains exactly what an earlier Snapshot's Persist wrote.
	Restore(r io.Reader) error
}

// FSMSnapshot is a single in-flight snapshot of FSM state.
type FSMSnapshot interface {
	// Persist writes the snapshot's serialized form to w.
	Persist(w io.Writer) error
	// Release lets the FSM reclaim any resources held by this snapshot.
	// Called exactly once after Persist returns (success or failure).
	Release()
}

// noopFSM is a no-state FSM used when a node has no application — its
// Apply is a no-op, its Snapshot is empty, its Restore reads nothing.
type noopFSM struct{}

// NewNoopFSM returns an FSM that does nothing. Used by raft-only tests
// that exercise consensus without an application.
func NewNoopFSM() FSM { return noopFSM{} }

// Apply implements FSM. No-op.
func (noopFSM) Apply(Entry) any { return nil }

// Snapshot implements FSM. Returns an empty noopFSMSnapshot.
func (noopFSM) Snapshot() (FSMSnapshot, error) { return noopFSMSnapshot{}, nil }

// Restore implements FSM. Drains and discards r.
func (noopFSM) Restore(r io.Reader) error { _, err := io.Copy(io.Discard, r); return err }

type noopFSMSnapshot struct{}

// Persist implements FSMSnapshot. No-op.
func (noopFSMSnapshot) Persist(io.Writer) error { return nil }

// Release implements FSMSnapshot. No-op.
func (noopFSMSnapshot) Release() {}
