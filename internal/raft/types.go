package raft

import "fmt"

// Term is a Raft term: a monotonically increasing logical clock that
// advances on every election. Term 0 is the pre-genesis sentinel.
type Term uint64

// Index is a 1-based position in the replicated log. Index 0 is the
// sentinel "before the first entry".
type Index uint64

// NodeID is a stable identifier for a cluster member. It never changes
// for the life of a node and is distinct from the node's network
// address (which can change).
type NodeID string

// EntryType tags a log entry's payload so the run loop and the FSM can
// tell application commands apart from internal bookkeeping.
type EntryType uint8

const (
	// EntryNoOp is the empty entry a new leader appends at the start of
	// its term to discover the commit index and serve as a barrier. The
	// FSM is not asked to apply it.
	EntryNoOp EntryType = iota
	// EntryNormal carries an opaque application command. The FSM applies
	// it and the result resolves the proposer's future.
	EntryNormal
	// EntryConfig carries a marshalled Configuration. The run loop adopts
	// it on append (not on commit); the FSM is not asked to apply it.
	EntryConfig
)

func (t EntryType) String() string {
	switch t {
	case EntryNoOp:
		return "noop"
	case EntryNormal:
		return "normal"
	case EntryConfig:
		return "config"
	default:
		return fmt.Sprintf("entrytype(%d)", uint8(t))
	}
}

// Entry is one record in the replicated log. Index and Term together
// uniquely identify it; once an entry is committed at a given index it
// is never overwritten.
type Entry struct {
	Index Index
	Term  Term
	Type  EntryType
	Data  []byte
}

// HardState is the subset of a node's state that must survive a crash:
// it is fsync'd before any RPC reply that depends on it. CommitIndex is
// included as an optimisation (recoverable from a quorum, but cheap to
// persist) so a restarted node can re-apply without waiting for a leader.
type HardState struct {
	CurrentTerm Term
	VotedFor    NodeID // empty == voted for no one this term
	CommitIndex Index
}

// Configuration is the set of voting members of the cluster. dflockd's
// Raft supports only single-server changes, so exactly one member is
// added or removed per EntryConfig.
type Configuration struct {
	Voters map[NodeID]string // node id -> raft transport address
}

// Clone returns a deep copy so callers can mutate it without disturbing
// the run loop's copy.
func (c Configuration) Clone() Configuration {
	out := Configuration{Voters: make(map[NodeID]string, len(c.Voters))}
	for id, addr := range c.Voters {
		out.Voters[id] = addr
	}
	return out
}

// Has reports whether id is a voting member.
func (c Configuration) Has(id NodeID) bool {
	_, ok := c.Voters[id]
	return ok
}

// Quorum is the number of votes that constitutes a majority of the
// current configuration.
func (c Configuration) Quorum() int {
	return len(c.Voters)/2 + 1
}

// IDs returns the member ids in no particular order.
func (c Configuration) IDs() []NodeID {
	out := make([]NodeID, 0, len(c.Voters))
	for id := range c.Voters {
		out = append(out, id)
	}
	return out
}

// SnapshotMeta describes a persisted FSM snapshot: the log position it
// summarises and the cluster configuration in effect at that point.
type SnapshotMeta struct {
	LastIncludedIndex Index
	LastIncludedTerm  Term
	Configuration     Configuration
}
