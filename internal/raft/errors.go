package raft

import "errors"

// Sentinel errors returned by Node operations.
var (
	// ErrNotLeader is returned by Propose / ProposeConfChange / ReadIndex
	// when this node is not the leader. Callers should redirect to the
	// node named by Node.LeaderID (which may be empty if unknown).
	ErrNotLeader = errors.New("raft: not leader")

	// ErrLeadershipLost is delivered to a proposal future when the node
	// stepped down before the entry committed. The command may or may not
	// eventually commit under a new leader; the caller must retry.
	ErrLeadershipLost = errors.New("raft: leadership lost")

	// ErrStopped is returned once Close has been called (or the run loop
	// has exited).
	ErrStopped = errors.New("raft: node stopped")

	// ErrConfigChangeInProgress is returned by ProposeConfChange when an
	// earlier configuration change has not yet committed.
	ErrConfigChangeInProgress = errors.New("raft: configuration change in progress")

	// ErrUnknownPeer is returned when a membership change names a node
	// that isn't (for removal) or already is (for addition) a member.
	ErrUnknownPeer = errors.New("raft: unknown peer")

	// ErrTimeout is returned when a context-less wait exceeds an internal
	// deadline (used sparingly; most waits take a context).
	ErrTimeout = errors.New("raft: timeout")
)
