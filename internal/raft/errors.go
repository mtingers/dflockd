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

	// ErrNotStarted is returned when an operation is submitted before Start.
	ErrNotStarted = errors.New("raft: node not started")

	// ErrAlreadyStarted is returned by a duplicate Start call.
	ErrAlreadyStarted = errors.New("raft: node already started")

	// ErrEntryTooLarge is returned before a proposal can be appended when its
	// payload exceeds the durable and wire codec limit.
	ErrEntryTooLarge = errors.New("raft: entry too large")

	// ErrConfigChangeInProgress is returned by ProposeConfChange when an
	// earlier configuration change has not yet committed.
	ErrConfigChangeInProgress = errors.New("raft: configuration change in progress")

	// ErrAlreadyVoter is returned when an addition names a current voter.
	ErrAlreadyVoter = errors.New("raft: already a voter")

	// ErrUnknownPeer is returned when a removal names a non-member.
	ErrUnknownPeer = errors.New("raft: unknown peer")

	// ErrLastVoter is returned when a removal would leave no voters.
	ErrLastVoter = errors.New("raft: cannot remove the last voter")

	// ErrTimeout is returned when a context-less wait exceeds an internal
	// deadline (used sparingly; most waits take a context).
	ErrTimeout = errors.New("raft: timeout")
)
