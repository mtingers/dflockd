package raft

import (
	"fmt"
	"time"
)

// Config holds the tunables of a Raft node. Use DefaultConfig and adjust
// from there; Validate enforces the invariants the algorithm needs
// (notably HeartbeatInterval << ElectionTimeoutMin <= ElectionTimeoutMax).
type Config struct {
	// ID is this node's stable identifier. Required.
	ID NodeID

	// HeartbeatInterval is how often a leader sends AppendEntries to each
	// follower (empty ones count as heartbeats). Must be much smaller than
	// ElectionTimeoutMin so a healthy leader is never timed out.
	HeartbeatInterval time.Duration

	// ElectionTimeoutMin / ElectionTimeoutMax bound the randomized
	// election timeout a follower waits before starting a (pre-)vote.
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration

	// MaxAppendEntries caps how many log entries a leader sends in one
	// AppendEntries RPC.
	MaxAppendEntries int

	// SnapshotThresholdEntries triggers a snapshot once the applied log
	// has at least this many entries beyond the last snapshot. 0 disables
	// entry-count-triggered snapshots.
	SnapshotThresholdEntries uint64

	// MaxSnapshotBytes bounds the FSM payload of a snapshot. It may be lowered
	// by an application but cannot exceed the package's wire-safe limit.
	MaxSnapshotBytes int

	// ApplyChanDepth is the buffer between the run loop and the apply
	// goroutine.
	ApplyChanDepth int

	// PreVote enables the PreVote phase (recommended): a partitioned node
	// confirms it could win before incrementing its term, so rejoining
	// it doesn't force an unnecessary election.
	PreVote bool
}

// DefaultConfig returns a Config tuned for a low-latency LAN. ID must
// still be set by the caller.
func DefaultConfig() Config {
	return Config{
		HeartbeatInterval:        100 * time.Millisecond,
		ElectionTimeoutMin:       600 * time.Millisecond,
		ElectionTimeoutMax:       900 * time.Millisecond,
		MaxAppendEntries:         256,
		SnapshotThresholdEntries: 8192,
		MaxSnapshotBytes:         maxSnapshotDataBytes,
		ApplyChanDepth:           256,
		PreVote:                  true,
	}
}

// configValidators is the canonical list of single-purpose checks. Order
// is not load-bearing.
var configValidators = []func(*Config) error{
	validateID,
	validateHeartbeat,
	validateElectionRange,
	validateHeartbeatVsElection,
	validateMaxAppendEntries,
	validateMaxSnapshotBytes,
	validateApplyChanDepth,
}

// Validate reports the first invariant Config violates, or nil.
func (c *Config) Validate() error {
	for _, v := range configValidators {
		if err := v(c); err != nil {
			return err
		}
	}
	return nil
}

func validateID(c *Config) error {
	if c.ID == "" {
		return fmt.Errorf("raft: Config.ID is required")
	}
	if len(c.ID) > maxRPCNodeIDBytes {
		return fmt.Errorf("raft: Config.ID length %d exceeds max %d", len(c.ID), maxRPCNodeIDBytes)
	}
	return nil
}

func validateHeartbeat(c *Config) error {
	if c.HeartbeatInterval <= 0 {
		return fmt.Errorf("raft: HeartbeatInterval must be > 0")
	}
	return nil
}

func validateElectionRange(c *Config) error {
	if c.ElectionTimeoutMin <= 0 || c.ElectionTimeoutMax < c.ElectionTimeoutMin {
		return fmt.Errorf("raft: need 0 < ElectionTimeoutMin (%v) <= ElectionTimeoutMax (%v)",
			c.ElectionTimeoutMin, c.ElectionTimeoutMax)
	}
	return nil
}

func validateHeartbeatVsElection(c *Config) error {
	if c.HeartbeatInterval*3 > c.ElectionTimeoutMin {
		return fmt.Errorf("raft: HeartbeatInterval (%v) too large vs ElectionTimeoutMin (%v); want roughly 10x smaller",
			c.HeartbeatInterval, c.ElectionTimeoutMin)
	}
	return nil
}

func validateMaxAppendEntries(c *Config) error {
	if c.MaxAppendEntries <= 0 {
		return fmt.Errorf("raft: MaxAppendEntries must be > 0")
	}
	return nil
}

func validateMaxSnapshotBytes(c *Config) error {
	if c.MaxSnapshotBytes <= 0 {
		return fmt.Errorf("raft: MaxSnapshotBytes must be > 0")
	}
	if c.MaxSnapshotBytes > maxSnapshotDataBytes {
		return fmt.Errorf("raft: MaxSnapshotBytes %d exceeds wire-safe max %d", c.MaxSnapshotBytes, maxSnapshotDataBytes)
	}
	return nil
}

func validateApplyChanDepth(c *Config) error {
	if c.ApplyChanDepth <= 0 {
		return fmt.Errorf("raft: ApplyChanDepth must be > 0")
	}
	return nil
}
