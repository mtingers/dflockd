package raft

import (
	"sync/atomic"
	"time"
)

// Counters tracks monotonic cluster-mode counters: proposal volume,
// apply throughput, and leader-change count. Every field is updated
// with atomic adds from arbitrary goroutines and read with atomic
// loads. Zero value is ready to use; nil-receiver methods are no-ops
// so call sites can be unconditional.
type Counters struct {
	proposals       atomic.Uint64
	proposalsFailed atomic.Uint64
	applies         atomic.Uint64
	appliesFailed   atomic.Uint64
	applyNanosTotal atomic.Uint64
	leaderChanges   atomic.Uint64
}

// CountersSnapshot is a point-in-time read of every counter. Safe to
// pass by value.
type CountersSnapshot struct {
	Proposals       uint64
	ProposalsFailed uint64
	Applies         uint64
	AppliesFailed   uint64
	ApplyNanosTotal uint64
	LeaderChanges   uint64
}

// IncProposals records one successful Propose / ProposeConfChange /
// Barrier call (the call returned without ErrNotLeader / ErrStopped /
// validation error — it actually entered the run loop).
func (c *Counters) IncProposals() {
	if c == nil {
		return
	}
	c.proposals.Add(1)
}

// IncProposalsFailed records a Propose call that errored before the
// entry could be appended (not-leader at submit time, ctx cancellation,
// validation rejection).
func (c *Counters) IncProposalsFailed() {
	if c == nil {
		return
	}
	c.proposalsFailed.Add(1)
}

// IncApply records one successful FSM apply. dur is the wall time the
// apply took (used for an average-latency derived metric).
func (c *Counters) IncApply(dur time.Duration) {
	if c == nil {
		return
	}
	c.applies.Add(1)
	if dur > 0 {
		c.applyNanosTotal.Add(uint64(dur.Nanoseconds()))
	}
}

// IncApplyFailed records one FSM apply that returned an error. Today
// this is only used for cluster-command decode/validate failures; the
// FSM itself does not return Go errors.
func (c *Counters) IncApplyFailed() {
	if c == nil {
		return
	}
	c.appliesFailed.Add(1)
}

// IncLeaderChange records one transition into the leader role for this
// node. Counts only this node's transitions — not cluster-wide leader
// changes — so summing across nodes gives the cluster-wide total.
func (c *Counters) IncLeaderChange() {
	if c == nil {
		return
	}
	c.leaderChanges.Add(1)
}

// Snapshot returns the current counter values as a flat struct. Each
// field is read with its own atomic Load, so the snapshot may not be
// consistent across fields under a concurrent burst — that's expected
// for Prometheus-style scrape semantics.
func (c *Counters) Snapshot() CountersSnapshot {
	if c == nil {
		return CountersSnapshot{}
	}
	return CountersSnapshot{
		Proposals:       c.proposals.Load(),
		ProposalsFailed: c.proposalsFailed.Load(),
		Applies:         c.applies.Load(),
		AppliesFailed:   c.appliesFailed.Load(),
		ApplyNanosTotal: c.applyNanosTotal.Load(),
		LeaderChanges:   c.leaderChanges.Load(),
	}
}

// Counters returns this node's counter handle (always non-nil after
// NewNode). The handle is safe to use after Close.
func (n *Node) Counters() *Counters { return n.counters }

// ClusterMetrics is the snapshot the consensus layer + the cluster
// orchestration layer hand to operators (the HTTP /metrics handler).
// It bundles raft counters with cluster-orchestration counters
// (admin add/remove ops) so the metrics handler doesn't need to
// import the cluster package.
type ClusterMetrics struct {
	Raft              CountersSnapshot
	AdminAddVoter     uint64
	AdminAddVoterFail uint64
	AdminRemoveServer uint64
	AdminRemoveFail   uint64
}
