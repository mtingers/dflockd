package raft

import (
	"sort"
	"sync/atomic"
	"time"
)

// ApplyDurationBucketCount includes the +Inf bucket.
const ApplyDurationBucketCount = 17

var applyDurationBounds = [ApplyDurationBucketCount - 1]time.Duration{
	50 * time.Microsecond,
	100 * time.Microsecond,
	250 * time.Microsecond,
	500 * time.Microsecond,
	time.Millisecond,
	2500 * time.Microsecond,
	5 * time.Millisecond,
	10 * time.Millisecond,
	25 * time.Millisecond,
	50 * time.Millisecond,
	100 * time.Millisecond,
	250 * time.Millisecond,
	500 * time.Millisecond,
	time.Second,
	2500 * time.Millisecond,
	5 * time.Second,
}

// Counters tracks monotonic cluster-mode counters and the apply-latency
// histogram. Every field is updated with atomic adds from arbitrary
// goroutines and read with atomic loads. Zero value is ready to use;
// nil-receiver methods are no-ops so call sites can be unconditional.
type Counters struct {
	proposals       atomic.Uint64
	proposalsFailed atomic.Uint64
	applies         atomic.Uint64
	appliesFailed   atomic.Uint64
	applyNanosTotal atomic.Uint64
	applyBuckets    [ApplyDurationBucketCount]atomic.Uint64
	leaderChanges   atomic.Uint64
}

// CountersSnapshot is a point-in-time read of every counter. Safe to
// pass by value.
type CountersSnapshot struct {
	Proposals            uint64
	ProposalsFailed      uint64
	Applies              uint64
	AppliesFailed        uint64
	ApplyNanosTotal      uint64
	ApplyDurationBuckets [ApplyDurationBucketCount]uint64
	LeaderChanges        uint64
}

// ApplyDurationBounds returns the finite upper bounds for the apply
// latency histogram. The final ApplyDurationBuckets element is +Inf.
func ApplyDurationBounds() [ApplyDurationBucketCount - 1]time.Duration {
	return applyDurationBounds
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

// IncApply records one successful FSM apply and its wall duration.
func (c *Counters) IncApply(dur time.Duration) {
	if c == nil {
		return
	}
	c.applies.Add(1)
	c.observeApplyDuration(dur)
	if dur > 0 {
		c.applyNanosTotal.Add(uint64(dur.Nanoseconds()))
	}
}

func (c *Counters) observeApplyDuration(dur time.Duration) {
	bucket := sort.Search(len(applyDurationBounds), func(i int) bool {
		return dur <= applyDurationBounds[i]
	})
	c.applyBuckets[bucket].Add(1)
}

// IncApplyFailed records an FSM apply rejected by the containment
// boundary. The FSM interface has no error result, so this currently
// means an unexpected panic.
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

// Snapshot returns the current metric values as a flat struct. Each
// field is read with its own atomic Load, so the snapshot may not be
// consistent across fields under a concurrent burst — that's expected
// for Prometheus-style scrape semantics.
func (c *Counters) Snapshot() CountersSnapshot {
	if c == nil {
		return CountersSnapshot{}
	}
	var buckets [ApplyDurationBucketCount]uint64
	for i := range c.applyBuckets {
		buckets[i] = c.applyBuckets[i].Load()
	}
	return CountersSnapshot{
		Proposals:            c.proposals.Load(),
		ProposalsFailed:      c.proposalsFailed.Load(),
		Applies:              c.applies.Load(),
		AppliesFailed:        c.appliesFailed.Load(),
		ApplyNanosTotal:      c.applyNanosTotal.Load(),
		ApplyDurationBuckets: buckets,
		LeaderChanges:        c.leaderChanges.Load(),
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
