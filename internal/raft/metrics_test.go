package raft

import (
	"testing"
	"time"
)

// TestCounters_NilSafe ensures nil-receiver calls are no-ops so call
// sites can be unconditional (rolls with the "always non-nil after
// NewNode" invariant; nil only happens in tests that don't go through
// NewNode).
func TestCounters_NilSafe(t *testing.T) {
	var c *Counters
	c.IncProposals()
	c.IncProposalsFailed()
	c.IncApply(time.Millisecond)
	c.IncApplyFailed()
	c.IncLeaderChange()
	if got := c.Snapshot(); got != (CountersSnapshot{}) {
		t.Fatalf("nil receiver Snapshot: want zero, got %+v", got)
	}
}

// TestCounters_BasicIncrement verifies each counter ticks independently
// and that Snapshot returns the right values.
func TestCounters_BasicIncrement(t *testing.T) {
	c := &Counters{}
	c.IncProposals()
	c.IncProposals()
	c.IncProposalsFailed()
	c.IncApply(2 * time.Millisecond)
	c.IncApply(3 * time.Millisecond)
	c.IncApplyFailed()
	c.IncLeaderChange()

	s := c.Snapshot()
	if s.Proposals != 2 {
		t.Errorf("Proposals: want 2, got %d", s.Proposals)
	}
	if s.ProposalsFailed != 1 {
		t.Errorf("ProposalsFailed: want 1, got %d", s.ProposalsFailed)
	}
	if s.Applies != 2 {
		t.Errorf("Applies: want 2, got %d", s.Applies)
	}
	if s.AppliesFailed != 1 {
		t.Errorf("AppliesFailed: want 1, got %d", s.AppliesFailed)
	}
	wantNanos := uint64((5 * time.Millisecond).Nanoseconds())
	if s.ApplyNanosTotal != wantNanos {
		t.Errorf("ApplyNanosTotal: want %d, got %d", wantNanos, s.ApplyNanosTotal)
	}
	if s.LeaderChanges != 1 {
		t.Errorf("LeaderChanges: want 1, got %d", s.LeaderChanges)
	}
}

// TestCounters_NegativeDurationIgnored verifies a zero/negative duration
// doesn't bump the ApplyNanosTotal (defense against clock-skew weirdness).
func TestCounters_NegativeDurationIgnored(t *testing.T) {
	c := &Counters{}
	c.IncApply(0)
	c.IncApply(-time.Second)
	s := c.Snapshot()
	if s.Applies != 2 {
		t.Errorf("Applies should still tick: want 2, got %d", s.Applies)
	}
	if s.ApplyNanosTotal != 0 {
		t.Errorf("ApplyNanosTotal should be 0 for non-positive durations, got %d", s.ApplyNanosTotal)
	}
}
