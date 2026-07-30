package raft

import "testing"

// Only the leader may hand off leadership. Transport authorization proves the
// sender is a current voter matching the LeaderID it claims, but that alone
// would let any voter force its peers to campaign and depose a healthy leader.
func TestTimeoutNowRejectedFromNonLeaderVoter(t *testing.T) {
	n := mustNewNode(t, fastConfigID("a"), NewMemStorage(), NewMemNetwork().Transport("a"),
		Configuration{Voters: map[NodeID]string{"a": "a", "b": "b", "c": "c"}})
	defer n.Close()

	n.becomeFollower(5, "b") // "b" is the leader we recognize
	termBefore := n.term

	// "c" is a voter, but not the leader.
	n.handleTimeoutNow("c", &TimeoutNowReq{Term: 5, LeaderID: "c"})
	if n.role == roleCandidate || n.term != termBefore {
		t.Fatalf("a non-leader voter forced an election: role=%v term=%d (was %d)",
			n.role, n.term, termBefore)
	}

	// The recognized leader may hand off.
	n.handleTimeoutNow("b", &TimeoutNowReq{Term: 5, LeaderID: "b"})
	if n.term == termBefore {
		t.Fatalf("the recognized leader could not hand off: term still %d", termBefore)
	}
}

// With no recognized leader the handoff is still accepted, so a transfer that
// arrives after a higher term cleared leaderID does not silently stall.
func TestTimeoutNowAcceptedWithNoKnownLeader(t *testing.T) {
	n := mustNewNode(t, fastConfigID("a"), NewMemStorage(), NewMemNetwork().Transport("a"),
		Configuration{Voters: map[NodeID]string{"a": "a", "b": "b"}})
	defer n.Close()

	n.becomeFollower(3, "")
	termBefore := n.term
	n.handleTimeoutNow("b", &TimeoutNowReq{Term: 3, LeaderID: "b"})
	if n.term == termBefore {
		t.Fatalf("handoff ignored with no known leader: term still %d", termBefore)
	}
}

// A batch must never come back empty for a non-empty input: shipping an empty
// AppendEntries leaves the follower's matchIndex stuck before the oversize
// entry, and the leader rebuilds the same batch on every retry.
func TestAppendEntriesBatchAlwaysMakesProgress(t *testing.T) {
	entries := []Entry{
		{Index: 1, Term: 1, Type: EntryNormal, Data: make([]byte, 512)},
		{Index: 2, Term: 1, Type: EntryNormal, Data: make([]byte, 512)},
	}
	// A budget too small even for the first entry.
	got := limitAppendEntriesByBudget(entries, "leader", 8)
	if len(got) != 1 {
		t.Fatalf("limitAppendEntriesByBudget returned %d entries under an impossible budget, want 1",
			len(got))
	}
	if got[0].Index != 1 {
		t.Fatalf("returned entry index %d, want the first entry", got[0].Index)
	}
}

func TestAppendEntriesBatchStopsAtBudget(t *testing.T) {
	entries := []Entry{
		{Index: 1, Term: 1, Type: EntryNormal, Data: make([]byte, 100)},
		{Index: 2, Term: 1, Type: EntryNormal, Data: make([]byte, 100)},
		{Index: 3, Term: 1, Type: EntryNormal, Data: make([]byte, 100)},
	}
	budget := appendEntriesPayloadBaseBytes("leader") + 2*(21+100)
	got := limitAppendEntriesByBudget(entries, "leader", budget)
	if len(got) != 2 {
		t.Fatalf("batch of %d entries, want 2 to fit the budget", len(got))
	}
}
