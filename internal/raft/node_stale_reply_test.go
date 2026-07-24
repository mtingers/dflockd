package raft

import "testing"

// An RPC reply belongs to the round that produced it. These tests pin
// the rule that a reply carrying a term other than the current round's
// is discarded — a late reply from an abandoned election must not
// contribute to the one now in progress.

// newUnstartedNode builds a node whose run loop is NOT running, so a
// test can drive the run-loop handlers directly and inspect the state
// they leave behind. The caller must not Close it (Close waits on a
// run loop that never started); closing the transport is enough.
func newUnstartedNode(t *testing.T, id NodeID, ids ...NodeID) (*Node, *MemTransport) {
	t.Helper()
	tr := NewMemNetwork().Transport(id)
	t.Cleanup(func() { _ = tr.Close() })
	cfg := fastConfig()
	cfg.ID = id
	n, err := NewNode(cfg, NewNoopFSM(), NewMemStorage(), tr, configFor(ids), nil)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return n, tr
}

// A granted vote from an earlier election must not count toward the
// current one: the peer that granted it may have voted for a different
// candidate in the term now being contested, so counting it can elect
// two leaders in one term.
func TestStaleTermVoteIsNotCounted(t *testing.T) {
	n, _ := newUnstartedNode(t, "a", "a", "b", "c")
	n.role, n.term, n.votedFor = roleCandidate, 6, "a"
	n.votes = map[NodeID]bool{"a": true}

	n.onRPCReply(rpcReply{from: "b", msg: &RequestVoteResp{Term: 5, VoteGranted: true}})

	if n.role == roleLeader {
		t.Fatalf("stale term-5 vote elected the node leader in term %d", n.term)
	}
	if got := n.countGrants(); got != 1 {
		t.Fatalf("grants = %d, want 1 (self only)", got)
	}
}

// The same reply at the current term is counted — the guard must not
// break ordinary elections.
func TestCurrentTermVoteIsCounted(t *testing.T) {
	n, _ := newUnstartedNode(t, "a", "a", "b", "c")
	n.role, n.term, n.votedFor = roleCandidate, 6, "a"
	n.votes = map[NodeID]bool{"a": true}

	n.onRPCReply(rpcReply{from: "b", msg: &RequestVoteResp{Term: 6, VoteGranted: true}})

	if n.role != roleLeader {
		t.Fatalf("role = %v, want leader after a current-term quorum", n.role)
	}
}

// A granted pre-vote echoes the candidate's hypothetical term (one past
// its own), so it must not be mistaken for "a peer is ahead of us" and
// step the pre-candidate down.
func TestGrantedPreVoteWithFutureTermDoesNotStepDown(t *testing.T) {
	n, _ := newUnstartedNode(t, "a", "a", "b", "c")
	n.role, n.term = rolePreCandidate, 5
	n.preVote = true
	n.votes = map[NodeID]bool{"a": true}

	n.onRPCReply(rpcReply{from: "b", msg: &RequestVoteResp{Term: 6, VoteGranted: true, PreVote: true}})

	if n.role != roleCandidate {
		t.Fatalf("role = %v, want candidate (pre-vote quorum should promote)", n.role)
	}
	if n.term != 6 {
		t.Fatalf("term = %d, want 6", n.term)
	}
}

// A rejected pre-vote carrying a higher term does step us down — that
// term is real, it's the responder's own.
func TestRejectedPreVoteWithHigherTermStepsDown(t *testing.T) {
	n, _ := newUnstartedNode(t, "a", "a", "b", "c")
	n.role, n.term = rolePreCandidate, 5
	n.preVote = true
	n.votes = map[NodeID]bool{"a": true}

	n.onRPCReply(rpcReply{from: "b", msg: &RequestVoteResp{Term: 9, VoteGranted: false, PreVote: true}})

	if n.role != roleFollower {
		t.Fatalf("role = %v, want follower", n.role)
	}
	if n.term != 9 {
		t.Fatalf("term = %d, want 9", n.term)
	}
}

// A granted pre-vote must carry the candidate's proposed term so the
// candidate can match the reply to the round it started; its own term
// may legitimately be lower.
func TestPreVoteGrantEchoesCandidateTerm(t *testing.T) {
	n, _ := newUnstartedNode(t, "a", "a", "b", "c")
	n.term = 3 // behind the candidate

	resp := n.handleRequestVote(&RequestVoteReq{Term: 5, CandidateID: "b", PreVote: true})

	if !resp.VoteGranted {
		t.Fatalf("pre-vote not granted: %+v", resp)
	}
	if resp.Term != 5 {
		t.Fatalf("granted pre-vote Term = %d, want the candidate's 5", resp.Term)
	}
}

// An AppendEntries success from a term this node no longer leads must
// not move that peer's progress: the follower's log at those indices
// may since have been overwritten.
func TestStaleTermAppendRespIsIgnored(t *testing.T) {
	n, _ := newUnstartedNode(t, "a", "a", "b", "c")
	n.role, n.term = roleLeader, 9
	n.progress = map[NodeID]*peerProgress{"a": {nextIndex: 1}, "b": {nextIndex: 1}, "c": {nextIndex: 1}}

	n.onRPCReply(rpcReply{from: "b", msg: &AppendEntriesResp{Term: 3, Success: true, MatchIndex: 42}})

	if got := n.progress["b"].matchIndex; got != 0 {
		t.Fatalf("matchIndex = %d, want 0 (stale reply ignored)", got)
	}
}

// An InstallSnapshot response from an older term is ignored for the
// same reason.
func TestStaleTermInstallSnapshotRespIsIgnored(t *testing.T) {
	n, _ := newUnstartedNode(t, "a", "a", "b", "c")
	n.role, n.term = roleLeader, 9
	n.progress = map[NodeID]*peerProgress{
		"a": {nextIndex: 1},
		"b": {nextIndex: 1, snapshotInFlight: true},
		"c": {nextIndex: 1},
	}

	n.onRPCReply(rpcReply{
		from: "b",
		req:  &InstallSnapshotReq{Term: 4},
		msg:  &InstallSnapshotResp{Term: 4, LastIndex: 77},
	})

	if got := n.progress["b"].matchIndex; got != 0 {
		t.Fatalf("matchIndex = %d, want 0 (stale reply ignored)", got)
	}
	if !n.progress["b"].snapshotInFlight {
		t.Fatal("stale reply cleared the current term's snapshot gate")
	}
}
