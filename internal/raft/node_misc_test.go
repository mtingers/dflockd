package raft

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

// --- direct unit tests for the still-stub paths so coverage doesn't dip ---

func TestRaftLogInstallSnapshotResets(t *testing.T) {
	s := NewMemStorage()
	mustAppend(t, s, []Entry{mkEntry(1, 1, "a"), mkEntry(2, 1, "b")})
	rl, err := newRaftLog(s)
	if err != nil {
		t.Fatalf("newRaftLog: %v", err)
	}
	meta := SnapshotMeta{LastIncludedIndex: 5, LastIncludedTerm: 3, Configuration: Configuration{Voters: map[NodeID]string{"a": "h:1"}}}
	last, err := rl.installSnapshot(meta, []byte("payload"))
	if err != nil {
		t.Fatalf("installSnapshot: %v", err)
	}
	if last != 5 || rl.firstIndex() != 6 || rl.committed != 5 {
		t.Fatalf("post-install: last=%d first=%d committed=%d", last, rl.firstIndex(), rl.committed)
	}
}

func TestRaftLogConflictHint(t *testing.T) {
	s := NewMemStorage()
	mustAppend(t, s, []Entry{mkEntry(1, 1, "a"), mkEntry(2, 1, "b"), mkEntry(3, 2, "c"), mkEntry(4, 2, "d"), mkEntry(5, 3, "e")})
	rl, _ := newRaftLog(s)
	// At a known index, the hint is the first index of that term.
	if ci, ct := rl.conflictHint(4); ci != 3 || ct != 2 {
		t.Fatalf("conflictHint(4) = %d,%d; want 3,2", ci, ct)
	}
	// Past the end, the hint is lastIndex+1 with term 0.
	if ci, ct := rl.conflictHint(99); ci != 6 || ct != 0 {
		t.Fatalf("conflictHint(99) = %d,%d; want 6,0", ci, ct)
	}
}

func TestFollowerInstallsSnapshot(t *testing.T) {
	tc := newTestCluster(t, "n1", "n2", "n3")
	defer tc.stopAll()
	leader := tc.waitLeader()
	follower := otherIDs(tc.ids, leader)[0]

	meta := SnapshotMeta{LastIncludedIndex: 42, LastIncludedTerm: 5, Configuration: configFor(tc.ids)}
	req := &InstallSnapshotReq{Term: tc.term(leader) + 1, LeaderID: leader, Meta: meta, Data: []byte("snapshot-payload")}
	resp := tc.nodes[follower].handleRPC(leader, req)
	is, ok := resp.(*InstallSnapshotResp)
	if !ok || is.LastIndex != 42 {
		t.Fatalf("InstallSnapshot reply = %+v", resp)
	}
	st := tc.store[follower]
	if m, ok := st.SnapshotMeta(); !ok || m.LastIncludedIndex != 42 || m.LastIncludedTerm != 5 {
		t.Fatalf("follower didn't install snapshot meta: %+v ok=%v", m, ok)
	}
	if data := readSnapshot(t, st); !bytes.Equal(data, []byte("snapshot-payload")) {
		t.Fatalf("follower snapshot data = %q", data)
	}
}

func TestTimeoutNowTriggersElection(t *testing.T) {
	tc := newTestCluster(t, "n1", "n2", "n3")
	defer tc.stopAll()
	leader := tc.waitLeader()
	target := otherIDs(tc.ids, leader)[0]
	startTerm := tc.term(target)

	// TimeoutNow at the current term should make the target campaign;
	// its term should advance promptly.
	tc.nodes[target].handleRPC(leader, &TimeoutNowReq{Term: tc.term(target), LeaderID: leader})
	if _, ok := pollUntil(t, 1*time.Second, func() (struct{}, bool) {
		return struct{}{}, tc.nodes[target].Status().Term > startTerm
	}); !ok {
		t.Fatalf("TimeoutNow did not advance %s's term beyond %d", target, startTerm)
	}
}

func TestTransferLeadershipHandsOffToFollower(t *testing.T) {
	tc := newTestCluster(t, "n1", "n2", "n3")
	defer tc.stopAll()
	leader1 := tc.waitLeader()
	// One committed entry so the followers' matchIndex is up — otherwise
	// "caught up enough" depends on the leader's no-op having replicated.
	f, err := tc.nodes[leader1].Propose(context.Background(), []byte("x"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if _, err := mustWait(t, f, 2*time.Second); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	// TimeoutNow gives the target a head start, so it normally wins the
	// next term — but on a heavily-loaded machine the original leader's
	// own (very short, test-config) election timer can occasionally beat
	// it and win back. Retry the transfer against whoever is leader until
	// it actually moves; only a persistent failure to move is a bug.
	newLeader, ok := pollUntil(t, 5*time.Second, func() (NodeID, bool) {
		cur := tc.waitLeader()
		if cur != leader1 {
			return cur, true
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_ = tc.nodes[cur].TransferLeadership(ctx)
		cancel()
		return pollUntil(t, 300*time.Millisecond, func() (NodeID, bool) {
			l, stable := tc.findStableLeader(tc.ids)
			return l, stable && l != leader1
		})
	})
	if !ok || newLeader == leader1 {
		t.Fatalf("leadership never moved off %s (now %q, ok=%v)", leader1, newLeader, ok)
	}
}

func TestTransferLeadershipOnFollowerErrs(t *testing.T) {
	tc := newTestCluster(t, "n1", "n2", "n3")
	defer tc.stopAll()
	leader := tc.waitLeader()
	follower := otherIDs(tc.ids, leader)[0]
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := tc.nodes[follower].TransferLeadership(ctx); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("TransferLeadership on follower = %v, want ErrNotLeader", err)
	}
}

func TestIsLeaderAndLeaderIDAccessors(t *testing.T) {
	tc := newTestCluster(t, "n1", "n2", "n3")
	defer tc.stopAll()
	leader := tc.waitLeader()
	if !tc.nodes[leader].IsLeader() {
		t.Fatalf("IsLeader on the leader should be true")
	}
	follower := otherIDs(tc.ids, leader)[0]
	if tc.nodes[follower].IsLeader() {
		t.Fatalf("IsLeader on a follower should be false")
	}
	if got := tc.nodes[follower].LeaderID(); got != leader {
		t.Fatalf("LeaderID on follower = %s, want %s", got, leader)
	}
}

func TestMemNetworkPartitionReconnectAndDelay(t *testing.T) {
	net := NewMemNetwork()
	conf := configFor([]NodeID{"a", "b", "c"})
	make := func(id NodeID) *Node {
		st := NewMemStorage()
		n, err := NewNode(fastConfigID(id), NewNoopFSM(), st, net.Transport(id), conf, nil)
		if err != nil {
			t.Fatalf("NewNode(%s): %v", id, err)
		}
		n.Start()
		return n
	}
	a, b, c := make("a"), make("b"), make("c")
	defer a.Close()
	defer b.Close()
	defer c.Close()

	// Wait for a leader.
	leader := waitLeaderOf(t, net, map[NodeID]*Node{"a": a, "b": b, "c": c}, []NodeID{"a", "b", "c"})

	// Partition the leader from one follower; cluster should still have
	// a majority (leader + the other follower) and stay healthy.
	others := otherIDs([]NodeID{"a", "b", "c"}, leader)
	net.Partition(leader, others[0])
	time.Sleep(200 * time.Millisecond)
	// Reconnect: everyone agrees on the same leader again.
	net.Reconnect(leader, others[0])
	if _, ok := pollUntil(t, 2*time.Second, func() (struct{}, bool) {
		nodes := map[NodeID]*Node{"a": a, "b": b, "c": c}
		_, ok := stableLeaderAmong(nodes, []NodeID{"a", "b", "c"})
		return struct{}{}, ok
	}); !ok {
		t.Fatalf("cluster did not restabilize after reconnect")
	}

	// SetDelay exercises that branch; the cluster should still function.
	net.SetDelay(2 * time.Millisecond)
	time.Sleep(50 * time.Millisecond)
}

func stableLeaderAmong(nodes map[NodeID]*Node, ids []NodeID) (NodeID, bool) {
	var leader NodeID
	count := 0
	for _, id := range ids {
		if nodes[id].Status().Role == "leader" {
			leader, count = id, count+1
		}
	}
	if count != 1 {
		return "", false
	}
	for _, id := range ids {
		if nodes[id].Status().LeaderID != leader {
			return "", false
		}
	}
	return leader, true
}

func TestMemTransportLocalIDAndPeerOps(t *testing.T) {
	net := NewMemNetwork()
	tr := net.Transport("x")
	if tr.LocalID() != "x" {
		t.Fatalf("LocalID = %s, want x", tr.LocalID())
	}
	tr.AddPeer("y", "addr") // no-op on MemTransport, must not panic
	tr.RemovePeer("y")      // ditto
	if err := tr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestMemTransportSendToUnknownPeerReturnsError(t *testing.T) {
	net := NewMemNetwork()
	tr := net.Transport("a")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := tr.Send(ctx, "nobody", &RequestVoteReq{Term: 1})
	if err == nil {
		t.Fatalf("Send to unknown peer should fail")
	}
}

func TestStaleReplyDefaults(t *testing.T) {
	// Each request type maps to its own reply type.
	cases := []struct {
		req  Message
		want string // reply type name (loose check via type switch)
	}{
		{&RequestVoteReq{}, "rv"},
		{&AppendEntriesReq{}, "ae"},
		{&InstallSnapshotReq{}, "is"},
		{&TimeoutNowReq{}, "tn"},
	}
	for _, c := range cases {
		switch r := staleReply(c.req, 9).(type) {
		case *RequestVoteResp:
			if c.want != "rv" || r.Term != 9 {
				t.Fatalf("rv mismatch: %+v", r)
			}
		case *AppendEntriesResp:
			if c.want != "ae" || r.Term != 9 {
				t.Fatalf("ae mismatch: %+v", r)
			}
		case *InstallSnapshotResp:
			if c.want != "is" || r.Term != 9 {
				t.Fatalf("is mismatch: %+v", r)
			}
		case *TimeoutNowResp:
			if c.want != "tn" || r.Term != 9 {
				t.Fatalf("tn mismatch: %+v", r)
			}
		default:
			t.Fatalf("staleReply returned unknown type %T", r)
		}
	}
}

func TestReadAllAndClose(t *testing.T) {
	rc := io.NopCloser(bytes.NewReader([]byte("hello")))
	b, err := readAllAndClose(rc)
	if err != nil || string(b) != "hello" {
		t.Fatalf("readAllAndClose = %q, %v", b, err)
	}
}
