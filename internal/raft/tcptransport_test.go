package raft

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

// Stand up two real TCP transports on loopback, wire them as the
// transports for two raft.Nodes, and run an election + restart.

func TestTCPTransportTwoNodesElectLeader(t *testing.T) {
	trA, err := NewTCPTransport("a", "127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("listen a: %v", err)
	}
	trB, err := NewTCPTransport("b", "127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("listen b: %v", err)
	}
	defer trA.Close()
	defer trB.Close()
	members := map[NodeID]string{"a": trA.ListenAddr(), "b": trB.ListenAddr()}
	trA.AddPeer("b", trB.ListenAddr())
	trB.AddPeer("a", trA.ListenAddr())

	cfgA := fastConfigID("a")
	cfgB := fastConfigID("b")
	stA := NewMemStorage()
	stB := NewMemStorage()
	a, err := NewNode(cfgA, NewNoopFSM(), stA, trA, Configuration{Voters: members}, nil)
	if err != nil {
		t.Fatalf("NewNode a: %v", err)
	}
	b, err := NewNode(cfgB, NewNoopFSM(), stB, trB, Configuration{Voters: members}, nil)
	if err != nil {
		t.Fatalf("NewNode b: %v", err)
	}
	a.Start()
	b.Start()
	defer a.Close()
	defer b.Close()

	// Wait for one to become leader.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if (a.IsLeader() || b.IsLeader()) && (a.LeaderID() == b.LeaderID()) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no agreement on leader: aLeader=%v bLeader=%v aLeaderID=%q bLeaderID=%q",
		a.IsLeader(), b.IsLeader(), a.LeaderID(), b.LeaderID())
}

func TestTCPTransportThreeNodesProposeReplicates(t *testing.T) {
	trs := map[NodeID]*TCPTransport{}
	ids := []NodeID{"a", "b", "c"}
	for _, id := range ids {
		tr, err := NewTCPTransport(id, "127.0.0.1:0", nil)
		if err != nil {
			t.Fatalf("NewTCPTransport(%s): %v", id, err)
		}
		trs[id] = tr
	}
	defer func() {
		for _, tr := range trs {
			tr.Close()
		}
	}()
	members := map[NodeID]string{}
	for id, tr := range trs {
		members[id] = tr.ListenAddr()
	}
	for _, tr := range trs {
		for id, addr := range members {
			if id != tr.LocalID() {
				tr.AddPeer(id, addr)
			}
		}
	}

	nodes := map[NodeID]*Node{}
	fsms := map[NodeID]*recordingFSM{}
	for _, id := range ids {
		fsm := newRecordingFSM()
		n, err := NewNode(fastConfigID(id), fsm, NewMemStorage(), trs[id], Configuration{Voters: members}, nil)
		if err != nil {
			t.Fatalf("NewNode(%s): %v", id, err)
		}
		n.Start()
		nodes[id], fsms[id] = n, fsm
	}
	defer func() {
		for _, n := range nodes {
			n.Close()
		}
	}()

	leader := waitTCPLeader(t, nodes, ids)

	// Propose a payload through the leader; expect every node's FSM to
	// see it.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	fut, err := nodes[leader].Propose(ctx, []byte("hello-tcp"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if _, err := fut.Wait(ctx); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	for _, id := range ids {
		ok := waitFor(t, 2*time.Second, func() bool { return fsms[id].count() == 1 })
		if !ok {
			t.Fatalf("%s FSM count = %d, want 1", id, fsms[id].count())
		}
	}
}

func waitTCPLeader(t *testing.T, nodes map[NodeID]*Node, ids []NodeID) NodeID {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var leader NodeID
		count := 0
		for _, id := range ids {
			if nodes[id].IsLeader() {
				leader, count = id, count+1
			}
		}
		if count == 1 {
			// every node agrees
			agreed := true
			for _, id := range ids {
				if nodes[id].LeaderID() != leader {
					agreed = false
					break
				}
			}
			if agreed {
				return leader
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no stable leader")
	return ""
}

func waitFor(t *testing.T, d time.Duration, f func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if f() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

func TestTCPTransportFrameRoundTrip(t *testing.T) {
	cases := []Message{
		&RequestVoteReq{Term: 5, CandidateID: "c", LastLogIndex: 3, LastLogTerm: 2, PreVote: true},
		&RequestVoteResp{Term: 5, VoteGranted: true, PreVote: true},
		&AppendEntriesReq{Term: 7, LeaderID: "L", PrevLogIndex: 4, PrevLogTerm: 3, Entries: []Entry{{Index: 5, Term: 7, Type: EntryNormal, Data: []byte("x")}}, LeaderCommit: 4},
		&AppendEntriesResp{Term: 7, Success: true, MatchIndex: 5},
		&InstallSnapshotReq{Term: 9, LeaderID: "L", Meta: SnapshotMeta{LastIncludedIndex: 10, LastIncludedTerm: 8}, Data: []byte("snap")},
		&InstallSnapshotResp{Term: 9, LastIndex: 10},
		&TimeoutNowReq{Term: 11, LeaderID: "L"},
		&TimeoutNowResp{Term: 11},
	}
	for _, m := range cases {
		body, err := encodeRPC(frameRequest, 42, m)
		if err != nil {
			t.Fatalf("encode(%T): %v", m, err)
		}
		kind, reqID, got, err := decodeRPC(body)
		if err != nil || kind != frameRequest || reqID != 42 {
			t.Fatalf("decode(%T) = %d %d %v", m, kind, reqID, err)
		}
		// Compare terms to keep this loose (json marshal/unmarshal of
		// embedded byte slices is exact; we just spot-check).
		if got.messageTerm() != m.messageTerm() {
			t.Fatalf("term roundtrip for %T: got %d want %d", m, got.messageTerm(), m.messageTerm())
		}
	}
}

func TestTCPTransportRejectsUnknownPeer(t *testing.T) {
	tr, err := NewTCPTransport("only", "127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("NewTCPTransport: %v", err)
	}
	defer tr.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err = tr.Send(ctx, "ghost", &RequestVoteReq{Term: 1})
	if err == nil {
		t.Fatalf("Send to unknown peer should fail")
	}
}

func TestTCPTransportCloseIsIdempotent(t *testing.T) {
	tr, err := NewTCPTransport("x", "127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("NewTCPTransport: %v", err)
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("Close 2: %v", err)
	}
}

// readFrame must clear a deadline left over from a prior call (the
// handshake sets a 5s read deadline); a steady-state read with
// deadline<=0 must not inherit it. Regression test for the bug where
// every conn died ~5s after it was established.
func TestReadFrameClearsStaleDeadline(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	// Pretend the handshake left an absolute deadline that's already past.
	_ = c1.SetReadDeadline(time.Now().Add(-time.Second))
	done := make(chan error, 1)
	go func() { _, err := readFrame(c1, 0); done <- err }()
	select {
	case err := <-done:
		t.Fatalf("readFrame returned early — stale deadline not cleared: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := writeFrame(c2, []byte("hi")); err != nil {
		t.Fatalf("writeFrame: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("readFrame after deadline cleared: %v", err)
	}
}

func TestTCPTransportDialBackoff(t *testing.T) {
	tr, err := NewTCPTransport("a", "127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("NewTCPTransport: %v", err)
	}
	defer tr.Close()
	tr.AddPeer("dead", "127.0.0.1:1") // refuses connections
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := tr.Send(ctx, "dead", &RequestVoteReq{Term: 1}); err == nil {
		t.Fatalf("Send to a dead peer should fail")
	}
	// An immediate retry should be short-circuited by the dial cool-down,
	// not attempt another (slow) dial.
	start := time.Now()
	if _, err := tr.Send(context.Background(), "dead", &RequestVoteReq{Term: 1}); err == nil {
		t.Fatalf("retry to a dead peer should fail")
	}
	if d := time.Since(start); d > 50*time.Millisecond {
		t.Fatalf("retry took %s — dial cool-down didn't short-circuit", d)
	}
}

func TestTCPTransportSendAfterCloseFails(t *testing.T) {
	trA, _ := NewTCPTransport("a", "127.0.0.1:0", nil)
	trB, _ := NewTCPTransport("b", "127.0.0.1:0", nil)
	defer trB.Close()
	trA.AddPeer("b", trB.ListenAddr())
	if err := trA.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_, err := trA.Send(context.Background(), "b", &RequestVoteReq{Term: 1})
	if !errors.Is(err, errTransportClosed) {
		t.Fatalf("Send after Close = %v, want errTransportClosed", err)
	}
}

// A connection must keep working well past the handshake read deadline —
// the steady-state read loop refreshes the deadline on every read.
func TestTCPTransportConnSurvivesPastHandshakeDeadline(t *testing.T) {
	if testing.Short() {
		t.Skip("sleeps past handshakeTimeout")
	}
	trA, _ := NewTCPTransport("a", "127.0.0.1:0", nil)
	trB, _ := NewTCPTransport("b", "127.0.0.1:0", nil)
	defer trA.Close()
	defer trB.Close()
	trA.AddPeer("b", trB.ListenAddr())
	trB.SetHandler(func(from NodeID, req Message) Message {
		return &RequestVoteResp{Term: req.messageTerm(), VoteGranted: true}
	})
	send := func(term Term) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		resp, err := trA.Send(ctx, "b", &RequestVoteReq{Term: term, CandidateID: "a"})
		if err != nil {
			t.Fatalf("Send(term=%d): %v", term, err)
		}
		if rv, ok := resp.(*RequestVoteResp); !ok || rv.Term != term {
			t.Fatalf("Send(term=%d) resp = %+v", term, resp)
		}
	}
	send(1) // establishes the conn (handshake sets a 5s read deadline)
	time.Sleep(handshakeTimeout + time.Second)
	send(2) // must still work — same conn, deadline was refreshed
}

func TestTCPTransportConcurrentSends(t *testing.T) {
	trA, _ := NewTCPTransport("a", "127.0.0.1:0", nil)
	trB, _ := NewTCPTransport("b", "127.0.0.1:0", nil)
	defer trA.Close()
	defer trB.Close()
	trA.AddPeer("b", trB.ListenAddr())

	// B's handler echoes the term as a RequestVoteResp.
	trB.SetHandler(func(from NodeID, req Message) Message {
		if rv, ok := req.(*RequestVoteReq); ok {
			return &RequestVoteResp{Term: rv.Term, VoteGranted: true}
		}
		return &RequestVoteResp{Term: 0}
	})

	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			resp, err := trA.Send(ctx, "b", &RequestVoteReq{Term: Term(i + 1), CandidateID: "a"})
			if err != nil {
				t.Errorf("Send %d: %v", i, err)
				return
			}
			if rv, ok := resp.(*RequestVoteResp); !ok || rv.Term != Term(i+1) {
				t.Errorf("Send %d resp = %+v", i, resp)
			}
		}()
	}
	wg.Wait()
}
