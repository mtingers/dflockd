package raft

import (
	"context"
	"errors"
	"testing"
	"time"
)

type appendSendResult struct {
	msg Message
	err error
}

type blockingAppendTransport struct {
	sends   chan Message
	replies chan appendSendResult
}

func newBlockingAppendTransport() *blockingAppendTransport {
	return &blockingAppendTransport{
		sends:   make(chan Message, 2),
		replies: make(chan appendSendResult, 2),
	}
}

func (t *blockingAppendTransport) Send(ctx context.Context, _ NodeID, req Message) (Message, error) {
	select {
	case t.sends <- req:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case result := <-t.replies:
		return result.msg, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (t *blockingAppendTransport) SetHandler(func(NodeID, Message) Message) {}
func (t *blockingAppendTransport) AddPeer(NodeID, string)                   {}
func (t *blockingAppendTransport) RemovePeer(NodeID)                        {}
func (t *blockingAppendTransport) LocalID() NodeID                          { return "a" }
func (t *blockingAppendTransport) Close() error                             { return nil }

func TestAppendReplicationIsSingleFlightAndRetriesAfterError(t *testing.T) {
	n, transport := newAppendTestNode(t, 256)
	n.sendAppendEntries("b")
	awaitAppendSend(t, transport.sends)
	if !n.progress["b"].appendInFlight {
		t.Fatal("appendInFlight = false while send is blocked")
	}
	n.sendAppendEntries("b")
	assertNoAppendSend(t, transport.sends)

	transport.replies <- appendSendResult{err: errors.New("peer unavailable")}
	n.onRPCReply(awaitRPCReply(t, n.rpcReplyc))
	if n.progress["b"].appendInFlight {
		t.Fatal("appendInFlight = true after transport error")
	}

	n.sendAppendEntries("b")
	awaitAppendSend(t, transport.sends)
	transport.replies <- appendSendResult{msg: &AppendEntriesResp{Term: 3, Success: true}}
	n.onRPCReply(awaitRPCReply(t, n.rpcReplyc))
	if n.progress["b"].appendInFlight {
		t.Fatal("appendInFlight = true after response")
	}
	n.rpcWG.Wait()
}

func TestAppendReplicationChainsBatchesAfterSuccess(t *testing.T) {
	n, transport := newAppendTestNode(t, 1)
	if err := n.log.append([]Entry{
		{Index: 1, Term: 2}, {Index: 2, Term: 2}, {Index: 3, Term: 2},
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	n.sendAppendEntries("b")
	for index := Index(1); index <= 3; index++ {
		req := awaitAppendSend(t, transport.sends)
		if len(req.Entries) != 1 || req.Entries[0].Index != index {
			t.Fatalf("batch %d = %+v", index, req.Entries)
		}
		transport.replies <- appendSendResult{
			msg: &AppendEntriesResp{Term: 3, Success: true, MatchIndex: index},
		}
		n.onRPCReply(awaitRPCReply(t, n.rpcReplyc))
	}
	assertNoAppendSend(t, transport.sends)
	if p := n.progress["b"]; p.appendInFlight || p.matchIndex != 3 || p.nextIndex != 4 {
		t.Fatalf("progress = %+v, want caught up and idle", p)
	}
	n.rpcWG.Wait()
}

func TestAppendReplicationBatchesByEncodedBytes(t *testing.T) {
	data := make([]byte, 100)
	entries := make([]Entry, 4)
	for i := range entries {
		entries[i] = Entry{Index: Index(i + 1), Term: 2, Type: EntryNormal, Data: data}
	}
	budget := appendEntriesPayloadBaseBytes("a") + 3*(21+len(data))
	selected := limitAppendEntriesByBudget(entries, "a", budget)
	if got, want := len(selected), 3; got != want {
		t.Fatalf("entry batch = %d, want %d", got, want)
	}
	req := &AppendEntriesReq{Term: 3, LeaderID: "a", Entries: selected}
	if payload, err := encodeAppendEntriesReq(req); err != nil {
		t.Fatalf("selected batch does not fit: %v", err)
	} else if len(payload) != budget {
		t.Fatalf("selected payload = %d, budget %d", len(payload), budget)
	}
}

func newAppendTestNode(t *testing.T, maxEntries int) (*Node, *blockingAppendTransport) {
	t.Helper()
	transport := newBlockingAppendTransport()
	cfg := fastConfigID("a")
	cfg.MaxAppendEntries = maxEntries
	n, err := NewNode(cfg, NewNoopFSM(), NewMemStorage(), transport, configFor([]NodeID{"a", "b"}), nil)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	n.role, n.term = roleLeader, 3
	n.progress = map[NodeID]*peerProgress{
		"a": {nextIndex: 1},
		"b": {nextIndex: 1},
	}
	return n, transport
}

func awaitAppendSend(t *testing.T, sends <-chan Message) *AppendEntriesReq {
	t.Helper()
	select {
	case msg := <-sends:
		req, ok := msg.(*AppendEntriesReq)
		if !ok {
			t.Fatalf("send = %T, want *AppendEntriesReq", msg)
		}
		return req
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for AppendEntries")
		return nil
	}
}

func assertNoAppendSend(t *testing.T, sends <-chan Message) {
	t.Helper()
	select {
	case duplicate := <-sends:
		t.Fatalf("unexpected duplicate send: %T", duplicate)
	case <-time.After(20 * time.Millisecond):
	}
}
