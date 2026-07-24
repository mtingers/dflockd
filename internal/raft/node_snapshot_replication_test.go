package raft

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

type snapshotSendResult struct {
	msg Message
	err error
}

type blockingSnapshotTransport struct {
	sends   chan Message
	replies chan snapshotSendResult
}

func newBlockingSnapshotTransport() *blockingSnapshotTransport {
	return &blockingSnapshotTransport{
		sends:   make(chan Message, 2),
		replies: make(chan snapshotSendResult, 2),
	}
}

func (t *blockingSnapshotTransport) Send(ctx context.Context, _ NodeID, req Message) (Message, error) {
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

func (t *blockingSnapshotTransport) SetHandler(func(NodeID, Message) Message) {}
func (t *blockingSnapshotTransport) AddPeer(NodeID, string)                   {}
func (t *blockingSnapshotTransport) RemovePeer(NodeID)                        {}
func (t *blockingSnapshotTransport) LocalID() NodeID                          { return "a" }
func (t *blockingSnapshotTransport) Close() error                             { return nil }

func TestSnapshotReplicationIsSingleFlightAndRetriesAfterError(t *testing.T) {
	const (
		leader   NodeID = "a"
		follower NodeID = "b"
		term     Term   = 3
		snapIdx  Index  = 10
	)
	meta := SnapshotMeta{
		LastIncludedIndex: snapIdx,
		LastIncludedTerm:  term - 1,
		Configuration:     configFor([]NodeID{leader, follower}),
	}
	storage := NewMemStorage()
	if err := storage.SaveSnapshot(meta, bytes.NewReader([]byte("snapshot"))); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	transport := newBlockingSnapshotTransport()
	cfg := fastConfigID(leader)
	n, err := NewNode(cfg, NewNoopFSM(), storage, transport, meta.Configuration, nil)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	n.role, n.term = roleLeader, term
	n.progress = map[NodeID]*peerProgress{
		leader:   {nextIndex: snapIdx + 1},
		follower: {nextIndex: 1},
	}

	n.sendAppendEntries(follower)
	first := awaitSnapshotSend(t, transport.sends)
	if !n.progress[follower].snapshotInFlight {
		t.Fatal("snapshotInFlight = false while the first send is blocked")
	}

	n.sendAppendEntries(follower)
	select {
	case duplicate := <-transport.sends:
		t.Fatalf("duplicate send while snapshot is in flight: %T", duplicate)
	case <-time.After(20 * time.Millisecond):
	}

	transport.replies <- snapshotSendResult{err: errors.New("peer unavailable")}
	n.onRPCReply(awaitRPCReply(t, n.rpcReplyc))
	if n.progress[follower].snapshotInFlight {
		t.Fatal("snapshotInFlight = true after transport error")
	}

	n.sendAppendEntries(follower)
	second := awaitSnapshotSend(t, transport.sends)
	if second.Meta.LastIncludedIndex != first.Meta.LastIncludedIndex ||
		second.Meta.LastIncludedTerm != first.Meta.LastIncludedTerm ||
		!bytes.Equal(second.Data, first.Data) {
		t.Fatalf("retry snapshot differs: first=%+v second=%+v", first, second)
	}
	transport.replies <- snapshotSendResult{
		msg: &InstallSnapshotResp{Term: term, LastIndex: snapIdx},
	}
	n.onRPCReply(awaitRPCReply(t, n.rpcReplyc))
	if n.progress[follower].snapshotInFlight {
		t.Fatal("snapshotInFlight = true after response")
	}
	if got := n.progress[follower].nextIndex; got != snapIdx+1 {
		t.Fatalf("nextIndex = %d, want %d", got, snapIdx+1)
	}
	n.rpcWG.Wait()
}

func awaitSnapshotSend(t *testing.T, sends <-chan Message) *InstallSnapshotReq {
	t.Helper()
	select {
	case msg := <-sends:
		req, ok := msg.(*InstallSnapshotReq)
		if !ok {
			t.Fatalf("send = %T, want *InstallSnapshotReq", msg)
		}
		return req
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for InstallSnapshot")
		return nil
	}
}

func awaitRPCReply(t *testing.T, replies <-chan rpcReply) rpcReply {
	t.Helper()
	select {
	case rep := <-replies:
		return rep
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for RPC reply")
		return rpcReply{}
	}
}
