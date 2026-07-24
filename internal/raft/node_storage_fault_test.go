package raft

import (
	"errors"
	"io"
	"testing"
	"time"
)

type faultStorage struct {
	Storage
	saveHardStateErr error
	appendErr        error
	saveSnapshotErr  error
}

func (s *faultStorage) SaveHardState(hs HardState) error {
	if s.saveHardStateErr != nil {
		return s.saveHardStateErr
	}
	return s.Storage.SaveHardState(hs)
}

func (s *faultStorage) Append(entries []Entry) error {
	if s.appendErr != nil {
		return s.appendErr
	}
	return s.Storage.Append(entries)
}

func (s *faultStorage) SaveSnapshot(meta SnapshotMeta, data io.Reader) error {
	if s.saveSnapshotErr != nil {
		return s.saveSnapshotErr
	}
	return s.Storage.SaveSnapshot(meta, data)
}

func newUnstartedFaultNode(t *testing.T, storage Storage) *Node {
	t.Helper()
	const id NodeID = "a"
	tr := NewMemNetwork().Transport(id)
	t.Cleanup(func() { _ = tr.Close() })
	cfg := fastConfigID(id)
	n, err := NewNode(cfg, NewNoopFSM(), storage, tr, configFor([]NodeID{id}), nil)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return n
}

func TestHardStateFailureStopsNodeBeforeGrantingVote(t *testing.T) {
	diskErr := errors.New("injected hard-state failure")
	storage := &faultStorage{Storage: NewMemStorage()}
	n := newUnstartedFaultNode(t, storage)
	n.term = 4
	storage.saveHardStateErr = diskErr

	granted := n.grantVote(&RequestVoteReq{Term: 4, CandidateID: "b"})

	if granted {
		t.Fatal("vote granted without durable hard state")
	}
	assertNodeStopping(t, n)
}

func TestAppendFailureStopsNodeAndFailsProposal(t *testing.T) {
	diskErr := errors.New("injected WAL append failure")
	storage := &faultStorage{Storage: NewMemStorage()}
	n := newUnstartedFaultNode(t, storage)
	n.role, n.term = roleLeader, 2
	n.publishLeadership()
	n.progress = map[NodeID]*peerProgress{"a": {nextIndex: 1}}
	storage.appendErr = diskErr
	p := &proposal{data: []byte("command"), typ: EntryNormal, future: newFuture()}

	n.onPropose(p)

	_, err := mustWait(t, p.future, time.Second)
	if !errors.Is(err, diskErr) {
		t.Fatalf("proposal error = %v, want %v", err, diskErr)
	}
	if n.IsLeader() {
		t.Fatal("storage-faulted node still reports itself as leader")
	}
	assertNodeStopping(t, n)
}

func TestSnapshotSaveFailureStopsNode(t *testing.T) {
	diskErr := errors.New("injected snapshot failure")
	storage := &faultStorage{Storage: NewMemStorage(), saveSnapshotErr: diskErr}
	n := newUnstartedFaultNode(t, storage)

	n.onSnapshotSave(snapSaveReq{
		meta: SnapshotMeta{LastIncludedIndex: 1, LastIncludedTerm: 1},
		data: []byte("snapshot"),
	})

	assertNodeStopping(t, n)
}

func assertNodeStopping(t *testing.T, n *Node) {
	t.Helper()
	select {
	case <-n.stopc:
	default:
		t.Fatal("node did not stop after fatal storage failure")
	}
}
