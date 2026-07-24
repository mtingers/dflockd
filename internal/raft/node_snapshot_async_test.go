package raft

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"
)

type blockingSnapshotStorage struct {
	*MemStorage
	started     chan struct{}
	release     chan struct{}
	startedOnce sync.Once
	aborted     chan struct{}
}

type blockingSnapshotPreparation struct {
	meta SnapshotMeta
	data []byte
}

func (*blockingSnapshotPreparation) isPreparedSnapshot() {}

func newBlockingSnapshotStorage() *blockingSnapshotStorage {
	return &blockingSnapshotStorage{
		MemStorage: NewMemStorage(),
		started:    make(chan struct{}),
		release:    make(chan struct{}),
		aborted:    make(chan struct{}, 1),
	}
}

func (s *blockingSnapshotStorage) prepareSnapshot(
	meta SnapshotMeta,
	data []byte,
	_ []Entry,
) (preparedSnapshot, error) {
	s.blockSnapshotWrite()
	return &blockingSnapshotPreparation{
		meta: meta,
		data: append([]byte(nil), data...),
	}, nil
}

func (s *blockingSnapshotStorage) SaveSnapshot(meta SnapshotMeta, data io.Reader) error {
	fsm, err := io.ReadAll(data)
	if err != nil {
		return err
	}
	s.blockSnapshotWrite()
	return s.MemStorage.SaveSnapshot(meta, bytes.NewReader(fsm))
}

func (s *blockingSnapshotStorage) blockSnapshotWrite() {
	s.startedOnce.Do(func() { close(s.started) })
	<-s.release
}

func (s *blockingSnapshotStorage) commitPreparedSnapshot(
	prepared preparedSnapshot,
	_ []Entry,
) error {
	p := prepared.(*blockingSnapshotPreparation)
	return s.MemStorage.SaveSnapshot(p.meta, bytes.NewReader(p.data))
}

func (s *blockingSnapshotStorage) abortPreparedSnapshot(preparedSnapshot) {
	select {
	case s.aborted <- struct{}{}:
	default:
	}
}

func TestAsyncSnapshotPreparationDoesNotBlockRunLoop(t *testing.T) {
	storage := newBlockingSnapshotStorage()
	n := startSingleNodeWithStorage(t, storage)
	defer func() {
		unblockSnapshotStorage(storage)
		_ = n.Close()
	}()

	select {
	case <-storage.started:
	case <-time.After(2 * time.Second):
		t.Fatal("snapshot preparation did not start")
	}

	statusc := make(chan NodeStatus, 1)
	go func() { statusc <- n.Status() }()
	select {
	case status := <-statusc:
		if status.Role != "leader" {
			t.Fatalf("Status role = %q, want leader", status.Role)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Status blocked behind snapshot preparation")
	}

	future, err := n.Propose(context.Background(), []byte("during-snapshot"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if _, err := mustWait(t, future, time.Second); err != nil {
		t.Fatalf("proposal during snapshot preparation: %v", err)
	}
	wantSnapshot := n.Status().LastLogIndex

	unblockSnapshotStorage(storage)
	if _, ok := pollUntil(t, 2*time.Second, func() (struct{}, bool) {
		return struct{}{}, n.Status().LastSnapshotIndex >= wantSnapshot
	}); !ok {
		t.Fatalf("pending snapshot was not committed through index %d", wantSnapshot)
	}
}

func TestAsyncSnapshotPreparationKeepsHeartbeatsFlowing(t *testing.T) {
	ids := []NodeID{"a", "b", "c"}
	net := NewMemNetwork()
	nodes := make(map[NodeID]*Node, len(ids))
	storages := make(map[NodeID]*blockingSnapshotStorage, len(ids))
	transports := make(map[NodeID]*MemTransport, len(ids))
	defer func() {
		for _, storage := range storages {
			unblockSnapshotStorage(storage)
		}
		for _, n := range nodes {
			_ = n.Close()
		}
		for _, transport := range transports {
			_ = transport.Close()
		}
	}()
	for _, id := range ids {
		cfg := fastConfigID(id)
		cfg.SnapshotThresholdEntries = 0
		storage := newBlockingSnapshotStorage()
		transport := net.Transport(id)
		n, err := NewNode(
			cfg,
			newRecordingFSM(),
			storage,
			transport,
			configFor(ids),
			nil,
		)
		if err != nil {
			t.Fatalf("NewNode(%s): %v", id, err)
		}
		n.Start()
		nodes[id], storages[id], transports[id] = n, storage, transport
	}

	leader := waitLeaderOf(t, net, nodes, ids)
	barrier, err := nodes[leader].Barrier(context.Background())
	if err != nil {
		t.Fatalf("Barrier: %v", err)
	}
	if _, err := mustWait(t, barrier, time.Second); err != nil {
		t.Fatalf("wait for Barrier: %v", err)
	}
	status := nodes[leader].Status()
	nodes[leader].snapSavec <- snapSaveReq{
		meta: SnapshotMeta{
			LastIncludedIndex: status.CommitIndex,
			LastIncludedTerm:  status.Term,
			Configuration:     configFor(ids),
		},
		data: []byte{0, 0, 0, 0},
	}
	select {
	case <-storages[leader].started:
	case <-time.After(time.Second):
		t.Fatal("leader snapshot preparation did not start")
	}

	time.Sleep(4 * fastConfig().ElectionTimeoutMax)
	for _, id := range ids {
		got := statusWithin(t, nodes[id], 200*time.Millisecond)
		if got.Term != status.Term {
			t.Fatalf("term(%s) = %d, want stable term %d", id, got.Term, status.Term)
		}
		if got.LeaderID != leader {
			t.Fatalf("leader(%s) = %q, want %q", id, got.LeaderID, leader)
		}
	}
	if got := statusWithin(t, nodes[leader], 200*time.Millisecond); got.Role != "leader" {
		t.Fatalf("original leader role = %q after blocked snapshot, want leader", got.Role)
	}
}

func TestCloseWaitsForAsyncSnapshotPreparation(t *testing.T) {
	storage := newBlockingSnapshotStorage()
	n := startSingleNodeWithStorage(t, storage)

	select {
	case <-storage.started:
	case <-time.After(2 * time.Second):
		t.Fatal("snapshot preparation did not start")
	}

	closed := make(chan struct{})
	go func() {
		_ = n.Close()
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("Close returned while snapshot worker was blocked")
	case <-time.After(50 * time.Millisecond):
	}

	unblockSnapshotStorage(storage)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not join released snapshot worker")
	}
	select {
	case <-storage.aborted:
	case <-time.After(time.Second):
		t.Fatal("stopped snapshot worker did not abort its preparation")
	}
}

func startSingleNodeWithStorage(t *testing.T, storage Storage) *Node {
	t.Helper()
	const id NodeID = "only"
	net := NewMemNetwork()
	transport := net.Transport(id)
	t.Cleanup(func() { _ = transport.Close() })
	cfg := fastConfigID(id)
	cfg.SnapshotThresholdEntries = 1
	n, err := NewNode(
		cfg,
		newRecordingFSM(),
		storage,
		transport,
		configFor([]NodeID{id}),
		nil,
	)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	n.Start()
	if _, ok := pollUntil(t, 2*time.Second, func() (struct{}, bool) {
		return struct{}{}, n.IsLeader()
	}); !ok {
		if blocking, ok := storage.(*blockingSnapshotStorage); ok {
			unblockSnapshotStorage(blocking)
		}
		_ = n.Close()
		t.Fatal("node did not become leader")
	}
	return n
}

func unblockSnapshotStorage(storage *blockingSnapshotStorage) {
	select {
	case <-storage.release:
	default:
		close(storage.release)
	}
}

func statusWithin(t *testing.T, n *Node, timeout time.Duration) NodeStatus {
	t.Helper()
	statusc := make(chan NodeStatus, 1)
	go func() { statusc <- n.Status() }()
	select {
	case status := <-statusc:
		return status
	case <-time.After(timeout):
		t.Fatal("Status blocked behind snapshot preparation")
		return NodeStatus{}
	}
}

var _ asyncSnapshotStorage = (*blockingSnapshotStorage)(nil)
var _ io.Closer = (*blockingSnapshotStorage)(nil)
