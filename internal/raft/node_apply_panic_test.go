package raft

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

type faultingFSM struct {
	mu         sync.Mutex
	panicOn    string
	restoreErr error
	applied    []string
}

func (f *faultingFSM) Apply(e Entry) any {
	if string(e.Data) == f.panicOn {
		panic("injected apply panic")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applied = append(f.applied, string(e.Data))
	return nil
}

func (*faultingFSM) Snapshot() (FSMSnapshot, error) { return noopFSMSnapshot{}, nil }

func (f *faultingFSM) Restore(r io.Reader) error {
	if f.restoreErr != nil {
		return f.restoreErr
	}
	_, err := io.Copy(io.Discard, r)
	return err
}

func (f *faultingFSM) appliedCopy() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.applied...)
}

func newFaultingFSMNode(t *testing.T, fsm FSM) *Node {
	t.Helper()
	const id NodeID = "faulting"
	tr := NewMemNetwork().Transport(id)
	t.Cleanup(func() { _ = tr.Close() })
	n, err := NewNode(fastConfigID(id), fsm, NewMemStorage(), tr, configFor([]NodeID{id}), nil)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return n
}

func TestApplyPanicFailStopsRunningNode(t *testing.T) {
	fsm := &faultingFSM{panicOn: "panic"}
	n := newFaultingFSMNode(t, fsm)
	n.Start()
	t.Cleanup(func() { _ = n.Close() })
	if _, ok := pollUntil(t, 2*time.Second, func() (struct{}, bool) {
		return struct{}{}, n.IsLeader()
	}); !ok {
		t.Fatal("node did not become leader")
	}

	future, err := n.Propose(context.Background(), []byte("panic"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	_, err = mustWait(t, future, 2*time.Second)
	if err == nil || !strings.Contains(err.Error(), "FSM Apply panicked") {
		t.Fatalf("future error = %v, want FSM panic", err)
	}
	waitNodeStopped(t, n)
	if n.IsLeader() {
		t.Fatal("fail-stopped node still reports leader")
	}
	if n.Err() == nil || !strings.Contains(n.Err().Error(), "FSM apply") {
		t.Fatalf("fatal cause = %v, want FSM apply", n.Err())
	}
	if n.Ready() {
		t.Fatal("fail-stopped node reports ready")
	}
	if got := n.Counters().Snapshot().AppliesFailed; got != 1 {
		t.Fatalf("failed apply count = %d, want 1", got)
	}
	if _, err := n.Propose(context.Background(), []byte("after-stop")); !errors.Is(err, ErrStopped) {
		t.Fatalf("Propose after fail-stop = %v, want ErrStopped", err)
	}
}

func TestApplyPanicFailsQueuedProposalsWithoutFurtherApply(t *testing.T) {
	fsm := &faultingFSM{panicOn: "panic"}
	n := newFaultingFSMNode(t, fsm)
	beforeFuture := newFuture()
	panicFuture := newFuture()
	sameBatchFuture := newFuture()
	nextBatchFuture := newFuture()
	go n.runApply()
	n.applyc <- applyReq{
		entries: []Entry{
			{Index: 1, Term: 1, Type: EntryNormal, Data: []byte("before")},
			{Index: 2, Term: 1, Type: EntryNormal, Data: []byte("panic")},
			{Index: 3, Term: 1, Type: EntryNormal, Data: []byte("same-batch")},
		},
		proposals: map[Index]*proposal{
			1: {future: beforeFuture},
			2: {future: panicFuture},
			3: {future: sameBatchFuture},
		},
	}
	n.applyc <- applyReq{
		entries: []Entry{{Index: 4, Term: 1, Type: EntryNormal, Data: []byte("next-batch")}},
		proposals: map[Index]*proposal{
			4: {future: nextBatchFuture},
		},
	}
	close(n.applyc)
	waitClosed(t, n.applyDone, "apply goroutine")

	assertFutureOK(t, beforeFuture)
	assertFutureErrorContains(t, panicFuture, "FSM Apply panicked")
	assertFutureErrorIs(t, sameBatchFuture, ErrStopped)
	assertFutureErrorIs(t, nextBatchFuture, ErrStopped)
	if got := fsm.appliedCopy(); len(got) != 1 || got[0] != "before" {
		t.Fatalf("applied entries = %v, want [before]", got)
	}
}

func TestRestoreFailureFailStopsAndSkipsQueuedApply(t *testing.T) {
	restoreErr := errors.New("injected restore failure")
	fsm := &faultingFSM{restoreErr: restoreErr}
	n := newFaultingFSMNode(t, fsm)
	future := newFuture()
	go n.runApply()
	n.applyc <- applyReq{
		restore:     true,
		restoreData: []byte("snapshot"),
		restoreMeta: SnapshotMeta{
			LastIncludedIndex: 7,
			LastIncludedTerm:  2,
		},
	}
	n.applyc <- applyReq{
		entries:   []Entry{{Index: 8, Term: 2, Type: EntryNormal, Data: []byte("after-restore")}},
		proposals: map[Index]*proposal{8: {future: future}},
	}
	close(n.applyc)
	waitClosed(t, n.applyDone, "apply goroutine")

	assertStopRequested(t, n)
	assertFutureErrorIs(t, future, ErrStopped)
	if got := fsm.appliedCopy(); len(got) != 0 {
		t.Fatalf("applied after failed restore: %v", got)
	}
}

func waitNodeStopped(t *testing.T, n *Node) {
	t.Helper()
	waitClosed(t, n.donec, "run loop")
	waitClosed(t, n.applyDone, "apply goroutine")
}

func assertStopRequested(t *testing.T, n *Node) {
	t.Helper()
	select {
	case <-n.stopc:
	default:
		t.Fatal("node did not request stop after fatal FSM fault")
	}
}

func waitClosed(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("%s did not stop", what)
	}
}

func assertFutureErrorContains(t *testing.T, future *Future, want string) {
	t.Helper()
	_, err := mustWait(t, future, time.Second)
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("future error = %v, want substring %q", err, want)
	}
}

func assertFutureOK(t *testing.T, future *Future) {
	t.Helper()
	if _, err := mustWait(t, future, time.Second); err != nil {
		t.Fatalf("future error = %v, want nil", err)
	}
}

func assertFutureErrorIs(t *testing.T, future *Future, want error) {
	t.Helper()
	_, err := mustWait(t, future, time.Second)
	if !errors.Is(err, want) {
		t.Fatalf("future error = %v, want %v", err, want)
	}
}
