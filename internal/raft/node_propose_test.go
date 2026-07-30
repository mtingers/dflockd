package raft

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestProposeRejectsOversizedEntryBeforeSubmission(t *testing.T) {
	n := mustNewNode(t, fastConfigID("only"), NewMemStorage(), NewMemNetwork().Transport("only"), configFor([]NodeID{"only"}))
	future, err := n.Propose(context.Background(), make([]byte, maxEntryDataBytes+1))
	if !errors.Is(err, ErrEntryTooLarge) {
		t.Fatalf("Propose error = %v, want ErrEntryTooLarge", err)
	}
	if future != nil {
		t.Fatal("oversized proposal returned a future")
	}
	if got := n.log.lastIndex(); got != 0 {
		t.Fatalf("last index = %d, want 0", got)
	}
}

// Wait helper bounded by the test timeout.
func mustWait(t *testing.T, f *Future, d time.Duration) (any, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return f.Wait(ctx)
}

func TestSingleNodeProposeAppliesImmediately(t *testing.T) {
	tc := newTestCluster(t, "n1")
	defer tc.stopAll()
	tc.waitLeader()
	f, err := tc.nodes["n1"].Propose(context.Background(), []byte("hello"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	result, err := mustWait(t, f, 2*time.Second)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if idx, _ := result.(Index); idx != 1 {
		t.Fatalf("apply result = %v, want Index(1)", result)
	}
	if got := tc.fsm["n1"].count(); got != 1 {
		t.Fatalf("FSM applied %d entries, want 1", got)
	}
	if data := tc.fsm["n1"].appliedCopy()[0]; !bytes.Equal(data, []byte("hello")) {
		t.Fatalf("applied data = %q, want hello", data)
	}
}

func TestThreeNodeProposeReplicatesAndApplies(t *testing.T) {
	tc := newTestCluster(t, "n1", "n2", "n3")
	defer tc.stopAll()
	leader := tc.waitLeader()

	// Propose three entries; each future should resolve once the entry
	// has applied on the proposing leader.
	want := [][]byte{[]byte("first"), []byte("second"), []byte("third")}
	for _, data := range want {
		f, err := tc.nodes[leader].Propose(context.Background(), data)
		if err != nil {
			t.Fatalf("Propose: %v", err)
		}
		if _, err := mustWait(t, f, 2*time.Second); err != nil {
			t.Fatalf("Wait: %v", err)
		}
	}

	// Every node's FSM must converge to those three entries in order.
	for _, id := range tc.ids {
		if _, ok := pollUntil(t, 2*time.Second, func() (struct{}, bool) {
			return struct{}{}, tc.fsm[id].count() == len(want)
		}); !ok {
			t.Fatalf("FSM(%s) count = %d, want %d", id, tc.fsm[id].count(), len(want))
		}
		got := tc.fsm[id].appliedCopy()
		for i, w := range want {
			if !bytes.Equal(got[i], w) {
				t.Fatalf("FSM(%s)[%d] = %q, want %q", id, i, got[i], w)
			}
		}
	}
}

func TestProposeOnFollowerErrsNotLeader(t *testing.T) {
	tc := newTestCluster(t, "n1", "n2", "n3")
	defer tc.stopAll()
	leader := tc.waitLeader()
	follower := otherIDs(tc.ids, leader)[0]

	f, err := tc.nodes[follower].Propose(context.Background(), []byte("ignored"))
	if err != nil {
		t.Fatalf("Propose submission: %v", err)
	}
	_, werr := mustWait(t, f, 2*time.Second)
	if !errors.Is(werr, ErrNotLeader) {
		t.Fatalf("Propose on follower: want ErrNotLeader, got %v", werr)
	}
}

func TestProposeAcrossLeadershipLossErrs(t *testing.T) {
	tc := newTestCluster(t, "n1", "n2", "n3")
	defer tc.stopAll()
	leader := tc.waitLeader()

	// Isolate the leader so its proposal cannot commit, propose, then
	// force the leader to step down by injecting a higher-term
	// AppendEntries directly (modelling what the eventual real heartbeat
	// from a new leader would do). The in-flight proposal must surface
	// ErrLeadershipLost.
	tc.net.Isolate(leader)
	f, err := tc.nodes[leader].Propose(context.Background(), []byte("doomed"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	sender := otherIDs(tc.ids, leader)[0]
	resp := tc.nodes[leader].handleRPC(sender, &AppendEntriesReq{
		Term: tc.term(leader) + 10, LeaderID: sender,
	})
	if r, ok := resp.(*AppendEntriesResp); !ok || !r.Success {
		t.Fatalf("inject higher-term AppendEntries failed: %+v", resp)
	}
	_, werr := mustWait(t, f, 2*time.Second)
	if !errors.Is(werr, ErrLeadershipLost) {
		t.Fatalf("Propose after stepdown: want ErrLeadershipLost, got %v", werr)
	}
}

func TestProposeWithCanceledContextDoesNotSubmit(t *testing.T) {
	tc := newTestCluster(t, "n1")
	defer tc.stopAll()
	tc.waitLeader()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := tc.nodes["n1"].Propose(ctx, []byte("late"))
	if err == nil {
		t.Fatalf("Propose with cancelled ctx should error")
	}
}

func TestBarrierAppliesAsNoOp(t *testing.T) {
	tc := newTestCluster(t, "n1", "n2", "n3")
	defer tc.stopAll()
	leader := tc.waitLeader()
	f, err := tc.nodes[leader].Barrier(context.Background())
	if err != nil {
		t.Fatalf("Barrier: %v", err)
	}
	result, err := mustWait(t, f, 2*time.Second)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if result != nil {
		t.Fatalf("Barrier result = %v, want nil (no FSM call for NoOp)", result)
	}
}

func TestEntriesPersistAcrossRestart(t *testing.T) {
	// File-backed cluster of 1, so restart actually exercises disk.
	dir := t.TempDir()
	net := NewMemNetwork()
	conf := configFor([]NodeID{"only"})
	cfg := fastConfigID("only")
	st, err := OpenFileStorage(dir)
	if err != nil {
		t.Fatalf("OpenFileStorage: %v", err)
	}
	fsm := newRecordingFSM()
	n, err := NewNode(cfg, fsm, st, net.Transport("only"), conf, nil)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	n.Start()
	if _, ok := pollUntil(t, 2*time.Second, func() (struct{}, bool) { return struct{}{}, n.IsLeader() }); !ok {
		t.Fatalf("did not become leader")
	}
	for i := 0; i < 3; i++ {
		f, err := n.Propose(context.Background(), []byte(fmt.Sprintf("e%d", i)))
		if err != nil {
			t.Fatalf("Propose %d: %v", i, err)
		}
		if _, err := mustWait(t, f, 2*time.Second); err != nil {
			t.Fatalf("Wait %d: %v", i, err)
		}
	}
	if err := n.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("storage Close: %v", err)
	}

	// Reopen — entries should be in the log, the new FSM should be fed
	// them on Start so it converges to the same state.
	st2, err := OpenFileStorage(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()
	fsm2 := newRecordingFSM()
	n2, err := NewNode(cfg, fsm2, st2, net.Transport("only"), conf, nil)
	if err != nil {
		t.Fatalf("NewNode reopen: %v", err)
	}
	n2.Start()
	defer n2.Close()
	if _, ok := pollUntil(t, 3*time.Second, func() (struct{}, bool) {
		return struct{}{}, fsm2.count() == 3
	}); !ok {
		t.Fatalf("after restart: FSM applied %d entries, want 3", fsm2.count())
	}
	got := fsm2.appliedCopy()
	for i := 0; i < 3; i++ {
		want := []byte(fmt.Sprintf("e%d", i))
		if !bytes.Equal(got[i], want) {
			t.Fatalf("FSM[%d] = %q, want %q", i, got[i], want)
		}
	}
}

func TestSnapshotTriggersAndCompactsLog(t *testing.T) {
	storages := []struct {
		name string
		open func(*testing.T) Storage
	}{
		{name: "mem", open: func(*testing.T) Storage { return NewMemStorage() }},
		{name: "file", open: func(t *testing.T) Storage { return mustOpenFileStorage(t, t.TempDir()) }},
	}
	for _, storage := range storages {
		t.Run(storage.name, func(t *testing.T) {
			// Set the threshold low so a handful of proposals triggers a
			// snapshot; then check that storage no longer holds the old entries.
			net := NewMemNetwork()
			conf := configFor([]NodeID{"only"})
			cfg := fastConfigID("only")
			cfg.SnapshotThresholdEntries = 3
			st := storage.open(t)
			defer st.Close()
			fsm := newRecordingFSM()
			transport := net.Transport("only")
			defer transport.Close()
			n, err := NewNode(cfg, fsm, st, transport, conf, nil)
			if err != nil {
				t.Fatalf("NewNode: %v", err)
			}
			n.Start()
			defer n.Close()
			if _, ok := pollUntil(t, 2*time.Second, func() (struct{}, bool) {
				return struct{}{}, n.IsLeader()
			}); !ok {
				t.Fatal("did not become leader")
			}

			// Propose enough entries that the apply loop crosses the threshold.
			for i := 0; i < 5; i++ {
				f, _ := n.Propose(context.Background(), []byte(fmt.Sprintf("e%d", i)))
				if _, err := mustWait(t, f, 2*time.Second); err != nil {
					t.Fatalf("Wait %d: %v", i, err)
				}
			}

			// Observe through Status to avoid touching storage concurrently.
			if _, ok := pollUntil(t, 2*time.Second, func() (struct{}, bool) {
				s := n.Status()
				return struct{}{}, s.LastSnapshotIndex > 0 && s.LogFirstIndex > 1
			}); !ok {
				s := n.Status()
				t.Fatalf("expected a snapshot: snapshotIdx=%d firstIndex=%d",
					s.LastSnapshotIndex, s.LogFirstIndex)
			}
		})
	}
}
