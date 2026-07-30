package raft

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRejectsOverlappingConfigurationChanges(t *testing.T) {
	net := NewMemNetwork()
	initial := Configuration{Voters: map[NodeID]string{"a": "a"}}
	n := mustNewNode(t, fastConfigID("a"), NewMemStorage(), net.Transport("a"), initial)
	n.role = roleLeader
	n.term = 1
	n.progress = map[NodeID]*peerProgress{"a": {nextIndex: 1}}

	first := &confChange{add: true, id: "b", addr: "b", future: newFuture()}
	n.onConfChange(first)
	if n.log.committed != 0 || n.cfgIndex != 1 {
		t.Fatalf("first change state: committed=%d cfg_index=%d", n.log.committed, n.cfgIndex)
	}

	second := &confChange{add: true, id: "c", addr: "c", future: newFuture()}
	n.onConfChange(second)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := second.future.Wait(ctx); !errors.Is(err, ErrConfigChangeInProgress) {
		t.Fatalf("second change error = %v, want ErrConfigChangeInProgress", err)
	}
	if n.log.lastIndex() != 1 || n.config.Has("c") {
		t.Fatalf("second change mutated state: last=%d voters=%v", n.log.lastIndex(), n.config.Voters)
	}
}

func TestFollowerRestoresConfigurationAfterConflictTruncation(t *testing.T) {
	net := NewMemNetwork()
	storage := NewMemStorage()
	initial := Configuration{Voters: map[NodeID]string{"a": "a", "b": "b"}}
	stale := Configuration{Voters: map[NodeID]string{"a": "a", "c": "c"}}
	if err := storage.Append([]Entry{{
		Index: 1,
		Term:  1,
		Type:  EntryConfig,
		Data:  encodeConfig(nil, stale),
	}}); err != nil {
		t.Fatal(err)
	}
	n := mustNewNode(t, fastConfigID("b"), storage, net.Transport("b"), initial)
	if !n.config.Has("c") {
		t.Fatalf("setup did not adopt stale configuration: %v", n.config.Voters)
	}

	resp := n.handleAppendEntries("a", &AppendEntriesReq{
		Term:         2,
		LeaderID:     "a",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []Entry{{
			Index: 1,
			Term:  2,
			Type:  EntryNormal,
			Data:  []byte("replacement"),
		}},
	})
	if !resp.Success {
		t.Fatal("replacement append was rejected")
	}
	if !n.config.Has("b") || n.config.Has("c") || n.cfgIndex != 0 {
		t.Fatalf("configuration not restored: cfg_index=%d voters=%v", n.cfgIndex, n.config.Voters)
	}
}

func TestInstallSnapshotAdoptsConfiguration(t *testing.T) {
	net := NewMemNetwork()
	initial := Configuration{Voters: map[NodeID]string{"a": "a", "b": "b"}}
	n := mustNewNode(t, fastConfigID("b"), NewMemStorage(), net.Transport("b"), initial)
	n.applyc = make(chan applyReq, 1)
	n.term = 1

	snapshotConfig := Configuration{Voters: map[NodeID]string{"a": "a", "c": "c"}}
	resp := n.handleInstallSnapshot("a", &InstallSnapshotReq{
		Term:     1,
		LeaderID: "a",
		Meta: SnapshotMeta{
			LastIncludedIndex: 5,
			LastIncludedTerm:  1,
			Configuration:     snapshotConfig,
		},
		Data: []byte("snapshot"),
	})
	if resp.LastIndex != 5 {
		t.Fatalf("installed snapshot last index = %d, want 5", resp.LastIndex)
	}
	if n.config.Has("b") || !n.config.Has("c") || n.cfgIndex != 5 {
		t.Fatalf("snapshot configuration not adopted: cfg_index=%d voters=%v", n.cfgIndex, n.config.Voters)
	}
}

func TestApplyBatchUsesConfigurationAtBatchIndex(t *testing.T) {
	net := NewMemNetwork()
	storage := NewMemStorage()
	initial := Configuration{Voters: map[NodeID]string{"a": "a", "b": "b"}}
	future := Configuration{Voters: map[NodeID]string{"a": "a", "c": "c"}}
	if err := storage.Append([]Entry{
		{Index: 1, Term: 1, Type: EntryNormal, Data: []byte("committed")},
		{Index: 2, Term: 1, Type: EntryConfig, Data: encodeConfig(nil, future)},
	}); err != nil {
		t.Fatal(err)
	}
	if err := storage.SaveHardState(HardState{CurrentTerm: 1, CommitIndex: 1}); err != nil {
		t.Fatal(err)
	}
	n := mustNewNode(t, fastConfigID("b"), storage, net.Transport("b"), initial)
	n.applyc = make(chan applyReq, 1)

	entries, err := n.log.entries(1, 2)
	if err != nil {
		t.Fatal(err)
	}
	n.shipApplyBatch(entries)
	req := <-n.applyc
	if !req.configAtBatch.Has("b") || req.configAtBatch.Has("c") {
		t.Fatalf("batch at index 1 stamped with future config: %v", req.configAtBatch.Voters)
	}
}

func TestRejectsUnauthorizedRPCBeforeTermChange(t *testing.T) {
	tests := []struct {
		name string
		from NodeID
		msg  Message
	}{
		{
			name: "unknown append sender",
			from: "removed",
			msg:  &AppendEntriesReq{Term: 3, LeaderID: "removed"},
		},
		{
			name: "mismatched append identity",
			from: "b",
			msg:  &AppendEntriesReq{Term: 3, LeaderID: "c"},
		},
		{
			name: "mismatched vote identity",
			from: "b",
			msg:  &RequestVoteReq{Term: 3, CandidateID: "c"},
		},
		{
			name: "mismatched snapshot identity",
			from: "b",
			msg:  &InstallSnapshotReq{Term: 3, LeaderID: "c"},
		},
		{
			name: "mismatched timeout-now identity",
			from: "b",
			msg:  &TimeoutNowReq{Term: 3, LeaderID: "c"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			net := NewMemNetwork()
			conf := Configuration{Voters: map[NodeID]string{"a": "a", "b": "b", "c": "c"}}
			n := mustNewNode(t, fastConfigID("a"), NewMemStorage(), net.Transport("a"), conf)
			n.role = roleLeader
			n.term = 2
			reply := make(chan Message, 1)

			n.onRPC(rpcRequest{from: tt.from, msg: tt.msg, reply: reply})
			if got := (<-reply).messageTerm(); got != 2 {
				t.Fatalf("reply term = %d, want 2", got)
			}
			if n.role != roleLeader || n.term != 2 || n.votedFor != "" {
				t.Fatalf("unauthorized RPC mutated state: role=%v term=%d vote=%q", n.role, n.term, n.votedFor)
			}
		})
	}
}

func TestLeaderStepsDownAfterCommittedSelfRemoval(t *testing.T) {
	tc := newTestCluster(t, "n1", "n2", "n3")
	defer tc.stopAll()
	leader := tc.waitLeader()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	fut, err := tc.nodes[leader].RemoveServer(ctx, leader)
	if err != nil {
		t.Fatalf("RemoveServer: %v", err)
	}
	if _, err := fut.Wait(ctx); err != nil {
		t.Fatalf("wait self-removal: %v", err)
	}
	if _, ok := pollUntil(t, time.Second, func() (struct{}, bool) {
		return struct{}{}, !tc.nodes[leader].IsLeader()
	}); !ok {
		t.Fatal("self-removed leader did not step down")
	}
	if tc.nodes[leader].Ready() {
		t.Fatal("self-removed node still reports ready")
	}

	var remaining []NodeID
	for _, id := range tc.ids {
		if id != leader {
			remaining = append(remaining, id)
		}
	}
	if next := tc.waitLeader(remaining...); next == leader {
		t.Fatal("removed node was re-elected")
	}
}
