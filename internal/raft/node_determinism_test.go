package raft

import (
	"io"
	"sync/atomic"
	"testing"
	"time"
)

// Snapshot metadata must be byte-identical across replicas at identical log
// state. n.config carries bootstrap-supplied client metadata, so the slow
// (uncommitted-later-config) path has to apply the same normalization — else
// one replica stamps a snapshot with client metadata and another without, and
// regenerating that generation is rejected as conflicting metadata.
func TestConfigurationAtIsCanonicalOnBothPaths(t *testing.T) {
	logged := Configuration{Voters: map[NodeID]string{"a": "a", "b": "b"}}
	bootstrap := Configuration{
		Voters:      map[NodeID]string{"a": "a", "b": "b"},
		ClientAddrs: map[NodeID]string{"a": "a:client", "b": "b:client"},
	}

	// Fast path: no configuration entry beyond the requested index.
	fastStore := NewMemStorage()
	mustAppend(t, fastStore, []Entry{
		{Index: 1, Term: 1, Type: EntryConfig, Data: encodeConfig(nil, logged)},
		{Index: 2, Term: 1, Type: EntryNoOp},
	})
	fast := mustNewNode(t, fastConfigID("a"), fastStore, NewMemNetwork().Transport("a"), bootstrap)
	defer fast.Close()

	// Slow path: an additional, later configuration entry forces reconstruction
	// from durable state for index 2.
	slowStore := NewMemStorage()
	mustAppend(t, slowStore, []Entry{
		{Index: 1, Term: 1, Type: EntryConfig, Data: encodeConfig(nil, logged)},
		{Index: 2, Term: 1, Type: EntryNoOp},
		{Index: 3, Term: 1, Type: EntryConfig, Data: encodeConfig(nil, logged)},
	})
	slow := mustNewNode(t, fastConfigID("b"), slowStore, NewMemNetwork().Transport("b"), bootstrap)
	defer slow.Close()

	fastCfg, _, err := fast.configurationAt(2)
	if err != nil {
		t.Fatalf("fast path: %v", err)
	}
	slowCfg, _, err := slow.configurationAt(2)
	if err != nil {
		t.Fatalf("slow path: %v", err)
	}
	if slow.cfgIndex <= 2 {
		t.Fatalf("setup did not exercise the slow path: cfgIndex=%d", slow.cfgIndex)
	}
	if !configurationsEqual(fastCfg, slowCfg) {
		t.Fatalf("configurationAt(2) differs by path:\n fast=%+v\n slow=%+v", fastCfg, slowCfg)
	}
}

// countingRestoreFSM counts Restore calls and accepts any payload, including
// an empty one, so a test can distinguish "restore ran" from "restore failed".
type countingRestoreFSM struct {
	noopFSM
	restores atomic.Int64
}

func (f *countingRestoreFSM) Restore(io.Reader) error {
	f.restores.Add(1)
	return nil
}

// A snapshot carrying zero FSM bytes must still drive FSM.Restore. Storage and
// applyDispatched advance regardless, so skipping the restore leaves the FSM
// holding pre-snapshot state that nothing will ever correct.
func TestZeroLengthSnapshotStillRestoresFSM(t *testing.T) {
	fsm := &countingRestoreFSM{}
	n, err := NewNode(fastConfigID("a"), fsm, NewMemStorage(), NewMemNetwork().Transport("a"),
		Configuration{Voters: map[NodeID]string{"a": "a", "b": "b"}}, nil)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if err := n.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer n.Close()

	// Empty payload, exactly as a decoded wire message with no data yields.
	n.scheduleFSMRestore(SnapshotMeta{LastIncludedIndex: 5, LastIncludedTerm: 2}, nil)

	if _, ok := pollUntil(t, 3*time.Second, func() (bool, bool) {
		return true, fsm.restores.Load() > 0
	}); !ok {
		t.Fatal("a zero-length snapshot skipped FSM.Restore while storage and " +
			"applyDispatched advanced past it, leaving the FSM silently stale")
	}
}
