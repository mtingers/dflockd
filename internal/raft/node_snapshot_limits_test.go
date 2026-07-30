package raft

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

type fixedSnapshotFSM struct{ data []byte }

func (*fixedSnapshotFSM) Apply(Entry) any { return nil }
func (f *fixedSnapshotFSM) Snapshot() (FSMSnapshot, error) {
	return fixedFSMSnapshot{data: f.data}, nil
}
func (*fixedSnapshotFSM) Restore(io.Reader) error { return nil }

type fixedFSMSnapshot struct{ data []byte }

func (s fixedFSMSnapshot) Persist(w io.Writer) error {
	_, err := w.Write(s.data)
	return err
}
func (fixedFSMSnapshot) Release() {}

func TestSnapshotWireBudgetIncludesWorstCaseMetadata(t *testing.T) {
	got := maxSnapshotDataBytes + installSnapshotFixedBytes + maxConfigBytes
	if got != maxRPCPayloadBytes {
		t.Fatalf("derived snapshot payload = %d, want RPC budget %d", got, maxRPCPayloadBytes)
	}
	if maxSnapshotFileBytes != snapshotEnvelopeBytes+maxConfigBytes+maxSnapshotDataBytes {
		t.Fatal("snapshot file cap does not include its complete envelope")
	}
}

func TestSnapshotCaptureEnforcesConfiguredBoundary(t *testing.T) {
	cfg := fastConfigID("only")
	cfg.MaxSnapshotBytes = 4
	fsm := &fixedSnapshotFSM{data: []byte("1234")}
	n, err := NewNode(cfg, fsm, NewMemStorage(), NewMemNetwork().Transport("only"), configFor([]NodeID{"only"}), nil)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	if err := n.captureAndQueueSnapshot(1, 1, configFor([]NodeID{"only"})); err != nil {
		t.Fatalf("exact-boundary capture: %v", err)
	}
	req := <-n.snapSavec
	if !bytes.Equal(req.data, []byte("1234")) {
		t.Fatalf("captured data = %q", req.data)
	}

	fsm.data = []byte("12345")
	if err := n.captureAndQueueSnapshot(2, 1, configFor([]NodeID{"only"})); err == nil ||
		!strings.Contains(err.Error(), "configured max 4") {
		t.Fatalf("over-boundary capture error = %v", err)
	}
}

func TestInstallSnapshotRejectsConfiguredOversizeBeforeTermChange(t *testing.T) {
	cfg := fastConfigID("b")
	cfg.MaxSnapshotBytes = 4
	n, err := NewNode(cfg, NewNoopFSM(), NewMemStorage(), NewMemNetwork().Transport("b"), configFor([]NodeID{"a", "b"}), nil)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	n.term = 2
	meta := SnapshotMeta{LastIncludedIndex: 3, LastIncludedTerm: 2, Configuration: configFor([]NodeID{"a", "b"})}

	reply := make(chan Message, 1)
	n.onRPC(rpcRequest{
		from:  "a",
		msg:   &InstallSnapshotReq{Term: 3, LeaderID: "a", Meta: meta, Data: []byte("12345")},
		reply: reply,
	})
	resp := (<-reply).(*InstallSnapshotResp)
	if resp.Term != 2 || n.term != 2 {
		t.Fatalf("oversized snapshot changed term: resp=%d node=%d", resp.Term, n.term)
	}
	if _, ok := n.log.storage.SnapshotMeta(); ok {
		t.Fatal("oversized snapshot was persisted")
	}

	resp = n.handleInstallSnapshot("a", &InstallSnapshotReq{Term: 3, LeaderID: "a", Meta: meta, Data: []byte("1234")})
	if resp.LastIndex != 3 {
		t.Fatalf("exact-boundary snapshot last index = %d, want 3", resp.LastIndex)
	}
}
