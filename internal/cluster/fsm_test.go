package cluster

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/mtingers/dflockd/internal/lock"
	"github.com/mtingers/dflockd/internal/raft"
)

// TestFSMAdapterSnapshotPersistRestoreRoundTrip exercises the
// cluster.fsm raft.FSM adapter directly: it builds an fsm over a
// LockManager, drives some Apply* state in, then walks the
// Snapshot → Persist → Restore round-trip and asserts a re-snapshot
// is byte-identical to the first. Coverage of fsm.go's Snapshot,
// Restore, fsmSnapshot.Persist, and fsmSnapshot.Release entry points
// (which the higher-level cluster tests never naturally trigger
// because their state is too small to cross a snapshot threshold).
func TestFSMAdapterSnapshotPersistRestoreRoundTrip(t *testing.T) {
	lm := newClusterLM(t)
	f := &fsm{lm: lm}

	now := time.Now()
	if _, _, err := lm.ApplyAcquire(now, "lock:k1", 1, "refA", 1, 30*time.Second, saltOf(1)); err != nil {
		t.Fatalf("ApplyAcquire k1: %v", err)
	}
	if _, _, err := lm.ApplyAcquire(now, "sem:s1", 3, "refB", 2, 30*time.Second, saltOf(2)); err != nil {
		t.Fatalf("ApplyAcquire s1: %v", err)
	}

	snap, err := f.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&buf); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	snap.Release() // no-op, but exercise the entry point

	if buf.Len() == 0 {
		t.Fatal("snapshot bytes are empty")
	}

	lm2 := newClusterLM(t)
	f2 := &fsm{lm: lm2}
	if err := f2.Restore(io.NopCloser(bytes.NewReader(buf.Bytes()))); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	snap2, err := f2.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot after Restore: %v", err)
	}
	var buf2 bytes.Buffer
	if err := snap2.Persist(&buf2); err != nil {
		t.Fatalf("Persist after Restore: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), buf2.Bytes()) {
		t.Fatalf("snapshot bytes differ after round-trip: %d vs %d bytes", buf.Len(), buf2.Len())
	}
}

// TestNodeLockManagerAccessor covers cluster.Node.LockManager, the
// thin accessor that the server-side integration uses to install grant
// listeners on the FSM-backing LockManager.
func TestNodeLockManagerAccessor(t *testing.T) {
	tc := newCluster(t, "n1")
	defer tc.stopAll()
	tc.waitLeader()
	if got := tc.nodes["n1"].LockManager(); got != tc.lms["n1"] {
		t.Fatalf("LockManager() returned %p, want %p", got, tc.lms["n1"])
	}
}

// TestSetDeleteMember covers the two private membership-state
// accessors directly. AddVoter / RemoveServer themselves are covered
// at the raft layer (internal/raft/membership_test.go) and going
// through a real raft round-trip from cluster.Node would require a
// reachable second listener; the wrapper logic this test pins is the
// mutex-guarded cfg.Members map mutation, which is what's actually
// owned by cluster.Node (not raft).
func TestSetDeleteMember(t *testing.T) {
	tc := newCluster(t, "n1")
	defer tc.stopAll()
	n := tc.nodes["n1"]

	n.setMember("n9", Member{RaftAddr: "127.0.0.1:9999", ClientAddr: "127.0.0.1:8888"})
	n.membersMu.Lock()
	m, ok := n.cfg.Members["n9"]
	n.membersMu.Unlock()
	if !ok || m.RaftAddr != "127.0.0.1:9999" || m.ClientAddr != "127.0.0.1:8888" {
		t.Fatalf("setMember: got %+v, ok=%v", m, ok)
	}

	n.deleteMember("n9")
	n.membersMu.Lock()
	_, ok = n.cfg.Members["n9"]
	n.membersMu.Unlock()
	if ok {
		t.Fatal("deleteMember did not remove the entry")
	}
}

// Sanity check on the embedded import shape — keeps the linter quiet
// if a future refactor splits raft.NodeID out from the wider package.
var _ raft.NodeID = "n1"
var _ lock.ApplyResult = lock.ApplyResult{}
