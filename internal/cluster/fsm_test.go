package cluster

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/mtingers/dflockd/internal/config"
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

func TestFSMReplicatedPolicyOverridesLocalConfigDeterministically(t *testing.T) {
	lmA := newPolicyTestLM(t, 128, 64)
	lmB := newPolicyTestLM(t, 1, 0)
	fA, fB := newFSM(lmA, nil), newFSM(lmB, nil)
	policy := lmA.ConfiguredFSMPolicy()
	cmd := Command{
		Kind: KindAcquire, NowNanos: time.Unix(100, 0).UnixNano(),
		Key: "lock:k", Limit: 1, Ref: "worker", ConnID: 7,
		LeaseTTLNanos: int64(30 * time.Second), SaltB64: EncodeSalt(saltOf(1)),
		Policy: &policy,
	}

	gotA := applyFSMCommand(t, fA, cmd)
	gotB := applyFSMCommand(t, fB, cmd)
	if gotA != gotB || gotA.Status != lock.StatusOK {
		t.Fatalf("replica results differ: A=%+v B=%+v", gotA, gotB)
	}
	activeB, installed := lmB.ActiveFSMPolicy()
	if !installed || activeB != policy {
		t.Fatalf("replica B policy = %+v installed=%v, want %+v", activeB, installed, policy)
	}
	if a, b := snapshotFSMBytes(t, fA), snapshotFSMBytes(t, fB); !bytes.Equal(a, b) {
		t.Fatalf("replica snapshots differ: %d vs %d bytes", len(a), len(b))
	}
}

func TestFSMRejectsPolicyDriftBeforeMutation(t *testing.T) {
	lm := newPolicyTestLM(t, 128, 64)
	f := newFSM(lm, nil)
	policy := lm.ConfiguredFSMPolicy()
	applyFSMCommand(t, f, Command{
		Kind: KindAcquire, NowNanos: time.Unix(100, 0).UnixNano(),
		Key: "lock:k", Limit: 1, Ref: "worker", ConnID: 7,
		LeaseTTLNanos: int64(time.Minute), SaltB64: EncodeSalt(saltOf(1)),
		Policy: &policy,
	})
	before := snapshotFSMBytes(t, f)

	drifted := policy
	drifted.MaxLocks++
	data, err := (Command{
		Kind: KindAcquire, NowNanos: time.Unix(101, 0).UnixNano(),
		Key: "lock:other", Limit: 1, Ref: "other", ConnID: 8,
		LeaseTTLNanos: int64(time.Minute), SaltB64: EncodeSalt(saltOf(2)),
		Policy: &drifted,
	}).Encode()
	if err != nil {
		t.Fatalf("encode drifted command: %v", err)
	}
	got, ok := f.Apply(raft.Entry{Type: raft.EntryNormal, Data: data}).(applyErrTyped)
	if !ok || !errors.Is(got.Err, ErrPolicyMismatch) {
		t.Fatalf("drift result = %#v, want ErrPolicyMismatch", got)
	}
	after := snapshotFSMBytes(t, f)
	if !bytes.Equal(before, after) {
		t.Fatal("policy mismatch mutated FSM state")
	}
}

func TestFSMSnapshotRestoresReplicatedPolicy(t *testing.T) {
	srcLM := newPolicyTestLM(t, 128, 64)
	src := newFSM(srcLM, nil)
	policy := srcLM.ConfiguredFSMPolicy()
	applyFSMCommand(t, src, Command{
		Kind: KindAcquire, NowNanos: time.Unix(100, 0).UnixNano(),
		Key: "lock:k", Limit: 1, Ref: "worker", ConnID: 7,
		LeaseTTLNanos: int64(time.Minute), SaltB64: EncodeSalt(saltOf(1)),
		Policy: &policy,
	})
	snapshot := snapshotFSMBytes(t, src)

	dstLM := newPolicyTestLM(t, 1, 0)
	dst := newFSM(dstLM, nil)
	if err := dst.Restore(bytes.NewReader(snapshot)); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	active, installed := dstLM.ActiveFSMPolicy()
	if !installed || active != policy || dst.policy == nil || *dst.policy != policy {
		t.Fatalf("restored policy = %+v installed=%v fsm=%+v", active, installed, dst.policy)
	}
	if restored := snapshotFSMBytes(t, dst); !bytes.Equal(snapshot, restored) {
		t.Fatalf("restored snapshot differs: %d vs %d bytes", len(snapshot), len(restored))
	}
}

func newPolicyTestLM(t *testing.T, maxLocks, maxWaiters int) *lock.LockManager {
	t.Helper()
	cfg := &config.Config{
		MaxLocks: maxLocks, MaxWaiters: maxWaiters,
		DefaultLeaseTTL: 30 * time.Second, GCMaxIdleTime: time.Minute,
		AutoReleaseOnDisconnect: true,
	}
	lm, err := lock.NewLockManager(cfg, slog.Default())
	if err != nil {
		t.Fatalf("NewLockManager: %v", err)
	}
	t.Cleanup(func() { _ = lm.Close() })
	return lm
}

func applyFSMCommand(t *testing.T, f *fsm, cmd Command) lock.ApplyResult {
	t.Helper()
	data, err := cmd.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got := f.Apply(raft.Entry{Type: raft.EntryNormal, Data: data})
	result, ok := got.(lock.ApplyResult)
	if !ok {
		t.Fatalf("Apply result = %#v", got)
	}
	return result
}

func snapshotFSMBytes(t *testing.T, f *fsm) []byte {
	t.Helper()
	snapshot, err := f.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	defer snapshot.Release()
	var out bytes.Buffer
	if err := snapshot.Persist(&out); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	return out.Bytes()
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

func TestMemberUsesReplicatedMetadata(t *testing.T) {
	tc := newCluster(t, "n1")
	defer tc.stopAll()
	n := tc.nodes["n1"]
	tc.waitLeader()

	n.cfg.Members["n1"] = Member{RaftAddr: "stale-raft", ClientAddr: "stale-client"}
	m, ok := n.member("n1")
	if !ok || m.RaftAddr != "raft-n1" || m.ClientAddr != "client-n1:0" {
		t.Fatalf("replicated member = %+v, ok=%v", m, ok)
	}
}

// Sanity check on the embedded import shape — keeps the linter quiet
// if a future refactor splits raft.NodeID out from the wider package.
var _ raft.NodeID = "n1"
var _ lock.ApplyResult = lock.ApplyResult{}
