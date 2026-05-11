package cluster

import (
	"bytes"
	"io"
	"time"

	"github.com/mtingers/dflockd/internal/lock"
	"github.com/mtingers/dflockd/internal/raft"
)

// fsm is the raft.FSM that dispatches committed entries into the
// LockManager's deterministic ApplyX methods. The raft apply goroutine
// is the sole caller, so fsm itself needs no locking; routing of the
// returned grants happens after each Apply (the listener registry
// inside LockManager is goroutine-safe on its own).
type fsm struct {
	lm *lock.LockManager
}

var _ raft.FSM = (*fsm)(nil)

func newFSM(lm *lock.LockManager) *fsm { return &fsm{lm: lm} }

// Apply decodes one log entry into a Command, dispatches to the matching
// LockManager ApplyX, routes any produced grants, and returns the
// ApplyResult — which the raft package surfaces as the proposer's
// Future result.
func (f *fsm) Apply(e raft.Entry) any {
	if e.Type != raft.EntryNormal {
		return nil
	}
	cmd, err := Decode(e.Data)
	if err != nil {
		return applyErrResult(err)
	}
	return f.dispatch(cmd)
}

func (f *fsm) dispatch(cmd Command) any {
	now := time.Unix(0, cmd.NowNanos)
	switch cmd.Kind {
	case KindAcquire:
		return f.applyAcquire(now, cmd)
	case KindEnqueue:
		return f.applyEnqueue(now, cmd)
	case KindRelease:
		return f.applyRelease(now, cmd)
	case KindRenew:
		return f.applyRenew(now, cmd)
	case KindEvict:
		return f.applyEvict(now, cmd)
	case KindCleanupConn:
		return f.applyCleanupConn(now, cmd)
	case KindGC:
		return f.lm.ApplyGC(now)
	case KindBarrier:
		return lock.ApplyResult{Status: lock.StatusOK}
	default:
		return applyErrResult(errUnknownKind)
	}
}

func (f *fsm) applyAcquire(now time.Time, cmd Command) any {
	salt, err := DecodeSalt(cmd.SaltB64)
	if err != nil {
		return applyErrResult(err)
	}
	result, grants, err := f.lm.ApplyAcquire(now, cmd.Key, cmd.Limit, cmd.Ref, cmd.ConnID, time.Duration(cmd.LeaseTTLNanos), salt)
	f.lm.RouteGrants(grants)
	return resultOr(result, err)
}

func (f *fsm) applyEnqueue(now time.Time, cmd Command) any {
	salt, err := DecodeSalt(cmd.SaltB64)
	if err != nil {
		return applyErrResult(err)
	}
	result, grants, err := f.lm.ApplyEnqueue(now, cmd.Key, cmd.Limit, cmd.Ref, cmd.ConnID, time.Duration(cmd.LeaseTTLNanos), salt)
	f.lm.RouteGrants(grants)
	return resultOr(result, err)
}

func (f *fsm) applyRelease(now time.Time, cmd Command) any {
	result, grants, err := f.lm.ApplyRelease(now, cmd.Key, cmd.Token)
	f.lm.RouteGrants(grants)
	return resultOr(result, err)
}

func (f *fsm) applyRenew(now time.Time, cmd Command) any {
	result, grants, err := f.lm.ApplyRenew(now, cmd.Key, cmd.Token, time.Duration(cmd.LeaseTTLNanos))
	f.lm.RouteGrants(grants)
	return resultOr(result, err)
}

func (f *fsm) applyEvict(now time.Time, cmd Command) any {
	result, grants, err := f.lm.ApplyEvict(now, cmd.Key, cmd.Token)
	f.lm.RouteGrants(grants)
	return resultOr(result, err)
}

func (f *fsm) applyCleanupConn(now time.Time, cmd Command) any {
	result, grants, err := f.lm.ApplyCleanupConn(now, cmd.Ref, cmd.ConnID)
	f.lm.RouteGrants(grants)
	return resultOr(result, err)
}

// Snapshot captures FSM state for log compaction. raft.FSM.Snapshot is
// allowed to return a value that Persist() encodes concurrently with
// later Apply calls — lock.LockManager's Snapshot copies state out
// under shard locks, so the persisted bytes are stable.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	var buf bytes.Buffer
	if err := f.lm.Snapshot(&buf); err != nil {
		return nil, err
	}
	return &fsmSnapshot{data: buf.Bytes()}, nil
}

// Restore loads FSM state from r, replacing whatever the LockManager
// currently holds. raft invokes this on startup (from a persisted
// snapshot) and on follower InstallSnapshot.
func (f *fsm) Restore(r io.Reader) error { return f.lm.Restore(r) }

type fsmSnapshot struct{ data []byte }

func (s *fsmSnapshot) Persist(w io.Writer) error {
	_, err := w.Write(s.data)
	return err
}
func (s *fsmSnapshot) Release() {}
