package cluster

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
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
	lm     *lock.LockManager
	policy *lock.FSMPolicy
	log    *slog.Logger
}

var _ raft.FSM = (*fsm)(nil)

func newFSM(lm *lock.LockManager, logger *slog.Logger) *fsm {
	if logger == nil {
		logger = slog.Default()
	}
	return &fsm{lm: lm, log: logger}
}

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

// fsmHandlers maps a command kind to its apply function. Using a table
// (rather than a switch) keeps dispatch's cyclomatic complexity flat as
// kinds are added.
var fsmHandlers = map[Kind]func(*fsm, time.Time, Command) any{
	KindAcquire:      (*fsm).applyAcquire,
	KindEnqueue:      (*fsm).applyEnqueue,
	KindRelease:      (*fsm).applyRelease,
	KindRenew:        (*fsm).applyRenew,
	KindEvict:        (*fsm).applyEvict,
	KindCleanupConn:  (*fsm).applyCleanupConn,
	KindGC:           func(f *fsm, now time.Time, _ Command) any { return f.lm.ApplyGC(now) },
	KindEvictExpired: (*fsm).applyEvictExpired,
	KindCancel:       (*fsm).applyCancel,
	KindAttach:       (*fsm).applyAttach,
	KindBarrier:      func(_ *fsm, _ time.Time, _ Command) any { return lock.ApplyResult{Status: lock.StatusOK} },
}

func (f *fsm) dispatch(cmd Command) any {
	if err := f.ensurePolicy(cmd.Policy); err != nil {
		return applyErrResult(err)
	}
	h, ok := fsmHandlers[cmd.Kind]
	if !ok {
		return applyErrResult(errUnknownKind)
	}
	return h(f, time.Unix(0, cmd.NowNanos), cmd)
}

func (f *fsm) ensurePolicy(proposed *lock.FSMPolicy) error {
	if proposed == nil {
		return nil // legacy command; retained for pre-policy log replay
	}
	if err := proposed.Validate(); err != nil {
		return err
	}
	if f.policy != nil {
		if *f.policy != *proposed {
			return fmt.Errorf("%w: committed=%+v proposed=%+v", ErrPolicyMismatch, *f.policy, *proposed)
		}
		return nil
	}
	if err := f.lm.InstallFSMPolicy(*proposed); err != nil {
		return err
	}
	f.warnIfPolicyOverridesLocal(*proposed)
	copy := *proposed
	f.policy = &copy
	return nil
}

// warnIfPolicyOverridesLocal reports that the cluster adopted a policy other
// than this node's configuration. The replicated policy is authoritative -
// that is what makes Apply deterministic - but silently ignoring an operator's
// flags is exactly the kind of surprise that shows up later as an unexplained
// limit, so it is worth one line in the log.
func (f *fsm) warnIfPolicyOverridesLocal(adopted lock.FSMPolicy) {
	local := f.lm.ConfiguredFSMPolicy()
	if local == adopted {
		return
	}
	f.log.Warn("cluster FSM policy differs from this node's configuration; the replicated policy wins",
		"replicated", fmt.Sprintf("%+v", adopted),
		"local", fmt.Sprintf("%+v", local))
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
	result, grants, err := f.lm.ApplyRenewOwned(now, cmd.Key, cmd.Token, cmd.Ref, cmd.ConnID, time.Duration(cmd.LeaseTTLNanos))
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

func (f *fsm) applyCancel(now time.Time, cmd Command) any {
	salt, err := DecodeSalt(cmd.SaltB64)
	if err != nil {
		return applyErrResult(err)
	}
	result, grants, err := f.lm.ApplyCancel(now, cmd.Key, cmd.Ref, cmd.ConnID, salt, cmd.SaltB64 != "")
	f.lm.RouteGrants(grants)
	return resultOr(result, err)
}

func (f *fsm) applyAttach(now time.Time, cmd Command) any {
	result, grants, err := f.lm.ApplyAttach(now, cmd.Key, cmd.Ref, cmd.ConnID)
	f.lm.RouteGrants(grants)
	return resultOr(result, err)
}

func (f *fsm) applyEvictExpired(now time.Time, _ Command) any {
	result, grants, err := f.lm.ApplyEvictExpired(now)
	f.lm.RouteGrants(grants)
	return resultOr(result, err)
}

// Snapshot captures FSM state for log compaction. raft.FSM.Snapshot is
// allowed to return a value that Persist() encodes concurrently with
// later Apply calls — lock.LockManager's Snapshot copies state out
// under shard locks, so the persisted bytes are stable.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	var lockState bytes.Buffer
	if err := f.lm.Snapshot(&lockState); err != nil {
		return nil, err
	}
	data, err := encodeFSMSnapshot(f.policy, lockState.Bytes())
	if err != nil {
		return nil, err
	}
	return &fsmSnapshot{data: data}, nil
}

// Restore loads FSM state from r, replacing whatever the LockManager
// currently holds. raft invokes this on startup (from a persisted
// snapshot) and on follower InstallSnapshot.
func (f *fsm) Restore(r io.Reader) error {
	raw, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	policy, lockState, legacy, err := decodeFSMSnapshot(raw)
	if err != nil {
		return err
	}
	if legacy {
		f.policy = nil
		f.lm.ClearFSMPolicy()
		return f.lm.Restore(bytes.NewReader(raw))
	}
	if policy == nil {
		f.policy = nil
		f.lm.ClearFSMPolicy()
	} else {
		if err := f.lm.InstallFSMPolicy(*policy); err != nil {
			return err
		}
		copy := *policy
		f.policy = &copy
	}
	return f.lm.Restore(bytes.NewReader(lockState))
}

var fsmSnapshotMagic = [8]byte{'d', 'f', 'l', 'c', 'f', 's', 'm', '2'}

const maxFSMPolicyBytes = 64 << 10

func encodeFSMSnapshot(policy *lock.FSMPolicy, lockState []byte) ([]byte, error) {
	var policyData []byte
	var err error
	if policy != nil {
		policyData, err = json.Marshal(policy)
		if err != nil {
			return nil, fmt.Errorf("cluster: encode FSM policy: %w", err)
		}
	}
	if len(policyData) > maxFSMPolicyBytes {
		return nil, fmt.Errorf("cluster: FSM policy is too large: %d", len(policyData))
	}
	out := make([]byte, 0, len(fsmSnapshotMagic)+4+len(policyData)+len(lockState))
	out = append(out, fsmSnapshotMagic[:]...)
	out = binary.BigEndian.AppendUint32(out, uint32(len(policyData)))
	out = append(out, policyData...)
	out = append(out, lockState...)
	return out, nil
}

func decodeFSMSnapshot(raw []byte) (policy *lock.FSMPolicy, lockState []byte, legacy bool, err error) {
	if len(raw) < len(fsmSnapshotMagic) || !bytes.Equal(raw[:len(fsmSnapshotMagic)], fsmSnapshotMagic[:]) {
		return nil, nil, true, nil
	}
	header := len(fsmSnapshotMagic) + 4
	if len(raw) < header {
		return nil, nil, false, fmt.Errorf("cluster: truncated FSM snapshot header")
	}
	policyLen := int(binary.BigEndian.Uint32(raw[len(fsmSnapshotMagic):header]))
	if policyLen > maxFSMPolicyBytes || policyLen > len(raw)-header {
		return nil, nil, false, fmt.Errorf("cluster: invalid FSM policy length %d", policyLen)
	}
	if policyLen > 0 {
		var decoded lock.FSMPolicy
		if err := json.Unmarshal(raw[header:header+policyLen], &decoded); err != nil {
			return nil, nil, false, fmt.Errorf("cluster: decode FSM policy: %w", err)
		}
		if err := decoded.Validate(); err != nil {
			return nil, nil, false, err
		}
		policy = &decoded
	}
	return policy, raw[header+policyLen:], false, nil
}

// fsmSnapshot is an immutable bytes view of a LockManager snapshot,
// owned by raft until Release. It is intentionally simple: the heavy
// lifting (sorted serialisation) happens at Snapshot time, so Persist
// is a single Write call.
type fsmSnapshot struct{ data []byte }

// Persist implements raft.FSMSnapshot.
func (s *fsmSnapshot) Persist(w io.Writer) error {
	_, err := w.Write(s.data)
	return err
}

// Release implements raft.FSMSnapshot. No-op — the snapshot is a
// plain byte slice and Go's GC reclaims it.
func (s *fsmSnapshot) Release() {}
