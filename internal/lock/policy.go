package lock

import (
	"fmt"
	"time"
)

// FSMPolicy is the versioned cluster policy that affects replicated state
// transitions. Values use fixed-width wire representations so every replica
// compares the same data regardless of architecture.
type FSMPolicy struct {
	Version                 uint8 `json:"v"`
	MaxLocks                int64 `json:"max_locks"`
	MaxWaiters              int64 `json:"max_waiters"`
	OrphanTTLNanos          int64 `json:"orphan_ttl_ns"`
	GCMaxIdleTimeNanos      int64 `json:"gc_max_idle_ns"`
	AutoReleaseOnDisconnect bool  `json:"auto_release"`
}

const CurrentFSMPolicyVersion = 1

// Validate rejects policy values that cannot be applied safely.
func (p FSMPolicy) Validate() error {
	if p.Version != CurrentFSMPolicyVersion {
		return fmt.Errorf("lock: unsupported FSM policy version %d", p.Version)
	}
	if p.MaxLocks <= 0 {
		return fmt.Errorf("lock: FSM policy MaxLocks must be positive")
	}
	if p.MaxWaiters < 0 {
		return fmt.Errorf("lock: FSM policy MaxWaiters must be non-negative")
	}
	if p.OrphanTTLNanos < 0 || p.GCMaxIdleTimeNanos < 0 {
		return fmt.Errorf("lock: FSM policy durations must be non-negative")
	}
	return nil
}

func (p FSMPolicy) orphanTTL() time.Duration     { return time.Duration(p.OrphanTTLNanos) }
func (p FSMPolicy) gcMaxIdleTime() time.Duration { return time.Duration(p.GCMaxIdleTimeNanos) }

// ConfiguredFSMPolicy returns the policy represented by this process's local
// configuration. The cluster FSM adopts the first replicated policy and then
// rejects attempts to change it implicitly.
func (lm *LockManager) ConfiguredFSMPolicy() FSMPolicy {
	return FSMPolicy{
		Version:                 CurrentFSMPolicyVersion,
		MaxLocks:                int64(lm.cfg.MaxLocks),
		MaxWaiters:              int64(lm.cfg.MaxWaiters),
		OrphanTTLNanos:          int64(lm.cfg.OrphanTTL),
		GCMaxIdleTimeNanos:      int64(lm.cfg.GCMaxIdleTime),
		AutoReleaseOnDisconnect: lm.cfg.AutoReleaseOnDisconnect,
	}
}

// ActiveFSMPolicy returns the replicated policy when one has been installed,
// or the local configured policy before the first policy-bearing command.
func (lm *LockManager) ActiveFSMPolicy() (FSMPolicy, bool) {
	if p := lm.fsmPolicy.Load(); p != nil {
		return *p, true
	}
	return lm.ConfiguredFSMPolicy(), false
}

// InstallFSMPolicy activates a policy for Apply operations and rebuilds the
// stable-ref derived indexes used for failover reattachment.
func (lm *LockManager) InstallFSMPolicy(policy FSMPolicy) error {
	if err := policy.Validate(); err != nil {
		return err
	}
	copy := policy
	lm.fsmPolicy.Store(&copy)
	lm.rebuildRefIndexes(true)
	return nil
}

// ClearFSMPolicy restores legacy/local behavior. It is used only when loading
// a pre-policy snapshot.
func (lm *LockManager) ClearFSMPolicy() {
	lm.fsmPolicy.Store(nil)
	lm.rebuildRefIndexes(true)
}

func (lm *LockManager) activeFSMPolicy() FSMPolicy {
	policy, _ := lm.ActiveFSMPolicy()
	return policy
}

func (lm *LockManager) rebuildRefIndexes(enabled bool) {
	for i := range lm.shards {
		sh := &lm.shards[i]
		sh.mu.Lock()
		for _, state := range sh.resources {
			state.indexRefs = enabled
			state.refs = nil
			if !enabled {
				continue
			}
			for token, holder := range state.Holders {
				state.indexHolder(token, holder)
			}
			for j := state.WaiterHead; j < len(state.Waiters); j++ {
				state.indexWaiter(state.Waiters[j])
			}
		}
		sh.mu.Unlock()
	}
}
