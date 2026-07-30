package lock

import "time"

// EvictionDue reports whether an EvictExpired command applied at now would
// remove at least one expired lease or orphan. It is an advisory local check:
// callers still replicate ApplyEvictExpired for the authoritative mutation.
func (lm *LockManager) EvictionDue(now time.Time) bool {
	for i := range lm.shards {
		sh := &lm.shards[i]
		sh.mu.Lock()
		due := lm.shardEvictionDue(sh, now)
		sh.mu.Unlock()
		if due {
			return true
		}
	}
	return false
}

func (lm *LockManager) shardEvictionDue(sh *shard, now time.Time) bool {
	for _, st := range sh.resources {
		if lm.resourceEvictionDue(st, now) {
			return true
		}
	}
	return false
}

func (lm *LockManager) resourceEvictionDue(st *ResourceState, now time.Time) bool {
	for _, h := range st.Holders {
		if leaseHasExpired(h, now) || lm.orphanPastTTL(h.abandonedAtNanos, now) {
			return true
		}
	}
	for i := st.WaiterHead; i < len(st.Waiters); i++ {
		w := st.Waiters[i]
		if w != nil && lm.orphanPastTTL(w.abandonedAtNanos, now) {
			return true
		}
	}
	return false
}

// GCDue reports whether a GC command applied at now would remove at least one
// idle resource. Like EvictionDue, this is only a local proposal gate.
func (lm *LockManager) GCDue(now time.Time) bool {
	maxIdle := lm.activeFSMPolicy().gcMaxIdleTime()
	for i := range lm.shards {
		sh := &lm.shards[i]
		sh.mu.Lock()
		due := shardGCDue(sh, now, maxIdle)
		sh.mu.Unlock()
		if due {
			return true
		}
	}
	return false
}

func shardGCDue(sh *shard, now time.Time, maxIdle time.Duration) bool {
	for _, st := range sh.resources {
		if isIdleResource(st, now, maxIdle) {
			return true
		}
	}
	return false
}
