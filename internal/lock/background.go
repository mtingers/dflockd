package lock

import (
	"context"
	"time"
)

// LeaseExpiryLoop runs the periodic lease-expiry sweep until ctx is
// cancelled.
func (lm *LockManager) LeaseExpiryLoop(ctx context.Context) {
	lm.log.Debug("lease_expiry_loop: starting")
	runTickerLoop(ctx, lm.cfg.LeaseSweepInterval, func(now time.Time) {
		lm.sweepLeases(now)
	})
}

// GCLoop prunes resource state idle longer than GCMaxIdleTime.
func (lm *LockManager) GCLoop(ctx context.Context) {
	lm.log.Debug("lock_gc_loop: starting")
	runTickerLoop(ctx, lm.cfg.GCInterval, func(now time.Time) {
		lm.gcOnce(now)
	})
}

// runTickerLoop fires fn every interval until ctx is cancelled.
func runTickerLoop(ctx context.Context, interval time.Duration, fn func(time.Time)) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			fn(now)
		}
	}
}

// sweepLeases evicts every expired holder across all shards.
func (lm *LockManager) sweepLeases(now time.Time) {
	for i := range lm.shards {
		lm.sweepShard(&lm.shards[i], now)
	}
}

// sweepShard runs one shard's sweep under sh.mu.
func (lm *LockManager) sweepShard(sh *shard, now time.Time) {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	for key, st := range sh.resources {
		lm.sweepResource(sh, key, st, now)
	}
}

// sweepResource evicts expired holders from one resource and grants
// freed slots to waiters.
func (lm *LockManager) sweepResource(sh *shard, key string, st *ResourceState, now time.Time) {
	if !lm.evictExpiredHolders(sh, key, st, now) {
		return
	}
	st.LastActivity = now
	if err := lm.grantNext(sh, key, st); err != nil {
		lm.log.Error("grant after lease expiry failed", "key", key, "err", err)
	}
}

// evictExpiredHolders walks st.Holders and removes any expired
// holders. Returns true if at least one was removed.
func (lm *LockManager) evictExpiredHolders(sh *shard, key string, st *ResourceState, now time.Time) bool {
	any := false
	for token, h := range st.Holders {
		if !leaseHasExpired(h, now) {
			continue
		}
		lm.evictHolder(sh, st, key, h.connID, token)
		any = true
	}
	return any
}

// evictHolder drops the (key, token) holder entry plus any matching
// connEnqueued bookkeeping. Caller holds sh.mu.
func (lm *LockManager) evictHolder(sh *shard, st *ResourceState, key string, connID uint64, token string) {
	lm.log.Warn("lease expired", "key", key, "conn", connID)
	sh.removeOwned(connID, key, token)
	dropMatchingEnqueued(sh, connID, key, token)
	st.removeHolder(token)
}

// gcOnce performs one GC pass across all shards.
func (lm *LockManager) gcOnce(now time.Time) {
	for i := range lm.shards {
		lm.gcShard(&lm.shards[i], now)
	}
}

// gcShard prunes idle resources in one shard.
func (lm *LockManager) gcShard(sh *shard, now time.Time) {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	expired := collectIdleKeys(sh, now, lm.cfg.GCMaxIdleTime)
	if len(expired) == 0 {
		return
	}
	lm.resourceTotal.Add(-int64(len(expired)))
	lm.deleteResources(sh, expired)
}

// collectIdleKeys returns keys whose resources are idle and unheld.
func collectIdleKeys(sh *shard, now time.Time, maxIdle time.Duration) []string {
	var out []string
	for key, st := range sh.resources {
		if isIdleResource(st, now, maxIdle) {
			out = append(out, key)
		}
	}
	return out
}

func isIdleResource(st *ResourceState, now time.Time, maxIdle time.Duration) bool {
	idle := now.Sub(st.LastActivity)
	return idle > maxIdle && len(st.Holders) == 0 && st.waiterCount() == 0
}

func (lm *LockManager) deleteResources(sh *shard, keys []string) {
	for _, key := range keys {
		lm.log.Debug("gc: pruning idle state", "key", key)
		delete(sh.resources, key)
	}
}
