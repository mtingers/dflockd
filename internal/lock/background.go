package lock

import (
	"context"
	"time"
)

// LeaseExpiryLoop runs the periodic lease-expiry sweep. Runs until ctx
// is cancelled.
func (lm *LockManager) LeaseExpiryLoop(ctx context.Context) {
	lm.log.Debug("lease_expiry_loop: starting")
	ticker := time.NewTicker(lm.cfg.LeaseSweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			lm.sweepLeases(time.Now())
		}
	}
}

// sweepLeases evicts every expired holder across all shards and grants
// freed slots to the next waiters. Exposed for tests.
func (lm *LockManager) sweepLeases(now time.Time) {
	for i := range lm.shards {
		sh := &lm.shards[i]
		sh.mu.Lock()
		for key, st := range sh.resources {
			any := false
			for token, h := range st.Holders {
				if h.leaseExpires.IsZero() {
					continue
				}
				if !now.Before(h.leaseExpires) {
					lm.log.Warn("lease expired", "key", key, "conn", h.connID)
					sh.removeOwned(h.connID, key, token)
					eqKey := connKey{ConnID: h.connID, Key: key}
					if es, ok := sh.connEnqueued[eqKey]; ok && es.token == token {
						sh.removeEnqueued(eqKey)
					}
					delete(st.Holders, token)
					any = true
				}
			}
			if any {
				st.LastActivity = now
				lm.grantNext(sh, key, st)
			}
		}
		sh.mu.Unlock()
	}
}

// GCLoop prunes resource state that has been idle for longer than
// GCMaxIdleTime. A resource is idle when it has no holders, no waiters,
// and LastActivity has not been touched within the cutoff.
func (lm *LockManager) GCLoop(ctx context.Context) {
	lm.log.Debug("lock_gc_loop: starting")
	ticker := time.NewTicker(lm.cfg.GCInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			lm.gcOnce(time.Now())
		}
	}
}

// gcOnce performs one GC pass. Exposed for tests.
func (lm *LockManager) gcOnce(now time.Time) {
	for i := range lm.shards {
		sh := &lm.shards[i]
		sh.mu.Lock()
		var expired []string
		for key, st := range sh.resources {
			idle := now.Sub(st.LastActivity)
			if idle > lm.cfg.GCMaxIdleTime && len(st.Holders) == 0 && st.waiterCount() == 0 {
				expired = append(expired, key)
			}
		}
		if n := len(expired); n > 0 {
			lm.resourceTotal.Add(-int64(n))
			for _, key := range expired {
				lm.log.Debug("gc: pruning idle state", "key", key)
				delete(sh.resources, key)
			}
		}
		sh.mu.Unlock()
	}
}
