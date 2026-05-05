// Package lock implements the distributed FIFO locking primitive.
//
// A LockManager exposes acquire / release / renew / enqueue / wait
// operations for both single-resource locks (Limit==1) and
// multi-resource semaphores (Limit>1). Grants are FIFO: the head of the
// waiter queue gets the next freed slot. Each grant is bound to a token
// the caller must present to release or renew.
//
// State is sharded by key (FNV-1a hash → 64 shards) so independent keys
// do not contend on a single mutex. Per-connection bookkeeping enables
// CleanupConnection to release everything a disconnected client held
// without scanning every key.
package lock

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/mtingers/dflockd/internal/config"
)

// Sentinel errors returned by LockManager methods.
var (
	ErrMaxLocks        = errors.New("max locks reached")
	ErrMaxWaiters      = errors.New("max waiters reached")
	ErrNotEnqueued     = errors.New("not enqueued for this key")
	ErrAlreadyEnqueued = errors.New("already enqueued for this key")
	ErrLimitMismatch   = errors.New("limit mismatch for semaphore key")
	ErrLeaseExpired    = errors.New("lease expired before grant could be observed")
	ErrWaiterClosed    = errors.New("waiter channel closed (connection torn down)")
)

const numShards = 64

// LockManager is the public surface for lock operations.
type LockManager struct {
	shards        [numShards]shard
	resourceTotal atomic.Int64 // total resources across all shards (cap enforcement)
	cfg           *config.Config
	log           *slog.Logger
	tokBuf        tokenBuf
}

// NewLockManager creates a LockManager bound to the given config.
func NewLockManager(cfg *config.Config, log *slog.Logger) *LockManager {
	lm := &LockManager{cfg: cfg, log: log, tokBuf: newTokenBuf()}
	for i := range lm.shards {
		lm.shards[i].init()
	}
	return lm
}

// shardIndex hashes key with FNV-1a (32-bit) and reduces to a shard
// index. Hand-rolled to avoid hash/fnv's per-call allocation.
func shardIndex(key string) int {
	const offset32 = uint32(2166136261)
	const prime32 = uint32(16777619)
	h := offset32
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= prime32
	}
	return int(h % numShards)
}

func (lm *LockManager) shardFor(key string) *shard {
	return &lm.shards[shardIndex(key)]
}

func (lm *LockManager) newToken() string {
	return lm.tokBuf.next()
}

// getOrCreate returns the existing ResourceState for key, or creates a
// fresh one (subject to MaxLocks). A request whose limit doesn't match
// an existing state returns ErrLimitMismatch — locks and semaphores on
// the same key would be ambiguous.
//
// MaxLocks is enforced via a CAS loop on resourceTotal so the cap holds
// across all shards even under racing creators.
func (lm *LockManager) getOrCreate(sh *shard, key string, limit int) (*ResourceState, error) {
	if st, ok := sh.resources[key]; ok {
		if st.Limit != limit {
			return nil, ErrLimitMismatch
		}
		return st, nil
	}
	maxLocks := int64(lm.cfg.MaxLocks)
	for {
		current := lm.resourceTotal.Load()
		if current >= maxLocks {
			return nil, ErrMaxLocks
		}
		if lm.resourceTotal.CompareAndSwap(current, current+1) {
			break
		}
	}
	st := &ResourceState{
		Limit:        limit,
		Holders:      make(map[string]*holder),
		LastActivity: time.Now(),
	}
	sh.resources[key] = st
	return st, nil
}

// grantNext drains the waiter queue while capacity is available. Each
// granted waiter is removed from the queue, sent its token, and added
// to Holders. Must be called with sh.mu held.
func (lm *LockManager) grantNext(sh *shard, key string, st *ResourceState) {
	now := time.Now()
	for st.WaiterHead < len(st.Waiters) && len(st.Holders) < st.Limit {
		w := st.Waiters[st.WaiterHead]
		st.Waiters[st.WaiterHead] = nil
		st.WaiterHead++

		token := lm.newToken()
		// Non-blocking send: if the receiver hasn't reached its select
		// yet, skip this grant for them. The caller (cleanup or timeout)
		// will dequeue them or grant an eventual slot.
		if !lm.trySendGrant(w.ch, token, key, w.connID) {
			continue
		}

		eqKey := connKey{ConnID: w.connID, Key: key}
		if es, ok := sh.connEnqueued[eqKey]; ok && es.waiter == w {
			es.waiter = nil
			es.token = token
		}
		st.Holders[token] = &holder{
			connID:       w.connID,
			leaseExpires: now.Add(w.leaseTTL),
		}
		st.LastActivity = now
		sh.addOwned(w.connID, key, token)
	}
	st.compactWaiters()
}

// trySendGrant performs a non-blocking send on ch, returning false if
// the channel is full or already closed. We recover from the
// closed-channel panic and log it because that path is meant to be
// unreachable: callers always remove a waiter from the queue before
// closing its channel.
func (lm *LockManager) trySendGrant(ch chan<- string, token, key string, connID uint64) (sent bool) {
	defer func() {
		if r := recover(); r != nil {
			lm.log.Error("grant send panicked (closed channel?)",
				"key", key, "conn_id", connID, "recovered", r)
			sent = false
		}
	}()
	select {
	case ch <- token:
		return true
	default:
		return false
	}
}

// evictExpired removes any holders past their lease and grants the
// freed slots to the next waiters. Must be called with sh.mu held.
func (lm *LockManager) evictExpired(sh *shard, key string, st *ResourceState) {
	now := time.Now()
	any := false
	for token, h := range st.Holders {
		if h.leaseExpires.IsZero() || now.Before(h.leaseExpires) {
			continue
		}
		lm.log.Warn("evicting expired lease on acquire", "key", key, "conn", h.connID)
		sh.removeOwned(h.connID, key, token)
		eqKey := connKey{ConnID: h.connID, Key: key}
		if es, ok := sh.connEnqueued[eqKey]; ok && es.token == token {
			sh.removeEnqueued(eqKey)
		}
		delete(st.Holders, token)
		any = true
	}
	if any {
		st.LastActivity = now
		lm.grantNext(sh, key, st)
	}
}

// Acquire is the single-phase acquire, used by the "l" and "sl"
// protocol commands. Returns the granted token on success, or one of:
//
//	ErrMaxLocks         - cluster-wide cap reached
//	ErrLimitMismatch    - existing key has a different limit
//	ErrMaxWaiters       - per-key waiter cap reached
//	ErrLeaseExpired     - granted slot's lease expired before observation
//	ErrWaiterClosed     - the waiter channel was closed (cleanup ran)
//	context.Canceled    - parent ctx cancelled (the just-granted slot,
//	                      if any, is released back to the queue)
//	nil with empty tok  - acquire timeout fired
func (lm *LockManager) Acquire(ctx context.Context, key string, timeout, leaseTTL time.Duration, connID uint64, limit int) (string, error) {
	sh := lm.shardFor(key)

	sh.mu.Lock()
	st, err := lm.getOrCreate(sh, key, limit)
	if err != nil {
		sh.mu.Unlock()
		return "", err
	}
	now := time.Now()
	st.LastActivity = now
	lm.evictExpired(sh, key, st)

	// Fast path: capacity available, no waiters ahead of us.
	if len(st.Holders) < st.Limit && st.waiterCount() == 0 {
		token := lm.newToken()
		st.Holders[token] = &holder{connID: connID, leaseExpires: now.Add(leaseTTL)}
		sh.addOwned(connID, key, token)
		sh.mu.Unlock()
		return token, nil
	}

	// Slow path: enqueue and wait.
	if max := lm.cfg.MaxWaiters; max > 0 && st.waiterCount() >= max {
		sh.mu.Unlock()
		return "", ErrMaxWaiters
	}
	w := &waiter{
		ch:       make(chan string, 1),
		connID:   connID,
		leaseTTL: leaseTTL,
	}
	st.Waiters = append(st.Waiters, w)
	sh.mu.Unlock()

	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return lm.waitForGrant(ctx, timeoutCtx, sh, key, w, leaseTTL)
}

// waitForGrant blocks on the waiter's channel until grant, timeout, or
// context cancellation. On parent-context cancellation it releases any
// just-granted slot back to the queue so an abandoning caller doesn't
// silently strand a token until lease expiry.
func (lm *LockManager) waitForGrant(parentCtx, timeoutCtx context.Context, sh *shard, key string, w *waiter, leaseTTL time.Duration) (string, error) {
	select {
	case token, ok := <-w.ch:
		if !ok || token == "" {
			return "", ErrWaiterClosed
		}
		return lm.commitGrant(parentCtx, sh, key, token, leaseTTL)

	case <-timeoutCtx.Done():
		// Race window: the grant may have arrived between cancellation
		// and our acquiring the mutex. Drain it under the lock.
		sh.mu.Lock()
		var grantedToken string
		select {
		case t, ok := <-w.ch:
			if ok && t != "" {
				grantedToken = t
			}
		default:
		}
		if grantedToken != "" {
			return lm.commitGrantLocked(parentCtx, sh, key, grantedToken, leaseTTL)
		}
		if st := sh.resources[key]; st != nil {
			st.LastActivity = time.Now()
			st.removeWaiter(w)
		}
		sh.mu.Unlock()
		if parentCtx.Err() != nil {
			return "", parentCtx.Err()
		}
		return "", nil
	}
}

// commitGrant locks the shard, then delegates to commitGrantLocked.
func (lm *LockManager) commitGrant(parentCtx context.Context, sh *shard, key, token string, leaseTTL time.Duration) (string, error) {
	sh.mu.Lock()
	return lm.commitGrantLocked(parentCtx, sh, key, token, leaseTTL)
}

// commitGrantLocked finalises a grant under sh.mu. If the parent ctx
// was cancelled simultaneously with the grant, the slot is released back
// and returned to the next waiter — the caller asked to abandon, so we
// don't hand them a lock they don't expect to hold. Always releases
// sh.mu before returning.
func (lm *LockManager) commitGrantLocked(parentCtx context.Context, sh *shard, key, token string, leaseTTL time.Duration) (string, error) {
	st := sh.resources[key]
	if st == nil {
		sh.mu.Unlock()
		return "", ErrLeaseExpired
	}
	h, ok := st.Holders[token]
	if !ok {
		// Token granted but holder evicted before we observed it.
		sh.mu.Unlock()
		return "", ErrLeaseExpired
	}
	if parentCtx.Err() != nil {
		sh.removeOwned(h.connID, key, token)
		delete(st.Holders, token)
		st.LastActivity = time.Now()
		lm.grantNext(sh, key, st)
		sh.mu.Unlock()
		return "", parentCtx.Err()
	}
	h.leaseExpires = time.Now().Add(leaseTTL)
	st.LastActivity = time.Now()
	sh.mu.Unlock()
	return token, nil
}

// Enqueue is phase 1 of two-phase acquire (commands "e" and "se").
// Returns one of:
//
//	("acquired", token, leaseSec, nil) - capacity available, slot held
//	("queued", "", 0, nil)             - waiter registered, call Wait next
//	("", "", 0, ErrAlreadyEnqueued)    - this conn already has phase-1 state
//	("", "", 0, ErrMaxLocks/...)       - failure conditions
func (lm *LockManager) Enqueue(key string, leaseTTL time.Duration, connID uint64, limit int) (string, string, int, error) {
	eqKey := connKey{ConnID: connID, Key: key}
	sh := lm.shardFor(key)

	sh.mu.Lock()
	defer sh.mu.Unlock()

	if _, exists := sh.connEnqueued[eqKey]; exists {
		return "", "", 0, ErrAlreadyEnqueued
	}
	st, err := lm.getOrCreate(sh, key, limit)
	if err != nil {
		return "", "", 0, err
	}
	now := time.Now()
	st.LastActivity = now
	leaseSec := int(leaseTTL / time.Second)
	lm.evictExpired(sh, key, st)

	// Fast path.
	if len(st.Holders) < st.Limit && st.waiterCount() == 0 {
		token := lm.newToken()
		st.Holders[token] = &holder{connID: connID, leaseExpires: now.Add(leaseTTL)}
		sh.addOwned(connID, key, token)
		sh.setEnqueued(eqKey, &enqueuedState{token: token, leaseTTL: leaseTTL})
		return "acquired", token, leaseSec, nil
	}

	if max := lm.cfg.MaxWaiters; max > 0 && st.waiterCount() >= max {
		return "", "", 0, ErrMaxWaiters
	}
	w := &waiter{
		ch:       make(chan string, 1),
		connID:   connID,
		leaseTTL: leaseTTL,
	}
	st.Waiters = append(st.Waiters, w)
	sh.setEnqueued(eqKey, &enqueuedState{waiter: w, leaseTTL: leaseTTL})
	return "queued", "", 0, nil
}

// Wait is phase 2 of two-phase acquire (commands "w" and "sw"). Must
// be called from the same connID that issued the matching Enqueue.
// Returns (token, leaseSec, err); empty token with nil err signals
// timeout.
func (lm *LockManager) Wait(ctx context.Context, key string, timeout time.Duration, connID uint64) (string, int, error) {
	eqKey := connKey{ConnID: connID, Key: key}
	sh := lm.shardFor(key)

	sh.mu.Lock()
	es, ok := sh.connEnqueued[eqKey]
	if !ok {
		sh.mu.Unlock()
		return "", 0, ErrNotEnqueued
	}
	leaseTTL := es.leaseTTL
	leaseSec := int(leaseTTL / time.Second)
	preToken := es.token
	w := es.waiter

	// Fast path: Enqueue's same-tick acquire already granted us a slot.
	if preToken != "" {
		sh.removeEnqueued(eqKey)
		now := time.Now()
		st := sh.resources[key]
		if st == nil {
			sh.removeOwned(connID, key, preToken)
			sh.mu.Unlock()
			return "", 0, ErrLeaseExpired
		}
		h, hOK := st.Holders[preToken]
		if !hOK {
			sh.removeOwned(connID, key, preToken)
			sh.mu.Unlock()
			return "", 0, ErrLeaseExpired
		}
		// Lease may have expired in the gap between Enqueue and Wait.
		if !h.leaseExpires.IsZero() && !now.Before(h.leaseExpires) {
			sh.removeOwned(connID, key, preToken)
			delete(st.Holders, preToken)
			st.LastActivity = now
			lm.grantNext(sh, key, st)
			sh.mu.Unlock()
			return "", 0, ErrLeaseExpired
		}
		h.leaseExpires = now.Add(leaseTTL)
		st.LastActivity = now
		sh.mu.Unlock()
		return preToken, leaseSec, nil
	}
	sh.mu.Unlock()

	// Slow path: block on waiter.
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	token, err := lm.waitForGrant(ctx, timeoutCtx, sh, key, w, leaseTTL)
	// Always pop the enqueued state when Wait returns.
	sh.mu.Lock()
	sh.removeEnqueued(eqKey)
	sh.mu.Unlock()
	if err != nil {
		return "", 0, err
	}
	if token == "" {
		return "", 0, nil
	}
	return token, leaseSec, nil
}

// Release frees one held slot if (key, token) match. Returns false when
// the token isn't held — the caller may have raced lease expiry.
func (lm *LockManager) Release(key, token string) bool {
	sh := lm.shardFor(key)

	sh.mu.Lock()
	defer sh.mu.Unlock()

	st := sh.resources[key]
	if st == nil {
		return false
	}
	h, ok := st.Holders[token]
	if !ok {
		// Don't bump LastActivity: a bogus token must not extend life.
		return false
	}
	now := time.Now()
	st.LastActivity = now
	sh.removeOwned(h.connID, key, token)
	eqKey := connKey{ConnID: h.connID, Key: key}
	if es, ok := sh.connEnqueued[eqKey]; ok && es.token == token {
		sh.removeEnqueued(eqKey)
	}
	delete(st.Holders, token)
	lm.grantNext(sh, key, st)
	return true
}

// Renew extends the lease on a held slot by leaseTTL. Returns the new
// remaining seconds and ok=true on success; ok=false means the token
// isn't held (or its lease already expired and was evicted).
func (lm *LockManager) Renew(key, token string, leaseTTL time.Duration) (int, bool) {
	sh := lm.shardFor(key)

	sh.mu.Lock()
	defer sh.mu.Unlock()

	st := sh.resources[key]
	if st == nil {
		return 0, false
	}
	h, ok := st.Holders[token]
	if !ok {
		return 0, false
	}
	now := time.Now()
	st.LastActivity = now
	if !h.leaseExpires.IsZero() && !now.Before(h.leaseExpires) {
		// Already expired; evict and reject the renew.
		lm.log.Warn("renew rejected (already expired)", "key", key, "conn", h.connID)
		sh.removeOwned(h.connID, key, token)
		eqKey := connKey{ConnID: h.connID, Key: key}
		if es, ok := sh.connEnqueued[eqKey]; ok && es.token == token {
			sh.removeEnqueued(eqKey)
		}
		delete(st.Holders, token)
		lm.grantNext(sh, key, st)
		return 0, false
	}
	h.leaseExpires = now.Add(leaseTTL)
	remaining := int(leaseTTL.Seconds())
	if remaining < 0 {
		remaining = 0
	}
	return remaining, true
}

// CleanupConnection clears all state for a connection that has gone away.
// Called from the connection handler's defer chain.
//
// Pending waiters and enqueued state are always cleaned (a torn-down
// connection can never observe a future grant). Held slots are released
// only when AutoReleaseOnDisconnect is true; otherwise they remain
// reachable via lease expiry.
func (lm *LockManager) CleanupConnection(connID uint64) {
	closed := make(map[chan string]struct{})

	for i := range lm.shards {
		sh := &lm.shards[i]
		sh.mu.Lock()

		// Two-phase enqueued state: tracked by per-conn index so cost is
		// proportional to this conn's enqueues, not every shard's load.
		for _, ck := range sh.enqueuedKeys(connID) {
			es := sh.connEnqueued[ck]
			sh.removeEnqueued(ck)
			if es != nil && es.waiter != nil {
				if _, already := closed[es.waiter.ch]; !already {
					close(es.waiter.ch)
					closed[es.waiter.ch] = struct{}{}
				}
				if st := sh.resources[ck.Key]; st != nil {
					st.removeWaiter(es.waiter)
				}
			}
		}

		// Single-phase pending waiters.
		for _, st := range sh.resources {
			st.removeWaitersByConn(connID, closed)
		}

		// Optionally release held slots.
		if lm.cfg.AutoReleaseOnDisconnect {
			if owned := sh.connOwned[connID]; owned != nil {
				for key, tokens := range owned {
					st := sh.resources[key]
					if st == nil {
						continue
					}
					for token := range tokens {
						h, ok := st.Holders[token]
						if !ok || h.connID != connID {
							continue
						}
						lm.log.Warn("disconnect cleanup: releasing", "key", key, "conn_id", connID)
						delete(st.Holders, token)
					}
					st.LastActivity = time.Now()
					lm.grantNext(sh, key, st)
				}
				delete(sh.connOwned, connID)
			}
		}

		sh.mu.Unlock()
	}
}
