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
	"encoding/binary"
	"encoding/hex"
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
	randBuf       randBuf
	// fenceCounter is the source of the lex-sortable prefix encoded in
	// every issued token. Seeded at startup with time.Now().UnixNano()
	// so values keep advancing across server restarts on a sane clock.
	fenceCounter atomic.Uint64
}

// NewLockManager creates a LockManager bound to the given config.
func NewLockManager(cfg *config.Config, log *slog.Logger) *LockManager {
	lm := &LockManager{cfg: cfg, log: log, randBuf: newRandBuf()}
	lm.fenceCounter.Store(uint64(time.Now().UnixNano()))
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

// newToken returns a 32-char lowercase-hex token: a server-monotonic
// uint64 fence prefix (big-endian, first 16 chars) followed by 8 bytes
// of random salt. The prefix lets a token also serve as a fencing
// token — lex-comparing two tokens for the same key reflects the
// order their grants were issued.
func (lm *LockManager) newToken() string {
	return encodeToken(lm.fenceCounter.Add(1), lm.saltBytes())
}

// saltBytes draws 8 unguessable bytes for a token's random suffix.
func (lm *LockManager) saltBytes() [8]byte {
	var salt [8]byte
	lm.randBuf.fill(salt[:])
	return salt
}

// encodeToken formats a (fence, salt) pair as 32 lowercase hex chars.
func encodeToken(fence uint64, salt [8]byte) string {
	var prefix [8]byte
	binary.BigEndian.PutUint64(prefix[:], fence)
	var out [32]byte
	hex.Encode(out[:16], prefix[:])
	hex.Encode(out[16:], salt[:])
	return string(out[:])
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
	st, err := lm.acquireLockShard(sh, key, limit)
	if err != nil {
		return "", err
	}
	if tok, ok := lm.tryFastAcquire(sh, st, key, connID, leaseTTL); ok {
		return tok, nil
	}
	w, err := lm.enqueueWaiter(sh, st, connID, leaseTTL)
	if err != nil {
		return "", err
	}
	return lm.blockOnWaiter(ctx, sh, key, w, leaseTTL, timeout)
}

// acquireLockShard locks sh.mu and resolves the resource state for
// (key, limit). On error, sh.mu is released. On ok, the caller owns
// sh.mu and any expired holders have already been evicted.
func (lm *LockManager) acquireLockShard(sh *shard, key string, limit int) (*ResourceState, error) {
	sh.mu.Lock()
	st, err := lm.getOrCreate(sh, key, limit)
	if err != nil {
		sh.mu.Unlock()
		return nil, err
	}
	st.LastActivity = time.Now()
	lm.evictExpired(sh, key, st)
	return st, nil
}

// tryFastAcquire grants a slot directly when capacity is free and no
// one is queued. Releases sh.mu only on the fast path.
func (lm *LockManager) tryFastAcquire(sh *shard, st *ResourceState, key string, connID uint64, leaseTTL time.Duration) (string, bool) {
	if len(st.Holders) >= st.Limit || st.waiterCount() > 0 {
		return "", false
	}
	token := lm.newToken()
	st.Holders[token] = &holder{connID: connID, leaseExpires: time.Now().Add(leaseTTL)}
	sh.addOwned(connID, key, token)
	sh.mu.Unlock()
	return token, true
}

// enqueueWaiter records a new waiter against MaxWaiters. Releases
// sh.mu on success or failure.
func (lm *LockManager) enqueueWaiter(sh *shard, st *ResourceState, connID uint64, leaseTTL time.Duration) (*waiter, error) {
	if !lm.waiterCapAvailable(st) {
		sh.mu.Unlock()
		return nil, ErrMaxWaiters
	}
	w := &waiter{ch: make(chan string, 1), connID: connID, leaseTTL: leaseTTL}
	st.Waiters = append(st.Waiters, w)
	sh.mu.Unlock()
	return w, nil
}

// waiterCapAvailable reports whether enqueuing one more waiter is
// permitted by MaxWaiters.
func (lm *LockManager) waiterCapAvailable(st *ResourceState) bool {
	max := lm.cfg.MaxWaiters
	return max <= 0 || st.waiterCount() < max
}

// blockOnWaiter spans waitForGrant under a timeout-bound context.
func (lm *LockManager) blockOnWaiter(ctx context.Context, sh *shard, key string, w *waiter, leaseTTL, timeout time.Duration) (string, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return lm.waitForGrant(ctx, timeoutCtx, sh, key, w, leaseTTL)
}

// waitForGrant blocks on the waiter's channel until grant, timeout,
// or context cancellation.
func (lm *LockManager) waitForGrant(parentCtx, timeoutCtx context.Context, sh *shard, key string, w *waiter, leaseTTL time.Duration) (string, error) {
	select {
	case token, ok := <-w.ch:
		return lm.handleGrantedToken(parentCtx, sh, key, token, ok, leaseTTL)
	case <-timeoutCtx.Done():
		return lm.handleWaitTimeout(parentCtx, sh, key, w, leaseTTL)
	}
}

// handleGrantedToken acts on the grant arriving on w.ch.
func (lm *LockManager) handleGrantedToken(parentCtx context.Context, sh *shard, key, token string, ok bool, leaseTTL time.Duration) (string, error) {
	if !ok || token == "" {
		return "", ErrWaiterClosed
	}
	return lm.commitGrant(parentCtx, sh, key, token, leaseTTL)
}

// handleWaitTimeout runs after the timeout fires. The grant may
// still have arrived in the race; drain w.ch under sh.mu and either
// commit or remove the waiter.
func (lm *LockManager) handleWaitTimeout(parentCtx context.Context, sh *shard, key string, w *waiter, leaseTTL time.Duration) (string, error) {
	sh.mu.Lock()
	if t, ok := drainGrant(w); ok {
		return lm.commitGrantLocked(parentCtx, sh, key, t, leaseTTL)
	}
	removeWaiterFromKey(sh, key, w)
	sh.mu.Unlock()
	if parentCtx.Err() != nil {
		return "", parentCtx.Err()
	}
	return "", nil
}

// drainGrant attempts a non-blocking receive on w.ch. Returns the
// token + true if a grant was waiting; "", false otherwise.
func drainGrant(w *waiter) (string, bool) {
	select {
	case t, ok := <-w.ch:
		if ok && t != "" {
			return t, true
		}
	default:
	}
	return "", false
}

// removeWaiterFromKey removes w from key's waiter queue if the
// resource still exists. Caller holds sh.mu.
func removeWaiterFromKey(sh *shard, key string, w *waiter) {
	st := sh.resources[key]
	if st == nil {
		return
	}
	st.LastActivity = time.Now()
	st.removeWaiter(w)
}

// commitGrant locks the shard, then delegates to commitGrantLocked.
func (lm *LockManager) commitGrant(parentCtx context.Context, sh *shard, key, token string, leaseTTL time.Duration) (string, error) {
	sh.mu.Lock()
	return lm.commitGrantLocked(parentCtx, sh, key, token, leaseTTL)
}

// commitGrantLocked finalises a grant under sh.mu. Always releases
// sh.mu before returning.
func (lm *LockManager) commitGrantLocked(parentCtx context.Context, sh *shard, key, token string, leaseTTL time.Duration) (string, error) {
	defer sh.mu.Unlock()
	st, h, ok := lookupHolder(sh, key, token)
	if !ok {
		return "", ErrLeaseExpired
	}
	if parentCtx.Err() != nil {
		lm.releaseGrantOnCancel(sh, st, key, token, h, parentCtx.Err())
		return "", parentCtx.Err()
	}
	h.leaseExpires = time.Now().Add(leaseTTL)
	st.LastActivity = time.Now()
	return token, nil
}

// releaseGrantOnCancel undoes a grant whose owner cancelled before we
// observed it. The slot is handed to the next waiter.
func (lm *LockManager) releaseGrantOnCancel(sh *shard, st *ResourceState, key, token string, h *holder, _ error) {
	sh.removeOwned(h.connID, key, token)
	delete(st.Holders, token)
	st.LastActivity = time.Now()
	lm.grantNext(sh, key, st)
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
	st.LastActivity = time.Now()
	lm.evictExpired(sh, key, st)
	return lm.enqueueOnto(sh, st, eqKey, key, connID, leaseTTL)
}

// enqueueOnto either grants a fast-path token or registers a waiter.
// Caller holds sh.mu.
func (lm *LockManager) enqueueOnto(sh *shard, st *ResourceState, eqKey connKey, key string, connID uint64, leaseTTL time.Duration) (string, string, int, error) {
	leaseSec := int(leaseTTL / time.Second)
	if len(st.Holders) < st.Limit && st.waiterCount() == 0 {
		token := lm.fastEnqueueGrant(sh, st, eqKey, key, connID, leaseTTL)
		return "acquired", token, leaseSec, nil
	}
	if !lm.waiterCapAvailable(st) {
		return "", "", 0, ErrMaxWaiters
	}
	lm.queueEnqueueWaiter(sh, st, eqKey, connID, leaseTTL)
	return "queued", "", 0, nil
}

// fastEnqueueGrant is the same-tick acquire used by Enqueue's fast
// path. Returns the freshly-minted token.
func (lm *LockManager) fastEnqueueGrant(sh *shard, st *ResourceState, eqKey connKey, key string, connID uint64, leaseTTL time.Duration) string {
	token := lm.newToken()
	st.Holders[token] = &holder{connID: connID, leaseExpires: time.Now().Add(leaseTTL)}
	sh.addOwned(connID, key, token)
	sh.setEnqueued(eqKey, &enqueuedState{token: token, leaseTTL: leaseTTL})
	return token
}

// queueEnqueueWaiter parks a waiter and records the matching
// connEnqueued entry pointing at it.
func (lm *LockManager) queueEnqueueWaiter(sh *shard, st *ResourceState, eqKey connKey, connID uint64, leaseTTL time.Duration) {
	w := &waiter{ch: make(chan string, 1), connID: connID, leaseTTL: leaseTTL}
	st.Waiters = append(st.Waiters, w)
	sh.setEnqueued(eqKey, &enqueuedState{waiter: w, leaseTTL: leaseTTL})
}

// Wait is phase 2 of two-phase acquire (commands "w" and "sw"). Must
// be called from the same connID that issued the matching Enqueue.
// Returns (token, leaseSec, err); empty token with nil err signals
// timeout.
func (lm *LockManager) Wait(ctx context.Context, key string, timeout time.Duration, connID uint64) (string, int, error) {
	eqKey := connKey{ConnID: connID, Key: key}
	sh := lm.shardFor(key)

	es, ok := loadEnqueuedStateLocked(sh, eqKey)
	if !ok {
		return "", 0, ErrNotEnqueued
	}
	if es.token != "" {
		return lm.consumePreGrantedToken(sh, eqKey, key, connID, es)
	}
	leaseTTL, w := es.leaseTTL, es.waiter
	sh.mu.Unlock()
	return lm.waitQueuedGrant(ctx, sh, eqKey, key, w, leaseTTL, timeout)
}

// loadEnqueuedStateLocked locks sh.mu and returns the enqueued state
// for eqKey. Releases sh.mu and returns ok=false when there's no
// enqueued state. On ok=true the caller owns sh.mu.
func loadEnqueuedStateLocked(sh *shard, eqKey connKey) (*enqueuedState, bool) {
	sh.mu.Lock()
	es, ok := sh.connEnqueued[eqKey]
	if !ok {
		sh.mu.Unlock()
		return nil, false
	}
	return es, true
}

// consumePreGrantedToken is the fast path: the waiter was already
// promoted during Enqueue. Caller must hold sh.mu; this function
// always releases it.
func (lm *LockManager) consumePreGrantedToken(sh *shard, eqKey connKey, key string, connID uint64, es *enqueuedState) (string, int, error) {
	defer sh.mu.Unlock()
	sh.removeEnqueued(eqKey)
	leaseSec := int(es.leaseTTL / time.Second)
	h, ok := lm.refreshPromotedHolder(sh, key, connID, es)
	if !ok {
		return "", 0, ErrLeaseExpired
	}
	h.leaseExpires = time.Now().Add(es.leaseTTL)
	sh.resources[key].LastActivity = time.Now()
	return es.token, leaseSec, nil
}

// refreshPromotedHolder validates that the promoted holder still
// exists and isn't expired. Evicts and returns ok=false otherwise.
// Caller holds sh.mu.
func (lm *LockManager) refreshPromotedHolder(sh *shard, key string, connID uint64, es *enqueuedState) (*holder, bool) {
	st := sh.resources[key]
	if st == nil {
		sh.removeOwned(connID, key, es.token)
		return nil, false
	}
	h, ok := st.Holders[es.token]
	if !ok {
		sh.removeOwned(connID, key, es.token)
		return nil, false
	}
	if leaseHasExpired(h, time.Now()) {
		lm.evictExpiredHolder(sh, st, key, connID, es.token)
		return nil, false
	}
	return h, true
}

// evictExpiredHolder removes a holder whose lease has elapsed and
// hands its slot to the next waiter. Caller holds sh.mu.
func (lm *LockManager) evictExpiredHolder(sh *shard, st *ResourceState, key string, connID uint64, token string) {
	sh.removeOwned(connID, key, token)
	delete(st.Holders, token)
	st.LastActivity = time.Now()
	lm.grantNext(sh, key, st)
}

// waitQueuedGrant is the slow path: the waiter is parked on its
// channel. Pops the enqueued state regardless of outcome.
func (lm *LockManager) waitQueuedGrant(ctx context.Context, sh *shard, eqKey connKey, key string, w *waiter, leaseTTL, timeout time.Duration) (string, int, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	token, err := lm.waitForGrant(ctx, timeoutCtx, sh, key, w, leaseTTL)
	lm.popEnqueued(sh, eqKey)
	return finishWaitResult(token, leaseTTL, err)
}

// popEnqueued removes eqKey from connEnqueued under sh.mu.
func (lm *LockManager) popEnqueued(sh *shard, eqKey connKey) {
	sh.mu.Lock()
	sh.removeEnqueued(eqKey)
	sh.mu.Unlock()
}

// finishWaitResult shapes the (token, lease, err) tuple from a slow-
// path Wait into the public return signature.
func finishWaitResult(token string, leaseTTL time.Duration, err error) (string, int, error) {
	if err != nil {
		return "", 0, err
	}
	if token == "" {
		return "", 0, nil
	}
	return token, int(leaseTTL / time.Second), nil
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
// remaining seconds and ok=true on success.
func (lm *LockManager) Renew(key, token string, leaseTTL time.Duration) (int, bool) {
	sh := lm.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	st, h, ok := lookupHolder(sh, key, token)
	if !ok {
		return 0, false
	}
	if leaseHasExpired(h, time.Now()) {
		lm.rejectExpiredRenew(sh, st, key, token, h)
		return 0, false
	}
	return extendLease(st, h, leaseTTL), true
}

// lookupHolder returns the (resource, holder) pair if both exist for
// (key, token), or ok=false. Caller holds sh.mu.
func lookupHolder(sh *shard, key, token string) (*ResourceState, *holder, bool) {
	st := sh.resources[key]
	if st == nil {
		return nil, nil, false
	}
	h, ok := st.Holders[token]
	if !ok {
		return nil, nil, false
	}
	return st, h, true
}

// leaseHasExpired reports whether h's lease deadline has elapsed.
// Holders with an unset deadline (zero time) are treated as live.
func leaseHasExpired(h *holder, now time.Time) bool {
	return !h.leaseExpires.IsZero() && !now.Before(h.leaseExpires)
}

// rejectExpiredRenew evicts an expired holder, drops any stale
// connEnqueued entry pointing at the same token, and grants the
// freed slot to the next waiter. Caller holds sh.mu.
func (lm *LockManager) rejectExpiredRenew(sh *shard, st *ResourceState, key, token string, h *holder) {
	lm.log.Warn("renew rejected (already expired)", "key", key, "conn", h.connID)
	sh.removeOwned(h.connID, key, token)
	dropMatchingEnqueued(sh, h.connID, key, token)
	delete(st.Holders, token)
	st.LastActivity = time.Now()
	lm.grantNext(sh, key, st)
}

// dropMatchingEnqueued removes the connEnqueued entry for (connID,
// key) when its token matches. Caller holds sh.mu.
func dropMatchingEnqueued(sh *shard, connID uint64, key, token string) {
	eqKey := connKey{ConnID: connID, Key: key}
	if es, ok := sh.connEnqueued[eqKey]; ok && es.token == token {
		sh.removeEnqueued(eqKey)
	}
}

// extendLease installs a fresh expiry on h and returns the new
// remaining seconds (clamped at 0).
func extendLease(st *ResourceState, h *holder, leaseTTL time.Duration) int {
	now := time.Now()
	st.LastActivity = now
	h.leaseExpires = now.Add(leaseTTL)
	remaining := int(leaseTTL.Seconds())
	if remaining < 0 {
		return 0
	}
	return remaining
}

// CleanupConnection clears all state for a connection that has gone
// away. Pending waiters and enqueued state are always cleaned; held
// slots are released only when AutoReleaseOnDisconnect is true.
func (lm *LockManager) CleanupConnection(connID uint64) {
	closed := make(map[chan string]struct{})
	for i := range lm.shards {
		lm.cleanupShard(&lm.shards[i], connID, closed)
	}
}

// cleanupShard runs every per-connection cleanup step on one shard
// under sh.mu.
func (lm *LockManager) cleanupShard(sh *shard, connID uint64, closed map[chan string]struct{}) {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	cleanupEnqueuedForConn(sh, connID, closed)
	cleanupPendingWaitersForConn(sh, connID, closed)
	if lm.cfg.AutoReleaseOnDisconnect {
		lm.releaseOwnedForConn(sh, connID)
	}
}

// cleanupEnqueuedForConn drops the two-phase enqueued state for
// connID. Tracked via the per-conn index so cost is proportional to
// this conn's enqueues, not the shard's total load.
func cleanupEnqueuedForConn(sh *shard, connID uint64, closed map[chan string]struct{}) {
	for _, ck := range sh.enqueuedKeys(connID) {
		es := sh.connEnqueued[ck]
		sh.removeEnqueued(ck)
		dropEnqueuedWaiter(sh, ck.Key, es, closed)
	}
}

// dropEnqueuedWaiter closes a waiter's channel (once) and removes it
// from its resource queue.
func dropEnqueuedWaiter(sh *shard, key string, es *enqueuedState, closed map[chan string]struct{}) {
	if es == nil || es.waiter == nil {
		return
	}
	closeOnce(es.waiter.ch, closed)
	if st := sh.resources[key]; st != nil {
		st.removeWaiter(es.waiter)
	}
}

// closeOnce closes ch the first time we see it, recording the close
// in the shared `closed` set so a waiter blocked on multiple shards
// isn't double-closed.
func closeOnce(ch chan string, closed map[chan string]struct{}) {
	if _, already := closed[ch]; already {
		return
	}
	close(ch)
	closed[ch] = struct{}{}
}

// cleanupPendingWaitersForConn cancels every single-phase waiter
// belonging to connID across all resources in the shard.
func cleanupPendingWaitersForConn(sh *shard, connID uint64, closed map[chan string]struct{}) {
	for _, st := range sh.resources {
		st.removeWaitersByConn(connID, closed)
	}
}

// releaseOwnedForConn releases every held slot for connID and grants
// the freed slots to the next waiters.
func (lm *LockManager) releaseOwnedForConn(sh *shard, connID uint64) {
	owned := sh.connOwned[connID]
	if owned == nil {
		return
	}
	for key, tokens := range owned {
		lm.releaseOwnedKey(sh, connID, key, tokens)
	}
	delete(sh.connOwned, connID)
}

func (lm *LockManager) releaseOwnedKey(sh *shard, connID uint64, key string, tokens map[string]struct{}) {
	st := sh.resources[key]
	if st == nil {
		return
	}
	for token := range tokens {
		lm.releaseHolderEntry(st, key, token, connID)
	}
	st.LastActivity = time.Now()
	lm.grantNext(sh, key, st)
}

func (lm *LockManager) releaseHolderEntry(st *ResourceState, key, token string, connID uint64) {
	h, ok := st.Holders[token]
	if !ok || h.connID != connID {
		return
	}
	lm.log.Warn("disconnect cleanup: releasing", "key", key, "conn_id", connID)
	delete(st.Holders, token)
}
