// Package-internal: deterministic FSM apply path for the Raft cluster.
//
// Apply* methods mutate state purely as a function of (current state,
// arguments) — they take `now` (the leader's propose-time wall clock,
// shared by all nodes) and a salt for token minting, and bump
// fsmFenceCounter on every grant. They never call time.Now, crypto/rand,
// or anything else that would differ across replicas. Returned grants
// are routed by the caller (RouteGrants) — the FSM state itself doesn't
// hold channels.
//
// The existing direct methods (Acquire / Release / Renew / Enqueue /
// Wait) remain the single-node code path and are unchanged: they use the
// in-memory waiter channels and the disk-backed fenceAlloc. The two
// paths share the underlying ResourceState; nothing in single-node mode
// observes fsmFenceCounter or Ref/Salt fields.

package lock

import (
	"errors"
	"sort"
	"time"
)

// ApplyStatus is the categorical outcome of an Apply call.
type ApplyStatus uint8

const (
	StatusUnused   ApplyStatus = iota
	StatusOK                   // granted (Acquire) or successful release/renew/evict/gc
	StatusQueued               // Acquire / Enqueue parked a waiter
	StatusAcquired             // Enqueue fast-path
	StatusNotHeld              // Release/Renew of an unknown or expired token
	StatusErrMaxLocks
	StatusErrMaxWaiters
	StatusErrLimitMismatch
	StatusErrAlreadyEnqueued
	StatusErrNotEnqueued
	StatusErrLeaseExpired
)

// ApplyResult is what an Apply call hands back to its proposer (and to
// the caller's Future in cluster mode).
type ApplyResult struct {
	Status   ApplyStatus
	Token    string
	LeaseSec int
}

// ApplyAcquire is the FSM-side single-phase acquire. now is the
// command's wall-clock time. If the resource is free it mints a token
// from fsmFenceCounter + salt and grants immediately; otherwise it
// enqueues a waiter keyed by ref (no channel — routing is external).
//
// MUST be called serially (the apply goroutine guarantees this).
func (lm *LockManager) ApplyAcquire(now time.Time, key string, limit int, ref string, connID uint64, leaseTTL time.Duration, salt [8]byte) (ApplyResult, []Grant, error) {
	sh := lm.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	st, res, grants, err := lm.applyAcquireLocked(sh, key, limit, ref, connID, leaseTTL, salt, now)
	if err != nil {
		return ApplyResult{}, nil, err
	}
	_ = st
	return res, grants, nil
}

func (lm *LockManager) applyAcquireLocked(sh *shard, key string, limit int, ref string, connID uint64, leaseTTL time.Duration, salt [8]byte, now time.Time) (*ResourceState, ApplyResult, []Grant, error) {
	st, err := lm.getOrCreateAt(sh, key, limit, now)
	if err != nil {
		return nil, applyErr(err), nil, nil
	}
	pre, err := lm.evictExpiredAt(sh, key, st, now)
	if err != nil {
		return nil, ApplyResult{}, nil, err
	}
	if len(st.Holders) < st.Limit && st.waiterCount() == 0 {
		token, err := lm.mintFSMToken(salt)
		if err != nil {
			return nil, ApplyResult{}, pre, err
		}
		lm.recordHolder(sh, st, key, ref, connID, token, now, leaseTTL)
		return st, ApplyResult{Status: StatusOK, Token: token, LeaseSec: secondsOf(leaseTTL)}, pre, nil
	}
	if !lm.waiterCapAvailable(st) {
		return nil, applyErr(ErrMaxWaiters), pre, nil
	}
	st.Waiters = append(st.Waiters, &waiter{ref: ref, connID: connID, leaseTTL: leaseTTL, salt: salt})
	return st, ApplyResult{Status: StatusQueued}, pre, nil
}

// ApplyEnqueue is the FSM-side phase-1 enqueue (two-phase). Returns
// either StatusAcquired with a token (fast path) or StatusQueued.
//
// Stable-ref re-adopt path: when OrphanTTL > 0 and the caller supplied
// a non-empty ref, the FSM first looks for an existing orphaned holder
// or waiter on (key, ref). A matching holder → re-adopt and return the
// original token (StatusAcquired). A matching waiter → re-adopt and
// return StatusQueued. Otherwise the regular fast-path / queue logic
// runs.
func (lm *LockManager) ApplyEnqueue(now time.Time, key string, limit int, ref string, connID uint64, leaseTTL time.Duration, salt [8]byte) (ApplyResult, []Grant, error) {
	eqKey := connKey{ConnID: connID, Key: key}
	sh := lm.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if _, exists := sh.connEnqueued[eqKey]; exists {
		return applyErr(ErrAlreadyEnqueued), nil, nil
	}
	st, err := lm.getOrCreateAt(sh, key, limit, now)
	if err != nil {
		return applyErr(err), nil, nil
	}
	pre, err := lm.evictExpiredAt(sh, key, st, now)
	if err != nil {
		return ApplyResult{}, nil, err
	}
	if res, ok := lm.tryReAdopt(sh, st, eqKey, key, ref, connID, leaseTTL, now); ok {
		return res, pre, nil
	}
	return lm.applyEnqueueOnto(sh, st, eqKey, key, ref, connID, leaseTTL, salt, now, pre)
}

// tryReAdopt looks for an orphaned holder or waiter on (key, ref) and,
// if found, re-attaches it to the new connID. Returns (result, true)
// if a re-adopt happened; (zero, false) otherwise. Caller holds sh.mu.
func (lm *LockManager) tryReAdopt(sh *shard, st *ResourceState, eqKey connKey, key, ref string, connID uint64, leaseTTL time.Duration, now time.Time) (ApplyResult, bool) {
	if ref == "" || lm.cfg.OrphanTTL <= 0 {
		return ApplyResult{}, false
	}
	if tok, h := findOrphanHolder(st, ref); h != nil {
		h.connID = connID
		h.abandonedAtNanos = 0
		// Recompute leaseExpires off the caller's new TTL request — a
		// reconnect implicitly renews the lease the same way a direct
		// renew would. (The new TTL is what the FSM sees on the apply,
		// so it's deterministic.)
		h.leaseExpires = now.Add(leaseTTL)
		sh.addOwned(connID, key, tok)
		sh.setEnqueued(eqKey, &enqueuedState{token: tok, leaseTTL: leaseTTL})
		return ApplyResult{Status: StatusAcquired, Token: tok, LeaseSec: secondsOf(leaseTTL)}, true
	}
	if w := findOrphanWaiter(st, ref); w != nil {
		w.connID = connID
		w.abandonedAtNanos = 0
		w.leaseTTL = leaseTTL
		sh.setEnqueued(eqKey, &enqueuedState{waiter: w, leaseTTL: leaseTTL})
		return ApplyResult{Status: StatusQueued}, true
	}
	return ApplyResult{}, false
}

// findOrphanHolder returns (token, holder) for the first orphaned
// holder on st with matching ref, or ("", nil).
func findOrphanHolder(st *ResourceState, ref string) (string, *holder) {
	for tok, h := range st.Holders {
		if h.ref == ref && h.abandonedAtNanos != 0 {
			return tok, h
		}
	}
	return "", nil
}

// findOrphanWaiter returns the first orphaned waiter on st with
// matching ref, or nil.
func findOrphanWaiter(st *ResourceState, ref string) *waiter {
	for i := st.WaiterHead; i < len(st.Waiters); i++ {
		w := st.Waiters[i]
		if w != nil && w.ref == ref && w.abandonedAtNanos != 0 {
			return w
		}
	}
	return nil
}

func (lm *LockManager) applyEnqueueOnto(sh *shard, st *ResourceState, eqKey connKey, key, ref string, connID uint64, leaseTTL time.Duration, salt [8]byte, now time.Time, pre []Grant) (ApplyResult, []Grant, error) {
	if len(st.Holders) < st.Limit && st.waiterCount() == 0 {
		token, err := lm.mintFSMToken(salt)
		if err != nil {
			return ApplyResult{}, pre, err
		}
		lm.recordHolder(sh, st, key, ref, connID, token, now, leaseTTL)
		sh.setEnqueued(eqKey, &enqueuedState{token: token, leaseTTL: leaseTTL})
		return ApplyResult{Status: StatusAcquired, Token: token, LeaseSec: secondsOf(leaseTTL)}, pre, nil
	}
	if !lm.waiterCapAvailable(st) {
		return applyErr(ErrMaxWaiters), pre, nil
	}
	w := &waiter{ref: ref, connID: connID, leaseTTL: leaseTTL, salt: salt}
	st.Waiters = append(st.Waiters, w)
	sh.setEnqueued(eqKey, &enqueuedState{waiter: w, leaseTTL: leaseTTL})
	return ApplyResult{Status: StatusQueued}, pre, nil
}

// ApplyRelease removes the holder at (key, token) and promotes any
// queued waiters. Returns the promoted-waiter grants for routing.
func (lm *LockManager) ApplyRelease(now time.Time, key, token string) (ApplyResult, []Grant, error) {
	sh := lm.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	st := sh.resources[key]
	if st == nil {
		return ApplyResult{Status: StatusNotHeld}, nil, nil
	}
	h, ok := st.Holders[token]
	if !ok {
		return ApplyResult{Status: StatusNotHeld}, nil, nil
	}
	lm.dropHolder(sh, st, key, token, h, now)
	grants, err := lm.grantNextAt(sh, key, st, now)
	return ApplyResult{Status: StatusOK}, grants, err
}

// ApplyRenew extends the lease of (key, token). Returns StatusErrLeaseExpired
// (with grants from the eviction-induced promotion) if already past
// deadline, StatusNotHeld if the token isn't there at all, StatusOK
// otherwise.
func (lm *LockManager) ApplyRenew(now time.Time, key, token string, leaseTTL time.Duration) (ApplyResult, []Grant, error) {
	sh := lm.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	st := sh.resources[key]
	if st == nil {
		return ApplyResult{Status: StatusNotHeld}, nil, nil
	}
	h, ok := st.Holders[token]
	if !ok {
		return ApplyResult{Status: StatusNotHeld}, nil, nil
	}
	if leaseHasExpired(h, now) {
		return lm.applyRenewExpired(sh, st, key, token, h, now)
	}
	h.leaseExpires = now.Add(leaseTTL)
	st.LastActivity = now
	return ApplyResult{Status: StatusOK, LeaseSec: secondsOf(leaseTTL)}, nil, nil
}

func (lm *LockManager) applyRenewExpired(sh *shard, st *ResourceState, key, token string, h *holder, now time.Time) (ApplyResult, []Grant, error) {
	lm.dropHolder(sh, st, key, token, h, now)
	grants, err := lm.grantNextAt(sh, key, st, now)
	return ApplyResult{Status: StatusErrLeaseExpired}, grants, err
}

// ApplyEvict removes (key, token) unconditionally (the leader's lease
// sweep proposed it because deadline < now). Idempotent — already-gone
// is treated as success.
func (lm *LockManager) ApplyEvict(now time.Time, key, token string) (ApplyResult, []Grant, error) {
	sh := lm.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	st := sh.resources[key]
	if st == nil {
		return ApplyResult{Status: StatusOK}, nil, nil
	}
	h, ok := st.Holders[token]
	if !ok {
		return ApplyResult{Status: StatusOK}, nil, nil
	}
	lm.dropHolder(sh, st, key, token, h, now)
	grants, err := lm.grantNextAt(sh, key, st, now)
	return ApplyResult{Status: StatusOK}, grants, err
}

// ApplyCleanupConn releases every holder owned by ref and drops every
// waiter/enqueued entry for it across all shards.
func (lm *LockManager) ApplyCleanupConn(now time.Time, ref string, connID uint64) (ApplyResult, []Grant, error) {
	var grants []Grant
	for i := range lm.shards {
		g, err := lm.applyCleanupConnShard(&lm.shards[i], ref, connID, now)
		if err != nil {
			return ApplyResult{}, grants, err
		}
		grants = append(grants, g...)
	}
	return ApplyResult{Status: StatusOK}, grants, nil
}

func (lm *LockManager) applyCleanupConnShard(sh *shard, ref string, connID uint64, now time.Time) ([]Grant, error) {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	lm.cleanupEnqueuedForRef(sh, ref, connID, now)
	lm.cleanupPendingWaitersForRef(sh, ref, connID, now)
	return lm.releaseOwnedForRef(sh, ref, connID, now)
}

func (lm *LockManager) cleanupEnqueuedForRef(sh *shard, ref string, connID uint64, now time.Time) {
	_ = ref // routed via connID; the per-waiter ref drives orphan vs remove
	for _, ck := range sh.enqueuedKeys(connID) {
		es := sh.connEnqueued[ck]
		sh.removeEnqueued(ck)
		if es == nil || es.waiter == nil {
			continue
		}
		st := sh.resources[ck.Key]
		if st == nil {
			continue
		}
		if lm.orphanWaiterIfStable(es.waiter, now) {
			continue // kept in st.Waiters, abandoned, awaiting re-adopt
		}
		st.removeWaiter(es.waiter)
	}
}

// orphanWaiterIfStable returns true iff the waiter was marked abandoned
// (kept in the FSM for re-adopt). Otherwise false → caller removes it
// today's way.
func (lm *LockManager) orphanWaiterIfStable(w *waiter, now time.Time) bool {
	if w == nil || w.ref == "" || lm.cfg.OrphanTTL <= 0 {
		return false
	}
	w.abandonedAtNanos = now.UnixNano()
	w.connID = 0 // dead conn; new connection re-binds on re-adopt
	return true
}

func (lm *LockManager) cleanupPendingWaitersForRef(sh *shard, ref string, connID uint64, now time.Time) {
	_ = ref
	closed := map[chan string]struct{}{}
	for _, st := range sh.resources {
		lm.orphanOrRemoveWaitersByConn(st, connID, now, closed)
	}
}

// orphanOrRemoveWaitersByConn walks st.Waiters, partitioning each
// waiter owned by connID into orphan-keep vs remove-now. Mirrors
// ResourceState.removeWaitersByConn but with the stable-ref carve-out.
func (lm *LockManager) orphanOrRemoveWaitersByConn(st *ResourceState, connID uint64, now time.Time, closed map[chan string]struct{}) {
	n := st.WaiterHead
	for i := st.WaiterHead; i < len(st.Waiters); i++ {
		w := st.Waiters[i]
		if w.connID != connID {
			st.Waiters[n] = w
			n++
			continue
		}
		if lm.orphanWaiterIfStable(w, now) {
			st.Waiters[n] = w
			n++
			continue
		}
		if _, already := closed[w.ch]; !already && w.ch != nil {
			close(w.ch)
			closed[w.ch] = struct{}{}
		}
	}
	for i := n; i < len(st.Waiters); i++ {
		st.Waiters[i] = nil
	}
	st.Waiters = st.Waiters[:n]
}

func (lm *LockManager) releaseOwnedForRef(sh *shard, ref string, connID uint64, now time.Time) ([]Grant, error) {
	_ = ref // per-holder ref drives orphan vs drop
	owned := sh.connOwned[connID]
	var grants []Grant
	// Iterate keys in sorted order: grantNextAt mints fence numbers
	// (fsmFenceCounter++), so the order in which keys are processed must
	// be identical on every replica — Go map iteration order is not.
	for _, key := range sortedKeys(owned) {
		st := sh.resources[key]
		if st == nil {
			continue
		}
		anyDropped := false
		for token := range owned[key] {
			h, ok := st.Holders[token]
			if !ok {
				continue
			}
			if lm.orphanHolderIfStable(h, now) {
				continue // kept in st.Holders; awaits re-adopt
			}
			lm.dropHolder(sh, st, key, token, h, now)
			anyDropped = true
		}
		if anyDropped {
			g, err := lm.grantNextAt(sh, key, st, now)
			if err != nil {
				return grants, err
			}
			grants = append(grants, g...)
		}
	}
	delete(sh.connOwned, connID)
	return grants, nil
}

// orphanHolderIfStable returns true iff the holder was marked
// abandoned. Mirrors orphanWaiterIfStable.
func (lm *LockManager) orphanHolderIfStable(h *holder, now time.Time) bool {
	if h == nil || h.ref == "" || lm.cfg.OrphanTTL <= 0 {
		return false
	}
	h.abandonedAtNanos = now.UnixNano()
	h.connID = 0
	return true
}

// sortedKeys returns m's keys in ascending order (m may be nil).
func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// ApplyEvictExpired drops every holder whose lease deadline is before
// `now`, across all shards, and promotes waiters into the freed slots.
// The leader's sweep loop proposes it periodically so a holder whose
// client crashed (no graceful release/cleanup) is reclaimed even if
// nobody else ever touches the key. Deterministic: leaseExpires was
// stamped from the acquiring command's time, and `now` is the same on
// every replica.
func (lm *LockManager) ApplyEvictExpired(now time.Time) (ApplyResult, []Grant, error) {
	var grants []Grant
	for i := range lm.shards {
		g, err := lm.evictExpiredShard(&lm.shards[i], now)
		if err != nil {
			return ApplyResult{}, grants, err
		}
		grants = append(grants, g...)
	}
	return ApplyResult{Status: StatusOK}, grants, nil
}

func (lm *LockManager) evictExpiredShard(sh *shard, now time.Time) ([]Grant, error) {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	var grants []Grant
	// Sorted key order: evictExpiredAt mints fences via grantNextAt, so
	// the per-key processing order must match across replicas.
	for _, key := range sortedKeys(sh.resources) {
		g, err := lm.evictExpiredAt(sh, key, sh.resources[key], now)
		if err != nil {
			return grants, err
		}
		grants = append(grants, g...)
	}
	return grants, nil
}

// ApplyGC drops resources that have been idle longer than the
// configured threshold across all shards. The leader's GC loop proposes
// it periodically.
func (lm *LockManager) ApplyGC(now time.Time) ApplyResult {
	for i := range lm.shards {
		lm.applyGCShard(&lm.shards[i], now)
	}
	return ApplyResult{Status: StatusOK}
}

func (lm *LockManager) applyGCShard(sh *shard, now time.Time) {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	expired := collectIdleKeysAt(sh, now, lm.cfg.GCMaxIdleTime)
	if len(expired) == 0 {
		return
	}
	lm.resourceTotal.Add(-int64(len(expired)))
	for _, k := range expired {
		delete(sh.resources, k)
	}
}

// ---------------------------------------------------------------------------
// Internal helpers used only by the apply path
// ---------------------------------------------------------------------------

// getOrCreateAt is getOrCreate with `now` injected. Used only by the
// apply path; the single-node code keeps the time.Now-based variant.
func (lm *LockManager) getOrCreateAt(sh *shard, key string, limit int, now time.Time) (*ResourceState, error) {
	if st, ok := sh.resources[key]; ok {
		if st.Limit != limit {
			return nil, ErrLimitMismatch
		}
		st.LastActivity = now
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
	st := &ResourceState{Limit: limit, Holders: map[string]*holder{}, LastActivity: now}
	sh.resources[key] = st
	return st, nil
}

// evictExpiredAt is evictExpired with `now` injected, returning grants
// from any promotions that follow.
func (lm *LockManager) evictExpiredAt(sh *shard, key string, st *ResourceState, now time.Time) ([]Grant, error) {
	any := false
	for token, h := range st.Holders {
		if leaseHasExpired(h, now) || lm.orphanPastTTL(h.abandonedAtNanos, now) {
			lm.dropHolder(sh, st, key, token, h, now)
			any = true
		}
	}
	if lm.evictExpiredOrphanWaiters(st, now) {
		any = true
	}
	if !any {
		return nil, nil
	}
	return lm.grantNextAt(sh, key, st, now)
}

// orphanPastTTL reports whether an entry's abandonedAtNanos is older
// than OrphanTTL relative to now. Returns false when not configured
// (OrphanTTL == 0) or the entry isn't abandoned.
func (lm *LockManager) orphanPastTTL(abandonedAtNanos int64, now time.Time) bool {
	if abandonedAtNanos == 0 || lm.cfg.OrphanTTL <= 0 {
		return false
	}
	return now.UnixNano()-abandonedAtNanos > int64(lm.cfg.OrphanTTL)
}

// evictExpiredOrphanWaiters removes waiters whose abandonedAtNanos is
// older than OrphanTTL. Returns true iff at least one was removed
// (caller uses this to decide whether to promote the next waiter).
func (lm *LockManager) evictExpiredOrphanWaiters(st *ResourceState, now time.Time) bool {
	removed := false
	n := st.WaiterHead
	for i := st.WaiterHead; i < len(st.Waiters); i++ {
		w := st.Waiters[i]
		if w == nil {
			continue
		}
		if lm.orphanPastTTL(w.abandonedAtNanos, now) {
			removed = true
			continue
		}
		st.Waiters[n] = w
		n++
	}
	for i := n; i < len(st.Waiters); i++ {
		st.Waiters[i] = nil
	}
	st.Waiters = st.Waiters[:n]
	return removed
}

// grantNextAt drains the waiter queue while capacity is available,
// minting tokens deterministically from fsmFenceCounter + the waiter's
// salt. Returns one Grant per promotion. Holders are added even if
// nobody listens — lease expiry is the backstop (cluster failover case).
func (lm *LockManager) grantNextAt(sh *shard, key string, st *ResourceState, now time.Time) ([]Grant, error) {
	var grants []Grant
	for st.WaiterHead < len(st.Waiters) && len(st.Holders) < st.Limit {
		w := st.Waiters[st.WaiterHead]
		st.Waiters[st.WaiterHead] = nil
		st.WaiterHead++
		token, err := lm.mintFSMToken(w.salt)
		if err != nil {
			st.compactWaiters()
			return grants, err
		}
		grants = append(grants, lm.promoteWaiter(sh, st, key, w, token, now))
	}
	st.compactWaiters()
	return grants, nil
}

func (lm *LockManager) promoteWaiter(sh *shard, st *ResourceState, key string, w *waiter, token string, now time.Time) Grant {
	eqKey := connKey{ConnID: w.connID, Key: key}
	if es, ok := sh.connEnqueued[eqKey]; ok && es.waiter == w {
		es.waiter = nil
		es.token = token
	}
	st.Holders[token] = &holder{connID: w.connID, leaseExpires: now.Add(w.leaseTTL), ref: w.ref}
	st.LastActivity = now
	sh.addOwned(w.connID, key, token)
	return Grant{Key: key, Ref: w.ref, Token: token, LeaseSec: secondsOf(w.leaseTTL), ConnID: w.connID}
}

// dropHolder removes (key, token) from state and the related
// bookkeeping. Caller holds sh.mu.
func (lm *LockManager) dropHolder(sh *shard, st *ResourceState, key, token string, h *holder, now time.Time) {
	sh.removeOwned(h.connID, key, token)
	eqKey := connKey{ConnID: h.connID, Key: key}
	if es, ok := sh.connEnqueued[eqKey]; ok && constantTimeTokenEqual(es.token, token) {
		sh.removeEnqueued(eqKey)
	}
	delete(st.Holders, token)
	st.LastActivity = now
}

// recordHolder installs (token, holder) in st with a lease of leaseTTL
// from now, and updates the per-conn owned index.
func (lm *LockManager) recordHolder(sh *shard, st *ResourceState, key, ref string, connID uint64, token string, now time.Time, leaseTTL time.Duration) {
	st.Holders[token] = &holder{connID: connID, leaseExpires: now.Add(leaseTTL), ref: ref}
	st.LastActivity = now
	sh.addOwned(connID, key, token)
}

// mintFSMToken builds a token from fsmFenceCounter+1 + salt. Must be
// called with the relevant shard mutex held (so updates are linearised
// against other state mutations).
func (lm *LockManager) mintFSMToken(salt [8]byte) (string, error) {
	lm.fsmFenceCounter++
	return encodeToken(lm.fsmFenceCounter, salt), nil
}

// collectIdleKeysAt mirrors collectIdleKeys but uses an injected `now`.
func collectIdleKeysAt(sh *shard, now time.Time, maxIdle time.Duration) []string {
	var out []string
	for key, st := range sh.resources {
		if now.Sub(st.LastActivity) > maxIdle && len(st.Holders) == 0 && st.waiterCount() == 0 {
			out = append(out, key)
		}
	}
	return out
}

// secondsOf returns d truncated to whole seconds, floored at 0.
func secondsOf(d time.Duration) int {
	s := int(d / time.Second)
	if s < 0 {
		return 0
	}
	return s
}

// applyErr packages a sentinel error from getOrCreate / enqueue into an
// ApplyResult with the matching status; the err is also surfaced so the
// caller can route it on its return path.
func applyErr(err error) ApplyResult {
	switch {
	case errors.Is(err, ErrMaxLocks):
		return ApplyResult{Status: StatusErrMaxLocks}
	case errors.Is(err, ErrMaxWaiters):
		return ApplyResult{Status: StatusErrMaxWaiters}
	case errors.Is(err, ErrLimitMismatch):
		return ApplyResult{Status: StatusErrLimitMismatch}
	case errors.Is(err, ErrAlreadyEnqueued):
		return ApplyResult{Status: StatusErrAlreadyEnqueued}
	case errors.Is(err, ErrNotEnqueued):
		return ApplyResult{Status: StatusErrNotEnqueued}
	case errors.Is(err, ErrLeaseExpired):
		return ApplyResult{Status: StatusErrLeaseExpired}
	}
	return ApplyResult{Status: StatusUnused}
}
