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
	"fmt"
	"math"
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
	_, res, grants, err := lm.applyAcquireLocked(sh, key, limit, ref, connID, leaseTTL, salt, now)
	if err != nil {
		return ApplyResult{}, nil, err
	}
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
	// Re-adopt first: a reconnecting stable ref must reclaim its existing
	// holder/waiter rather than mint a second slot (which, on a semaphore
	// with a free slot, would let one ref hold twice).
	if kind, tok, _ := lm.reAttachByRef(sh, st, key, ref, connID, leaseTTL, now); kind != reAdoptNone {
		return st, acquireReAdoptResult(kind, tok, leaseTTL), pre, nil
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
	st.appendWaiter(&waiter{ref: ref, connID: connID, leaseTTL: leaseTTL, salt: salt})
	return st, ApplyResult{Status: StatusQueued}, pre, nil
}

// acquireReAdoptResult maps a re-adopt outcome to the single-phase
// Acquire result: a re-adopted holder grants immediately (StatusOK with
// the original token); a re-adopted waiter stays queued.
func acquireReAdoptResult(kind reAdoptKind, tok string, leaseTTL time.Duration) ApplyResult {
	if kind == reAdoptHolder {
		return ApplyResult{Status: StatusOK, Token: tok, LeaseSec: secondsOf(leaseTTL)}
	}
	return ApplyResult{Status: StatusQueued}
}

// ApplyEnqueue is the FSM-side phase-1 enqueue (two-phase). Returns
// either StatusAcquired with a token (fast path) or StatusQueued.
//
// Stable-ref re-adopt path: when the caller supplied a non-empty ref, the FSM
// first looks for an existing holder
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

// reAdoptKind classifies what reAttachByRef re-adopted.
type reAdoptKind uint8

const (
	reAdoptNone reAdoptKind = iota
	reAdoptHolder
	reAdoptWaiter
)

// tryReAdopt is the two-phase Enqueue's re-adopt wrapper: it re-attaches
// any holder/waiter on (key, ref) to connID and records the new
// connection's two-phase index entry. Returns (result, true) on a
// re-adopt; (zero, false) otherwise. Caller holds sh.mu.
func (lm *LockManager) tryReAdopt(sh *shard, st *ResourceState, eqKey connKey, key, ref string, connID uint64, leaseTTL time.Duration, now time.Time) (ApplyResult, bool) {
	kind, tok, w := lm.reAttachByRef(sh, st, key, ref, connID, leaseTTL, now)
	switch kind {
	case reAdoptHolder:
		sh.setEnqueued(eqKey, &enqueuedState{token: tok, leaseTTL: leaseTTL})
		return ApplyResult{Status: StatusAcquired, Token: tok, LeaseSec: secondsOf(leaseTTL)}, true
	case reAdoptWaiter:
		sh.setEnqueued(eqKey, &enqueuedState{waiter: w, leaseTTL: leaseTTL})
		return ApplyResult{Status: StatusQueued}, true
	default:
		return ApplyResult{}, false
	}
}

// reAttachByRef re-attaches an existing holder or waiter on (key, ref)
// to newConnID — regardless of whether it was gracefully orphaned
// (abandonedAtNanos != 0) or hard-crashed (still 0). Matching by ref
// alone is what closes the hard-crash failover gap: a new leader that
// inherited the FSM via snapshot never saw a CleanupConn for the dead
// connection, so the entry's orphan stamp is 0 — but the ref still
// identifies the reconnecting client. The previous (now-dead)
// connection's stale index entries are evicted. Holder takes precedence
// over waiter (a ref holds at most one of either per key). Returns
// reAdoptNone when nothing matches. Caller holds sh.mu.
//
// Gated on a non-empty ref, so connID-only single-node callers are inert. The
// ref-is-one-live-connection assumption is the same one the grant router
// (WatchGrants(ref)) already relies on. OrphanTTL controls how long graceful
// disconnects retain state; hard-failover reattachment does not require it.
func (lm *LockManager) reAttachByRef(sh *shard, st *ResourceState, key, ref string, newConnID uint64, leaseTTL time.Duration, now time.Time) (reAdoptKind, string, *waiter) {
	if ref == "" {
		return reAdoptNone, "", nil
	}
	if tok, h := findHolderByRef(st, ref, newConnID); h != nil {
		lm.rebindHolder(sh, st, key, tok, h, ref, newConnID, leaseTTL, now)
		return reAdoptHolder, tok, nil
	}
	if w := findWaiterByRef(st, ref, newConnID); w != nil {
		lm.rebindWaiter(sh, key, w, newConnID, leaseTTL)
		return reAdoptWaiter, "", w
	}
	return reAdoptNone, "", nil
}

// connIDProcessTagShift selects the randomized high 24 bits used as a
// process-lineage tag. The low 40 bits are a monotonic counter that fails
// closed at exhaustion, so the tag never changes during a process lifetime.
const connIDProcessTagShift = 40

// reclaimableBy reports whether an entry owned by ownerConnID may be
// re-adopted by a request arriving on newConnID.
//
// `ref` comes off the wire, so matching on it alone would let any
// caller who names another client's ref be handed that client's slot —
// and, for a holder, its fencing token. The FSM can't see which
// connections are live, but it can see two things that are replicated:
//
//   - an orphan stamp, set when a graceful CleanupConn observed the
//     owner's connection go away (the stamp also zeroes connID); and
//   - the owner's connID process tag. IDs minted in the same allocation range
//     share this tag, so a *different* tag
//     means the owner's connection belongs to some other process — a
//     crashed leader, or this node before a restart. Either way it
//     cannot still be serving that client.
//
// A live connection on this process matches neither, so its slot stays
// its own. The cost is that a client which hard-crashes without its
// TCP connection being reaped can't re-attach on the same leader until
// the conn teardown stamps it (or the lease expires) — the conservative
// direction.
func reclaimableBy(abandonedAtNanos int64, ownerConnID, newConnID uint64) bool {
	if abandonedAtNanos != 0 {
		return true
	}
	return ownerConnID>>connIDProcessTagShift != newConnID>>connIDProcessTagShift
}

// rebindHolder re-points an existing holder to newConnID, clears the
// orphan stamp, renews the lease off the caller's TTL (a reconnect
// implicitly renews, deterministically — the new TTL is in the apply),
// indexes the new owner, and evicts the dead connection's stale index.
func (lm *LockManager) rebindHolder(sh *shard, st *ResourceState, key, tok string, h *holder, newRef string, newConnID uint64, leaseTTL time.Duration, now time.Time) {
	oldConnID := h.connID
	st.unindexHolder(tok, h)
	h.connID = newConnID
	h.ref = newRef
	h.abandonedAtNanos = 0
	h.leaseExpires = now.Add(leaseTTL)
	st.indexHolder(tok, h)
	sh.addOwned(newConnID, key, tok)
	lm.evictDeadConn(sh, oldConnID, newConnID, key, tok)
}

// rebindWaiter re-points an existing waiter to newConnID, clears the
// orphan stamp, refreshes its lease TTL, and evicts the dead
// connection's stale index. The waiter keeps its queue position and
// salt (so a later promotion mints the same deterministic token).
func (lm *LockManager) rebindWaiter(sh *shard, key string, w *waiter, newConnID uint64, leaseTTL time.Duration) {
	oldConnID := w.connID
	w.connID = newConnID
	w.abandonedAtNanos = 0
	w.leaseTTL = leaseTTL
	lm.evictDeadConn(sh, oldConnID, newConnID, key, "")
}

// evictDeadConn removes the previous (now-dead) connection's per-conn
// index for key after its holder/waiter was re-adopted by newConnID.
// token is "" for a re-adopted waiter (no owned slot to drop). No-op
// when oldConnID is the sentinel 0 (a gracefully-orphaned entry, whose
// index CleanupConn already removed) or equals newConnID (defensive — a
// reconnect always gets a fresh connID).
func (lm *LockManager) evictDeadConn(sh *shard, oldConnID, newConnID uint64, key, token string) {
	if oldConnID == 0 || oldConnID == newConnID {
		return
	}
	if token != "" {
		sh.removeOwned(oldConnID, key, token)
	}
	sh.removeEnqueued(connKey{ConnID: oldConnID, Key: key})
}

// findHolderByRef returns the holder on st whose ref matches and which
// newConnID is allowed to reclaim, preferring the lexicographically
// smallest token so the choice is deterministic across replicas if
// (pathologically) more than one matches. Returns ("", nil) when none
// match — including when the only match is a live connection's.
func findHolderByRef(st *ResourceState, ref string, newConnID uint64) (string, *holder) {
	state := st.refs[ref]
	if state == nil {
		return "", nil
	}
	var bestTok string
	var best *holder
	for tok, h := range state.holders {
		if !reclaimableBy(h.abandonedAtNanos, h.connID, newConnID) {
			continue
		}
		if best == nil || tok < bestTok {
			bestTok, best = tok, h
		}
	}
	return bestTok, best
}

// findWaiterByRef returns the first waiter (by queue position) on st
// whose ref matches and which newConnID is allowed to reclaim, or nil.
func findWaiterByRef(st *ResourceState, ref string, newConnID uint64) *waiter {
	state := st.refs[ref]
	if state == nil {
		return nil
	}
	for _, w := range state.waiters {
		if reclaimableBy(w.abandonedAtNanos, w.connID, newConnID) {
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
	st.appendWaiter(w)
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
	return lm.ApplyRenewOwned(now, key, token, "", 0, leaseTTL)
}

// ApplyRenewOwned renews a token and, when caller identity is present,
// rebinds the holder to the current connection. The token remains the
// authorization credential; identity controls disconnect cleanup and
// stable-ref reattachment after failover.
func (lm *LockManager) ApplyRenewOwned(now time.Time, key, token, ref string, connID uint64, leaseTTL time.Duration) (ApplyResult, []Grant, error) {
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
	if connID != 0 && (h.connID != connID || h.ref != ref || h.abandonedAtNanos != 0) {
		lm.rebindHolder(sh, st, key, token, h, ref, connID, leaseTTL, now)
	} else {
		h.leaseExpires = now.Add(leaseTTL)
	}
	st.LastActivity = now
	return ApplyResult{Status: StatusOK, LeaseSec: secondsOf(leaseTTL)}, nil, nil
}

// ApplyCancel atomically abandons one queued acquire/enqueue operation. It
// also removes a holder when promotion raced ahead of cancellation, then
// promotes the next waiter. matchSalt=true restricts matching to the exact
// connection identity; recovery paths may match a stable ref when the
// original connection-local metadata is unavailable.
func (lm *LockManager) ApplyCancel(now time.Time, key, ref string, connID uint64, salt [8]byte, matchSalt bool) (ApplyResult, []Grant, error) {
	sh := lm.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	st := sh.resources[key]
	if st == nil {
		return ApplyResult{Status: StatusOK}, nil, nil
	}

	eqKey := connKey{ConnID: connID, Key: key}
	if es := sh.connEnqueued[eqKey]; es != nil {
		if es.waiter != nil && cancelWaiterMatches(es.waiter, ref, connID, salt, matchSalt) {
			sh.removeEnqueued(eqKey)
			st.removeWaiter(es.waiter)
			st.LastActivity = now
			return ApplyResult{Status: StatusOK}, nil, nil
		}
		if es.token != "" {
			// The per-(connection,key) enqueue index already identifies the
			// current operation. A re-adopted holder retains its original
			// token salt, so do not require the new enqueue salt here.
			if h := st.Holders[es.token]; h != nil && cancelOwnerMatches(h.ref, h.connID, ref, connID, false) {
				sh.removeEnqueued(eqKey)
				lm.dropHolder(sh, st, key, es.token, h, now)
				grants, err := lm.grantNextAt(sh, key, st, now)
				return ApplyResult{Status: StatusOK}, grants, err
			}
		}
	}

	for i := st.WaiterHead; i < len(st.Waiters); i++ {
		w := st.Waiters[i]
		if cancelWaiterMatches(w, ref, connID, salt, matchSalt) {
			st.removeWaiter(w)
			st.LastActivity = now
			return ApplyResult{Status: StatusOK}, nil, nil
		}
	}
	for _, token := range sortedKeys(st.Holders) {
		h := st.Holders[token]
		if cancelHolderMatches(token, h, ref, connID, salt, matchSalt) {
			lm.dropHolder(sh, st, key, token, h, now)
			grants, err := lm.grantNextAt(sh, key, st, now)
			return ApplyResult{Status: StatusOK}, grants, err
		}
	}
	return ApplyResult{Status: StatusOK}, nil, nil
}

// ApplyAttach rebinds an existing two-phase waiter or promoted holder to the
// current stable session. It never creates a new queue slot and preserves the
// original lease deadline/TTL.
func (lm *LockManager) ApplyAttach(now time.Time, key, ref string, connID uint64) (ApplyResult, []Grant, error) {
	sh := lm.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	st := sh.resources[key]
	if st == nil {
		return applyErr(ErrNotEnqueued), nil, nil
	}
	pre, err := lm.evictExpiredAt(sh, key, st, now)
	if err != nil {
		return ApplyResult{}, pre, err
	}
	eqKey := connKey{ConnID: connID, Key: key}
	if es := sh.connEnqueued[eqKey]; es != nil {
		if es.token != "" {
			if h := st.Holders[es.token]; h != nil {
				st.LastActivity = now
				return ApplyResult{Status: StatusOK, Token: es.token, LeaseSec: secondsOf(h.leaseExpires.Sub(now))}, pre, nil
			}
		}
		if es.waiter != nil {
			st.LastActivity = now
			return ApplyResult{Status: StatusQueued}, pre, nil
		}
	}
	if token, h := findHolderForAttach(st, ref, connID); h != nil {
		remaining := h.leaseExpires.Sub(now)
		lm.rebindHolder(sh, st, key, token, h, ref, connID, remaining, now)
		sh.setEnqueued(eqKey, &enqueuedState{token: token, leaseTTL: remaining})
		st.LastActivity = now
		return ApplyResult{Status: StatusOK, Token: token, LeaseSec: secondsOf(remaining)}, pre, nil
	}
	if w := findWaiterForAttach(st, ref, connID); w != nil {
		if w.connID != connID {
			lm.rebindWaiter(sh, key, w, connID, w.leaseTTL)
		}
		sh.setEnqueued(eqKey, &enqueuedState{waiter: w, leaseTTL: w.leaseTTL})
		st.LastActivity = now
		return ApplyResult{Status: StatusQueued}, pre, nil
	}
	return applyErr(ErrNotEnqueued), pre, nil
}

func findHolderForAttach(st *ResourceState, ref string, connID uint64) (string, *holder) {
	state := st.refs[ref]
	if state == nil {
		return "", nil
	}
	var bestToken string
	var best *holder
	for token, h := range state.holders {
		if h.connID != connID && !reclaimableBy(h.abandonedAtNanos, h.connID, connID) {
			continue
		}
		if best == nil || token < bestToken {
			bestToken, best = token, h
		}
	}
	return bestToken, best
}

func findWaiterForAttach(st *ResourceState, ref string, connID uint64) *waiter {
	state := st.refs[ref]
	if state == nil {
		return nil
	}
	for _, w := range state.waiters {
		if w.connID == connID || reclaimableBy(w.abandonedAtNanos, w.connID, connID) {
			return w
		}
	}
	return nil
}

func cancelWaiterMatches(w *waiter, ref string, connID uint64, salt [8]byte, matchSalt bool) bool {
	if w == nil {
		return false
	}
	if !cancelOwnerMatches(w.ref, w.connID, ref, connID, !matchSalt) {
		return false
	}
	return !matchSalt || w.salt == salt
}

func cancelHolderMatches(token string, h *holder, ref string, connID uint64, salt [8]byte, matchSalt bool) bool {
	if h == nil {
		return false
	}
	if !cancelOwnerMatches(h.ref, h.connID, ref, connID, !matchSalt) {
		return false
	}
	return !matchSalt || tokenSaltMatches(token, salt)
}

func tokenSaltMatches(token string, salt [8]byte) bool {
	if len(token) != 32 {
		return false
	}
	return constantTimeTokenEqual(token[16:], encodeToken(0, salt)[16:])
}

func cancelOwnerMatches(ownerRef string, ownerConnID uint64, ref string, connID uint64, allowStableRef bool) bool {
	if ownerConnID == connID && connID != 0 {
		return ref == "" || ownerRef == ref
	}
	return allowStableRef && ref != "" && ownerRef == ref
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
	if !lm.activeFSMPolicy().AutoReleaseOnDisconnect {
		return nil, nil
	}
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
	if w == nil || w.ref == "" || lm.activeFSMPolicy().orphanTTL() <= 0 {
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
		st.unindexWaiter(w)
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
	if h == nil || h.ref == "" || lm.activeFSMPolicy().orphanTTL() <= 0 {
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
	expired := collectIdleKeysAt(sh, now, lm.activeFSMPolicy().gcMaxIdleTime())
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
	maxLocks := lm.activeFSMPolicy().MaxLocks
	for {
		current := lm.resourceTotal.Load()
		if current >= maxLocks {
			return nil, ErrMaxLocks
		}
		if lm.resourceTotal.CompareAndSwap(current, current+1) {
			break
		}
	}
	st := lm.newResourceState(limit, now)
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
	orphanTTL := lm.activeFSMPolicy().orphanTTL()
	if abandonedAtNanos == 0 || orphanTTL <= 0 {
		return false
	}
	return now.UnixNano()-abandonedAtNanos > int64(orphanTTL)
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
			st.unindexWaiter(w)
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
		st.unindexWaiter(w)
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
	st.addHolder(token, &holder{connID: w.connID, leaseExpires: now.Add(w.leaseTTL), ref: w.ref})
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
	st.removeHolder(token)
	st.LastActivity = now
}

// recordHolder installs (token, holder) in st with a lease of leaseTTL
// from now, and updates the per-conn owned index.
func (lm *LockManager) recordHolder(sh *shard, st *ResourceState, key, ref string, connID uint64, token string, now time.Time, leaseTTL time.Duration) {
	st.addHolder(token, &holder{connID: connID, leaseExpires: now.Add(leaseTTL), ref: ref})
	st.LastActivity = now
	sh.addOwned(connID, key, token)
}

// mintFSMToken builds a token from fsmFenceCounter+1 + salt. Must be
// called with the relevant shard mutex held (so updates are linearised
// against other state mutations).
func (lm *LockManager) mintFSMToken(salt [8]byte) (string, error) {
	if lm.fsmFenceCounter == math.MaxUint64 {
		return "", fmt.Errorf("%w: FSM counter exhausted", ErrFencePersistence)
	}
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
