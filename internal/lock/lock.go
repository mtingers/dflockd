package lock

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mtingers/dflockd/internal/config"
	"github.com/mtingers/dflockd/internal/signal"
)

var (
	ErrMaxLocks        = errors.New("max locks reached")
	ErrMaxWaiters      = errors.New("max waiters reached")
	ErrNotEnqueued     = errors.New("not enqueued for this key")
	ErrAlreadyEnqueued = errors.New("already enqueued for this key")
	ErrLimitMismatch   = errors.New("limit mismatch for semaphore key")
	ErrLeaseExpired    = errors.New("lease expired before wait")
	ErrWaiterClosed    = errors.New("waiter channel closed")
)

// tokenBuf amortises crypto/rand syscalls by buffering 4096 bytes (256 tokens)
// and dispensing 16 bytes at a time.
type tokenBuf struct {
	mu  sync.Mutex
	buf [4096]byte
	pos int // starts at len(buf) to force initial fill
}

func newTokenBuf() tokenBuf {
	return tokenBuf{pos: 4096} // force fill on first call
}

func (tb *tokenBuf) next() string {
	tb.mu.Lock()
	if tb.pos+16 > len(tb.buf) {
		if _, err := rand.Read(tb.buf[:]); err != nil {
			panic("crypto/rand failed: " + err.Error())
		}
		tb.pos = 0
	}
	tok := hex.EncodeToString(tb.buf[tb.pos : tb.pos+16])
	tb.pos += 16
	tb.mu.Unlock()
	return tok
}

type connKey struct {
	ConnID uint64
	Key    string
}

type waiter struct {
	ch       chan string
	connID   uint64
	leaseTTL time.Duration
}

type holder struct {
	connID       uint64
	leaseExpires time.Time
}

// ResourceState is the unified state for both locks (Limit==1) and
// semaphores (Limit>1). A lock is simply a semaphore with Limit 1.
type ResourceState struct {
	Limit        int
	Holders      map[string]*holder // token → holder
	Waiters      []*waiter
	WaiterHead   int // index of first active waiter
	LastActivity time.Time
}

// waiterCount returns the number of active waiters.
func (rs *ResourceState) waiterCount() int {
	return len(rs.Waiters) - rs.WaiterHead
}

// compactWaiters reclaims consumed waiter slots when more than half the slice
// is dead head space. Must be called with the protecting mutex held.
func (rs *ResourceState) compactWaiters() {
	if rs.WaiterHead > len(rs.Waiters)/2 {
		n := copy(rs.Waiters, rs.Waiters[rs.WaiterHead:])
		for i := n; i < len(rs.Waiters); i++ {
			rs.Waiters[i] = nil
		}
		rs.Waiters = rs.Waiters[:n]
		rs.WaiterHead = 0
	}
}

type enqueuedState struct {
	waiter   *waiter
	token    string
	leaseTTL time.Duration
}

// ---------------------------------------------------------------------------
// Sharded lock manager
// ---------------------------------------------------------------------------

const numShards = 64

type shard struct {
	mu           sync.Mutex
	resources    map[string]*ResourceState
	connOwned    map[uint64]map[string]map[string]struct{} // connID → key → set of tokens
	connEnqueued map[connKey]*enqueuedState
}

type LockManager struct {
	shards        [numShards]shard
	resourceTotal atomic.Int64 // total resources across all shards (avoids data race on map reads)
	cfg           *config.Config
	log           *slog.Logger
	tokBuf        tokenBuf
}

func NewLockManager(cfg *config.Config, log *slog.Logger) *LockManager {
	lm := &LockManager{
		cfg:    cfg,
		log:    log,
		tokBuf: newTokenBuf(),
	}
	for i := range lm.shards {
		lm.shards[i].resources = make(map[string]*ResourceState)
		lm.shards[i].connOwned = make(map[uint64]map[string]map[string]struct{})
		lm.shards[i].connEnqueued = make(map[connKey]*enqueuedState)
	}
	return lm
}

func shardIndex(key string) int {
	h := uint32(2166136261) // FNV-32a offset basis
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619 // FNV-32a prime
	}
	return int(h % numShards)
}

func (lm *LockManager) shardFor(key string) *shard {
	return &lm.shards[shardIndex(key)]
}

// newToken generates a token using the LockManager's buffered CSPRNG.
func (lm *LockManager) newToken() string {
	return lm.tokBuf.next()
}

// removeWaiterFromState removes target from the resource state's waiter queue.
// Searches only from WaiterHead onward. Must be called with the shard lock held.
func removeWaiterFromState(st *ResourceState, target *waiter) {
	for i := st.WaiterHead; i < len(st.Waiters); i++ {
		if st.Waiters[i] == target {
			copy(st.Waiters[i:], st.Waiters[i+1:])
			st.Waiters[len(st.Waiters)-1] = nil
			st.Waiters = st.Waiters[:len(st.Waiters)-1]
			return
		}
	}
}

// removeWaitersByConn removes all waiters for a given connID from the
// resource state, closing their channels unless already tracked in the
// closed set. Operates on the active portion [WaiterHead:].
func removeWaitersByConn(st *ResourceState, connID uint64, closed map[chan string]struct{}) {
	n := st.WaiterHead
	for i := st.WaiterHead; i < len(st.Waiters); i++ {
		w := st.Waiters[i]
		if w.connID == connID {
			if _, already := closed[w.ch]; !already {
				close(w.ch)
				closed[w.ch] = struct{}{}
			}
		} else {
			st.Waiters[n] = w
			n++
		}
	}
	for i := n; i < len(st.Waiters); i++ {
		st.Waiters[i] = nil
	}
	st.Waiters = st.Waiters[:n]
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

// connID == 0 is a sentinel meaning "skip per-connection bookkeeping" —
// the caller is responsible for their own cleanup (explicit Release or
// lease expiry). connAddOwned and connRemoveOwned must agree on this so
// CleanupConnection(0) remains a correct no-op rather than leaking
// entries that only ever got added.
//
// Today connSeq starts at 1 so no real connection uses 0; the sentinel is
// reserved for potential transport-agnostic callers (e.g. test helpers)
// that want to bypass the disconnect-cleanup path entirely.

// connAddOwned adds a token to the shard's connOwned map. Must be called with sh.mu held.
func (sh *shard) connAddOwned(connID uint64, key, token string) {
	if connID == 0 {
		return
	}
	m, ok := sh.connOwned[connID]
	if !ok {
		m = make(map[string]map[string]struct{})
		sh.connOwned[connID] = m
	}
	tokens, ok := m[key]
	if !ok {
		tokens = make(map[string]struct{})
		m[key] = tokens
	}
	tokens[token] = struct{}{}
}

// connRemoveOwned removes a token from the shard's connOwned. Must be called with sh.mu held.
func (sh *shard) connRemoveOwned(connID uint64, key, token string) {
	if connID == 0 {
		return
	}
	m, ok := sh.connOwned[connID]
	if !ok {
		return
	}
	tokens, ok := m[key]
	if !ok {
		return
	}
	delete(tokens, token)
	if len(tokens) == 0 {
		delete(m, key)
	}
	if len(m) == 0 {
		delete(sh.connOwned, connID)
	}
}

// grantNextWaiterLocked grants slots to FIFO waiters while capacity is
// available. Must be called with sh.mu held.
func (lm *LockManager) grantNextWaiterLocked(sh *shard, key string, st *ResourceState) {
	now := time.Now()
	for st.WaiterHead < len(st.Waiters) && len(st.Holders) < st.Limit {
		w := st.Waiters[st.WaiterHead]
		st.Waiters[st.WaiterHead] = nil // avoid memory leak
		st.WaiterHead++
		token := lm.newToken()
		// Non-blocking send: if the channel is full, the waiter isn't
		// reading yet (it's still in flight to its select) — we skip
		// this grant for them.
		//
		// A send on a closed channel would panic. Our protocol is that
		// callers always remove the waiter from the queue before
		// closing its channel (see removeWaitersByConn and the
		// connEnqueued cleanup in CleanupConnection), so this path
		// should be unreachable. We recover specifically from the send
		// and log loudly if it ever fires — a silent recover here
		// previously swallowed the invariant violation, leaving
		// phantom holders if any map mutation below panicked too.
		sent := lm.trySendGrant(w.ch, token, key, w.connID)
		if !sent {
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
		sh.connAddOwned(w.connID, key, token)
	}
	st.compactWaiters()
}

// trySendGrant performs the potentially-closed-channel send in isolation.
// Returns true if the token was delivered; false if the channel was full
// or closed. A recovered panic is logged with key+connID so the
// invariant-violation case doesn't go silent.
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

// evictExpiredLocked evicts any holders whose leases have expired and grants
// freed slots to waiting callers. Must be called with sh.mu held.
func (lm *LockManager) evictExpiredLocked(sh *shard, key string, st *ResourceState) {
	now := time.Now()
	anyExpired := false
	for token, h := range st.Holders {
		if !h.leaseExpires.IsZero() && !now.Before(h.leaseExpires) {
			lm.log.Warn("evicting expired lease on acquire",
				"key", key, "conn", h.connID)
			sh.connRemoveOwned(h.connID, key, token)
			eqKey := connKey{ConnID: h.connID, Key: key}
			if es, ok := sh.connEnqueued[eqKey]; ok && es.token == token {
				delete(sh.connEnqueued, eqKey)
			}
			delete(st.Holders, token)
			anyExpired = true
		}
	}
	if anyExpired {
		st.LastActivity = now
		lm.grantNextWaiterLocked(sh, key, st)
	}
}

// resourceCount returns total number of resources across all shards using
// the atomic counter, which is safe to read without holding shard locks.
func (lm *LockManager) resourceCount() int {
	return int(lm.resourceTotal.Load())
}

func (lm *LockManager) getOrCreateLocked(sh *shard, key string, limit int) (*ResourceState, error) {
	st, ok := sh.resources[key]
	if ok {
		if st.Limit != limit {
			return nil, ErrLimitMismatch
		}
		return st, nil
	}
	// CAS loop to enforce MaxLocks as a hard cap. The prior version used
	// a check-then-add pattern that two shards could race past, allowing
	// resourceTotal to temporarily exceed MaxLocks. CAS here ensures
	// that at most MaxLocks transitions 0→1 succeed across all shards.
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
	st = &ResourceState{
		Limit:        limit,
		Holders:      make(map[string]*holder),
		LastActivity: time.Now(),
	}
	sh.resources[key] = st
	return st, nil
}

// ---------------------------------------------------------------------------
// Public methods — unified for both locks (limit=1) and semaphores (limit>1)
// ---------------------------------------------------------------------------

// Acquire is the single-phase acquire (commands "l" and "sl").
func (lm *LockManager) Acquire(ctx context.Context, key string, timeout, leaseTTL time.Duration, connID uint64, limit int) (string, error) {
	sh := lm.shardFor(key)

	sh.mu.Lock()
	st, err := lm.getOrCreateLocked(sh, key, limit)
	if err != nil {
		sh.mu.Unlock()
		return "", err
	}

	now := time.Now()
	st.LastActivity = now

	// Opportunistic expired-lease eviction (avoids waiting for sweep tick)
	lm.evictExpiredLocked(sh, key, st)

	// Fast path: capacity available and no waiters — no waiter allocation needed
	if len(st.Holders) < st.Limit && st.waiterCount() == 0 {
		token := lm.newToken()
		st.Holders[token] = &holder{
			connID:       connID,
			leaseExpires: now.Add(leaseTTL),
		}
		sh.connAddOwned(connID, key, token)
		sh.mu.Unlock()
		return token, nil
	}

	// Slow path: allocate waiter and enqueue
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

	select {
	case token, ok := <-w.ch:
		if !ok || token == "" {
			return "", ErrWaiterClosed
		}
		sh.mu.Lock()
		if s := sh.resources[key]; s != nil {
			if _, ok := s.Holders[token]; ok {
				s.LastActivity = time.Now()
				sh.mu.Unlock()
				return token, nil
			}
		}
		// Token was granted but its lease expired before we received it.
		sh.mu.Unlock()
		return "", ErrLeaseExpired

	case <-timeoutCtx.Done():
		sh.mu.Lock()
		// Race check: token may have arrived between cancellation
		// and acquiring the mutex.
		select {
		case token, ok := <-w.ch:
			if ok && token != "" {
				if s := sh.resources[key]; s != nil {
					if h, hOK := s.Holders[token]; hOK {
						// If the PARENT ctx was cancelled (not just our
						// timeoutCtx), the caller asked to abandon —
						// they don't expect to end up holding a lock.
						// Release the just-granted token and pass it to
						// the next waiter instead of leaking it until
						// lease expiry.
						if ctx.Err() != nil {
							sh.connRemoveOwned(h.connID, key, token)
							delete(s.Holders, token)
							s.LastActivity = time.Now()
							lm.grantNextWaiterLocked(sh, key, s)
							sh.mu.Unlock()
							return "", ctx.Err()
						}
						s.LastActivity = time.Now()
						sh.mu.Unlock()
						return token, nil
					}
				}
				// Token was granted but expired; fall through to cleanup.
			}
		default:
		}
		if s := sh.resources[key]; s != nil {
			s.LastActivity = time.Now()
			removeWaiterFromState(s, w)
		}
		sh.mu.Unlock()
		// Distinguish parent context cancellation from timeout
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", nil
	}
}

// Enqueue is phase 1 of two-phase acquire (commands "e" and "se").
// Returns (status, token, leaseTTLSec, err).
func (lm *LockManager) Enqueue(key string, leaseTTL time.Duration, connID uint64, limit int) (string, string, int, error) {
	eqKey := connKey{ConnID: connID, Key: key}
	sh := lm.shardFor(key)

	sh.mu.Lock()
	defer sh.mu.Unlock()

	if _, exists := sh.connEnqueued[eqKey]; exists {
		return "", "", 0, ErrAlreadyEnqueued
	}

	st, err := lm.getOrCreateLocked(sh, key, limit)
	if err != nil {
		return "", "", 0, err
	}

	now := time.Now()
	st.LastActivity = now
	leaseSec := int(leaseTTL / time.Second)

	// Opportunistic expired-lease eviction (avoids waiting for sweep tick)
	lm.evictExpiredLocked(sh, key, st)

	// Fast path: capacity available and no waiters
	if len(st.Holders) < st.Limit && st.waiterCount() == 0 {
		token := lm.newToken()
		st.Holders[token] = &holder{
			connID:       connID,
			leaseExpires: now.Add(leaseTTL),
		}
		sh.connAddOwned(connID, key, token)
		sh.connEnqueued[eqKey] = &enqueuedState{token: token, leaseTTL: leaseTTL}
		return "acquired", token, leaseSec, nil
	}

	// Slow path: create waiter and enqueue
	if max := lm.cfg.MaxWaiters; max > 0 && st.waiterCount() >= max {
		return "", "", 0, ErrMaxWaiters
	}
	w := &waiter{
		ch:       make(chan string, 1),
		connID:   connID,
		leaseTTL: leaseTTL,
	}
	st.Waiters = append(st.Waiters, w)
	sh.connEnqueued[eqKey] = &enqueuedState{waiter: w, leaseTTL: leaseTTL}
	return "queued", "", 0, nil
}

// Wait is phase 2 of two-phase acquire (commands "w" and "sw").
// Returns (token, leaseTTLSec, err). Empty token means timeout.
func (lm *LockManager) Wait(ctx context.Context, key string, timeout time.Duration, connID uint64) (string, int, error) {
	eqKey := connKey{ConnID: connID, Key: key}
	sh := lm.shardFor(key)

	sh.mu.Lock()
	es, ok := sh.connEnqueued[eqKey]
	if !ok {
		sh.mu.Unlock()
		return "", 0, ErrNotEnqueued
	}

	// Snapshot immutable fields under lock.
	leaseTTL := es.leaseTTL
	leaseSec := int(leaseTTL / time.Second)
	esToken := es.token
	w := es.waiter
	sh.mu.Unlock()

	// Fast path: already acquired during enqueue
	if esToken != "" {
		sh.mu.Lock()
		delete(sh.connEnqueued, eqKey)
		now := time.Now()
		st := sh.resources[key]
		if st != nil {
			h, hOK := st.Holders[esToken]
			if hOK {
				// Verify still held (lease may have expired)
				if !h.leaseExpires.IsZero() && !now.Before(h.leaseExpires) {
					// Expired: clean up holder and grant to next waiter
					sh.connRemoveOwned(connID, key, esToken)
					delete(st.Holders, esToken)
					st.LastActivity = now
					lm.grantNextWaiterLocked(sh, key, st)
					sh.mu.Unlock()
					return "", 0, ErrLeaseExpired
				}
				// Reset lease
				h.leaseExpires = now.Add(leaseTTL)
				st.LastActivity = now
				sh.mu.Unlock()
				return esToken, leaseSec, nil
			}
		}
		// Slot was lost (expired and granted to another, or state GC'd)
		sh.connRemoveOwned(connID, key, esToken)
		sh.mu.Unlock()
		return "", 0, ErrLeaseExpired
	}

	// Slow path: waiter is pending
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	select {
	case token, ok := <-w.ch:
		if !ok || token == "" {
			sh.mu.Lock()
			delete(sh.connEnqueued, eqKey)
			sh.mu.Unlock()
			return "", 0, ErrWaiterClosed
		}
		sh.mu.Lock()
		delete(sh.connEnqueued, eqKey)
		now := time.Now()
		if st := sh.resources[key]; st != nil {
			if h, hOK := st.Holders[token]; hOK {
				h.leaseExpires = now.Add(leaseTTL)
				st.LastActivity = now
				sh.mu.Unlock()
				return token, leaseSec, nil
			}
		}
		// Token was granted but its lease expired before we received it.
		sh.mu.Unlock()
		return "", 0, ErrLeaseExpired

	case <-timeoutCtx.Done():
		sh.mu.Lock()
		delete(sh.connEnqueued, eqKey)
		select {
		case token, ok := <-w.ch:
			if ok && token != "" {
				now := time.Now()
				if st := sh.resources[key]; st != nil {
					if h, hOK := st.Holders[token]; hOK {
						// Parent ctx cancelled: the caller is
						// abandoning, don't hand them a lock they
						// didn't know they'd hold. Release and pass
						// the grant to the next waiter.
						if ctx.Err() != nil {
							sh.connRemoveOwned(h.connID, key, token)
							delete(st.Holders, token)
							st.LastActivity = now
							lm.grantNextWaiterLocked(sh, key, st)
							sh.mu.Unlock()
							return "", 0, ctx.Err()
						}
						h.leaseExpires = now.Add(leaseTTL)
						st.LastActivity = now
						sh.mu.Unlock()
						return token, leaseSec, nil
					}
				}
				// Token was granted but expired; fall through to cleanup.
			}
		default:
		}
		if st := sh.resources[key]; st != nil {
			st.LastActivity = time.Now()
			removeWaiterFromState(st, w)
		}
		sh.mu.Unlock()
		// Distinguish parent context cancellation from timeout
		if ctx.Err() != nil {
			return "", 0, ctx.Err()
		}
		return "", 0, nil
	}
}

// Release releases one held slot if the token matches (commands "r" and "sr").
func (lm *LockManager) Release(key, token string) bool {
	sh := lm.shardFor(key)

	sh.mu.Lock()
	defer sh.mu.Unlock()

	st := sh.resources[key]
	if st == nil {
		return false
	}

	now := time.Now()
	st.LastActivity = now

	h, ok := st.Holders[token]
	if !ok {
		return false
	}

	sh.connRemoveOwned(h.connID, key, token)
	eqKey := connKey{ConnID: h.connID, Key: key}
	if es, ok := sh.connEnqueued[eqKey]; ok && es.token == token {
		delete(sh.connEnqueued, eqKey)
	}
	delete(st.Holders, token)
	lm.grantNextWaiterLocked(sh, key, st)
	return true
}

// Renew renews the lease if the token matches (commands "n" and "sn").
// Returns (remaining seconds, ok).
func (lm *LockManager) Renew(key, token string, leaseTTL time.Duration) (int, bool) {
	sh := lm.shardFor(key)

	sh.mu.Lock()
	defer sh.mu.Unlock()

	st := sh.resources[key]
	if st == nil {
		return 0, false
	}

	now := time.Now()
	st.LastActivity = now

	h, ok := st.Holders[token]
	if !ok {
		return 0, false
	}

	// If already expired, reject and evict
	if !h.leaseExpires.IsZero() && !now.Before(h.leaseExpires) {
		lm.log.Warn("renew rejected (already expired)",
			"key", key, "conn", h.connID)
		sh.connRemoveOwned(h.connID, key, token)
		eqKey := connKey{ConnID: h.connID, Key: key}
		if es, ok := sh.connEnqueued[eqKey]; ok && es.token == token {
			delete(sh.connEnqueued, eqKey)
		}
		delete(st.Holders, token)
		st.LastActivity = now
		lm.grantNextWaiterLocked(sh, key, st)
		return 0, false
	}

	// Reset lease
	h.leaseExpires = now.Add(leaseTTL)
	st.LastActivity = now

	remaining := int(leaseTTL.Seconds())
	if remaining < 0 {
		remaining = 0
	}
	return remaining, true
}

// ---------------------------------------------------------------------------
// Connection cleanup
// ---------------------------------------------------------------------------

// CleanupConnection cleans up all state for a disconnected connection.
// Safe to call after the connection handler exits — no new operations from
// this connID are possible, so iterating shards one-at-a-time is correct.
func (lm *LockManager) CleanupConnection(connID uint64) {
	closed := make(map[chan string]struct{})

	for i := range lm.shards {
		sh := &lm.shards[i]
		sh.mu.Lock()

		// Clean up two-phase enqueued state.
		for ck, es := range sh.connEnqueued {
			if ck.ConnID != connID {
				continue
			}
			delete(sh.connEnqueued, ck)
			if es != nil && es.waiter != nil {
				if _, already := closed[es.waiter.ch]; !already {
					close(es.waiter.ch)
					closed[es.waiter.ch] = struct{}{}
				}
				if st := sh.resources[ck.Key]; st != nil {
					removeWaiterFromState(st, es.waiter)
				}
			}
		}

		// Cancel pending waiters from single-phase acquire path.
		for _, st := range sh.resources {
			removeWaitersByConn(st, connID, closed)
		}

		// Release owned slots only when configured to do so. Pending
		// waiters/enqueued state above are always cleaned; a disconnected
		// waiter can never observe a later grant.
		if lm.cfg.AutoReleaseOnDisconnect {
			if owned, ok := sh.connOwned[connID]; ok {
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
						lm.log.Warn("disconnect cleanup: releasing",
							"key", key, "conn_id", connID)
						delete(st.Holders, token)
					}
					st.LastActivity = time.Now()
					lm.grantNextWaiterLocked(sh, key, st)
				}
				delete(sh.connOwned, connID)
			}
		}

		sh.mu.Unlock()
	}
}

// ---------------------------------------------------------------------------
// Background loops
// ---------------------------------------------------------------------------

// LeaseExpiryLoop runs the lease expiry background loop.
func (lm *LockManager) LeaseExpiryLoop(ctx context.Context) {
	lm.log.Debug("lease_expiry_loop: [starting]")
	ticker := time.NewTicker(lm.cfg.LeaseSweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			for i := range lm.shards {
				sh := &lm.shards[i]
				sh.mu.Lock()
				for key, st := range sh.resources {
					anyExpired := false
					for token, h := range st.Holders {
						if h.leaseExpires.IsZero() {
							continue
						}
						if !now.Before(h.leaseExpires) {
							lm.log.Warn("lease expired",
								"key", key, "conn", h.connID)
							sh.connRemoveOwned(h.connID, key, token)
							eqKey := connKey{ConnID: h.connID, Key: key}
							if es, ok := sh.connEnqueued[eqKey]; ok && es.token == token {
								delete(sh.connEnqueued, eqKey)
							}
							delete(st.Holders, token)
							anyExpired = true
						}
					}
					if anyExpired {
						st.LastActivity = now
						lm.grantNextWaiterLocked(sh, key, st)
					}
				}
				sh.mu.Unlock()
			}
		}
	}
}

// GCLoop runs the lock state garbage collection loop.
func (lm *LockManager) GCLoop(ctx context.Context) {
	lm.log.Debug("lock_gc_loop: [starting]")
	ticker := time.NewTicker(lm.cfg.GCInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
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
				if len(expired) > 0 {
					lm.resourceTotal.Add(-int64(len(expired)))
				}
				for _, key := range expired {
					lm.log.Debug("GC: pruning unused state", "key", key)
					delete(sh.resources, key)
				}
				sh.mu.Unlock()
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

type LockInfo struct {
	Key             string  `json:"key"`
	OwnerConnID     uint64  `json:"owner_conn_id"`
	LeaseExpiresInS float64 `json:"lease_expires_in_s"`
	Waiters         int     `json:"waiters"`
}

type SemInfo struct {
	Key     string `json:"key"`
	Limit   int    `json:"limit"`
	Holders int    `json:"holders"`
	Waiters int    `json:"waiters"`
}

type IdleInfo struct {
	Key   string  `json:"key"`
	IdleS float64 `json:"idle_s"`
}

// SignalChannelInfo is an alias of the canonical type defined by the
// signal package. Kept as a name-preserving alias so existing external
// consumers of the lock package's Stats continue to compile; new code
// should reference signal.ChannelInfo directly.
type SignalChannelInfo = signal.ChannelInfo

type Stats struct {
	Connections    int64                `json:"connections"`
	Locks          []LockInfo           `json:"locks"`
	Semaphores     []SemInfo            `json:"semaphores"`
	IdleLocks      []IdleInfo           `json:"idle_locks"`
	IdleSemaphores []IdleInfo           `json:"idle_semaphores"`
	SignalChannels []signal.ChannelInfo `json:"signal_channels"`
}

// Stats returns a snapshot of the current lock manager state.
func (lm *LockManager) Stats(connections int64) *Stats {
	now := time.Now()
	s := &Stats{
		Connections:    connections,
		Locks:          []LockInfo{},
		Semaphores:     []SemInfo{},
		IdleLocks:      []IdleInfo{},
		IdleSemaphores: []IdleInfo{},
		SignalChannels: []signal.ChannelInfo{},
	}

	for i := range lm.shards {
		sh := &lm.shards[i]
		sh.mu.Lock()
		for key, st := range sh.resources {
			nw := st.waiterCount()
			if st.Limit == 1 {
				if len(st.Holders) > 0 {
					var ownerConn uint64
					var expires float64
					for _, h := range st.Holders {
						ownerConn = h.connID
						expires = h.leaseExpires.Sub(now).Seconds()
						if expires < 0 {
							expires = 0
						}
					}
					s.Locks = append(s.Locks, LockInfo{
						Key:             key,
						OwnerConnID:     ownerConn,
						LeaseExpiresInS: expires,
						Waiters:         nw,
					})
				} else if nw > 0 {
					s.Locks = append(s.Locks, LockInfo{
						Key:     key,
						Waiters: nw,
					})
				} else {
					s.IdleLocks = append(s.IdleLocks, IdleInfo{
						Key:   key,
						IdleS: now.Sub(st.LastActivity).Seconds(),
					})
				}
			} else {
				if len(st.Holders) > 0 {
					s.Semaphores = append(s.Semaphores, SemInfo{
						Key:     key,
						Limit:   st.Limit,
						Holders: len(st.Holders),
						Waiters: nw,
					})
				} else if nw > 0 {
					s.Semaphores = append(s.Semaphores, SemInfo{
						Key:     key,
						Limit:   st.Limit,
						Waiters: nw,
					})
				} else {
					s.IdleSemaphores = append(s.IdleSemaphores, IdleInfo{
						Key:   key,
						IdleS: now.Sub(st.LastActivity).Seconds(),
					})
				}
			}
		}
		sh.mu.Unlock()
	}

	return s
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// LockKeyForTest locks the shard mutex for the given key (for testing only).
func (lm *LockManager) LockKeyForTest(key string) { lm.shardFor(key).mu.Lock() }

// UnlockKeyForTest unlocks the shard mutex for the given key (for testing only).
func (lm *LockManager) UnlockKeyForTest(key string) { lm.shardFor(key).mu.Unlock() }

// ResourceForTest returns the ResourceState for the given key (for testing only).
// Must be called with the shard lock held (via LockKeyForTest).
func (lm *LockManager) ResourceForTest(key string) *ResourceState {
	return lm.shardFor(key).resources[key]
}

// ConnEnqueuedForTest returns the enqueued state for a given connKey (for testing only).
// Must be called with the shard lock held (via LockShardForTest with the key).
func (lm *LockManager) ConnEnqueuedForTest(ck connKey) *enqueuedState {
	return lm.shardFor(ck.Key).connEnqueued[ck]
}

// ConnOwnedForTest returns the owned map for a connID in the shard for the given key (for testing only).
// Must be called with the shard lock held (via LockShardForTest with the key).
func (lm *LockManager) ConnOwnedForTest(connID uint64, key string) map[string]map[string]struct{} {
	return lm.shardFor(key).connOwned[connID]
}

// ResourceCountForTest returns the total resource count across all shards (for testing only).
func (lm *LockManager) ResourceCountForTest() int {
	total := 0
	for i := range lm.shards {
		lm.shards[i].mu.Lock()
		total += len(lm.shards[i].resources)
		lm.shards[i].mu.Unlock()
	}
	return total
}

// ConnEnqueuedCountForTest returns the number of enqueued entries (for testing only).
func (lm *LockManager) ConnEnqueuedCountForTest() int {
	total := 0
	for i := range lm.shards {
		lm.shards[i].mu.Lock()
		total += len(lm.shards[i].connEnqueued)
		lm.shards[i].mu.Unlock()
	}
	return total
}

// ConnOwnedCountForTest returns the number of connOwned entries (for testing only).
func (lm *LockManager) ConnOwnedCountForTest() int {
	total := 0
	for i := range lm.shards {
		lm.shards[i].mu.Lock()
		total += len(lm.shards[i].connOwned)
		lm.shards[i].mu.Unlock()
	}
	return total
}

// LockShardForTest locks the shard for the given key (for testing only).
func (lm *LockManager) LockShardForTest(key string) { lm.shardFor(key).mu.Lock() }

// UnlockShardForTest unlocks the shard for the given key (for testing only).
func (lm *LockManager) UnlockShardForTest(key string) { lm.shardFor(key).mu.Unlock() }

// ResetLeaseForTest forces all holders of a key to expire immediately (for testing only).
func (lm *LockManager) ResetLeaseForTest(key string) {
	sh := lm.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if st, ok := sh.resources[key]; ok {
		for _, h := range st.Holders {
			h.leaseExpires = time.Now().Add(-1 * time.Second)
		}
	}
}

// ResetForTest clears all state (for testing only).
func (lm *LockManager) ResetForTest() {
	for i := range lm.shards {
		lm.shards[i].mu.Lock()
		lm.shards[i].resources = make(map[string]*ResourceState)
		lm.shards[i].connOwned = make(map[uint64]map[string]map[string]struct{})
		lm.shards[i].connEnqueued = make(map[connKey]*enqueuedState)
		lm.shards[i].mu.Unlock()
	}
	lm.resourceTotal.Store(0)
}
