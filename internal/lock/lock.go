package lock

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"hash/fnv"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mtingers/dflockd/internal/config"
)

var (
	ErrMaxLocks        = errors.New("max locks reached")
	ErrMaxWaiters      = errors.New("max waiters reached")
	ErrNotEnqueued     = errors.New("not enqueued for this key")
	ErrAlreadyEnqueued = errors.New("already enqueued for this key")
	ErrLimitMismatch   = errors.New("limit mismatch for semaphore key")
	ErrLeaseExpired    = errors.New("lease expired before wait")
	ErrWaiterClosed    = errors.New("waiter channel closed")
	ErrMaxKeys         = errors.New("max keys reached")
	ErrListFull        = errors.New("list at max length")
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

type counterState struct {
	Value        int64
	LastActivity time.Time
}

type kvEntry struct {
	Value        string
	ExpiresAt    time.Time // zero = no expiry
	LastActivity time.Time
}

type listState struct {
	Items        []string
	LastActivity time.Time
}

type shard struct {
	mu        sync.Mutex
	resources map[string]*ResourceState
	counters  map[string]*counterState
	kvStore   map[string]*kvEntry
	lists     map[string]*listState
}

type LockManager struct {
	shards        [numShards]shard
	connMu        sync.Mutex // protects connOwned and connEnqueued
	connOwned     map[uint64]map[string]map[string]struct{} // connID → key → set of tokens
	connEnqueued  map[connKey]*enqueuedState
	cfg           *config.Config
	log           *slog.Logger
	tokBuf        tokenBuf
	resourceTotal atomic.Int64 // total resources across all shards
	keyTotal      atomic.Int64 // total keys across all types (counters+kv+lists)
}

func NewLockManager(cfg *config.Config, log *slog.Logger) *LockManager {
	lm := &LockManager{
		connOwned:    make(map[uint64]map[string]map[string]struct{}),
		connEnqueued: make(map[connKey]*enqueuedState),
		cfg:          cfg,
		log:          log,
		tokBuf:       newTokenBuf(),
	}
	for i := range lm.shards {
		lm.shards[i].resources = make(map[string]*ResourceState)
		lm.shards[i].counters = make(map[string]*counterState)
		lm.shards[i].kvStore = make(map[string]*kvEntry)
		lm.shards[i].lists = make(map[string]*listState)
	}
	return lm
}

func shardIndex(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % numShards)
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

// connAddOwned adds a token to the connOwned map. Must be called with connMu held.
func (lm *LockManager) connAddOwned(connID uint64, key, token string) {
	m, ok := lm.connOwned[connID]
	if !ok {
		m = make(map[string]map[string]struct{})
		lm.connOwned[connID] = m
	}
	tokens, ok := m[key]
	if !ok {
		tokens = make(map[string]struct{})
		m[key] = tokens
	}
	tokens[token] = struct{}{}
}

// connRemoveOwned removes a token from connOwned. Must be called with connMu held.
func (lm *LockManager) connRemoveOwned(connID uint64, key, token string) {
	if connID == 0 {
		return
	}
	m, ok := lm.connOwned[connID]
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
		delete(lm.connOwned, connID)
	}
}

// grantNextWaiterLocked grants slots to FIFO waiters while capacity is
// available. Must be called with the shard lock AND connMu held.
func (lm *LockManager) grantNextWaiterLocked(key string, st *ResourceState) {
	for st.WaiterHead < len(st.Waiters) && len(st.Holders) < st.Limit {
		w := st.Waiters[st.WaiterHead]
		st.Waiters[st.WaiterHead] = nil // avoid memory leak
		st.WaiterHead++
		token := lm.newToken()
		select {
		case w.ch <- token:
			st.Holders[token] = &holder{
				connID:       w.connID,
				leaseExpires: time.Now().Add(w.leaseTTL),
			}
			st.LastActivity = time.Now()
			lm.connAddOwned(w.connID, key, token)
		default:
			// Channel closed or full — skip this waiter
			continue
		}
	}
	st.compactWaiters()
}

// evictExpiredLocked evicts any holders whose leases have expired and grants
// freed slots to waiting callers. Must be called with the shard lock AND connMu held.
func (lm *LockManager) evictExpiredLocked(key string, st *ResourceState) {
	now := time.Now()
	var expired []string
	for token, h := range st.Holders {
		if !h.leaseExpires.IsZero() && !now.Before(h.leaseExpires) {
			lm.log.Warn("evicting expired lease on acquire",
				"key", key, "conn", h.connID)
			lm.connRemoveOwned(h.connID, key, token)
			eqKey := connKey{ConnID: h.connID, Key: key}
			if es, ok := lm.connEnqueued[eqKey]; ok && es.token == token {
				delete(lm.connEnqueued, eqKey)
			}
			expired = append(expired, token)
		}
	}
	for _, token := range expired {
		delete(st.Holders, token)
	}
	if len(expired) > 0 {
		st.LastActivity = now
		lm.grantNextWaiterLocked(key, st)
	}
}

// resourceCount returns total number of resources across all shards.
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
	if lm.resourceCount() >= lm.cfg.MaxLocks {
		return nil, ErrMaxLocks
	}
	st = &ResourceState{
		Limit:        limit,
		Holders:      make(map[string]*holder),
		LastActivity: time.Now(),
	}
	sh.resources[key] = st
	lm.resourceTotal.Add(1)
	return st, nil
}

// ---------------------------------------------------------------------------
// Public methods — unified for both locks (limit=1) and semaphores (limit>1)
// ---------------------------------------------------------------------------

// Acquire is the single-phase acquire (commands "l" and "sl").
func (lm *LockManager) Acquire(ctx context.Context, key string, timeout, leaseTTL time.Duration, connID uint64, limit int) (string, error) {
	sh := lm.shardFor(key)

	lm.connMu.Lock()
	sh.mu.Lock()
	st, err := lm.getOrCreateLocked(sh, key, limit)
	if err != nil {
		sh.mu.Unlock()
		lm.connMu.Unlock()
		return "", err
	}

	st.LastActivity = time.Now()

	// Opportunistic expired-lease eviction (avoids waiting for sweep tick)
	lm.evictExpiredLocked(key, st)

	// Fast path: capacity available and no waiters — no waiter allocation needed
	if len(st.Holders) < st.Limit && st.waiterCount() == 0 {
		token := lm.newToken()
		st.Holders[token] = &holder{
			connID:       connID,
			leaseExpires: time.Now().Add(leaseTTL),
		}
		st.LastActivity = time.Now()
		lm.connAddOwned(connID, key, token)
		sh.mu.Unlock()
		lm.connMu.Unlock()
		return token, nil
	}

	// Slow path: allocate waiter and enqueue
	if max := lm.cfg.MaxWaiters; max > 0 && st.waiterCount() >= max {
		sh.mu.Unlock()
		lm.connMu.Unlock()
		return "", ErrMaxWaiters
	}
	w := &waiter{
		ch:       make(chan string, 1),
		connID:   connID,
		leaseTTL: leaseTTL,
	}
	st.Waiters = append(st.Waiters, w)
	sh.mu.Unlock()
	lm.connMu.Unlock()

	var timer *time.Timer
	if timeout > 0 {
		timer = time.NewTimer(timeout)
	} else {
		// Zero timeout: fire immediately
		timer = time.NewTimer(0)
	}
	defer timer.Stop()

	select {
	case token, ok := <-w.ch:
		if !ok || token == "" {
			return "", ErrWaiterClosed
		}
		sh.mu.Lock()
		if s := sh.resources[key]; s != nil {
			s.LastActivity = time.Now()
		}
		sh.mu.Unlock()
		return token, nil

	case <-ctx.Done():
		sh.mu.Lock()
		// Race check: token may have arrived between ctx cancellation
		// and acquiring the mutex.
		select {
		case token, ok := <-w.ch:
			if ok && token != "" {
				if s := sh.resources[key]; s != nil {
					s.LastActivity = time.Now()
				}
				sh.mu.Unlock()
				return token, nil
			}
		default:
		}
		if s := sh.resources[key]; s != nil {
			s.LastActivity = time.Now()
			removeWaiterFromState(s, w)
		}
		sh.mu.Unlock()
		return "", ctx.Err()

	case <-timer.C:
		// Timeout — remove from queue
		sh.mu.Lock()
		// Race check: token may have arrived
		select {
		case token, ok := <-w.ch:
			if ok && token != "" {
				if s := sh.resources[key]; s != nil {
					s.LastActivity = time.Now()
				}
				sh.mu.Unlock()
				return token, nil
			}
		default:
		}
		if s := sh.resources[key]; s != nil {
			s.LastActivity = time.Now()
			removeWaiterFromState(s, w)
		}
		sh.mu.Unlock()
		return "", nil
	}
}

// Enqueue is phase 1 of two-phase acquire (commands "e" and "se").
// Returns (status, token, leaseTTLSec, err).
func (lm *LockManager) Enqueue(key string, leaseTTL time.Duration, connID uint64, limit int) (string, string, int, error) {
	eqKey := connKey{ConnID: connID, Key: key}
	sh := lm.shardFor(key)

	lm.connMu.Lock()
	sh.mu.Lock()
	defer sh.mu.Unlock()
	defer lm.connMu.Unlock()

	if _, exists := lm.connEnqueued[eqKey]; exists {
		return "", "", 0, ErrAlreadyEnqueued
	}

	st, err := lm.getOrCreateLocked(sh, key, limit)
	if err != nil {
		return "", "", 0, err
	}

	st.LastActivity = time.Now()
	leaseSec := int(leaseTTL / time.Second)

	// Opportunistic expired-lease eviction (avoids waiting for sweep tick)
	lm.evictExpiredLocked(key, st)

	// Fast path: capacity available and no waiters
	if len(st.Holders) < st.Limit && st.waiterCount() == 0 {
		token := lm.newToken()
		st.Holders[token] = &holder{
			connID:       connID,
			leaseExpires: time.Now().Add(leaseTTL),
		}
		st.LastActivity = time.Now()
		lm.connAddOwned(connID, key, token)
		lm.connEnqueued[eqKey] = &enqueuedState{token: token, leaseTTL: leaseTTL}
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
	lm.connEnqueued[eqKey] = &enqueuedState{waiter: w, leaseTTL: leaseTTL}
	return "queued", "", 0, nil
}

// Wait is phase 2 of two-phase acquire (commands "w" and "sw").
// Returns (token, leaseTTLSec, err). Empty token means timeout.
func (lm *LockManager) Wait(ctx context.Context, key string, timeout time.Duration, connID uint64) (string, int, error) {
	eqKey := connKey{ConnID: connID, Key: key}
	sh := lm.shardFor(key)

	lm.connMu.Lock()
	es, ok := lm.connEnqueued[eqKey]
	if !ok {
		lm.connMu.Unlock()
		return "", 0, ErrNotEnqueued
	}

	// Snapshot immutable fields under lock.
	leaseTTL := es.leaseTTL
	leaseSec := int(leaseTTL / time.Second)
	esToken := es.token
	w := es.waiter
	lm.connMu.Unlock()

	// Fast path: already acquired during enqueue
	if esToken != "" {
		lm.connMu.Lock()
		sh.mu.Lock()
		delete(lm.connEnqueued, eqKey)
		st := sh.resources[key]
		if st != nil {
			h, hOK := st.Holders[esToken]
			if hOK {
				// Verify still held (lease may have expired)
				if !h.leaseExpires.IsZero() && !time.Now().Before(h.leaseExpires) {
					// Expired: clean up holder and grant to next waiter
					lm.connRemoveOwned(connID, key, esToken)
					delete(st.Holders, esToken)
					st.LastActivity = time.Now()
					lm.grantNextWaiterLocked(key, st)
					sh.mu.Unlock()
					lm.connMu.Unlock()
					return "", 0, ErrLeaseExpired
				}
				// Reset lease
				h.leaseExpires = time.Now().Add(leaseTTL)
				st.LastActivity = time.Now()
				sh.mu.Unlock()
				lm.connMu.Unlock()
				return esToken, leaseSec, nil
			}
		}
		// Slot was lost (expired and granted to another, or state GC'd)
		lm.connRemoveOwned(connID, key, esToken)
		sh.mu.Unlock()
		lm.connMu.Unlock()
		return "", 0, ErrLeaseExpired
	}

	// Slow path: waiter is pending
	var timer *time.Timer
	if timeout > 0 {
		timer = time.NewTimer(timeout)
	} else {
		timer = time.NewTimer(0)
	}
	defer timer.Stop()

	select {
	case token, ok := <-w.ch:
		if !ok || token == "" {
			lm.connMu.Lock()
			delete(lm.connEnqueued, eqKey)
			lm.connMu.Unlock()
			return "", 0, ErrWaiterClosed
		}
		lm.connMu.Lock()
		sh.mu.Lock()
		delete(lm.connEnqueued, eqKey)
		if st := sh.resources[key]; st != nil {
			if h, hOK := st.Holders[token]; hOK {
				h.leaseExpires = time.Now().Add(leaseTTL)
			}
			st.LastActivity = time.Now()
		}
		sh.mu.Unlock()
		lm.connMu.Unlock()
		return token, leaseSec, nil

	case <-ctx.Done():
		lm.connMu.Lock()
		sh.mu.Lock()
		delete(lm.connEnqueued, eqKey)
		select {
		case token, ok := <-w.ch:
			if ok && token != "" {
				if st := sh.resources[key]; st != nil {
					if h, hOK := st.Holders[token]; hOK {
						h.leaseExpires = time.Now().Add(leaseTTL)
					}
					st.LastActivity = time.Now()
				}
				sh.mu.Unlock()
				lm.connMu.Unlock()
				return token, leaseSec, nil
			}
		default:
		}
		if st := sh.resources[key]; st != nil {
			st.LastActivity = time.Now()
			removeWaiterFromState(st, w)
		}
		sh.mu.Unlock()
		lm.connMu.Unlock()
		return "", 0, ctx.Err()

	case <-timer.C:
		lm.connMu.Lock()
		sh.mu.Lock()
		delete(lm.connEnqueued, eqKey)
		// Race check: token may have arrived
		select {
		case token, ok := <-w.ch:
			if ok && token != "" {
				if st := sh.resources[key]; st != nil {
					if h, hOK := st.Holders[token]; hOK {
						h.leaseExpires = time.Now().Add(leaseTTL)
					}
					st.LastActivity = time.Now()
				}
				sh.mu.Unlock()
				lm.connMu.Unlock()
				return token, leaseSec, nil
			}
		default:
		}
		// Remove from queue
		if st := sh.resources[key]; st != nil {
			st.LastActivity = time.Now()
			removeWaiterFromState(st, w)
		}
		sh.mu.Unlock()
		lm.connMu.Unlock()
		return "", 0, nil
	}
}

// Release releases one held slot if the token matches (commands "r" and "sr").
func (lm *LockManager) Release(key, token string) bool {
	sh := lm.shardFor(key)

	lm.connMu.Lock()
	sh.mu.Lock()
	defer sh.mu.Unlock()
	defer lm.connMu.Unlock()

	st := sh.resources[key]
	if st == nil {
		return false
	}

	st.LastActivity = time.Now()

	h, ok := st.Holders[token]
	if !ok {
		return false
	}

	lm.connRemoveOwned(h.connID, key, token)
	eqKey := connKey{ConnID: h.connID, Key: key}
	if es, ok := lm.connEnqueued[eqKey]; ok && es.token == token {
		delete(lm.connEnqueued, eqKey)
	}
	delete(st.Holders, token)
	lm.grantNextWaiterLocked(key, st)
	return true
}

// Renew renews the lease if the token matches (commands "n" and "sn").
// Returns (remaining seconds, ok).
func (lm *LockManager) Renew(key, token string, leaseTTL time.Duration) (int, bool) {
	sh := lm.shardFor(key)

	lm.connMu.Lock()
	sh.mu.Lock()
	defer sh.mu.Unlock()
	defer lm.connMu.Unlock()

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
		lm.connRemoveOwned(h.connID, key, token)
		eqKey := connKey{ConnID: h.connID, Key: key}
		if es, ok := lm.connEnqueued[eqKey]; ok && es.token == token {
			delete(lm.connEnqueued, eqKey)
		}
		delete(st.Holders, token)
		st.LastActivity = now
		lm.grantNextWaiterLocked(key, st)
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
// Aggregate key count (across all types)
// ---------------------------------------------------------------------------

// totalKeyCount returns the total number of keys across all non-resource types
// (counters, kv, lists). Uses an atomic counter for race-free reads.
func (lm *LockManager) totalKeyCount() int {
	return int(lm.keyTotal.Load())
}

// ---------------------------------------------------------------------------
// Atomic Counters
// ---------------------------------------------------------------------------

func (lm *LockManager) Incr(key string, delta int64) (int64, error) {
	sh := lm.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	cs, ok := sh.counters[key]
	if !ok {
		if max := lm.cfg.MaxKeys; max > 0 && lm.totalKeyCount() >= max {
			return 0, ErrMaxKeys
		}
		cs = &counterState{LastActivity: time.Now()}
		sh.counters[key] = cs
		lm.keyTotal.Add(1)
	}
	cs.Value += delta
	cs.LastActivity = time.Now()
	return cs.Value, nil
}

func (lm *LockManager) Decr(key string, delta int64) (int64, error) {
	return lm.Incr(key, -delta)
}

func (lm *LockManager) GetCounter(key string) int64 {
	sh := lm.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	cs, ok := sh.counters[key]
	if !ok {
		return 0
	}
	cs.LastActivity = time.Now()
	return cs.Value
}

func (lm *LockManager) SetCounter(key string, value int64) error {
	sh := lm.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	cs, ok := sh.counters[key]
	if !ok {
		if max := lm.cfg.MaxKeys; max > 0 && lm.totalKeyCount() >= max {
			return ErrMaxKeys
		}
		cs = &counterState{}
		sh.counters[key] = cs
		lm.keyTotal.Add(1)
	}
	cs.Value = value
	cs.LastActivity = time.Now()
	return nil
}

// ---------------------------------------------------------------------------
// KV Store
// ---------------------------------------------------------------------------

func (lm *LockManager) KVSet(key, value string, ttlSeconds int) error {
	sh := lm.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	entry, ok := sh.kvStore[key]
	if !ok {
		if max := lm.cfg.MaxKeys; max > 0 && lm.totalKeyCount() >= max {
			return ErrMaxKeys
		}
		entry = &kvEntry{}
		sh.kvStore[key] = entry
		lm.keyTotal.Add(1)
	}
	entry.Value = value
	entry.LastActivity = time.Now()
	if ttlSeconds > 0 {
		entry.ExpiresAt = time.Now().Add(time.Duration(ttlSeconds) * time.Second)
	} else {
		entry.ExpiresAt = time.Time{}
	}
	return nil
}

func (lm *LockManager) KVGet(key string) (string, bool) {
	sh := lm.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	entry, ok := sh.kvStore[key]
	if !ok {
		return "", false
	}
	// Lazy expiry check
	if !entry.ExpiresAt.IsZero() && !time.Now().Before(entry.ExpiresAt) {
		delete(sh.kvStore, key)
		lm.keyTotal.Add(-1)
		return "", false
	}
	entry.LastActivity = time.Now()
	return entry.Value, true
}

func (lm *LockManager) KVDel(key string) {
	sh := lm.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if _, ok := sh.kvStore[key]; ok {
		delete(sh.kvStore, key)
		lm.keyTotal.Add(-1)
	}
}

// ---------------------------------------------------------------------------
// Lists/Queues
// ---------------------------------------------------------------------------

func (lm *LockManager) LPush(key, value string) (int, error) {
	sh := lm.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	ls, ok := sh.lists[key]
	if !ok {
		if max := lm.cfg.MaxKeys; max > 0 && lm.totalKeyCount() >= max {
			return 0, ErrMaxKeys
		}
		ls = &listState{LastActivity: time.Now()}
		sh.lists[key] = ls
		lm.keyTotal.Add(1)
	}
	if max := lm.cfg.MaxListLength; max > 0 && len(ls.Items) >= max {
		return 0, ErrListFull
	}
	ls.Items = append([]string{value}, ls.Items...)
	ls.LastActivity = time.Now()
	return len(ls.Items), nil
}

func (lm *LockManager) RPush(key, value string) (int, error) {
	sh := lm.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	ls, ok := sh.lists[key]
	if !ok {
		if max := lm.cfg.MaxKeys; max > 0 && lm.totalKeyCount() >= max {
			return 0, ErrMaxKeys
		}
		ls = &listState{LastActivity: time.Now()}
		sh.lists[key] = ls
		lm.keyTotal.Add(1)
	}
	if max := lm.cfg.MaxListLength; max > 0 && len(ls.Items) >= max {
		return 0, ErrListFull
	}
	ls.Items = append(ls.Items, value)
	ls.LastActivity = time.Now()
	return len(ls.Items), nil
}

func (lm *LockManager) LPop(key string) (string, bool) {
	sh := lm.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	ls, ok := sh.lists[key]
	if !ok || len(ls.Items) == 0 {
		return "", false
	}
	val := ls.Items[0]
	ls.Items[0] = "" // allow GC of the string
	ls.Items = ls.Items[1:]
	ls.LastActivity = time.Now()
	if len(ls.Items) == 0 {
		delete(sh.lists, key)
		lm.keyTotal.Add(-1)
	}
	return val, true
}

func (lm *LockManager) RPop(key string) (string, bool) {
	sh := lm.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	ls, ok := sh.lists[key]
	if !ok || len(ls.Items) == 0 {
		return "", false
	}
	last := len(ls.Items) - 1
	val := ls.Items[last]
	ls.Items[last] = "" // allow GC of the string
	ls.Items = ls.Items[:last]
	ls.LastActivity = time.Now()
	if len(ls.Items) == 0 {
		delete(sh.lists, key)
		lm.keyTotal.Add(-1)
	}
	return val, true
}

func (lm *LockManager) LLen(key string) int {
	sh := lm.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	ls, ok := sh.lists[key]
	if !ok {
		return 0
	}
	ls.LastActivity = time.Now()
	return len(ls.Items)
}

func (lm *LockManager) LRange(key string, start, stop int) []string {
	sh := lm.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	ls, ok := sh.lists[key]
	if !ok || len(ls.Items) == 0 {
		return []string{}
	}

	n := len(ls.Items)
	// Normalize negative indices
	if start < 0 {
		start = n + start
	}
	if stop < 0 {
		stop = n + stop
	}
	// Clamp
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop {
		return []string{}
	}

	ls.LastActivity = time.Now()
	result := make([]string, stop-start+1)
	copy(result, ls.Items[start:stop+1])
	return result
}

// ---------------------------------------------------------------------------
// Connection cleanup
// ---------------------------------------------------------------------------

// CleanupConnection cleans up all state for a disconnected connection.
func (lm *LockManager) CleanupConnection(connID uint64) {
	if !lm.cfg.AutoReleaseOnDisconnect {
		return
	}

	lm.connMu.Lock()

	// Snapshot enqueued keys and owned keys+tokens for this connection.
	var enqueuedKeys []string
	for ck := range lm.connEnqueued {
		if ck.ConnID == connID {
			enqueuedKeys = append(enqueuedKeys, ck.Key)
		}
	}

	type ownedEntry struct {
		key    string
		tokens map[string]struct{}
	}
	var ownedEntries []ownedEntry
	if owned, ok := lm.connOwned[connID]; ok {
		for key, tokens := range owned {
			// Copy the token set
			cp := make(map[string]struct{}, len(tokens))
			for t := range tokens {
				cp[t] = struct{}{}
			}
			ownedEntries = append(ownedEntries, ownedEntry{key: key, tokens: cp})
		}
	}

	closed := make(map[chan string]struct{})

	// Clean up two-phase enqueued state — group by shard to avoid lock juggling.
	for _, key := range enqueuedKeys {
		sh := lm.shardFor(key)
		sh.mu.Lock()
		ck := connKey{ConnID: connID, Key: key}
		es := lm.connEnqueued[ck]
		delete(lm.connEnqueued, ck)
		if es != nil && es.waiter != nil {
			if _, already := closed[es.waiter.ch]; !already {
				close(es.waiter.ch)
				closed[es.waiter.ch] = struct{}{}
			}
			if st := sh.resources[key]; st != nil {
				removeWaiterFromState(st, es.waiter)
			}
		}
		sh.mu.Unlock()
	}

	// Cancel pending waiters from single-phase acquire path.
	// We must iterate all shards since we don't track which shards
	// have waiters for a given connID.
	for i := range lm.shards {
		sh := &lm.shards[i]
		sh.mu.Lock()
		for _, st := range sh.resources {
			removeWaitersByConn(st, connID, closed)
		}
		sh.mu.Unlock()
	}

	// Release owned slots.
	for _, entry := range ownedEntries {
		sh := lm.shardFor(entry.key)
		sh.mu.Lock()
		st := sh.resources[entry.key]
		if st != nil {
			for token := range entry.tokens {
				h, ok := st.Holders[token]
				if !ok {
					continue
				}
				if h.connID != connID {
					continue
				}
				lm.log.Warn("disconnect cleanup: releasing",
					"key", entry.key, "conn_id", connID)
				delete(st.Holders, token)
			}
			st.LastActivity = time.Now()
			lm.grantNextWaiterLocked(entry.key, st)
		}
		sh.mu.Unlock()
	}
	delete(lm.connOwned, connID)

	lm.connMu.Unlock()
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
				lm.connMu.Lock()
				sh.mu.Lock()
				for key, st := range sh.resources {
					var expired []string
					for token, h := range st.Holders {
						if h.leaseExpires.IsZero() {
							continue
						}
						if !now.Before(h.leaseExpires) {
							lm.log.Warn("lease expired",
								"key", key, "conn", h.connID)
							lm.connRemoveOwned(h.connID, key, token)
							eqKey := connKey{ConnID: h.connID, Key: key}
							if es, ok := lm.connEnqueued[eqKey]; ok && es.token == token {
								delete(lm.connEnqueued, eqKey)
							}
							expired = append(expired, token)
						}
					}
					for _, token := range expired {
						delete(st.Holders, token)
					}
					if len(expired) > 0 {
						st.LastActivity = now
						lm.grantNextWaiterLocked(key, st)
					}
				}
				// Sweep expired KV entries
				for key, entry := range sh.kvStore {
					if !entry.ExpiresAt.IsZero() && !now.Before(entry.ExpiresAt) {
						delete(sh.kvStore, key)
						lm.keyTotal.Add(-1)
					}
				}
				sh.mu.Unlock()
				lm.connMu.Unlock()
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
				for _, key := range expired {
					lm.log.Debug("GC: pruning unused state", "key", key)
					delete(sh.resources, key)
					lm.resourceTotal.Add(-1)
				}
				// GC idle counters (only when value == 0)
				var expiredCounters []string
				for key, cs := range sh.counters {
					if cs.Value == 0 && now.Sub(cs.LastActivity) > lm.cfg.GCMaxIdleTime {
						expiredCounters = append(expiredCounters, key)
					}
				}
				for _, key := range expiredCounters {
					lm.log.Debug("GC: pruning idle counter", "key", key)
					delete(sh.counters, key)
					lm.keyTotal.Add(-1)
				}
				// GC idle KV entries (no TTL, just idle)
				var expiredKV []string
				for key, entry := range sh.kvStore {
					if entry.ExpiresAt.IsZero() && now.Sub(entry.LastActivity) > lm.cfg.GCMaxIdleTime {
						expiredKV = append(expiredKV, key)
					}
				}
				for _, key := range expiredKV {
					lm.log.Debug("GC: pruning idle KV entry", "key", key)
					delete(sh.kvStore, key)
					lm.keyTotal.Add(-1)
				}
				// GC idle empty lists
				var expiredLists []string
				for key, ls := range sh.lists {
					if len(ls.Items) == 0 && now.Sub(ls.LastActivity) > lm.cfg.GCMaxIdleTime {
						expiredLists = append(expiredLists, key)
					}
				}
				for _, key := range expiredLists {
					lm.log.Debug("GC: pruning idle list", "key", key)
					delete(sh.lists, key)
					lm.keyTotal.Add(-1)
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

type CounterInfo struct {
	Key   string  `json:"key"`
	Value int64   `json:"value"`
	IdleS float64 `json:"idle_s"`
}

type KVInfo struct {
	Key      string  `json:"key"`
	ValueLen int     `json:"value_len"`
	TTL      float64 `json:"ttl"`
	IdleS    float64 `json:"idle_s"`
}

type ListInfo struct {
	Key   string  `json:"key"`
	Len   int     `json:"len"`
	IdleS float64 `json:"idle_s"`
}

type SignalChannelInfo struct {
	Pattern   string `json:"pattern"`
	Group     string `json:"group,omitempty"`
	Listeners int    `json:"listeners"`
}

type Stats struct {
	Connections    int64              `json:"connections"`
	Locks          []LockInfo         `json:"locks"`
	Semaphores     []SemInfo          `json:"semaphores"`
	IdleLocks      []IdleInfo         `json:"idle_locks"`
	IdleSemaphores []IdleInfo         `json:"idle_semaphores"`
	Counters       []CounterInfo      `json:"counters"`
	KVEntries      []KVInfo           `json:"kv_entries"`
	Lists          []ListInfo         `json:"lists"`
	SignalChannels []SignalChannelInfo `json:"signal_channels"`
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
		Counters:       []CounterInfo{},
		KVEntries:      []KVInfo{},
		Lists:          []ListInfo{},
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
		for key, cs := range sh.counters {
			s.Counters = append(s.Counters, CounterInfo{
				Key:   key,
				Value: cs.Value,
				IdleS: now.Sub(cs.LastActivity).Seconds(),
			})
		}
		for key, entry := range sh.kvStore {
			// Skip expired entries (consistent with KVGet lazy-deletion)
			if !entry.ExpiresAt.IsZero() && !now.Before(entry.ExpiresAt) {
				continue
			}
			ttl := 0.0
			if !entry.ExpiresAt.IsZero() {
				ttl = entry.ExpiresAt.Sub(now).Seconds()
			}
			s.KVEntries = append(s.KVEntries, KVInfo{
				Key:      key,
				ValueLen: len(entry.Value),
				TTL:      ttl,
				IdleS:    now.Sub(entry.LastActivity).Seconds(),
			})
		}
		for key, ls := range sh.lists {
			s.Lists = append(s.Lists, ListInfo{
				Key:   key,
				Len:   len(ls.Items),
				IdleS: now.Sub(ls.LastActivity).Seconds(),
			})
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
func (lm *LockManager) ConnEnqueuedForTest(ck connKey) *enqueuedState {
	return lm.connEnqueued[ck]
}

// ConnOwnedForTest returns the owned map for a connID (for testing only).
func (lm *LockManager) ConnOwnedForTest(connID uint64) map[string]map[string]struct{} {
	return lm.connOwned[connID]
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
	lm.connMu.Lock()
	defer lm.connMu.Unlock()
	return len(lm.connEnqueued)
}

// ConnOwnedCountForTest returns the number of connOwned entries (for testing only).
func (lm *LockManager) ConnOwnedCountForTest() int {
	lm.connMu.Lock()
	defer lm.connMu.Unlock()
	return len(lm.connOwned)
}

// LockConnMuForTest locks connMu (for testing only).
func (lm *LockManager) LockConnMuForTest() { lm.connMu.Lock() }

// UnlockConnMuForTest unlocks connMu (for testing only).
func (lm *LockManager) UnlockConnMuForTest() { lm.connMu.Unlock() }

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
	lm.connMu.Lock()
	defer lm.connMu.Unlock()
	for i := range lm.shards {
		lm.shards[i].mu.Lock()
		lm.shards[i].resources = make(map[string]*ResourceState)
		lm.shards[i].counters = make(map[string]*counterState)
		lm.shards[i].kvStore = make(map[string]*kvEntry)
		lm.shards[i].lists = make(map[string]*listState)
		lm.shards[i].mu.Unlock()
	}
	lm.connOwned = make(map[uint64]map[string]map[string]struct{})
	lm.connEnqueued = make(map[connKey]*enqueuedState)
	lm.resourceTotal.Store(0)
	lm.keyTotal.Store(0)
}
