package lock

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
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
	ErrCASConflict          = errors.New("cas conflict")
	ErrBarrierCountMismatch = errors.New("barrier count mismatch")
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
	mode     byte // 0=regular, 'r'=read, 'w'=write
}

type holder struct {
	connID       uint64
	leaseExpires time.Time
	fence        uint64
	mode         byte // 0=regular, 'r'=read, 'w'=write
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
	Items         []string
	PopWaiters    []*listWaiter
	PopWaiterHead int
	LastActivity  time.Time
}

type barrierParticipant struct {
	ch     chan struct{} // closed when barrier trips
	connID uint64
}

type barrierState struct {
	Count        int
	Participants []*barrierParticipant
	Tripped      bool
	Cancelled    chan struct{} // closed when barrier becomes unreachable (participant left)
	LastActivity time.Time
}

type leaderWatcher struct {
	ch         chan []byte
	cancelConn func() // called when ch is full (slow consumer)
	observer   bool   // true if registered via observe (not elect)
}

type shard struct {
	mu             sync.Mutex
	resources      map[string]*ResourceState
	counters       map[string]*counterState
	kvStore        map[string]*kvEntry
	lists          map[string]*listState
	barriers       map[string]*barrierState
	leaderWatchers map[string]map[uint64]*leaderWatcher // key → connID → watcher
}

type listWaiter struct {
	ch     chan string // receives the popped value
	connID uint64
}

type LockManager struct {
	shards             [numShards]shard
	connMu             sync.Mutex // protects connOwned, connEnqueued, connLeaderKeys
	connOwned          map[uint64]map[string]map[string]struct{} // connID → key → set of tokens
	connEnqueued       map[connKey]*enqueuedState
	connLeaderKeys     map[uint64]map[string]struct{} // connID → set of leader-watched keys
	cfg                *config.Config
	log                *slog.Logger
	tokBuf             tokenBuf
	resourceTotal      atomic.Int64  // total resources across all shards
	keyTotal           atomic.Int64  // total keys across all types (counters+kv+lists)
	fenceSeq           atomic.Uint64 // global monotonic fencing token counter
	OnLockRelease      func(key string) // optional: called when a lock is released by expiry or disconnect cleanup
}

func NewLockManager(cfg *config.Config, log *slog.Logger) *LockManager {
	lm := &LockManager{
		connOwned:      make(map[uint64]map[string]map[string]struct{}),
		connEnqueued:   make(map[connKey]*enqueuedState),
		connLeaderKeys: make(map[uint64]map[string]struct{}),
		cfg:            cfg,
		log:            log,
		tokBuf:         newTokenBuf(),
	}
	for i := range lm.shards {
		lm.shards[i].resources = make(map[string]*ResourceState)
		lm.shards[i].counters = make(map[string]*counterState)
		lm.shards[i].kvStore = make(map[string]*kvEntry)
		lm.shards[i].lists = make(map[string]*listState)
		lm.shards[i].barriers = make(map[string]*barrierState)
		lm.shards[i].leaderWatchers = make(map[string]map[uint64]*leaderWatcher)
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
	sh := lm.shardFor(key)
	for st.WaiterHead < len(st.Waiters) && len(st.Holders) < st.Limit {
		w := st.Waiters[st.WaiterHead]
		st.Waiters[st.WaiterHead] = nil // avoid memory leak
		st.WaiterHead++
		token := lm.newToken()
		select {
		case w.ch <- token:
			fence := lm.fenceSeq.Add(1)
			st.Holders[token] = &holder{
				connID:       w.connID,
				leaseExpires: time.Now().Add(w.leaseTTL),
				fence:        fence,
			}
			st.LastActivity = time.Now()
			lm.connAddOwned(w.connID, key, token)
			// Notify leader watchers on failover grant (only for mutex locks, not semaphores)
			if st.Limit == 1 {
				if watchers, ok := sh.leaderWatchers[key]; ok && len(watchers) > 0 {
					msg := []byte(fmt.Sprintf("leader failover %s\n", key))
					for _, lw := range watchers {
						select {
						case lw.ch <- msg:
						default:
							lw.cancelConn()
						}
					}
				}
			}
		default:
			// Channel closed or full — skip this waiter
			continue
		}
	}
	st.compactWaiters()
}

// grantNextLocked dispatches to the appropriate granter based on resource type.
// Must be called with shard lock AND connMu held.
func (lm *LockManager) grantNextLocked(key string, st *ResourceState) {
	if st.Limit == -1 {
		lm.grantNextRWWaiterLocked(key, st)
	} else {
		lm.grantNextWaiterLocked(key, st)
	}
}

// evictExpiredLocked evicts any holders whose leases have expired and grants
// freed slots to waiting callers. Returns true if any holders were evicted.
// Must be called with the shard lock AND connMu held.
func (lm *LockManager) evictExpiredLocked(key string, st *ResourceState) bool {
	sh := lm.shardFor(key)
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
			// Clean up leader watcher for the expired holder
			// (preserves observer registrations).
			lm.removeLeaderHolderWatcher(sh, key, h.connID)
			expired = append(expired, token)
		}
	}
	for _, token := range expired {
		delete(st.Holders, token)
	}
	if len(expired) > 0 {
		st.LastActivity = now
		lm.grantNextLocked(key, st)
	}
	return len(expired) > 0
}

// resourceCount returns total number of resources across all shards.
func (lm *LockManager) resourceCount() int {
	return int(lm.resourceTotal.Load())
}

func (lm *LockManager) getOrCreateLocked(sh *shard, key string, limit int) (*ResourceState, error) {
	st, ok := sh.resources[key]
	if ok {
		// RW lock (limit -1) vs regular lock/semaphore mismatch
		if (st.Limit == -1) != (limit == -1) {
			return nil, ErrTypeMismatch
		}
		if limit != -1 && st.Limit != limit {
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

// AcquireWithFence is the single-phase acquire that also returns a fencing token.
func (lm *LockManager) AcquireWithFence(ctx context.Context, key string, timeout, leaseTTL time.Duration, connID uint64, limit int) (string, uint64, error) {
	sh := lm.shardFor(key)

	lm.connMu.Lock()
	sh.mu.Lock()
	st, err := lm.getOrCreateLocked(sh, key, limit)
	if err != nil {
		sh.mu.Unlock()
		lm.connMu.Unlock()
		return "", 0, err
	}

	st.LastActivity = time.Now()

	// Opportunistic expired-lease eviction (avoids waiting for sweep tick)
	evicted := lm.evictExpiredLocked(key, st)
	notifyRelease := func() {
		if evicted && lm.OnLockRelease != nil {
			lm.OnLockRelease(key)
		}
	}

	// Fast path: capacity available and no waiters — no waiter allocation needed
	if len(st.Holders) < st.Limit && st.waiterCount() == 0 {
		token := lm.newToken()
		fence := lm.fenceSeq.Add(1)
		st.Holders[token] = &holder{
			connID:       connID,
			leaseExpires: time.Now().Add(leaseTTL),
			fence:        fence,
		}
		st.LastActivity = time.Now()
		lm.connAddOwned(connID, key, token)
		sh.mu.Unlock()
		lm.connMu.Unlock()
		notifyRelease()
		return token, fence, nil
	}

	// Slow path: allocate waiter and enqueue
	if max := lm.cfg.MaxWaiters; max > 0 && st.waiterCount() >= max {
		sh.mu.Unlock()
		lm.connMu.Unlock()
		notifyRelease()
		return "", 0, ErrMaxWaiters
	}
	w := &waiter{
		ch:       make(chan string, 1),
		connID:   connID,
		leaseTTL: leaseTTL,
	}
	st.Waiters = append(st.Waiters, w)
	sh.mu.Unlock()
	lm.connMu.Unlock()
	notifyRelease()

	// timeout == 0 means fire immediately (non-blocking try-acquire).
	var timerCh <-chan time.Time
	var timer *time.Timer
	if timeout > 0 {
		timer = time.NewTimer(timeout)
		timerCh = timer.C
		defer timer.Stop()
	} else {
		// Zero timeout: fire immediately for non-blocking acquire.
		timer = time.NewTimer(0)
		timerCh = timer.C
		defer timer.Stop()
	}

	// verifyAndResetLease checks that the token is still held and resets
	// the lease expiry. Returns (fence, true) if held, (0, false) if evicted.
	// Must be called with sh.mu held.
	verifyHolder := func(token string) (uint64, bool) {
		if s := sh.resources[key]; s != nil {
			s.LastActivity = time.Now()
			if h, ok := s.Holders[token]; ok {
				h.leaseExpires = time.Now().Add(leaseTTL)
				return h.fence, true
			}
		}
		return 0, false
	}

	select {
	case token, ok := <-w.ch:
		if !ok || token == "" {
			return "", 0, ErrWaiterClosed
		}
		lm.connMu.Lock()
		sh.mu.Lock()
		fence, held := verifyHolder(token)
		if !held {
			lm.connRemoveOwned(connID, key, token)
			sh.mu.Unlock()
			lm.connMu.Unlock()
			return "", 0, ErrLeaseExpired
		}
		sh.mu.Unlock()
		lm.connMu.Unlock()
		return token, fence, nil

	case <-ctx.Done():
		lm.connMu.Lock()
		sh.mu.Lock()
		// Race check: token may have arrived between ctx cancellation
		// and acquiring the mutex.
		select {
		case token, ok := <-w.ch:
			if ok && token != "" {
				fence, held := verifyHolder(token)
				if !held {
					lm.connRemoveOwned(connID, key, token)
					sh.mu.Unlock()
					lm.connMu.Unlock()
					return "", 0, ErrLeaseExpired
				}
				sh.mu.Unlock()
				lm.connMu.Unlock()
				return token, fence, nil
			}
		default:
		}
		if s := sh.resources[key]; s != nil {
			s.LastActivity = time.Now()
			removeWaiterFromState(s, w)
		}
		sh.mu.Unlock()
		lm.connMu.Unlock()
		return "", 0, ctx.Err()

	case <-timerCh:
		// Timeout — remove from queue
		lm.connMu.Lock()
		sh.mu.Lock()
		// Race check: token may have arrived
		select {
		case token, ok := <-w.ch:
			if ok && token != "" {
				fence, held := verifyHolder(token)
				if !held {
					lm.connRemoveOwned(connID, key, token)
					sh.mu.Unlock()
					lm.connMu.Unlock()
					return "", 0, ErrLeaseExpired
				}
				sh.mu.Unlock()
				lm.connMu.Unlock()
				return token, fence, nil
			}
		default:
		}
		if s := sh.resources[key]; s != nil {
			s.LastActivity = time.Now()
			removeWaiterFromState(s, w)
		}
		sh.mu.Unlock()
		lm.connMu.Unlock()
		return "", 0, nil
	}
}

// Acquire is the single-phase acquire (commands "l" and "sl").
func (lm *LockManager) Acquire(ctx context.Context, key string, timeout, leaseTTL time.Duration, connID uint64, limit int) (string, error) {
	token, _, err := lm.AcquireWithFence(ctx, key, timeout, leaseTTL, connID, limit)
	return token, err
}

// EnqueueWithFence is phase 1 of two-phase acquire that also returns a fencing token.
// Returns (status, token, leaseTTLSec, fence, err).
func (lm *LockManager) EnqueueWithFence(key string, leaseTTL time.Duration, connID uint64, limit int) (string, string, int, uint64, error) {
	eqKey := connKey{ConnID: connID, Key: key}
	sh := lm.shardFor(key)

	lm.connMu.Lock()
	sh.mu.Lock()

	if _, exists := lm.connEnqueued[eqKey]; exists {
		sh.mu.Unlock()
		lm.connMu.Unlock()
		return "", "", 0, 0, ErrAlreadyEnqueued
	}

	st, err := lm.getOrCreateLocked(sh, key, limit)
	if err != nil {
		sh.mu.Unlock()
		lm.connMu.Unlock()
		return "", "", 0, 0, err
	}

	st.LastActivity = time.Now()
	leaseSec := int(leaseTTL / time.Second)

	// Opportunistic expired-lease eviction (avoids waiting for sweep tick)
	evicted := lm.evictExpiredLocked(key, st)
	notifyRelease := func() {
		if evicted && lm.OnLockRelease != nil {
			lm.OnLockRelease(key)
		}
	}

	// Fast path: capacity available and no waiters
	if len(st.Holders) < st.Limit && st.waiterCount() == 0 {
		token := lm.newToken()
		fence := lm.fenceSeq.Add(1)
		st.Holders[token] = &holder{
			connID:       connID,
			leaseExpires: time.Now().Add(leaseTTL),
			fence:        fence,
		}
		st.LastActivity = time.Now()
		lm.connAddOwned(connID, key, token)
		lm.connEnqueued[eqKey] = &enqueuedState{token: token, leaseTTL: leaseTTL}
		sh.mu.Unlock()
		lm.connMu.Unlock()
		notifyRelease()
		return "acquired", token, leaseSec, fence, nil
	}

	// Slow path: create waiter and enqueue
	if max := lm.cfg.MaxWaiters; max > 0 && st.waiterCount() >= max {
		sh.mu.Unlock()
		lm.connMu.Unlock()
		notifyRelease()
		return "", "", 0, 0, ErrMaxWaiters
	}
	w := &waiter{
		ch:       make(chan string, 1),
		connID:   connID,
		leaseTTL: leaseTTL,
	}
	st.Waiters = append(st.Waiters, w)
	lm.connEnqueued[eqKey] = &enqueuedState{waiter: w, leaseTTL: leaseTTL}
	sh.mu.Unlock()
	lm.connMu.Unlock()
	notifyRelease()
	return "queued", "", 0, 0, nil
}

// Enqueue is phase 1 of two-phase acquire (commands "e" and "se").
// Returns (status, token, leaseTTLSec, err).
func (lm *LockManager) Enqueue(key string, leaseTTL time.Duration, connID uint64, limit int) (string, string, int, error) {
	status, token, lease, _, err := lm.EnqueueWithFence(key, leaseTTL, connID, limit)
	return status, token, lease, err
}

// WaitWithFence is phase 2 of two-phase acquire that also returns a fencing token.
// Returns (token, leaseTTLSec, fence, err). Empty token means timeout.
func (lm *LockManager) WaitWithFence(ctx context.Context, key string, timeout time.Duration, connID uint64) (string, int, uint64, error) {
	eqKey := connKey{ConnID: connID, Key: key}
	sh := lm.shardFor(key)

	lm.connMu.Lock()
	es, ok := lm.connEnqueued[eqKey]
	if !ok {
		lm.connMu.Unlock()
		return "", 0, 0, ErrNotEnqueued
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
					lm.grantNextLocked(key, st)
					sh.mu.Unlock()
					lm.connMu.Unlock()
					return "", 0, 0, ErrLeaseExpired
				}
				// Reset lease
				h.leaseExpires = time.Now().Add(leaseTTL)
				st.LastActivity = time.Now()
				fence := h.fence
				sh.mu.Unlock()
				lm.connMu.Unlock()
				return esToken, leaseSec, fence, nil
			}
		}
		// Slot was lost (expired and granted to another, or state GC'd)
		lm.connRemoveOwned(connID, key, esToken)
		sh.mu.Unlock()
		lm.connMu.Unlock()
		return "", 0, 0, ErrLeaseExpired
	}

	// Slow path: waiter is pending.
	// timeout == 0 means fire immediately (non-blocking wait).
	var timerCh <-chan time.Time
	var timer *time.Timer
	if timeout > 0 {
		timer = time.NewTimer(timeout)
		timerCh = timer.C
	} else {
		timer = time.NewTimer(0)
		timerCh = timer.C
	}
	defer timer.Stop()

	// verifyAndResetLease checks that the token is still held and resets
	// the lease expiry. Returns (fence, true) if held, (0, false) if evicted.
	// Must be called with sh.mu held.
	verifyAndResetLease := func(token string) (uint64, bool) {
		if st := sh.resources[key]; st != nil {
			st.LastActivity = time.Now()
			if h, ok := st.Holders[token]; ok {
				h.leaseExpires = time.Now().Add(leaseTTL)
				return h.fence, true
			}
		}
		return 0, false
	}

	select {
	case token, ok := <-w.ch:
		if !ok || token == "" {
			lm.connMu.Lock()
			delete(lm.connEnqueued, eqKey)
			lm.connMu.Unlock()
			return "", 0, 0, ErrWaiterClosed
		}
		lm.connMu.Lock()
		sh.mu.Lock()
		delete(lm.connEnqueued, eqKey)
		fence, held := verifyAndResetLease(token)
		sh.mu.Unlock()
		if !held {
			lm.connRemoveOwned(connID, key, token)
			lm.connMu.Unlock()
			return "", 0, 0, ErrLeaseExpired
		}
		lm.connMu.Unlock()
		return token, leaseSec, fence, nil

	case <-ctx.Done():
		lm.connMu.Lock()
		sh.mu.Lock()
		delete(lm.connEnqueued, eqKey)
		select {
		case token, ok := <-w.ch:
			if ok && token != "" {
				fence, held := verifyAndResetLease(token)
				sh.mu.Unlock()
				if !held {
					lm.connRemoveOwned(connID, key, token)
					lm.connMu.Unlock()
					return "", 0, 0, ErrLeaseExpired
				}
				lm.connMu.Unlock()
				return token, leaseSec, fence, nil
			}
		default:
		}
		if st := sh.resources[key]; st != nil {
			st.LastActivity = time.Now()
			removeWaiterFromState(st, w)
			lm.grantNextLocked(key, st)
		}
		sh.mu.Unlock()
		lm.connMu.Unlock()
		return "", 0, 0, ctx.Err()

	case <-timerCh:
		lm.connMu.Lock()
		sh.mu.Lock()
		delete(lm.connEnqueued, eqKey)
		// Race check: token may have arrived
		select {
		case token, ok := <-w.ch:
			if ok && token != "" {
				fence, held := verifyAndResetLease(token)
				sh.mu.Unlock()
				if !held {
					lm.connRemoveOwned(connID, key, token)
					lm.connMu.Unlock()
					return "", 0, 0, ErrLeaseExpired
				}
				lm.connMu.Unlock()
				return token, leaseSec, fence, nil
			}
		default:
		}
		// Remove from queue and try to grant next waiter (e.g. readers
		// queued behind a removed writer).
		if st := sh.resources[key]; st != nil {
			st.LastActivity = time.Now()
			removeWaiterFromState(st, w)
			lm.grantNextLocked(key, st)
		}
		sh.mu.Unlock()
		lm.connMu.Unlock()
		return "", 0, 0, nil
	}
}

// Wait is phase 2 of two-phase acquire (commands "w" and "sw").
// Returns (token, leaseTTLSec, err). Empty token means timeout.
func (lm *LockManager) Wait(ctx context.Context, key string, timeout time.Duration, connID uint64) (string, int, error) {
	token, lease, _, err := lm.WaitWithFence(ctx, key, timeout, connID)
	return token, lease, err
}

// Release releases one held slot if the token matches (commands "r" and "sr").
func (lm *LockManager) Release(key, token string) bool {
	sh := lm.shardFor(key)

	lm.connMu.Lock()
	sh.mu.Lock()

	st := sh.resources[key]
	if st == nil {
		sh.mu.Unlock()
		lm.connMu.Unlock()
		return false
	}

	st.LastActivity = time.Now()

	h, ok := st.Holders[token]
	if !ok {
		sh.mu.Unlock()
		lm.connMu.Unlock()
		return false
	}

	lm.connRemoveOwned(h.connID, key, token)
	eqKey := connKey{ConnID: h.connID, Key: key}
	if es, ok := lm.connEnqueued[eqKey]; ok && es.token == token {
		delete(lm.connEnqueued, eqKey)
	}
	delete(st.Holders, token)
	lm.grantNextLocked(key, st)

	sh.mu.Unlock()
	lm.connMu.Unlock()
	return true
}

// RenewWithFence renews the lease and returns the existing fence (no increment).
// Returns (remaining seconds, fence, ok).
func (lm *LockManager) RenewWithFence(key, token string, leaseTTL time.Duration) (int, uint64, bool) {
	sh := lm.shardFor(key)

	lm.connMu.Lock()
	sh.mu.Lock()

	st := sh.resources[key]
	if st == nil {
		sh.mu.Unlock()
		lm.connMu.Unlock()
		return 0, 0, false
	}

	now := time.Now()
	st.LastActivity = now

	h, ok := st.Holders[token]
	if !ok {
		sh.mu.Unlock()
		lm.connMu.Unlock()
		return 0, 0, false
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
		// Clean up leader watcher for the expired holder
		// (preserves observer registrations).
		lm.removeLeaderHolderWatcher(sh, key, h.connID)
		delete(st.Holders, token)
		st.LastActivity = now
		lm.grantNextLocked(key, st)
		sh.mu.Unlock()
		lm.connMu.Unlock()
		// Notify watchers of implicit release (outside locks)
		if lm.OnLockRelease != nil {
			lm.OnLockRelease(key)
		}
		return 0, 0, false
	}

	// Reset lease
	h.leaseExpires = now.Add(leaseTTL)
	st.LastActivity = now

	remaining := int(leaseTTL.Seconds())
	if remaining < 0 {
		remaining = 0
	}
	fence := h.fence
	sh.mu.Unlock()
	lm.connMu.Unlock()
	return remaining, fence, true
}

// Renew renews the lease if the token matches (commands "n" and "sn").
// Returns (remaining seconds, ok).
func (lm *LockManager) Renew(key, token string, leaseTTL time.Duration) (int, bool) {
	sh := lm.shardFor(key)

	lm.connMu.Lock()
	sh.mu.Lock()

	st := sh.resources[key]
	if st == nil {
		sh.mu.Unlock()
		lm.connMu.Unlock()
		return 0, false
	}

	now := time.Now()
	st.LastActivity = now

	h, ok := st.Holders[token]
	if !ok {
		sh.mu.Unlock()
		lm.connMu.Unlock()
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
		// Clean up leader watcher for the expired holder
		// (preserves observer registrations).
		lm.removeLeaderHolderWatcher(sh, key, h.connID)
		delete(st.Holders, token)
		st.LastActivity = now
		lm.grantNextLocked(key, st)
		sh.mu.Unlock()
		lm.connMu.Unlock()
		// Notify watchers of implicit release (outside locks)
		if lm.OnLockRelease != nil {
			lm.OnLockRelease(key)
		}
		return 0, false
	}

	// Reset lease
	h.leaseExpires = now.Add(leaseTTL)
	st.LastActivity = now

	remaining := int(leaseTTL.Seconds())
	if remaining < 0 {
		remaining = 0
	}
	sh.mu.Unlock()
	lm.connMu.Unlock()
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

// KVCAS performs an atomic compare-and-swap on a KV entry.
// If oldValue == "" and the key does not exist, it creates the key.
// Returns (true, nil) on success, (false, nil) on value mismatch.
func (lm *LockManager) KVCAS(key, oldValue, newValue string, ttlSeconds int) (bool, error) {
	sh := lm.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	entry, ok := sh.kvStore[key]
	if ok {
		// Lazy expiry check
		if !entry.ExpiresAt.IsZero() && !time.Now().Before(entry.ExpiresAt) {
			delete(sh.kvStore, key)
			lm.keyTotal.Add(-1)
			ok = false
			entry = nil
		}
	}

	if !ok {
		// Key doesn't exist — only succeed if oldValue is empty
		if oldValue != "" {
			return false, nil
		}
		if max := lm.cfg.MaxKeys; max > 0 && lm.totalKeyCount() >= max {
			return false, ErrMaxKeys
		}
		entry = &kvEntry{}
		sh.kvStore[key] = entry
		lm.keyTotal.Add(1)
	} else {
		// Key exists — compare current value
		if entry.Value != oldValue {
			return false, nil
		}
	}

	entry.Value = newValue
	entry.LastActivity = time.Now()
	if ttlSeconds > 0 {
		entry.ExpiresAt = time.Now().Add(time.Duration(ttlSeconds) * time.Second)
	} else {
		entry.ExpiresAt = time.Time{}
	}
	return true, nil
}

// ---------------------------------------------------------------------------
// Lists/Queues
// ---------------------------------------------------------------------------

// compactPopWaiters reclaims consumed pop waiter slots.
func compactPopWaiters(ls *listState) {
	if ls.PopWaiterHead > len(ls.PopWaiters)/2 {
		n := copy(ls.PopWaiters, ls.PopWaiters[ls.PopWaiterHead:])
		for i := n; i < len(ls.PopWaiters); i++ {
			ls.PopWaiters[i] = nil
		}
		ls.PopWaiters = ls.PopWaiters[:n]
		ls.PopWaiterHead = 0
	}
}

// tryHandoffToPopWaiter tries to hand a value directly to a waiting pop consumer.
// Returns true if the value was handed off (caller should NOT add to list).
// Must be called with the shard lock held.
func tryHandoffToPopWaiter(ls *listState, value string) bool {
	for ls.PopWaiterHead < len(ls.PopWaiters) {
		w := ls.PopWaiters[ls.PopWaiterHead]
		ls.PopWaiters[ls.PopWaiterHead] = nil
		ls.PopWaiterHead++
		if w == nil {
			continue
		}
		select {
		case w.ch <- value:
			compactPopWaiters(ls)
			return true
		default:
			// waiter gone, try next
			continue
		}
	}
	compactPopWaiters(ls)
	return false
}

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
	// Try to hand off to a waiting pop consumer
	if tryHandoffToPopWaiter(ls, value) {
		ls.LastActivity = time.Now()
		// Clean up empty list entry if no items and no pop waiters remain
		if len(ls.Items) == 0 && ls.PopWaiterHead >= len(ls.PopWaiters) {
			delete(sh.lists, key)
			lm.keyTotal.Add(-1)
		}
		return 0, nil
	}
	if max := lm.cfg.MaxListLength; max > 0 && len(ls.Items) >= max {
		return 0, ErrListFull
	}
	// Prepend: grow by one, shift right, insert at front.
	ls.Items = append(ls.Items, "")
	copy(ls.Items[1:], ls.Items)
	ls.Items[0] = value
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
	// Try to hand off to a waiting pop consumer
	if tryHandoffToPopWaiter(ls, value) {
		ls.LastActivity = time.Now()
		// Clean up empty list entry if no items and no pop waiters remain
		if len(ls.Items) == 0 && ls.PopWaiterHead >= len(ls.PopWaiters) {
			delete(sh.lists, key)
			lm.keyTotal.Add(-1)
		}
		return 0, nil
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
	if len(ls.Items) == 0 && ls.PopWaiterHead >= len(ls.PopWaiters) {
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
	if len(ls.Items) == 0 && ls.PopWaiterHead >= len(ls.PopWaiters) {
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
// Blocking List Pop
// ---------------------------------------------------------------------------

// BLPop blocks until an item is available at the left of the list, or timeout.
// Returns ("", nil) on timeout.
func (lm *LockManager) BLPop(ctx context.Context, key string, timeout time.Duration, connID uint64) (string, error) {
	return lm.blockingPop(ctx, key, timeout, connID, true)
}

// BRPop blocks until an item is available at the right of the list, or timeout.
// Returns ("", nil) on timeout.
func (lm *LockManager) BRPop(ctx context.Context, key string, timeout time.Duration, connID uint64) (string, error) {
	return lm.blockingPop(ctx, key, timeout, connID, false)
}

func (lm *LockManager) blockingPop(ctx context.Context, key string, timeout time.Duration, connID uint64, left bool) (string, error) {
	sh := lm.shardFor(key)
	sh.mu.Lock()

	ls, ok := sh.lists[key]
	if ok && len(ls.Items) > 0 {
		// Fast path: list has items
		var val string
		if left {
			val = ls.Items[0]
			ls.Items[0] = ""
			ls.Items = ls.Items[1:]
		} else {
			last := len(ls.Items) - 1
			val = ls.Items[last]
			ls.Items[last] = ""
			ls.Items = ls.Items[:last]
		}
		ls.LastActivity = time.Now()
		if len(ls.Items) == 0 && ls.PopWaiterHead >= len(ls.PopWaiters) {
			delete(sh.lists, key)
			lm.keyTotal.Add(-1)
		}
		sh.mu.Unlock()
		return val, nil
	}

	// Slow path: list empty or doesn't exist — create waiter
	if !ok {
		if max := lm.cfg.MaxKeys; max > 0 && lm.totalKeyCount() >= max {
			sh.mu.Unlock()
			return "", ErrMaxKeys
		}
		ls = &listState{LastActivity: time.Now()}
		sh.lists[key] = ls
		lm.keyTotal.Add(1)
	}

	w := &listWaiter{
		ch:     make(chan string, 1),
		connID: connID,
	}
	ls.PopWaiters = append(ls.PopWaiters, w)
	sh.mu.Unlock()

	// timeout == 0 means fire immediately (non-blocking try-pop).
	var timerCh <-chan time.Time
	var timer *time.Timer
	if timeout > 0 {
		timer = time.NewTimer(timeout)
		timerCh = timer.C
		defer timer.Stop()
	} else {
		timer = time.NewTimer(0)
		timerCh = timer.C
		defer timer.Stop()
	}

	select {
	case val, ok := <-w.ch:
		if !ok {
			return "", ErrWaiterClosed
		}
		return val, nil

	case <-ctx.Done():
		sh.mu.Lock()
		// Race check: value may have arrived
		select {
		case val, ok := <-w.ch:
			if ok {
				sh.mu.Unlock()
				return val, nil
			}
		default:
		}
		lm.removeListWaiter(sh, key, w)
		sh.mu.Unlock()
		return "", ctx.Err()

	case <-timerCh:
		sh.mu.Lock()
		// Race check
		select {
		case val, ok := <-w.ch:
			if ok {
				sh.mu.Unlock()
				return val, nil
			}
		default:
		}
		lm.removeListWaiter(sh, key, w)
		sh.mu.Unlock()
		return "", nil
	}
}

// removeListWaiter removes a list waiter and cleans up the list if empty.
// Must be called with shard lock held.
func (lm *LockManager) removeListWaiter(sh *shard, key string, target *listWaiter) {
	ls, ok := sh.lists[key]
	if !ok {
		return
	}
	for i := ls.PopWaiterHead; i < len(ls.PopWaiters); i++ {
		if ls.PopWaiters[i] == target {
			copy(ls.PopWaiters[i:], ls.PopWaiters[i+1:])
			ls.PopWaiters[len(ls.PopWaiters)-1] = nil
			ls.PopWaiters = ls.PopWaiters[:len(ls.PopWaiters)-1]
			break
		}
	}
	// If list is empty and no waiters, clean up
	if len(ls.Items) == 0 && ls.PopWaiterHead >= len(ls.PopWaiters) {
		delete(sh.lists, key)
		lm.keyTotal.Add(-1)
	}
}

// ---------------------------------------------------------------------------
// Read-Write Locks
// ---------------------------------------------------------------------------

var ErrTypeMismatch = errors.New("key type mismatch (regular vs rw)")

// RWAcquire acquires a read or write lock. mode must be 'r' or 'w'.
func (lm *LockManager) RWAcquire(ctx context.Context, key string, mode byte, timeout, leaseTTL time.Duration, connID uint64) (string, uint64, error) {
	sh := lm.shardFor(key)

	lm.connMu.Lock()
	sh.mu.Lock()
	st, err := lm.getOrCreateLocked(sh, key, -1)
	if err != nil {
		sh.mu.Unlock()
		lm.connMu.Unlock()
		return "", 0, err
	}

	st.LastActivity = time.Now()
	evicted := lm.evictExpiredLocked(key, st)
	notifyRelease := func() {
		if evicted && lm.OnLockRelease != nil {
			lm.OnLockRelease(key)
		}
	}

	// Fast path
	if st.waiterCount() == 0 {
		canGrant := false
		if mode == 'r' {
			// Grant if no writer holding
			hasWriter := false
			for _, h := range st.Holders {
				if h.mode == 'w' {
					hasWriter = true
					break
				}
			}
			canGrant = !hasWriter
		} else {
			// Writer: grant only if no holders at all
			canGrant = len(st.Holders) == 0
		}
		if canGrant {
			token := lm.newToken()
			fence := lm.fenceSeq.Add(1)
			st.Holders[token] = &holder{
				connID:       connID,
				leaseExpires: time.Now().Add(leaseTTL),
				fence:        fence,
				mode:         mode,
			}
			st.LastActivity = time.Now()
			lm.connAddOwned(connID, key, token)
			sh.mu.Unlock()
			lm.connMu.Unlock()
			notifyRelease()
			return token, fence, nil
		}
	}

	// Slow path
	if max := lm.cfg.MaxWaiters; max > 0 && st.waiterCount() >= max {
		sh.mu.Unlock()
		lm.connMu.Unlock()
		notifyRelease()
		return "", 0, ErrMaxWaiters
	}
	w := &waiter{
		ch:       make(chan string, 1),
		connID:   connID,
		leaseTTL: leaseTTL,
		mode:     mode,
	}
	st.Waiters = append(st.Waiters, w)
	sh.mu.Unlock()
	lm.connMu.Unlock()
	notifyRelease()

	// timeout == 0 means fire immediately (non-blocking try-acquire).
	var timerCh <-chan time.Time
	var timer *time.Timer
	if timeout > 0 {
		timer = time.NewTimer(timeout)
		timerCh = timer.C
		defer timer.Stop()
	} else {
		timer = time.NewTimer(0)
		timerCh = timer.C
		defer timer.Stop()
	}

	// verifyAndResetLease checks that the token is still held and resets
	// the lease expiry. Returns (fence, true) if held, (0, false) if evicted.
	// Must be called with sh.mu held.
	verifyHolder := func(token string) (uint64, bool) {
		if s := sh.resources[key]; s != nil {
			s.LastActivity = time.Now()
			if h, ok := s.Holders[token]; ok {
				h.leaseExpires = time.Now().Add(leaseTTL)
				return h.fence, true
			}
		}
		return 0, false
	}

	select {
	case token, ok := <-w.ch:
		if !ok || token == "" {
			return "", 0, ErrWaiterClosed
		}
		lm.connMu.Lock()
		sh.mu.Lock()
		fence, held := verifyHolder(token)
		if !held {
			lm.connRemoveOwned(connID, key, token)
			sh.mu.Unlock()
			lm.connMu.Unlock()
			return "", 0, ErrLeaseExpired
		}
		sh.mu.Unlock()
		lm.connMu.Unlock()
		return token, fence, nil

	case <-ctx.Done():
		lm.connMu.Lock()
		sh.mu.Lock()
		select {
		case token, ok := <-w.ch:
			if ok && token != "" {
				fence, held := verifyHolder(token)
				if !held {
					lm.connRemoveOwned(connID, key, token)
					sh.mu.Unlock()
					lm.connMu.Unlock()
					return "", 0, ErrLeaseExpired
				}
				sh.mu.Unlock()
				lm.connMu.Unlock()
				return token, fence, nil
			}
		default:
		}
		if s := sh.resources[key]; s != nil {
			s.LastActivity = time.Now()
			removeWaiterFromState(s, w)
			lm.grantNextLocked(key, s)
		}
		sh.mu.Unlock()
		lm.connMu.Unlock()
		return "", 0, ctx.Err()

	case <-timerCh:
		lm.connMu.Lock()
		sh.mu.Lock()
		select {
		case token, ok := <-w.ch:
			if ok && token != "" {
				fence, held := verifyHolder(token)
				if !held {
					lm.connRemoveOwned(connID, key, token)
					sh.mu.Unlock()
					lm.connMu.Unlock()
					return "", 0, ErrLeaseExpired
				}
				sh.mu.Unlock()
				lm.connMu.Unlock()
				return token, fence, nil
			}
		default:
		}
		if s := sh.resources[key]; s != nil {
			s.LastActivity = time.Now()
			removeWaiterFromState(s, w)
			lm.grantNextLocked(key, s)
		}
		sh.mu.Unlock()
		lm.connMu.Unlock()
		return "", 0, nil
	}
}

// RWRelease releases a read or write lock.
func (lm *LockManager) RWRelease(key, token string) bool {
	return lm.Release(key, token)
}

// RWRenew renews a read or write lock lease.
func (lm *LockManager) RWRenew(key, token string, leaseTTL time.Duration) (int, uint64, bool) {
	return lm.RenewWithFence(key, token, leaseTTL)
}

// RWEnqueue is phase 1 of two-phase RW lock acquire.
func (lm *LockManager) RWEnqueue(key string, mode byte, leaseTTL time.Duration, connID uint64) (string, string, int, uint64, error) {
	eqKey := connKey{ConnID: connID, Key: key}
	sh := lm.shardFor(key)

	lm.connMu.Lock()
	sh.mu.Lock()

	if _, exists := lm.connEnqueued[eqKey]; exists {
		sh.mu.Unlock()
		lm.connMu.Unlock()
		return "", "", 0, 0, ErrAlreadyEnqueued
	}

	st, err := lm.getOrCreateLocked(sh, key, -1)
	if err != nil {
		sh.mu.Unlock()
		lm.connMu.Unlock()
		return "", "", 0, 0, err
	}

	st.LastActivity = time.Now()
	leaseSec := int(leaseTTL / time.Second)
	evicted := lm.evictExpiredLocked(key, st)
	notifyRelease := func() {
		if evicted && lm.OnLockRelease != nil {
			lm.OnLockRelease(key)
		}
	}

	// Fast path
	if st.waiterCount() == 0 {
		canGrant := false
		if mode == 'r' {
			hasWriter := false
			for _, h := range st.Holders {
				if h.mode == 'w' {
					hasWriter = true
					break
				}
			}
			canGrant = !hasWriter
		} else {
			canGrant = len(st.Holders) == 0
		}
		if canGrant {
			token := lm.newToken()
			fence := lm.fenceSeq.Add(1)
			st.Holders[token] = &holder{
				connID:       connID,
				leaseExpires: time.Now().Add(leaseTTL),
				fence:        fence,
				mode:         mode,
			}
			st.LastActivity = time.Now()
			lm.connAddOwned(connID, key, token)
			lm.connEnqueued[eqKey] = &enqueuedState{token: token, leaseTTL: leaseTTL}
			sh.mu.Unlock()
			lm.connMu.Unlock()
			notifyRelease()
			return "acquired", token, leaseSec, fence, nil
		}
	}

	// Slow path
	if max := lm.cfg.MaxWaiters; max > 0 && st.waiterCount() >= max {
		sh.mu.Unlock()
		lm.connMu.Unlock()
		notifyRelease()
		return "", "", 0, 0, ErrMaxWaiters
	}
	w := &waiter{
		ch:       make(chan string, 1),
		connID:   connID,
		leaseTTL: leaseTTL,
		mode:     mode,
	}
	st.Waiters = append(st.Waiters, w)
	lm.connEnqueued[eqKey] = &enqueuedState{waiter: w, leaseTTL: leaseTTL}
	sh.mu.Unlock()
	lm.connMu.Unlock()
	notifyRelease()
	return "queued", "", 0, 0, nil
}

// RWWait is phase 2 of two-phase RW lock acquire.
func (lm *LockManager) RWWait(ctx context.Context, key string, timeout time.Duration, connID uint64) (string, int, uint64, error) {
	return lm.WaitWithFence(ctx, key, timeout, connID)
}

// grantNextRWWaiterLocked implements the FIFO fair granting for RW locks.
// Consecutive readers at the queue head are batch-granted. A writer in the
// queue blocks all subsequent grants. Must be called with shard lock AND connMu held.
func (lm *LockManager) grantNextRWWaiterLocked(key string, st *ResourceState) {
	for st.WaiterHead < len(st.Waiters) {
		w := st.Waiters[st.WaiterHead]
		if w.mode == 'w' {
			// Writer needs exclusive access
			if len(st.Holders) == 0 {
				st.Waiters[st.WaiterHead] = nil
				st.WaiterHead++
				token := lm.newToken()
				select {
				case w.ch <- token:
					fence := lm.fenceSeq.Add(1)
					st.Holders[token] = &holder{
						connID:       w.connID,
						leaseExpires: time.Now().Add(w.leaseTTL),
						fence:        fence,
						mode:         'w',
					}
					st.LastActivity = time.Now()
					lm.connAddOwned(w.connID, key, token)
				default:
					continue
				}
				break // writer is exclusive, done
			}
			break // can't grant writer, holders exist
		}
		// Reader
		hasWriter := false
		for _, h := range st.Holders {
			if h.mode == 'w' {
				hasWriter = true
				break
			}
		}
		if hasWriter {
			break // writer active, readers must wait
		}
		// Grant reader
		st.Waiters[st.WaiterHead] = nil
		st.WaiterHead++
		token := lm.newToken()
		select {
		case w.ch <- token:
			fence := lm.fenceSeq.Add(1)
			st.Holders[token] = &holder{
				connID:       w.connID,
				leaseExpires: time.Now().Add(w.leaseTTL),
				fence:        fence,
				mode:         'r',
			}
			st.LastActivity = time.Now()
			lm.connAddOwned(w.connID, key, token)
		default:
			continue
		}
		// Continue — batch-grant consecutive readers
	}
	st.compactWaiters()
}

// ---------------------------------------------------------------------------
// Barriers
// ---------------------------------------------------------------------------

// BarrierWait blocks until 'count' participants have arrived at the barrier, or
// the timeout/context expires. Returns true if the barrier tripped, false on timeout.
func (lm *LockManager) BarrierWait(ctx context.Context, key string, count int, timeout time.Duration, connID uint64) (bool, error) {
	sh := lm.shardFor(key)
	sh.mu.Lock()

	bs, ok := sh.barriers[key]
	if ok {
		// Note: tripped barriers are deleted atomically (under the shard lock)
		// when they trip, so bs.Tripped is always false here.
		if bs.Count != count {
			sh.mu.Unlock()
			return false, ErrBarrierCountMismatch
		}
	} else {
		if max := lm.cfg.MaxKeys; max > 0 && lm.totalKeyCount() >= max {
			sh.mu.Unlock()
			return false, ErrMaxKeys
		}
		bs = &barrierState{
			Count:        count,
			Cancelled:    make(chan struct{}),
			LastActivity: time.Now(),
		}
		sh.barriers[key] = bs
		lm.keyTotal.Add(1)
	}

	// Fix #8: Reject duplicate participation by the same connection.
	for _, pp := range bs.Participants {
		if pp.connID == connID {
			sh.mu.Unlock()
			return false, fmt.Errorf("duplicate barrier participant: connID=%d", connID)
		}
	}

	p := &barrierParticipant{
		ch:     make(chan struct{}),
		connID: connID,
	}
	bs.Participants = append(bs.Participants, p)
	bs.LastActivity = time.Now()

	if len(bs.Participants) >= count {
		// Barrier tripped — close all participant channels
		for _, pp := range bs.Participants {
			close(pp.ch)
		}
		bs.Tripped = true
		bs.Participants = nil
		// Remove the barrier and reclaim the key count
		delete(sh.barriers, key)
		lm.keyTotal.Add(-1)
		sh.mu.Unlock()
		return true, nil
	}

	cancelled := bs.Cancelled
	sh.mu.Unlock()

	// Wait for trip, cancellation, timeout, or context cancel.
	// Fix #7: For timeout <= 0, check if barrier tripped first (non-blocking)
	// before starting the timer to avoid non-deterministic select.
	if timeout <= 0 {
		select {
		case <-p.ch:
			return true, nil
		default:
		}
		sh.mu.Lock()
		// Re-check under lock: barrier may have tripped between the
		// non-blocking check and acquiring the shard lock.
		select {
		case <-p.ch:
			sh.mu.Unlock()
			return true, nil
		default:
		}
		lm.removeBarrierParticipant(sh, key, p)
		sh.mu.Unlock()
		return false, nil
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-p.ch:
		return true, nil
	case <-cancelled:
		// Re-check: barrier may have tripped concurrently with cancellation
		// (e.g. a new participant joined and completed the barrier).
		select {
		case <-p.ch:
			return true, nil
		default:
		}
		sh.mu.Lock()
		select {
		case <-p.ch:
			sh.mu.Unlock()
			return true, nil
		default:
		}
		lm.removeBarrierParticipant(sh, key, p)
		sh.mu.Unlock()
		return false, nil
	case <-ctx.Done():
		// Re-check: barrier may have tripped concurrently with cancellation.
		select {
		case <-p.ch:
			return true, nil
		default:
		}
		sh.mu.Lock()
		select {
		case <-p.ch:
			sh.mu.Unlock()
			return true, nil
		default:
		}
		lm.removeBarrierParticipant(sh, key, p)
		sh.mu.Unlock()
		return false, ctx.Err()
	case <-timer.C:
		// Re-check: barrier may have tripped concurrently with timer.
		select {
		case <-p.ch:
			return true, nil
		default:
		}
		sh.mu.Lock()
		select {
		case <-p.ch:
			sh.mu.Unlock()
			return true, nil
		default:
		}
		lm.removeBarrierParticipant(sh, key, p)
		sh.mu.Unlock()
		return false, nil
	}
}

// removeBarrierParticipant removes a participant from a barrier.
// When a participant leaves, the barrier can never trip (count unreachable),
// so remaining participants are notified via the Cancelled channel.
// Must be called with the shard lock held.
func (lm *LockManager) removeBarrierParticipant(sh *shard, key string, target *barrierParticipant) {
	bs, ok := sh.barriers[key]
	if !ok {
		return
	}
	for i, p := range bs.Participants {
		if p == target {
			copy(bs.Participants[i:], bs.Participants[i+1:])
			bs.Participants[len(bs.Participants)-1] = nil
			bs.Participants = bs.Participants[:len(bs.Participants)-1]
			break
		}
	}
	if !bs.Tripped && bs.Cancelled != nil {
		// Barrier is now unreachable — wake remaining participants.
		// Once a participant departs, the barrier cannot trip because the
		// count was set when the first participant joined and new participants
		// cannot fill departed slots.
		select {
		case <-bs.Cancelled:
			// already cancelled
		default:
			close(bs.Cancelled)
		}
	}
	if len(bs.Participants) == 0 && !bs.Tripped {
		delete(sh.barriers, key)
		lm.keyTotal.Add(-1)
	}
}

// ---------------------------------------------------------------------------
// Leader Election Watchers
// ---------------------------------------------------------------------------

// RegisterLeaderWatcher registers a connection to receive leader change notifications
// as an observer (not as the elected leader).
func (lm *LockManager) RegisterLeaderWatcher(key string, connID uint64, writeCh chan []byte, cancelConn func()) {
	sh := lm.shardFor(key)
	lm.connMu.Lock()
	sh.mu.Lock()
	watchers, ok := sh.leaderWatchers[key]
	if !ok {
		watchers = make(map[uint64]*leaderWatcher)
		sh.leaderWatchers[key] = watchers
	}
	if existing, ok := watchers[connID]; ok {
		// Already registered (e.g. via elect) — just mark as also an observer.
		existing.observer = true
	} else {
		watchers[connID] = &leaderWatcher{ch: writeCh, cancelConn: cancelConn, observer: true}
	}
	keys := lm.connLeaderKeys[connID]
	if keys == nil {
		keys = make(map[string]struct{})
		lm.connLeaderKeys[connID] = keys
	}
	keys[key] = struct{}{}
	sh.mu.Unlock()
	lm.connMu.Unlock()
}

// RegisterAndNotifyLeader atomically registers a watcher and notifies
// observers of a leader change.
func (lm *LockManager) RegisterAndNotifyLeader(key, eventType string, connID uint64, writeCh chan []byte, cancelConn func()) {
	sh := lm.shardFor(key)
	lm.connMu.Lock()
	sh.mu.Lock()
	watchers, ok := sh.leaderWatchers[key]
	if !ok {
		watchers = make(map[uint64]*leaderWatcher)
		sh.leaderWatchers[key] = watchers
	}
	existingObserver := false
	if existing, ok := watchers[connID]; ok {
		existingObserver = existing.observer
	}
	watchers[connID] = &leaderWatcher{ch: writeCh, cancelConn: cancelConn, observer: existingObserver}
	// Notify all pre-existing watchers (excluding the new leader).
	// Note: an observer that registered between AcquireWithFence and this call
	// may receive a duplicate "elected" notification (once from
	// RegisterLeaderWatcherWithStatus, once from here). This is benign —
	// "leader elected" is idempotent.
	msg := []byte(fmt.Sprintf("leader %s %s\n", eventType, key))
	for cid, lw := range watchers {
		if cid == connID {
			continue
		}
		select {
		case lw.ch <- msg:
		default:
			lw.cancelConn()
		}
	}
	keys := lm.connLeaderKeys[connID]
	if keys == nil {
		keys = make(map[string]struct{})
		lm.connLeaderKeys[connID] = keys
	}
	keys[key] = struct{}{}
	sh.mu.Unlock()
	lm.connMu.Unlock()
}

// removeLeaderHolderWatcher removes a leader watcher for an expired/released
// holder, but only if it was not registered as an observer. This prevents
// lease expiry from silently unregistering observe-registered watchers.
// Must be called with connMu and sh.mu held.
func (lm *LockManager) removeLeaderHolderWatcher(sh *shard, key string, connID uint64) {
	if watchers, ok := sh.leaderWatchers[key]; ok {
		if lw, exists := watchers[connID]; exists && !lw.observer {
			delete(watchers, connID)
			if len(watchers) == 0 {
				delete(sh.leaderWatchers, key)
			}
		}
	}
	if keys, ok := lm.connLeaderKeys[connID]; ok {
		// Only remove the key from connLeaderKeys if the watcher was
		// actually removed (i.e. not an observer).
		if _, stillExists := sh.leaderWatchers[key]; !stillExists {
			delete(keys, key)
		} else if watchers := sh.leaderWatchers[key]; watchers != nil {
			if _, has := watchers[connID]; !has {
				delete(keys, key)
			}
		}
		if len(keys) == 0 {
			delete(lm.connLeaderKeys, connID)
		}
	}
}

// UnregisterLeaderWatcher removes a leader change watcher.
func (lm *LockManager) UnregisterLeaderWatcher(key string, connID uint64) {
	sh := lm.shardFor(key)
	lm.connMu.Lock()
	sh.mu.Lock()
	if watchers, ok := sh.leaderWatchers[key]; ok {
		delete(watchers, connID)
		if len(watchers) == 0 {
			delete(sh.leaderWatchers, key)
		}
	}
	if keys, ok := lm.connLeaderKeys[connID]; ok {
		delete(keys, key)
		if len(keys) == 0 {
			delete(lm.connLeaderKeys, connID)
		}
	}
	sh.mu.Unlock()
	lm.connMu.Unlock()
}

// UnregisterAllLeaderWatchers removes all leader change watchers for a connection.
// Holds connMu across the entire operation to prevent concurrent RegisterLeaderWatcher
// from re-adding entries between the connLeaderKeys deletion and shard iteration.
func (lm *LockManager) UnregisterAllLeaderWatchers(connID uint64) {
	lm.connMu.Lock()
	keys := lm.connLeaderKeys[connID]
	delete(lm.connLeaderKeys, connID)

	for key := range keys {
		sh := lm.shardFor(key)
		sh.mu.Lock()
		if watchers, ok := sh.leaderWatchers[key]; ok {
			delete(watchers, connID)
			if len(watchers) == 0 {
				delete(sh.leaderWatchers, key)
			}
		}
		sh.mu.Unlock()
	}
	lm.connMu.Unlock()
}

// NotifyLeaderChange sends a leader change event to all watchers of a key.
// excludeConnID allows skipping the connection that triggered the event
// (pass 0 to notify all watchers). Disconnects slow consumers.
func (lm *LockManager) NotifyLeaderChange(key, eventType string, excludeConnID uint64) {
	sh := lm.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	watchers, ok := sh.leaderWatchers[key]
	if !ok {
		return
	}
	// msg is shared read-only across all recipients.
	msg := []byte(fmt.Sprintf("leader %s %s\n", eventType, key))
	for connID, lw := range watchers {
		if connID == excludeConnID {
			continue
		}
		select {
		case lw.ch <- msg:
		default:
			lw.cancelConn()
		}
	}
}

// LeaderHolder returns the connID of the current leader (lock holder) for a key,
// or 0 if no one holds the lock.
func (lm *LockManager) LeaderHolder(key string) uint64 {
	sh := lm.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	st, ok := sh.resources[key]
	if !ok {
		return 0
	}
	for _, h := range st.Holders {
		return h.connID
	}
	return 0
}

// ResignLeader atomically releases the lock, unregisters the leader watcher,
// and sends "resigned" notification followed by any failover notification.
func (lm *LockManager) ResignLeader(key, token string, connID uint64) bool {
	sh := lm.shardFor(key)

	lm.connMu.Lock()
	sh.mu.Lock()

	st := sh.resources[key]
	if st == nil {
		sh.mu.Unlock()
		lm.connMu.Unlock()
		return false
	}

	st.LastActivity = time.Now()

	h, ok := st.Holders[token]
	if !ok {
		sh.mu.Unlock()
		lm.connMu.Unlock()
		return false
	}

	// 1. Release the lock.
	lm.connRemoveOwned(h.connID, key, token)
	eqKey := connKey{ConnID: h.connID, Key: key}
	if es, ok := lm.connEnqueued[eqKey]; ok && es.token == token {
		delete(lm.connEnqueued, eqKey)
	}
	delete(st.Holders, token)

	// 2. Unregister the holder's leader watcher
	// (preserves observer registrations).
	lm.removeLeaderHolderWatcher(sh, key, h.connID)

	// 3. Notify "resigned" before granting to next waiter.
	if watchers, ok := sh.leaderWatchers[key]; ok && len(watchers) > 0 {
		msg := []byte(fmt.Sprintf("leader resigned %s\n", key))
		for _, lw := range watchers {
			select {
			case lw.ch <- msg:
			default:
				lw.cancelConn()
			}
		}
	}

	// 4. Grant next waiter (may trigger "failover" notification via grantNextWaiterLocked).
	lm.grantNextLocked(key, st)

	sh.mu.Unlock()
	lm.connMu.Unlock()
	return true
}

// RegisterLeaderWatcherWithStatus atomically registers a watcher and, if a
// leader currently exists, sends the initial "leader elected" notification
// while still holding the lock to prevent TOCTOU races with resign/failover.
func (lm *LockManager) RegisterLeaderWatcherWithStatus(key string, connID uint64, writeCh chan []byte, cancelConn func()) {
	sh := lm.shardFor(key)
	lm.connMu.Lock()
	sh.mu.Lock()

	watchers, ok := sh.leaderWatchers[key]
	if !ok {
		watchers = make(map[uint64]*leaderWatcher)
		sh.leaderWatchers[key] = watchers
	}
	if existing, ok := watchers[connID]; ok {
		// Already registered (e.g. via elect) — just mark as also an observer.
		existing.observer = true
	} else {
		watchers[connID] = &leaderWatcher{ch: writeCh, cancelConn: cancelConn, observer: true}
	}

	keys := lm.connLeaderKeys[connID]
	if keys == nil {
		keys = make(map[string]struct{})
		lm.connLeaderKeys[connID] = keys
	}
	keys[key] = struct{}{}

	// Send initial notification while holding the lock to avoid TOCTOU
	// where the leader resigns between the check and the send.
	hasLeader := false
	if st, ok := sh.resources[key]; ok {
		for range st.Holders {
			hasLeader = true
			break
		}
	}
	if hasLeader {
		msg := []byte(fmt.Sprintf("leader elected %s\n", key))
		select {
		case writeCh <- msg:
		default:
			cancelConn()
		}
	}

	sh.mu.Unlock()
	lm.connMu.Unlock()
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
		for key, st := range sh.resources {
			removeWaitersByConn(st, connID, closed)
			// Unblock waiters that were queued behind the removed waiter
			// (e.g. readers behind a removed writer in RW locks).
			lm.grantNextLocked(key, st)
		}
		sh.mu.Unlock()
	}

	// Release owned slots.
	var releasedKeys []string
	for _, entry := range ownedEntries {
		sh := lm.shardFor(entry.key)
		sh.mu.Lock()
		st := sh.resources[entry.key]
		if st != nil {
			released := false
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
				released = true
			}
			st.LastActivity = time.Now()
			lm.grantNextLocked(entry.key, st)
			if released {
				releasedKeys = append(releasedKeys, entry.key)
			}
		}
		sh.mu.Unlock()
	}
	delete(lm.connOwned, connID)

	// Clean up list pop waiters for this connection.
	for i := range lm.shards {
		sh := &lm.shards[i]
		sh.mu.Lock()
		for key, ls := range sh.lists {
			// Remove pop waiters for this connection by compacting the
			// active region, preserving order for other connections.
			n := ls.PopWaiterHead
			for j := ls.PopWaiterHead; j < len(ls.PopWaiters); j++ {
				w := ls.PopWaiters[j]
				if w != nil && w.connID == connID {
					close(w.ch)
				} else {
					ls.PopWaiters[n] = w
					n++
				}
			}
			for j := n; j < len(ls.PopWaiters); j++ {
				ls.PopWaiters[j] = nil
			}
			ls.PopWaiters = ls.PopWaiters[:n]
			// Remove the list if it's empty and all pop waiters are gone.
			if len(ls.Items) == 0 && ls.PopWaiterHead >= len(ls.PopWaiters) {
				delete(sh.lists, key)
				lm.keyTotal.Add(-1)
			}
		}
		// Clean up barrier participants for this connection.
		// When a participant disconnects, the barrier can never trip
		// (count can't be reached), so cancel it to wake remaining waiters.
		for key, bs := range sh.barriers {
			removed := false
			for i := len(bs.Participants) - 1; i >= 0; i-- {
				if bs.Participants[i].connID == connID {
					copy(bs.Participants[i:], bs.Participants[i+1:])
					bs.Participants[len(bs.Participants)-1] = nil
					bs.Participants = bs.Participants[:len(bs.Participants)-1]
					removed = true
				}
			}
			if removed && !bs.Tripped && bs.Cancelled != nil {
				// Signal remaining participants that barrier is unreachable
				select {
				case <-bs.Cancelled:
					// already cancelled
				default:
					close(bs.Cancelled)
				}
			}
			if len(bs.Participants) == 0 && !bs.Tripped {
				delete(sh.barriers, key)
				lm.keyTotal.Add(-1)
			}
		}
		sh.mu.Unlock()
	}

	lm.connMu.Unlock()

	// Notify watchers of released keys outside of locks.
	if lm.OnLockRelease != nil {
		for _, key := range releasedKeys {
			lm.OnLockRelease(key)
		}
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
				lm.connMu.Lock()
				sh.mu.Lock()
				var releasedKeys []string
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
							// Clean up leader watcher for the expired holder
							// (preserves observer registrations).
							lm.removeLeaderHolderWatcher(sh, key, h.connID)
							expired = append(expired, token)
						}
					}
					for _, token := range expired {
						delete(st.Holders, token)
					}
					if len(expired) > 0 {
						st.LastActivity = now
						lm.grantNextLocked(key, st)
						releasedKeys = append(releasedKeys, key)
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
				// Notify watchers of released keys outside of locks.
				if lm.OnLockRelease != nil {
					for _, key := range releasedKeys {
						lm.OnLockRelease(key)
					}
				}
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
				// GC idle empty lists (skip if active pop waiters)
				var expiredLists []string
				for key, ls := range sh.lists {
					hasWaiters := ls.PopWaiterHead < len(ls.PopWaiters)
					if len(ls.Items) == 0 && !hasWaiters && now.Sub(ls.LastActivity) > lm.cfg.GCMaxIdleTime {
						expiredLists = append(expiredLists, key)
					}
				}
				for _, key := range expiredLists {
					lm.log.Debug("GC: pruning idle list", "key", key)
					delete(sh.lists, key)
					lm.keyTotal.Add(-1)
				}
				// GC idle empty barriers
				var expiredBarriers []string
				for key, bs := range sh.barriers {
					if len(bs.Participants) == 0 && !bs.Tripped && now.Sub(bs.LastActivity) > lm.cfg.GCMaxIdleTime {
						expiredBarriers = append(expiredBarriers, key)
					}
				}
				for _, key := range expiredBarriers {
					lm.log.Debug("GC: pruning idle barrier", "key", key)
					delete(sh.barriers, key)
					lm.keyTotal.Add(-1)
				}
				// GC empty leaderWatchers maps (no watchers remaining)
				for key, watchers := range sh.leaderWatchers {
					if len(watchers) == 0 {
						delete(sh.leaderWatchers, key)
					}
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
	Fence           uint64  `json:"fence,omitempty"`
}

type SemInfo struct {
	Key     string `json:"key"`
	Limit   int    `json:"limit"`
	Holders int    `json:"holders"`
	Waiters int    `json:"waiters"`
}

type RWLockInfo struct {
	Key     string `json:"key"`
	Readers int    `json:"readers"`
	Writer  bool   `json:"writer"`
	Fence   uint64 `json:"fence,omitempty"`
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
	Key        string  `json:"key"`
	Len        int     `json:"len"`
	PopWaiters int     `json:"pop_waiters,omitempty"`
	IdleS      float64 `json:"idle_s"`
}

type SignalChannelInfo struct {
	Pattern   string `json:"pattern"`
	Group     string `json:"group,omitempty"`
	Listeners int    `json:"listeners"`
}

type WatchChannelInfo struct {
	Pattern  string `json:"pattern"`
	Watchers int    `json:"watchers"`
}

type BarrierInfo struct {
	Key          string  `json:"key"`
	Count        int     `json:"count"`
	Participants int     `json:"participants"`
	IdleS        float64 `json:"idle_s"`
}

type Stats struct {
	Connections    int64              `json:"connections"`
	Locks          []LockInfo         `json:"locks"`
	Semaphores     []SemInfo          `json:"semaphores"`
	RWLocks        []RWLockInfo       `json:"rw_locks"`
	IdleLocks      []IdleInfo         `json:"idle_locks"`
	IdleSemaphores []IdleInfo         `json:"idle_semaphores"`
	Counters       []CounterInfo      `json:"counters"`
	KVEntries      []KVInfo           `json:"kv_entries"`
	Lists          []ListInfo         `json:"lists"`
	SignalChannels []SignalChannelInfo `json:"signal_channels"`
	WatchChannels  []WatchChannelInfo `json:"watch_channels"`
	Barriers       []BarrierInfo      `json:"barriers"`
}

// Stats returns a snapshot of the current lock manager state.
func (lm *LockManager) Stats(connections int64) *Stats {
	now := time.Now()
	s := &Stats{
		Connections:    connections,
		Locks:          []LockInfo{},
		Semaphores:     []SemInfo{},
		RWLocks:        []RWLockInfo{},
		IdleLocks:      []IdleInfo{},
		IdleSemaphores: []IdleInfo{},
		Counters:       []CounterInfo{},
		KVEntries:      []KVInfo{},
		Lists:          []ListInfo{},
		Barriers:       []BarrierInfo{},
	}

	for i := range lm.shards {
		sh := &lm.shards[i]
		sh.mu.Lock()
		for key, st := range sh.resources {
			nw := st.waiterCount()
			if st.Limit == -1 {
				// RW lock
				readers := 0
				hasWriter := false
				var maxFence uint64
				for _, h := range st.Holders {
					if h.mode == 'w' {
						hasWriter = true
					} else {
						readers++
					}
					if h.fence > maxFence {
						maxFence = h.fence
					}
				}
				if readers > 0 || hasWriter || nw > 0 {
					s.RWLocks = append(s.RWLocks, RWLockInfo{
						Key:     key,
						Readers: readers,
						Writer:  hasWriter,
						Fence:   maxFence,
						Waiters: nw,
					})
				}
			} else if st.Limit == 1 {
				if len(st.Holders) > 0 {
					var ownerConn uint64
					var expires float64
					var fence uint64
					for _, h := range st.Holders {
						ownerConn = h.connID
						expires = h.leaseExpires.Sub(now).Seconds()
						if expires < 0 {
							expires = 0
						}
						fence = h.fence
					}
					s.Locks = append(s.Locks, LockInfo{
						Key:             key,
						OwnerConnID:     ownerConn,
						LeaseExpiresInS: expires,
						Waiters:         nw,
						Fence:           fence,
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
			pw := len(ls.PopWaiters) - ls.PopWaiterHead
			s.Lists = append(s.Lists, ListInfo{
				Key:        key,
				Len:        len(ls.Items),
				PopWaiters: pw,
				IdleS:      now.Sub(ls.LastActivity).Seconds(),
			})
		}
		for key, bs := range sh.barriers {
			s.Barriers = append(s.Barriers, BarrierInfo{
				Key:          key,
				Count:        bs.Count,
				Participants: len(bs.Participants),
				IdleS:        now.Sub(bs.LastActivity).Seconds(),
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
		lm.shards[i].barriers = make(map[string]*barrierState)
		lm.shards[i].leaderWatchers = make(map[string]map[uint64]*leaderWatcher)
		lm.shards[i].mu.Unlock()
	}
	lm.connOwned = make(map[uint64]map[string]map[string]struct{})
	lm.connEnqueued = make(map[connKey]*enqueuedState)
	lm.connLeaderKeys = make(map[uint64]map[string]struct{})
	lm.resourceTotal.Store(0)
	lm.keyTotal.Store(0)
}
