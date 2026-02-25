package lock

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"sync"
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
)

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
	LastActivity time.Time
}

type enqueuedState struct {
	waiter   *waiter
	token    string
	leaseTTL time.Duration
}

type LockManager struct {
	mu           sync.Mutex
	resources    map[string]*ResourceState
	connOwned    map[uint64]map[string]map[string]struct{} // connID → key → set of tokens
	connEnqueued map[connKey]*enqueuedState
	cfg          *config.Config
	log          *slog.Logger
}

func NewLockManager(cfg *config.Config, log *slog.Logger) *LockManager {
	return &LockManager{
		resources:    make(map[string]*ResourceState),
		connOwned:    make(map[uint64]map[string]map[string]struct{}),
		connEnqueued: make(map[connKey]*enqueuedState),
		cfg:          cfg,
		log:          log,
	}
}

func newToken() string {
	var b [16]byte
	// crypto/rand.Read on supported platforms (Linux 3.17+, macOS, Windows)
	// uses getrandom/getentropy and never returns an error. Panic is
	// appropriate here as a broken CSPRNG is unrecoverable.
	if _, err := rand.Read(b[:]); err != nil {
		panic("crypto/rand failed: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}

// removeWaiter removes target from a waiter slice preserving order, reusing
// the backing array to avoid unnecessary allocation.
func removeWaiter(waiters []*waiter, target *waiter) []*waiter {
	for i, w := range waiters {
		if w == target {
			copy(waiters[i:], waiters[i+1:])
			waiters[len(waiters)-1] = nil // avoid memory leak
			return waiters[:len(waiters)-1]
		}
	}
	return waiters
}

// removeWaitersByConn removes all waiters for a given connID, closing their
// channels unless already tracked in the closed set.
func removeWaitersByConn(waiters []*waiter, connID uint64, closed map[chan string]struct{}) []*waiter {
	n := 0
	for _, w := range waiters {
		if w.connID == connID {
			if _, already := closed[w.ch]; !already {
				close(w.ch)
				closed[w.ch] = struct{}{}
			}
		} else {
			waiters[n] = w
			n++
		}
	}
	// Clear trailing pointers to avoid memory leak.
	for i := n; i < len(waiters); i++ {
		waiters[i] = nil
	}
	return waiters[:n]
}

// ---------------------------------------------------------------------------
// Internal helpers (must be called with lm.mu held)
// ---------------------------------------------------------------------------

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
// available. For locks (Limit==1) this grants at most one waiter; for
// semaphores it grants up to Limit - len(Holders) waiters.
// Must be called with lm.mu held.
func (lm *LockManager) grantNextWaiterLocked(key string, st *ResourceState) {
	for len(st.Waiters) > 0 && len(st.Holders) < st.Limit {
		w := st.Waiters[0]
		copy(st.Waiters, st.Waiters[1:])
		st.Waiters[len(st.Waiters)-1] = nil // avoid memory leak
		st.Waiters = st.Waiters[:len(st.Waiters)-1]
		token := newToken()
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
}

// evictExpiredLocked evicts any holders whose leases have expired and grants
// freed slots to waiting callers. Called opportunistically on acquire paths
// so callers don't have to wait for the background sweep tick.
// Must be called with lm.mu held.
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

func (lm *LockManager) getOrCreateLocked(key string, limit int) (*ResourceState, error) {
	st, ok := lm.resources[key]
	if ok {
		if st.Limit != limit {
			return nil, ErrLimitMismatch
		}
		return st, nil
	}
	if len(lm.resources) >= lm.cfg.MaxLocks {
		return nil, ErrMaxLocks
	}
	st = &ResourceState{
		Limit:        limit,
		Holders:      make(map[string]*holder),
		LastActivity: time.Now(),
	}
	lm.resources[key] = st
	return st, nil
}

// ---------------------------------------------------------------------------
// Public methods — unified for both locks (limit=1) and semaphores (limit>1)
// ---------------------------------------------------------------------------

// Acquire is the single-phase acquire (commands "l" and "sl").
func (lm *LockManager) Acquire(ctx context.Context, key string, timeout, leaseTTL time.Duration, connID uint64, limit int) (string, error) {
	w := &waiter{
		ch:       make(chan string, 1),
		connID:   connID,
		leaseTTL: leaseTTL,
	}

	lm.mu.Lock()
	st, err := lm.getOrCreateLocked(key, limit)
	if err != nil {
		lm.mu.Unlock()
		return "", err
	}

	st.LastActivity = time.Now()

	// Opportunistic expired-lease eviction (avoids waiting for sweep tick)
	lm.evictExpiredLocked(key, st)

	// Fast path: capacity available and no waiters
	if len(st.Holders) < st.Limit && len(st.Waiters) == 0 {
		token := newToken()
		st.Holders[token] = &holder{
			connID:       connID,
			leaseExpires: time.Now().Add(leaseTTL),
		}
		st.LastActivity = time.Now()
		lm.connAddOwned(connID, key, token)
		lm.mu.Unlock()
		return token, nil
	}

	// Slow path: enqueue and wait
	if max := lm.cfg.MaxWaiters; max > 0 && len(st.Waiters) >= max {
		lm.mu.Unlock()
		return "", ErrMaxWaiters
	}
	st.Waiters = append(st.Waiters, w)
	lm.mu.Unlock()

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
		lm.mu.Lock()
		if s := lm.resources[key]; s != nil {
			s.LastActivity = time.Now()
		}
		lm.mu.Unlock()
		return token, nil

	case <-ctx.Done():
		lm.mu.Lock()
		// Race check: token may have arrived between ctx cancellation
		// and acquiring the mutex. Returning the won token is safe —
		// if the caller cannot deliver it (e.g. server shutdown), the
		// connection cleanup path will release the lock.
		select {
		case token, ok := <-w.ch:
			if ok && token != "" {
				if s := lm.resources[key]; s != nil {
					s.LastActivity = time.Now()
				}
				lm.mu.Unlock()
				return token, nil
			}
		default:
		}
		if s := lm.resources[key]; s != nil {
			s.LastActivity = time.Now()
			s.Waiters = removeWaiter(s.Waiters, w)
		}
		lm.mu.Unlock()
		return "", ctx.Err()

	case <-timer.C:
		// Timeout — remove from queue
		lm.mu.Lock()
		// Race check: token may have arrived
		select {
		case token, ok := <-w.ch:
			if ok && token != "" {
				if s := lm.resources[key]; s != nil {
					s.LastActivity = time.Now()
				}
				lm.mu.Unlock()
				return token, nil
			}
		default:
		}
		if s := lm.resources[key]; s != nil {
			s.LastActivity = time.Now()
			s.Waiters = removeWaiter(s.Waiters, w)
		}
		lm.mu.Unlock()
		return "", nil
	}
}

// Enqueue is phase 1 of two-phase acquire (commands "e" and "se").
// Returns (status, token, leaseTTLSec, err).
func (lm *LockManager) Enqueue(key string, leaseTTL time.Duration, connID uint64, limit int) (string, string, int, error) {
	eqKey := connKey{ConnID: connID, Key: key}

	lm.mu.Lock()
	defer lm.mu.Unlock()

	if _, exists := lm.connEnqueued[eqKey]; exists {
		return "", "", 0, ErrAlreadyEnqueued
	}

	st, err := lm.getOrCreateLocked(key, limit)
	if err != nil {
		return "", "", 0, err
	}

	st.LastActivity = time.Now()
	leaseSec := int(leaseTTL / time.Second)

	// Opportunistic expired-lease eviction (avoids waiting for sweep tick)
	lm.evictExpiredLocked(key, st)

	// Fast path: capacity available and no waiters
	if len(st.Holders) < st.Limit && len(st.Waiters) == 0 {
		token := newToken()
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
	if max := lm.cfg.MaxWaiters; max > 0 && len(st.Waiters) >= max {
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

	lm.mu.Lock()
	es, ok := lm.connEnqueued[eqKey]
	if !ok {
		lm.mu.Unlock()
		return "", 0, ErrNotEnqueued
	}

	// Snapshot immutable fields under lock. The enqueuedState is set once
	// during Enqueue and its fields are not mutated afterward; the only
	// concurrent operation is CleanupConnection closing the waiter channel,
	// which is safe to race with a channel receive.
	leaseTTL := es.leaseTTL
	leaseSec := int(leaseTTL / time.Second)
	esToken := es.token
	w := es.waiter
	lm.mu.Unlock()

	// Fast path: already acquired during enqueue
	if esToken != "" {
		lm.mu.Lock()
		delete(lm.connEnqueued, eqKey)
		st := lm.resources[key]
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
					lm.mu.Unlock()
					return "", 0, ErrLeaseExpired
				}
				// Reset lease
				h.leaseExpires = time.Now().Add(leaseTTL)
				st.LastActivity = time.Now()
				lm.mu.Unlock()
				return esToken, leaseSec, nil
			}
		}
		// Slot was lost (expired and granted to another, or state GC'd)
		lm.connRemoveOwned(connID, key, esToken)
		lm.mu.Unlock()
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
			lm.mu.Lock()
			delete(lm.connEnqueued, eqKey)
			lm.mu.Unlock()
			return "", 0, ErrWaiterClosed
		}
		lm.mu.Lock()
		delete(lm.connEnqueued, eqKey)
		if st := lm.resources[key]; st != nil {
			if h, hOK := st.Holders[token]; hOK {
				h.leaseExpires = time.Now().Add(leaseTTL)
			}
			st.LastActivity = time.Now()
		}
		lm.mu.Unlock()
		return token, leaseSec, nil

	case <-ctx.Done():
		lm.mu.Lock()
		delete(lm.connEnqueued, eqKey)
		select {
		case token, ok := <-w.ch:
			if ok && token != "" {
				if st := lm.resources[key]; st != nil {
					if h, hOK := st.Holders[token]; hOK {
						h.leaseExpires = time.Now().Add(leaseTTL)
					}
					st.LastActivity = time.Now()
				}
				lm.mu.Unlock()
				return token, leaseSec, nil
			}
		default:
		}
		if st := lm.resources[key]; st != nil {
			st.LastActivity = time.Now()
			st.Waiters = removeWaiter(st.Waiters, w)
		}
		lm.mu.Unlock()
		return "", 0, ctx.Err()

	case <-timer.C:
		lm.mu.Lock()
		delete(lm.connEnqueued, eqKey)
		// Race check: token may have arrived
		select {
		case token, ok := <-w.ch:
			if ok && token != "" {
				if st := lm.resources[key]; st != nil {
					if h, hOK := st.Holders[token]; hOK {
						h.leaseExpires = time.Now().Add(leaseTTL)
					}
					st.LastActivity = time.Now()
				}
				lm.mu.Unlock()
				return token, leaseSec, nil
			}
		default:
		}
		// Remove from queue
		if st := lm.resources[key]; st != nil {
			st.LastActivity = time.Now()
			st.Waiters = removeWaiter(st.Waiters, w)
		}
		lm.mu.Unlock()
		return "", 0, nil
	}
}

// Release releases one held slot if the token matches (commands "r" and "sr").
func (lm *LockManager) Release(key, token string) bool {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	st := lm.resources[key]
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
	lm.mu.Lock()
	defer lm.mu.Unlock()

	st := lm.resources[key]
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
// Connection cleanup
// ---------------------------------------------------------------------------

// CleanupConnection cleans up all state for a disconnected connection.
// All channel closes are tracked in a set to prevent double-close panics
// in case a waiter appears in both the enqueued map and the resource's waiter
// queue (which happens during two-phase acquire).
func (lm *LockManager) CleanupConnection(connID uint64) {
	if !lm.cfg.AutoReleaseOnDisconnect {
		return
	}

	lm.mu.Lock()
	defer lm.mu.Unlock()

	closed := make(map[chan string]struct{})

	// Clean up two-phase enqueued state
	var enqueuedKeys []string
	for ck := range lm.connEnqueued {
		if ck.ConnID == connID {
			enqueuedKeys = append(enqueuedKeys, ck.Key)
		}
	}
	for _, key := range enqueuedKeys {
		es := lm.connEnqueued[connKey{ConnID: connID, Key: key}]
		delete(lm.connEnqueued, connKey{ConnID: connID, Key: key})
		if es != nil && es.waiter != nil {
			if _, already := closed[es.waiter.ch]; !already {
				close(es.waiter.ch)
				closed[es.waiter.ch] = struct{}{}
			}
			if st := lm.resources[key]; st != nil {
				st.Waiters = removeWaiter(st.Waiters, es.waiter)
			}
		}
	}

	// Cancel pending waiters from single-phase acquire path
	for _, st := range lm.resources {
		st.Waiters = removeWaitersByConn(st.Waiters, connID, closed)
	}

	// Release owned slots
	if owned, ok := lm.connOwned[connID]; ok {
		for key, tokens := range owned {
			st := lm.resources[key]
			if st == nil {
				continue
			}
			for token := range tokens {
				h, ok := st.Holders[token]
				if !ok {
					continue
				}
				if h.connID != connID {
					continue
				}
				lm.log.Warn("disconnect cleanup: releasing",
					"key", key, "conn_id", connID)
				delete(st.Holders, token)
			}
			st.LastActivity = time.Now()
			lm.grantNextWaiterLocked(key, st)
		}
	}
	delete(lm.connOwned, connID)
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
			lm.mu.Lock()
			now := time.Now()
			for key, st := range lm.resources {
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
			lm.mu.Unlock()
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
			lm.mu.Lock()
			now := time.Now()
			var expired []string
			for key, st := range lm.resources {
				idle := now.Sub(st.LastActivity)
				if idle > lm.cfg.GCMaxIdleTime && len(st.Holders) == 0 && len(st.Waiters) == 0 {
					expired = append(expired, key)
				}
			}
			for _, key := range expired {
				lm.log.Debug("GC: pruning unused state", "key", key)
				delete(lm.resources, key)
			}
			lm.mu.Unlock()
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

type Stats struct {
	Connections    int64      `json:"connections"`
	Locks          []LockInfo `json:"locks"`
	Semaphores     []SemInfo  `json:"semaphores"`
	IdleLocks      []IdleInfo `json:"idle_locks"`
	IdleSemaphores []IdleInfo `json:"idle_semaphores"`
}

// Stats returns a snapshot of the current lock manager state.
// Locks (Limit==1) and semaphores (Limit>1) are reported separately
// for backward compatibility.
func (lm *LockManager) Stats(connections int64) *Stats {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	now := time.Now()
	s := &Stats{
		Connections:    connections,
		Locks:          []LockInfo{},
		Semaphores:     []SemInfo{},
		IdleLocks:      []IdleInfo{},
		IdleSemaphores: []IdleInfo{},
	}

	for key, st := range lm.resources {
		if st.Limit == 1 {
			// Report as a lock
			if len(st.Holders) > 0 {
				// Exactly one holder for a lock
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
					Waiters:         len(st.Waiters),
				})
			} else if len(st.Waiters) > 0 {
				s.Locks = append(s.Locks, LockInfo{
					Key:     key,
					Waiters: len(st.Waiters),
				})
			} else {
				s.IdleLocks = append(s.IdleLocks, IdleInfo{
					Key:   key,
					IdleS: now.Sub(st.LastActivity).Seconds(),
				})
			}
		} else {
			// Report as a semaphore
			if len(st.Holders) > 0 {
				s.Semaphores = append(s.Semaphores, SemInfo{
					Key:     key,
					Limit:   st.Limit,
					Holders: len(st.Holders),
					Waiters: len(st.Waiters),
				})
			} else if len(st.Waiters) > 0 {
				s.Semaphores = append(s.Semaphores, SemInfo{
					Key:     key,
					Limit:   st.Limit,
					Waiters: len(st.Waiters),
				})
			} else {
				s.IdleSemaphores = append(s.IdleSemaphores, IdleInfo{
					Key:   key,
					IdleS: now.Sub(st.LastActivity).Seconds(),
				})
			}
		}
	}

	return s
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// ResetLeaseForTest forces all holders of a key to expire immediately (for testing only).
func (lm *LockManager) ResetLeaseForTest(key string) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	if st, ok := lm.resources[key]; ok {
		for _, h := range st.Holders {
			h.leaseExpires = time.Now().Add(-1 * time.Second)
		}
	}
}

// ResetForTest clears all state (for testing only).
func (lm *LockManager) ResetForTest() {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.resources = make(map[string]*ResourceState)
	lm.connOwned = make(map[uint64]map[string]map[string]struct{})
	lm.connEnqueued = make(map[connKey]*enqueuedState)
}
