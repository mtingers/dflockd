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
	ErrMaxLocks    = errors.New("max locks reached")
	ErrNotEnqueued = errors.New("not enqueued for this key")
)

type connKey struct {
	ConnID uint64
	Key    string
}

type Waiter struct {
	Ch       chan string
	ConnID   uint64
	LeaseTTL time.Duration
	Enqueued time.Time
}

type LockState struct {
	OwnerToken   string
	OwnerConnID  uint64
	LeaseExpires time.Time
	Waiters      []*Waiter
	LastActivity time.Time
}

type EnqueuedState struct {
	Waiter   *Waiter
	Token    string
	LeaseTTL time.Duration
}

type LockManager struct {
	mu           sync.Mutex
	locks        map[string]*LockState
	connOwned    map[uint64]map[string]struct{}
	connEnqueued map[connKey]*EnqueuedState
	cfg          *config.Config
	log          *slog.Logger
}

func NewLockManager(cfg *config.Config, log *slog.Logger) *LockManager {
	return &LockManager{
		locks:        make(map[string]*LockState),
		connOwned:    make(map[uint64]map[string]struct{}),
		connEnqueued: make(map[connKey]*EnqueuedState),
		cfg:          cfg,
		log:          log,
	}
}

func newToken() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("crypto/rand failed: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}

func (lm *LockManager) connAddOwned(connID uint64, key string) {
	s, ok := lm.connOwned[connID]
	if !ok {
		s = make(map[string]struct{})
		lm.connOwned[connID] = s
	}
	s[key] = struct{}{}
}

func (lm *LockManager) connRemoveOwned(connID uint64, key string) {
	if connID == 0 {
		return
	}
	s, ok := lm.connOwned[connID]
	if !ok {
		return
	}
	delete(s, key)
	if len(s) == 0 {
		delete(lm.connOwned, connID)
	}
}

// grantNextWaiterLocked grants the lock to the next waiter in FIFO order.
// Must be called with lm.mu held.
func (lm *LockManager) grantNextWaiterLocked(key string, st *LockState) {
	for len(st.Waiters) > 0 {
		w := st.Waiters[0]
		st.Waiters = st.Waiters[1:]
		// Try to send token; if channel is closed (cancelled), skip.
		token := newToken()
		select {
		case w.Ch <- token:
			st.OwnerToken = token
			st.OwnerConnID = w.ConnID
			st.LeaseExpires = time.Now().Add(w.LeaseTTL)
			st.LastActivity = time.Now()
			lm.connAddOwned(w.ConnID, key)
			return
		default:
			// Channel closed or full — skip this waiter
			continue
		}
	}
	// No waiters: unlock
	st.OwnerToken = ""
	st.OwnerConnID = 0
	st.LeaseExpires = time.Time{}
	st.LastActivity = time.Now()
}

func (lm *LockManager) getOrCreateLocked(key string) (*LockState, error) {
	st, ok := lm.locks[key]
	if ok {
		return st, nil
	}
	if len(lm.locks) >= lm.cfg.MaxLocks {
		return nil, ErrMaxLocks
	}
	st = &LockState{LastActivity: time.Now()}
	lm.locks[key] = st
	return st, nil
}

// FIFOAcquire is the single-phase lock acquire (command "l").
func (lm *LockManager) FIFOAcquire(key string, timeout, leaseTTL time.Duration, connID uint64) (string, error) {
	waiter := &Waiter{
		Ch:       make(chan string, 1),
		ConnID:   connID,
		LeaseTTL: leaseTTL,
		Enqueued: time.Now(),
	}

	lm.mu.Lock()
	st, err := lm.getOrCreateLocked(key)
	if err != nil {
		lm.mu.Unlock()
		return "", err
	}

	st.LastActivity = time.Now()

	// Fast path: free and no waiters
	if st.OwnerToken == "" && len(st.Waiters) == 0 {
		token := newToken()
		st.OwnerToken = token
		st.OwnerConnID = connID
		st.LeaseExpires = time.Now().Add(leaseTTL)
		st.LastActivity = time.Now()
		lm.connAddOwned(connID, key)
		lm.mu.Unlock()
		return token, nil
	}

	// Slow path: enqueue and wait
	st.Waiters = append(st.Waiters, waiter)
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
	case token := <-waiter.Ch:
		lm.mu.Lock()
		if s := lm.locks[key]; s != nil {
			s.LastActivity = time.Now()
		}
		lm.mu.Unlock()
		return token, nil

	case <-timer.C:
		// Timeout — remove from queue
		lm.mu.Lock()
		// Race check: token may have arrived
		select {
		case token := <-waiter.Ch:
			if s := lm.locks[key]; s != nil {
				s.LastActivity = time.Now()
			}
			lm.mu.Unlock()
			return token, nil
		default:
		}
		if s := lm.locks[key]; s != nil {
			s.LastActivity = time.Now()
			filtered := make([]*Waiter, 0, len(s.Waiters))
			for _, w := range s.Waiters {
				if w != waiter {
					filtered = append(filtered, w)
				}
			}
			s.Waiters = filtered
		}
		lm.mu.Unlock()
		return "", nil
	}
}

// FIFOEnqueue is phase 1 of two-phase acquire (command "e").
// Returns (status, token, leaseTTLSec, err).
func (lm *LockManager) FIFOEnqueue(key string, leaseTTL time.Duration, connID uint64) (string, string, int, error) {
	eqKey := connKey{ConnID: connID, Key: key}

	lm.mu.Lock()
	defer lm.mu.Unlock()

	if _, exists := lm.connEnqueued[eqKey]; exists {
		return "", "", 0, &alreadyEnqueuedError{}
	}

	st, err := lm.getOrCreateLocked(key)
	if err != nil {
		return "", "", 0, err
	}

	st.LastActivity = time.Now()
	leaseSec := int(leaseTTL / time.Second)

	// Fast path: free and no waiters
	if st.OwnerToken == "" && len(st.Waiters) == 0 {
		token := newToken()
		st.OwnerToken = token
		st.OwnerConnID = connID
		st.LeaseExpires = time.Now().Add(leaseTTL)
		st.LastActivity = time.Now()
		lm.connAddOwned(connID, key)
		lm.connEnqueued[eqKey] = &EnqueuedState{Token: token, LeaseTTL: leaseTTL}
		return "acquired", token, leaseSec, nil
	}

	// Slow path: create waiter and enqueue
	waiter := &Waiter{
		Ch:       make(chan string, 1),
		ConnID:   connID,
		LeaseTTL: leaseTTL,
		Enqueued: time.Now(),
	}
	st.Waiters = append(st.Waiters, waiter)
	lm.connEnqueued[eqKey] = &EnqueuedState{Waiter: waiter, LeaseTTL: leaseTTL}
	return "queued", "", 0, nil
}

type alreadyEnqueuedError struct{}

func (e *alreadyEnqueuedError) Error() string {
	return "already enqueued for this key"
}

// IsAlreadyEnqueued checks if the error is an already-enqueued error.
func IsAlreadyEnqueued(err error) bool {
	var ae *alreadyEnqueuedError
	return errors.As(err, &ae)
}

// FIFOWait is phase 2 of two-phase acquire (command "w").
// Returns (token, leaseTTLSec, err). Empty token means timeout.
func (lm *LockManager) FIFOWait(key string, timeout time.Duration, connID uint64) (string, int, error) {
	eqKey := connKey{ConnID: connID, Key: key}

	lm.mu.Lock()
	es, ok := lm.connEnqueued[eqKey]
	if !ok {
		lm.mu.Unlock()
		return "", 0, ErrNotEnqueued
	}
	lm.mu.Unlock()

	leaseTTL := es.LeaseTTL
	leaseSec := int(leaseTTL / time.Second)

	// Fast path: already acquired during enqueue
	if es.Token != "" {
		lm.mu.Lock()
		delete(lm.connEnqueued, eqKey)
		st := lm.locks[key]
		if st != nil && st.OwnerToken == es.Token {
			// Verify lock still held (lease may have expired)
			if !st.LeaseExpires.IsZero() && !time.Now().Before(st.LeaseExpires) {
				lm.mu.Unlock()
				return "", 0, nil
			}
			// Reset lease
			st.LeaseExpires = time.Now().Add(leaseTTL)
			st.LastActivity = time.Now()
			lm.mu.Unlock()
			return es.Token, leaseSec, nil
		}
		// Lock was lost
		lm.mu.Unlock()
		return "", 0, nil
	}

	// Slow path: waiter is pending
	waiter := es.Waiter

	var timer *time.Timer
	if timeout > 0 {
		timer = time.NewTimer(timeout)
	} else {
		timer = time.NewTimer(0)
	}
	defer timer.Stop()

	select {
	case token, ok := <-waiter.Ch:
		if !ok {
			// Channel closed (disconnect cleanup)
			lm.mu.Lock()
			delete(lm.connEnqueued, eqKey)
			lm.mu.Unlock()
			return "", 0, nil
		}
		lm.mu.Lock()
		delete(lm.connEnqueued, eqKey)
		if st := lm.locks[key]; st != nil {
			st.LeaseExpires = time.Now().Add(leaseTTL)
			st.LastActivity = time.Now()
		}
		lm.mu.Unlock()
		return token, leaseSec, nil

	case <-timer.C:
		lm.mu.Lock()
		delete(lm.connEnqueued, eqKey)
		// Race check: token may have arrived
		select {
		case token, ok := <-waiter.Ch:
			if ok && token != "" {
				if st := lm.locks[key]; st != nil {
					st.LeaseExpires = time.Now().Add(leaseTTL)
					st.LastActivity = time.Now()
				}
				lm.mu.Unlock()
				return token, leaseSec, nil
			}
		default:
		}
		// Remove from queue
		if st := lm.locks[key]; st != nil {
			st.LastActivity = time.Now()
			filtered := make([]*Waiter, 0, len(st.Waiters))
			for _, w := range st.Waiters {
				if w != waiter {
					filtered = append(filtered, w)
				}
			}
			st.Waiters = filtered
		}
		lm.mu.Unlock()
		return "", 0, nil
	}
}

// FIFORelease releases a lock if the token matches (command "r").
func (lm *LockManager) FIFORelease(key, token string) bool {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	st := lm.locks[key]
	if st == nil {
		return false
	}

	st.LastActivity = time.Now()

	if st.OwnerToken == "" || st.OwnerToken != token {
		return false
	}

	lm.connRemoveOwned(st.OwnerConnID, key)
	lm.grantNextWaiterLocked(key, st)
	return true
}

// FIFORenew renews the lease if the token matches (command "n").
// Returns (remaining seconds, ok).
func (lm *LockManager) FIFORenew(key, token string, leaseTTL time.Duration) (int, bool) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	st := lm.locks[key]
	if st == nil {
		return 0, false
	}

	now := time.Now()
	st.LastActivity = now

	if st.OwnerToken == "" || st.OwnerToken != token {
		return 0, false
	}

	// If already expired, reject and evict
	if !st.LeaseExpires.IsZero() && !now.Before(st.LeaseExpires) {
		lm.log.Warn("renew rejected (already expired)",
			"key", key, "owner_conn", st.OwnerConnID)
		lm.connRemoveOwned(st.OwnerConnID, key)
		st.OwnerToken = ""
		st.OwnerConnID = 0
		st.LeaseExpires = time.Time{}
		st.LastActivity = now
		lm.grantNextWaiterLocked(key, st)
		return 0, false
	}

	// Reset lease
	st.LeaseExpires = now.Add(leaseTTL)
	st.LastActivity = now

	remaining := int(time.Until(st.LeaseExpires).Seconds())
	if remaining < 0 {
		remaining = 0
	}
	return remaining, true
}

// CleanupConnection cleans up all state for a disconnected connection.
func (lm *LockManager) CleanupConnection(connID uint64) {
	if !lm.cfg.AutoReleaseOnDisconnect {
		return
	}

	lm.mu.Lock()
	defer lm.mu.Unlock()

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
		if es != nil && es.Waiter != nil {
			close(es.Waiter.Ch)
			if st := lm.locks[key]; st != nil {
				filtered := make([]*Waiter, 0, len(st.Waiters))
				for _, w := range st.Waiters {
					if w != es.Waiter {
						filtered = append(filtered, w)
					}
				}
				st.Waiters = filtered
			}
		}
	}

	// Cancel pending waiters from l command path
	for _, st := range lm.locks {
		var remaining []*Waiter
		for _, w := range st.Waiters {
			if w.ConnID == connID {
				close(w.Ch)
			} else {
				remaining = append(remaining, w)
			}
		}
		st.Waiters = remaining
	}

	// Release owned locks
	keys := make([]string, 0)
	if owned, ok := lm.connOwned[connID]; ok {
		for k := range owned {
			keys = append(keys, k)
		}
	}
	if len(keys) == 0 {
		delete(lm.connOwned, connID)
		return
	}

	for _, key := range keys {
		st := lm.locks[key]
		if st == nil {
			lm.connRemoveOwned(connID, key)
			continue
		}
		if st.OwnerConnID != connID {
			lm.connRemoveOwned(connID, key)
			continue
		}
		lm.log.Warn("disconnect cleanup: releasing",
			"key", key, "conn_id", connID)
		lm.connRemoveOwned(connID, key)
		st.OwnerToken = ""
		st.OwnerConnID = 0
		st.LeaseExpires = time.Time{}
		st.LastActivity = time.Now()
		lm.grantNextWaiterLocked(key, st)
	}

	delete(lm.connOwned, connID)
}

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
			for key, st := range lm.locks {
				if st.OwnerToken == "" {
					continue
				}
				if st.LeaseExpires.IsZero() {
					continue
				}
				if !now.Before(st.LeaseExpires) {
					lm.log.Warn("lease expired",
						"key", key, "owner_conn", st.OwnerConnID)
					lm.connRemoveOwned(st.OwnerConnID, key)
					st.OwnerToken = ""
					st.OwnerConnID = 0
					st.LeaseExpires = time.Time{}
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
			for key, st := range lm.locks {
				idle := now.Sub(st.LastActivity)
				if idle > lm.cfg.GCMaxIdleTime && st.OwnerToken == "" && len(st.Waiters) == 0 {
					expired = append(expired, key)
				}
			}
			for _, key := range expired {
				lm.log.Debug("GC: pruning unused lock state", "key", key)
				delete(lm.locks, key)
			}
			lm.mu.Unlock()
		}
	}
}

// ResetForTest clears all state (for testing only).
func (lm *LockManager) ResetForTest() {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.locks = make(map[string]*LockState)
	lm.connOwned = make(map[uint64]map[string]struct{})
	lm.connEnqueued = make(map[connKey]*EnqueuedState)
}

// LocksForTest returns internal lock state (for testing only).
func (lm *LockManager) LocksForTest() map[string]*LockState {
	return lm.locks
}
