package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mtingers/dflockd/internal/lock"
	"github.com/mtingers/dflockd/internal/server"
)

// Session represents one HTTP-side virtual connection. Two-phase
// operations rely on a stable connID across requests, so the session
// holds one and is the unit of cleanup. The session has no transport
// of its own — it's pure metadata. Lock operations call LockManager
// methods directly using session.ConnID.
type Session struct {
	ID       string
	ConnID   uint64
	OwnerIP  string
	lastSeen atomic.Int64 // unix ns
}

// Touch records that this session was just used. Sessions whose
// lastSeen falls behind 2× the configured idle timeout are reaped by
// the sweeper.
func (s *Session) Touch() { s.lastSeen.Store(time.Now().UnixNano()) }

// SessionStore tracks active sessions, enforces caps, and runs the
// idle sweeper. Construction starts the sweeper; Shutdown stops it.
type SessionStore struct {
	srv         *server.Server
	idleTimeout time.Duration
	max         int
	maxPerIP    int

	mu       sync.Mutex
	sessions map[string]*Session
	ipCounts map[string]int

	ctx    context.Context
	cancel context.CancelFunc

	sweeperDone chan struct{}
}

// Errors returned to handlers.
var (
	ErrSessionGone      = errors.New("session gone")
	ErrMaxSessions      = errors.New("max sessions reached")
	ErrMaxSessionsPerIP = errors.New("max sessions per IP reached")
	ErrShuttingDown     = errors.New("shutting down")
)

// NewSessionStore creates a store and starts its idle sweeper.
func NewSessionStore(parent context.Context, srv *server.Server, idleTimeout time.Duration, max, maxPerIP int) *SessionStore {
	ctx, cancel := context.WithCancel(parent)
	st := &SessionStore{
		srv:         srv,
		idleTimeout: idleTimeout,
		max:         max,
		maxPerIP:    maxPerIP,
		sessions:    make(map[string]*Session),
		ipCounts:    make(map[string]int),
		ctx:         ctx,
		cancel:      cancel,
		sweeperDone: make(chan struct{}),
	}
	go st.sweeperLoop()
	return st
}

// Shutdown closes every session (cleaning up its lock state via the
// LockManager) and stops the idle sweeper.
func (st *SessionStore) Shutdown() {
	st.cancel()
	st.mu.Lock()
	sessions := make([]*Session, 0, len(st.sessions))
	for _, s := range st.sessions {
		sessions = append(sessions, s)
	}
	st.sessions = make(map[string]*Session)
	st.ipCounts = make(map[string]int)
	st.mu.Unlock()

	for _, s := range sessions {
		st.srv.LockManager().CleanupConnection(s.ConnID)
	}
	<-st.sweeperDone
}

// Create mints a new session for ownerIP. Returns ErrShuttingDown,
// ErrMaxSessions, or ErrMaxSessionsPerIP on failure.
func (st *SessionStore) Create(ownerIP string) (*Session, error) {
	if st.ctx.Err() != nil {
		return nil, ErrShuttingDown
	}
	id, err := mintSessionID()
	if err != nil {
		return nil, err
	}
	s := &Session{
		ID:      id,
		ConnID:  st.srv.NextConnID(),
		OwnerIP: ownerIP,
	}
	s.Touch()

	st.mu.Lock()
	if st.ctx.Err() != nil {
		st.mu.Unlock()
		return nil, ErrShuttingDown
	}
	if st.max > 0 && len(st.sessions) >= st.max {
		st.mu.Unlock()
		return nil, ErrMaxSessions
	}
	if st.maxPerIP > 0 && ownerIP != "" && st.ipCounts[ownerIP] >= st.maxPerIP {
		st.mu.Unlock()
		return nil, ErrMaxSessionsPerIP
	}
	st.sessions[id] = s
	if ownerIP != "" {
		st.ipCounts[ownerIP]++
	}
	st.mu.Unlock()
	return s, nil
}

// Lookup returns the session for id and refreshes its lastSeen, or
// ErrSessionGone. The lastSeen update happens under the same lock as
// the existence check so the sweeper can't reap a freshly-touched
// session out from under a concurrent caller.
func (st *SessionStore) Lookup(id string) (*Session, error) {
	st.mu.Lock()
	s, ok := st.sessions[id]
	if !ok {
		st.mu.Unlock()
		return nil, ErrSessionGone
	}
	s.Touch()
	st.mu.Unlock()
	return s, nil
}

// Delete removes the session and runs lock-state cleanup. Idempotent.
func (st *SessionStore) Delete(id string) error {
	st.mu.Lock()
	s, ok := st.sessions[id]
	if ok {
		delete(st.sessions, id)
		if s.OwnerIP != "" {
			st.ipCounts[s.OwnerIP]--
			if st.ipCounts[s.OwnerIP] <= 0 {
				delete(st.ipCounts, s.OwnerIP)
			}
		}
	}
	st.mu.Unlock()
	if !ok {
		return ErrSessionGone
	}
	st.srv.LockManager().CleanupConnection(s.ConnID)
	return nil
}

// Count returns the number of active sessions.
func (st *SessionStore) Count() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	return len(st.sessions)
}

// IdleTimeout reports the advisory idle timeout that's surfaced to
// clients in the create-session response.
func (st *SessionStore) IdleTimeout() time.Duration {
	return st.idleTimeout
}

// LockManager returns the underlying lock manager. Handlers go through
// this rather than reaching into srv directly so future changes (e.g.
// metrics interception) only need to land in one place.
func (st *SessionStore) LockManager() *lock.LockManager {
	return st.srv.LockManager()
}

// ConnCount returns active TCP-side connections; handlers add it to
// the HTTP session count for the /v1/stats Connections field.
func (st *SessionStore) ConnCount() int64 {
	return st.srv.ConnCount()
}

// sweeperLoop reaps sessions whose lastSeen has fallen behind the
// idle cutoff. Cutoff is 2× idleTimeout to give clients a grace
// window past the timeout we advertise.
func (st *SessionStore) sweeperLoop() {
	defer close(st.sweeperDone)
	interval := st.idleTimeout
	if interval <= 0 {
		interval = 10 * time.Second
	}
	if interval > 30*time.Second {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-st.ctx.Done():
			return
		case <-ticker.C:
			st.sweepOnce(time.Now())
		}
	}
}

// sweepOnce performs one idle-pass. Exposed for tests.
func (st *SessionStore) sweepOnce(now time.Time) {
	if st.idleTimeout <= 0 {
		return
	}
	cutoff := now.Add(-2 * st.idleTimeout)

	var doomed []*Session
	st.mu.Lock()
	for id, s := range st.sessions {
		last := time.Unix(0, s.lastSeen.Load())
		if last.Before(cutoff) {
			doomed = append(doomed, s)
			delete(st.sessions, id)
			if s.OwnerIP != "" {
				st.ipCounts[s.OwnerIP]--
				if st.ipCounts[s.OwnerIP] <= 0 {
					delete(st.ipCounts, s.OwnerIP)
				}
			}
		}
	}
	st.mu.Unlock()

	for _, s := range doomed {
		st.srv.LockManager().CleanupConnection(s.ConnID)
	}
}

// mintSessionID returns a 32-char lowercase hex id.
func mintSessionID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// IsValidSessionID matches mintSessionID's format: 32 lowercase hex chars.
func IsValidSessionID(id string) bool {
	if len(id) != 32 {
		return false
	}
	for _, c := range id {
		if !(c >= '0' && c <= '9') && !(c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
