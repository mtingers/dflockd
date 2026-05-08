package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
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
//
// mu serialises lock-modifying handlers (acquire/release/renew/
// enqueue/wait) so the HTTP API behaves like a single virtual
// connection — matching the old bridge's reqMu. /ping and DELETE
// bypass it so they can't be starved by a long blocking op.
//
// inFlight tracks how many handlers are currently using this session.
// The idle sweeper skips sessions with inFlight > 0 so a long-poll
// /wait isn't reaped mid-flight; lastSeen is also refreshed on
// handler exit so the next sweep cycle sees recent activity.
//
// ctx is the session-lifetime context. sealAndDrain cancels it so
// any in-flight LockManager call wakes immediately rather than
// blocking sealAndDrain on the handler's mu. Handlers obtain a
// merged context from RequestContext() that fires on either the
// HTTP request cancelling or the session being closed.
type Session struct {
	ID       string
	ConnID   uint64
	OwnerIP  string
	mu       sync.Mutex
	closed   atomic.Bool  // true once Delete/sweep/shutdown has claimed this session
	lastSeen atomic.Int64 // unix ns
	inFlight atomic.Int64
	ctx      context.Context
	cancel   context.CancelFunc
}

// Touch records that this session was just used. Sessions whose
// lastSeen falls behind 2× the configured idle timeout are reaped by
// the sweeper.
func (s *Session) Touch() { s.lastSeen.Store(time.Now().UnixNano()) }

// BeginRequest claims the per-session mutex and bumps inFlight so the
// sweeper won't reap this session mid-handler. Returns (done, true)
// on success; the caller MUST defer done().
//
// Returns (nil, false) once the session has been closed (Delete,
// idle sweep, or shutdown). Handlers must check ok and write 410
// session_gone instead of touching LockManager — without that check
// a handler running concurrently with Delete could mint a token
// against a connID whose state has already been cleaned up.
func (s *Session) BeginRequest() (func(), bool) {
	s.inFlight.Add(1)
	s.mu.Lock()
	if s.closed.Load() {
		s.mu.Unlock()
		s.inFlight.Add(-1)
		return nil, false
	}
	return func() {
		s.mu.Unlock()
		s.lastSeen.Store(time.Now().UnixNano())
		s.inFlight.Add(-1)
	}, true
}

// RequestContext returns a context that fires when either the HTTP
// request is cancelled or the session is closed (Delete / sweep /
// shutdown). Pass this to LockManager calls so a session DELETE
// can immediately wake a long-poll /wait or /acquire instead of
// waiting for it to time out naturally. The caller MUST call the
// returned cancel.
func (s *Session) RequestContext(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(s.ctx, cancel)
	return ctx, func() { stop(); cancel() }
}

// sealAndDrain marks the session closed, wakes any LockManager call
// blocked on the session ctx, then waits for in-flight handlers to
// release s.mu. After this returns, no new handler can observe an
// open session via BeginRequest and any concurrent handler has
// finished. Safe to follow with CleanupConnection.
func (s *Session) sealAndDrain() {
	s.closed.Store(true)
	s.cancel()
	s.mu.Lock()
	s.mu.Unlock()
}

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
	cleanupConn func(uint64) error
	log         *slog.Logger
}

// Errors returned to handlers.
var (
	ErrSessionGone      = errors.New("session gone")
	ErrMaxSessions      = errors.New("max sessions reached")
	ErrMaxSessionsPerIP = errors.New("max sessions per IP reached")
	ErrShuttingDown     = errors.New("shutting down")
)

// NewSessionStore creates a store and starts its idle sweeper.
func NewSessionStore(parent context.Context, srv *server.Server, idleTimeout time.Duration, max, maxPerIP int, log *slog.Logger) *SessionStore {
	ctx, cancel := context.WithCancel(parent)
	if log == nil {
		log = slog.Default()
	}
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
		cleanupConn: srv.LockManager().CleanupConnection,
		log:         log,
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
		st.closeAndLogCleanup(s)
	}
	<-st.sweeperDone
}

// Create mints a new session for ownerIP. Returns ErrShuttingDown,
// ErrMaxSessions, or ErrMaxSessionsPerIP on failure.
func (st *SessionStore) Create(ownerIP string) (*Session, error) {
	if st.ctx.Err() != nil {
		return nil, ErrShuttingDown
	}
	s, err := newSession(st.srv, ownerIP)
	if err != nil {
		return nil, err
	}
	installed, err := st.installSession(s, ownerIP)
	if err != nil {
		s.cancel() // session never reached the map; release its ctx resources
		return nil, err
	}
	return installed, nil
}

// newSession allocates a fresh Session with a random ID and a fresh
// connID, but doesn't add it to the store.
func newSession(srv *server.Server, ownerIP string) (*Session, error) {
	id, err := mintSessionID()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Session{
		ID: id, ConnID: srv.NextConnID(), OwnerIP: ownerIP,
		ctx: ctx, cancel: cancel,
	}
	s.Touch()
	return s, nil
}

// installSession adds s to the store under st.mu, applying every
// cap. Returns the populated session or the appropriate sentinel.
func (st *SessionStore) installSession(s *Session, ownerIP string) (*Session, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if err := st.checkInstallable(ownerIP); err != nil {
		return nil, err
	}
	st.recordSession(s, ownerIP)
	return s, nil
}

func (st *SessionStore) checkInstallable(ownerIP string) error {
	if st.ctx.Err() != nil {
		return ErrShuttingDown
	}
	if st.max > 0 && len(st.sessions) >= st.max {
		return ErrMaxSessions
	}
	if st.maxPerIP > 0 && ownerIP != "" && st.ipCounts[ownerIP] >= st.maxPerIP {
		return ErrMaxSessionsPerIP
	}
	return nil
}

func (st *SessionStore) recordSession(s *Session, ownerIP string) {
	st.sessions[s.ID] = s
	if ownerIP != "" {
		st.ipCounts[ownerIP]++
	}
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
//
// Coordination: after pulling the session out of the map, sealAndDrain
// blocks until any concurrently-running handler releases s.mu. This
// closes the race where a handler that already passed Lookup but
// hadn't yet finished its lm.Acquire could mint a token whose
// connID we're about to wipe in CleanupConnection.
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
	s.sealAndDrain()
	return st.cleanupSession(s)
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

// ConnCount returns active TCP-side connections only.
func (st *SessionStore) ConnCount() int64 {
	return st.srv.ConnCount()
}

// TotalConnCount returns active TCP connections plus any extras the
// server is tracking (HTTP sessions, when the HTTP API has registered
// itself). Handlers use this for the /v1/stats Connections field so
// the value matches the TCP "stats" command.
func (st *SessionStore) TotalConnCount() int64 {
	return st.srv.TotalConnCount()
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
	doomed := st.collectIdleSessions(now.Add(-2 * st.idleTimeout))
	for _, s := range doomed {
		st.closeAndLogCleanup(s)
	}
}

func (st *SessionStore) closeAndLogCleanup(s *Session) {
	if err := st.closeSession(s); err != nil {
		st.logCleanupErr(s, err)
	}
}

func (st *SessionStore) closeSession(s *Session) error {
	s.sealAndDrain()
	return st.cleanupSession(s)
}

func (st *SessionStore) cleanupSession(s *Session) error {
	if st.cleanupConn == nil {
		return nil
	}
	return st.cleanupConn(s.ConnID)
}

func (st *SessionStore) logCleanupErr(s *Session, err error) {
	st.log.Error("session cleanup failed", "session_id", s.ID, "conn_id", s.ConnID, "err", err)
}

// collectIdleSessions removes every reapable session from the map
// and returns it. In-flight handlers protect their session from
// being reaped (long-poll /wait would otherwise see session_gone).
func (st *SessionStore) collectIdleSessions(cutoff time.Time) []*Session {
	var doomed []*Session
	st.mu.Lock()
	defer st.mu.Unlock()
	for id, s := range st.sessions {
		if isReapable(s, cutoff) {
			st.removeFromMap(id, s)
			doomed = append(doomed, s)
		}
	}
	return doomed
}

func isReapable(s *Session, cutoff time.Time) bool {
	if s.inFlight.Load() > 0 {
		return false
	}
	return time.Unix(0, s.lastSeen.Load()).Before(cutoff)
}

func (st *SessionStore) removeFromMap(id string, s *Session) {
	delete(st.sessions, id)
	if s.OwnerIP == "" {
		return
	}
	st.ipCounts[s.OwnerIP]--
	if st.ipCounts[s.OwnerIP] <= 0 {
		delete(st.ipCounts, s.OwnerIP)
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
