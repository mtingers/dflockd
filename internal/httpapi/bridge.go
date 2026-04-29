// Package httpapi implements an HTTP REST + SSE layer on top of the existing
// line-based TCP protocol. Each HTTP session owns an in-process virtual
// connection (net.Pipe) that feeds into the unchanged server.ServeConn
// handler, so all lock semantics — FIFO ordering, leases, two-phase acquire,
// signal pub/sub, auto-release on disconnect — are inherited without
// duplication.
//
// Architecture:
//
//	POST /v1/locks/foo        session map
//	    │                          │
//	    ▼                          ▼
//	writeCmd("l\nfoo\n10\n") ──► client side of net.Pipe
//	                                  │
//	                                  ▼  (server side)
//	                               server.ServeConn(ctx, pipeServer, connID)
//	                                  │
//	                                  ▼
//	                               LockManager.Acquire(...)
//
// Command responses and asynchronous "sig ..." push messages share the same
// stream, so a per-session multiplexer goroutine splits them into respCh
// (consumed by writeCmd) and sigCh (consumed by SSE handlers).
package httpapi

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mtingers/dflockd/internal/config"
	"github.com/mtingers/dflockd/internal/lock"
	"github.com/mtingers/dflockd/internal/server"
	"github.com/mtingers/dflockd/internal/signal"
)

// sigChBuffer is the buffer size for a session's per-bridge sigCh.
// Matches client.signalChanBuffer and server.writeChBuffer so no stage
// of the signal pipeline bottlenecks independently.
const sigChBuffer = 64

// Bridge owns the set of active HTTP sessions and hands out new ones from
// /v1/sessions. It shares the LockManager, signal Manager, and connID
// counter with the TCP server so that cross-transport ordering (FIFO, etc.)
// is preserved.
type Bridge struct {
	srv       *server.Server
	cfg       *config.Config
	log       *slog.Logger
	authToken string

	idleTimeout      time.Duration
	maxSessions      int
	maxSessionsPerIP int

	mu            sync.Mutex
	sessions      map[string]*session
	sessionIPs    map[string]string
	sessionCounts map[string]int

	// Lifecycle: ctx is derived from the parent context passed to the
	// HTTP server. Cancelling it tears down every session's virtual conn.
	ctx    context.Context
	cancel context.CancelFunc

	// Background sweeper for orphaned sessions.
	sweeperDone chan struct{}
}

// NewBridge creates a Bridge wrapping the given server. The bridge uses the
// server's LockManager and signal Manager via exported accessors.
func NewBridge(parent context.Context, srv *server.Server, cfg *config.Config, log *slog.Logger, idleTimeout time.Duration, maxSessions int) *Bridge {
	ctx, cancel := context.WithCancel(parent)
	b := &Bridge{
		srv:              srv,
		cfg:              cfg,
		log:              log,
		authToken:        cfg.AuthToken,
		idleTimeout:      idleTimeout,
		maxSessions:      maxSessions,
		maxSessionsPerIP: cfg.HTTPMaxSessionsPerIP,
		sessions:         make(map[string]*session),
		sessionIPs:       make(map[string]string),
		sessionCounts:    make(map[string]int),
		ctx:              ctx,
		cancel:           cancel,
		sweeperDone:      make(chan struct{}),
	}
	go b.sweeperLoop()
	return b
}

// Shutdown closes all sessions and waits for their handler goroutines to
// drain. Safe to call multiple times.
func (b *Bridge) Shutdown() {
	b.cancel()

	b.mu.Lock()
	sessions := make([]*session, 0, len(b.sessions))
	for _, s := range b.sessions {
		sessions = append(sessions, s)
	}
	b.sessions = make(map[string]*session)
	b.sessionIPs = make(map[string]string)
	b.sessionCounts = make(map[string]int)
	b.mu.Unlock()

	for _, s := range sessions {
		s.close()
	}
	<-b.sweeperDone
}

// ---------------------------------------------------------------------------
// Session
// ---------------------------------------------------------------------------

// ErrSessionGone is returned when a session ID doesn't resolve, either
// because it was never created or has been swept.
var ErrSessionGone = errors.New("session gone")

// ErrMaxSessions is returned when the bridge has reached its session cap.
var ErrMaxSessions = errors.New("max sessions reached")

// ErrMaxSessionsPerIP is returned when one remote IP has reached its session cap.
var ErrMaxSessionsPerIP = errors.New("max sessions per ip reached")

// ErrBridgeShutdown is returned when CreateSession races a bridge Shutdown.
// Distinct from ErrSessionGone (which means a known session no longer exists)
// so callers can tell "the bridge is going away" from "this id is invalid".
var ErrBridgeShutdown = errors.New("bridge is shutting down")

// session represents a single HTTP-originated virtual connection into the
// protocol handler. It owns:
//   - one half of a net.Pipe (the other half is consumed by ServeConn)
//   - a multiplexer goroutine that splits responses vs sig push
//   - a ServeConn goroutine handling protocol commands
//   - the connID allocated for that virtual conn
type session struct {
	id     string
	connID uint64
	log    *slog.Logger

	clientSide net.Conn      // our end of the pipe; we Write commands, Read responses
	serverSide net.Conn      // handed to ServeConn; we don't touch it after
	reader     *bufio.Reader // wraps clientSide

	reqMu  sync.Mutex  // serializes one protocol command at a time
	respCh chan string // size 1; multiplexer → writeCmd
	sigCh  chan string // size 64; multiplexer → SSE handler (may drop on overflow)

	// ctx is the session's teardown signal. cancel() fires on close().
	// The multiplex and command selects block on ctx.Done() to detect
	// teardown. Previously we also maintained a separate `closed` chan;
	// the two signalled identically, so the chan was removed.
	ctx    context.Context
	cancel context.CancelFunc

	closeOnce sync.Once
	serveDone chan struct{} // closed when the ServeConn goroutine returns
	muxDone   chan struct{} // closed when the multiplexer goroutine returns

	lastSeen atomic.Int64 // unix nanoseconds of most recent activity
	inFlight atomic.Int64 // active HTTP requests using command()
	dead     atomic.Bool  // set when the multiplexer observes EOF
}

// CreateSession mints a fresh session, spins up the virtual connection, and
// returns the session ID. If the server has an auth token configured, the
// bridge performs protocol-level authentication transparently so the HTTP
// client never has to send the `auth` command.
func (b *Bridge) CreateSession(remoteIP ...string) (string, error) {
	ownerIP := ""
	if len(remoteIP) > 0 {
		ownerIP = remoteIP[0]
	}
	b.mu.Lock()
	if b.ctx.Err() != nil {
		b.mu.Unlock()
		return "", ErrBridgeShutdown
	}
	doomed := b.pruneDeadSessionsLocked()
	if b.maxSessions > 0 && len(b.sessions) >= b.maxSessions {
		b.mu.Unlock()
		closeSessions(doomed)
		return "", ErrMaxSessions
	}
	if b.maxSessionsPerIP > 0 && ownerIP != "" && b.sessionCounts[ownerIP] >= b.maxSessionsPerIP {
		b.mu.Unlock()
		closeSessions(doomed)
		return "", ErrMaxSessionsPerIP
	}
	b.mu.Unlock()
	closeSessions(doomed)

	id, err := mintSessionID()
	if err != nil {
		return "", fmt.Errorf("mint session id: %w", err)
	}

	s, err := b.newSession(id)
	if err != nil {
		return "", err
	}

	// Perform protocol-level auth if the server requires it. The bridge
	// injects the already-configured token so the HTTP caller only deals
	// with its own `Authorization: Bearer` header.
	if b.authToken != "" {
		resp, err := s.command("auth", "_", b.authToken)
		if err != nil {
			s.close()
			return "", fmt.Errorf("bridge auth: %w", err)
		}
		if resp != "ok" {
			s.close()
			return "", fmt.Errorf("bridge auth: unexpected response %q", resp)
		}
	}

	b.mu.Lock()
	// If Shutdown ran while we were creating + authenticating, b.ctx is
	// cancelled and Shutdown has already snapshotted/emptied b.sessions.
	// Adding now would leak a session whose sessionCtx is already cancelled
	// but which no one owns — its ServeConn would only exit after the
	// per-request ReadTimeout fires. Detect and refuse here.
	if b.ctx.Err() != nil {
		b.mu.Unlock()
		s.close()
		return "", ErrBridgeShutdown
	}
	doomed = b.pruneDeadSessionsLocked()
	// Re-check the cap under lock to avoid a race that overshoots.
	if b.maxSessions > 0 && len(b.sessions) >= b.maxSessions {
		b.mu.Unlock()
		closeSessions(doomed)
		s.close()
		return "", ErrMaxSessions
	}
	if b.maxSessionsPerIP > 0 && ownerIP != "" && b.sessionCounts[ownerIP] >= b.maxSessionsPerIP {
		b.mu.Unlock()
		closeSessions(doomed)
		s.close()
		return "", ErrMaxSessionsPerIP
	}
	b.sessions[id] = s
	if ownerIP != "" {
		b.sessionIPs[id] = ownerIP
		b.sessionCounts[ownerIP]++
	}
	b.mu.Unlock()
	closeSessions(doomed)
	return id, nil
}

// LookupSession returns the session for the given ID or ErrSessionGone.
// It also refreshes the session's lastSeen timestamp.
//
// The lastSeen refresh must happen under b.mu so that the idle sweeper —
// which iterates the map while holding b.mu — cannot observe a stale
// timestamp for a session that was just looked up and is about to be
// used. Without this, a sweeper tick falling between the map read and
// the Store could reap a session the caller has already resolved.
func (b *Bridge) LookupSession(id string) (*session, error) {
	b.mu.Lock()
	s, ok := b.sessions[id]
	if !ok {
		b.mu.Unlock()
		return nil, ErrSessionGone
	}
	if s.dead.Load() {
		b.deleteSessionLocked(id)
		b.mu.Unlock()
		s.close()
		return nil, ErrSessionGone
	}
	s.lastSeen.Store(time.Now().UnixNano())
	b.mu.Unlock()
	return s, nil
}

func (b *Bridge) pruneDeadSessionsLocked() []*session {
	var doomed []*session
	for id, s := range b.sessions {
		if s.dead.Load() {
			doomed = append(doomed, s)
			b.deleteSessionLocked(id)
		}
	}
	return doomed
}

func closeSessions(sessions []*session) {
	for _, s := range sessions {
		s.close()
	}
}

// DeleteSession closes the session synchronously, which triggers
// CleanupConnection in the protocol handler. Returns ErrSessionGone if the
// ID is unknown.
func (b *Bridge) DeleteSession(id string) error {
	b.mu.Lock()
	s, ok := b.sessions[id]
	if ok {
		b.deleteSessionLocked(id)
	}
	b.mu.Unlock()
	if !ok {
		return ErrSessionGone
	}
	s.close()
	return nil
}

func (b *Bridge) deleteSessionLocked(id string) {
	delete(b.sessions, id)
	if ip := b.sessionIPs[id]; ip != "" {
		delete(b.sessionIPs, id)
		b.sessionCounts[ip]--
		if b.sessionCounts[ip] <= 0 {
			delete(b.sessionCounts, ip)
		}
	}
}

// SessionCount returns the current number of active sessions. Used by
// tests and introspection.
func (b *Bridge) SessionCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.sessions)
}

// newSession wires up the pipes and spawns the handler goroutines.
func (b *Bridge) newSession(id string) (*session, error) {
	clientSide, serverSide := net.Pipe()

	connID := b.srv.NextConnID()
	sessionCtx, sessionCancel := context.WithCancel(b.ctx)
	s := &session{
		id:         id,
		connID:     connID,
		log:        b.log.With("session", id[:8], "conn_id", connID),
		clientSide: clientSide,
		serverSide: serverSide,
		reader:     bufio.NewReader(clientSide),
		respCh:     make(chan string, 1),
		sigCh:      make(chan string, sigChBuffer),
		ctx:        sessionCtx,
		cancel:     sessionCancel,
		serveDone:  make(chan struct{}),
		muxDone:    make(chan struct{}),
	}
	s.lastSeen.Store(time.Now().UnixNano())

	// ServeConn goroutine: reads protocol from serverSide, talks to
	// LockManager/signal.Manager. Exits when the pipe is closed or ctx
	// cancels.
	go func() {
		defer close(s.serveDone)
		b.srv.ServeConn(sessionCtx, serverSide, connID)
	}()

	// Multiplexer: reads one line at a time from clientSide. Lines starting
	// with "sig " are push frames and go to sigCh; anything else is a
	// command response and goes to respCh. Exits when the pipe EOFs.
	go s.multiplex()

	return s, nil
}

// multiplex runs as a goroutine per session.
//
// sigCh is closed exactly once, via the deferred cleanup. All exit paths
// (pipe EOF, s.ctx cancelled from session.close()) go through the same
// defer, so the SSE handler's `<-s.signals()` always observes the close
// and the handler doesn't need to wait for its next ping tick to notice
// the session died.
func (s *session) multiplex() {
	defer close(s.muxDone)
	defer close(s.sigCh)
	defer s.dead.Store(true)
	for {
		line, err := readLine(s.reader)
		if err != nil {
			// EOF / pipe closed — normal session teardown.
			return
		}
		if strings.HasPrefix(line, "sig ") {
			select {
			case s.sigCh <- line:
			case <-s.ctx.Done():
				return
			default:
				// SSE consumer is slow; drop silently. Matches the TCP
				// client library's behavior (client.go:1394). Caller can
				// observe via an eventual disconnect if the backlog grows.
			}
			continue
		}
		select {
		case s.respCh <- line:
		case <-s.ctx.Done():
			return
		}
	}
}

// command writes a 3-line protocol request and reads exactly one response
// line. Serializes through reqMu so that the respCh buffer (size 1) is
// always drained by the corresponding caller.
//
// command uses a background context. Callers that need HTTP request
// cancellation to abort a blocking protocol operation should use
// commandContext instead; cancellation closes this virtual connection,
// which is the protocol-level cancellation mechanism.
func (s *session) command(cmd, key, arg string) (string, error) {
	return s.commandContext(context.Background(), cmd, key, arg)
}

func (s *session) commandContext(ctx context.Context, cmd, key, arg string) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if s.dead.Load() {
		return "", ErrSessionGone
	}

	s.inFlight.Add(1)
	defer s.inFlight.Add(-1)
	// Wrap in a closure so time.Now() is evaluated when the defer fires
	// (i.e. on function exit), not when the defer is registered. Without
	// the closure, deferred-arg evaluation would capture the *entry* time,
	// so a long-poll Wait would leave lastSeen pointing far into the past
	// — and the bridge sweeper could reap the session moments after a
	// successful response.
	defer func() { s.lastSeen.Store(time.Now().UnixNano()) }()

	s.reqMu.Lock()
	defer s.reqMu.Unlock()

	// Drain any stale response that a previous abandoned caller left behind.
	// In practice this shouldn't happen because reqMu serializes, but the
	// drain is cheap and defensive.
	select {
	case <-s.respCh:
	default:
	}

	select {
	case <-ctx.Done():
		s.close()
		return "", ctx.Err()
	default:
	}

	msg := cmd + "\n" + key + "\n" + arg + "\n"
	if _, err := s.clientSide.Write([]byte(msg)); err != nil {
		return "", err
	}
	s.lastSeen.Store(time.Now().UnixNano())

	select {
	case resp := <-s.respCh:
		return resp, nil
	case <-ctx.Done():
		// Drain a response that arrived between the grant and ctx firing
		// before we close. Without this, a token that was granted just as
		// the HTTP request was cancelled is silently dropped — the
		// handler's maybeCleanupOnDisconnect can't release it because we
		// returned ErrCanceled instead of the grant. Returning the
		// response here lets the handler clean up the grant; the session
		// stays alive (the protocol command actually completed).
		select {
		case resp := <-s.respCh:
			return resp, nil
		default:
		}
		s.close()
		return "", ctx.Err()
	case <-s.ctx.Done():
		return "", ErrSessionGone
	case <-s.muxDone:
		// Multiplexer exited (pipe broke). Check for a late response that
		// arrived before EOF.
		select {
		case resp := <-s.respCh:
			return resp, nil
		default:
		}
		return "", ErrSessionGone
	}
}

// signals returns the channel of "sig <channel> <payload>" lines. Reading
// from it is the only way to consume pushed signals. The channel is closed
// when the session dies.
func (s *session) signals() <-chan string {
	return s.sigCh
}

// close is idempotent.
func (s *session) close() {
	s.closeOnce.Do(func() {
		s.cancel()
		// Closing the client side unblocks the multiplexer's Read.
		// Closing the server side unblocks ServeConn's read loop, which
		// triggers the unchanged CleanupConnection path.
		s.clientSide.Close()
		s.serverSide.Close()
	})
	<-s.muxDone
	<-s.serveDone
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// mintSessionID returns a random 32-char hex string. Uses crypto/rand
// directly rather than going through LockManager's tokenBuf because that
// buffer is unexported and this path is low-frequency (session create
// only).
func mintSessionID() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf[:]), nil
}

// readLine reads a single newline-terminated line. Caps at 64KB to protect
// against malformed protocol frames (the server's own responses can grow
// large for `stats`, so we're more permissive than the TCP side's 256-byte
// request cap).
func readLine(r *bufio.Reader) (string, error) {
	const maxLine = 65536
	var buf []byte
	for {
		b, err := r.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) && len(buf) == 0 {
				return "", io.EOF
			}
			return "", err
		}
		if b == '\n' {
			return strings.TrimRight(string(buf), "\r"), nil
		}
		if len(buf) >= maxLine {
			// Drain to newline to stay in sync.
			for {
				c, err := r.ReadByte()
				if err != nil || c == '\n' {
					break
				}
			}
			return "", fmt.Errorf("response line too long")
		}
		buf = append(buf, b)
	}
}

// ---------------------------------------------------------------------------
// Idle session sweeper
// ---------------------------------------------------------------------------

// sweeperLoop periodically prunes sessions that are dead (pipe broken) or
// idle past 2× idleTimeout. The advisory idle timeout reported to clients
// is idleTimeout; the hard cutoff is 2× that, giving clients a grace window
// past the advertised limit.
func (b *Bridge) sweeperLoop() {
	defer close(b.sweeperDone)
	interval := b.idleTimeout
	if interval <= 0 {
		interval = 10 * time.Second
	}
	if interval > 30*time.Second {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	cutoffMul := 2.0

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
			cutoff := time.Now().Add(-time.Duration(float64(b.idleTimeout) * cutoffMul))
			var doomed []*session
			b.mu.Lock()
			for id, s := range b.sessions {
				if s.dead.Load() {
					doomed = append(doomed, s)
					b.deleteSessionLocked(id)
					continue
				}
				if s.inFlight.Load() > 0 {
					continue
				}
				if b.idleTimeout > 0 {
					last := time.Unix(0, s.lastSeen.Load())
					if last.Before(cutoff) {
						doomed = append(doomed, s)
						b.deleteSessionLocked(id)
					}
				}
			}
			b.mu.Unlock()
			for _, s := range doomed {
				s.log.Debug("session swept (idle or dead)")
				s.close()
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Convenience accessors for handlers
// ---------------------------------------------------------------------------

// LockManager returns the shared LockManager. Handlers don't call it
// directly — they go through session.command — but /v1/stats uses it to
// synthesize responses without a session round-trip.
func (b *Bridge) LockManager() *lock.LockManager {
	return b.srv.LockManager()
}

// Signals returns the shared signal Manager.
func (b *Bridge) Signals() *signal.Manager {
	return b.srv.Signals()
}

// ConnCount returns the number of active TCP connections to the
// underlying server, so the HTTP stats endpoint can include them.
func (b *Bridge) ConnCount() int64 {
	return b.srv.ConnCount()
}

// IdleTimeout is the advisory timeout surfaced to clients in the
// session-create response so they know how often to ping when idle.
func (b *Bridge) IdleTimeout() time.Duration {
	return b.idleTimeout
}
