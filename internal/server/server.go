// Package server implements the line-based TCP server. It owns the
// accept loop, per-connection IP/total caps, auth handshake, and
// dispatch into the LockManager. Background lock-manager loops are
// spawned here so the server's lifecycle owns them too.
package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mtingers/dflockd/internal/config"
	"github.com/mtingers/dflockd/internal/lock"
)

// Server accepts TCP connections and dispatches requests to the
// LockManager. One Server per process.
type Server struct {
	lm        *lock.LockManager
	cfg       *config.Config
	log       *slog.Logger
	connSeq   atomic.Uint64
	connCount atomic.Int64
	conns     sync.Map // net.Conn → struct{}
}

// New constructs a Server wrapping lm. The Server does not start any
// background work until Run (or RunOnListener) is called.
func New(lm *lock.LockManager, cfg *config.Config, log *slog.Logger) *Server {
	return &Server{lm: lm, cfg: cfg, log: log}
}

// LockManager exposes the underlying LockManager (used by the HTTP API).
func (s *Server) LockManager() *lock.LockManager { return s.lm }

// Config exposes the server config (used by the HTTP API).
func (s *Server) Config() *config.Config { return s.cfg }

// NextConnID allocates a fresh connection ID. The HTTP API uses this to
// give every session a connID that's unique across both transports so
// CleanupConnection doesn't collide.
func (s *Server) NextConnID() uint64 { return s.connSeq.Add(1) }

// ConnCount returns the number of currently-active TCP connections.
func (s *Server) ConnCount() int64 { return s.connCount.Load() }

// Run starts the listener on the configured host:port and blocks until
// ctx is cancelled. Returns nil on clean shutdown.
func (s *Server) Run(ctx context.Context) error {
	listener, addr, err := s.configuredListener()
	if err != nil {
		return err
	}
	return s.serveLogged(ctx, listener, addr)
}

func (s *Server) configuredListener() (net.Listener, string, error) {
	if err := s.validateTLSConfig(); err != nil {
		return nil, "", err
	}
	return s.openConfiguredListener(s.listenAddr())
}

func (s *Server) openConfiguredListener(addr string) (net.Listener, string, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, "", fmt.Errorf("listen: %w", err)
	}
	return s.wrapTLS(listener, addr)
}

func (s *Server) validateTLSConfig() error {
	if tlsPairIncomplete(s.cfg.TLSCert, s.cfg.TLSKey) {
		return fmt.Errorf("both --tls-cert and --tls-key must be provided together")
	}
	return nil
}

func tlsPairIncomplete(cert, key string) bool { return (cert != "") != (key != "") }

func (s *Server) listenAddr() string {
	return net.JoinHostPort(s.cfg.Host, strconv.Itoa(s.cfg.Port))
}

func (s *Server) wrapTLS(listener net.Listener, addr string) (net.Listener, string, error) {
	if s.cfg.TLSCert == "" {
		return listener, addr, nil
	}
	return s.wrapRequiredTLS(listener, addr)
}

func (s *Server) wrapRequiredTLS(listener net.Listener, addr string) (net.Listener, string, error) {
	tlsListener, err := s.tlsListener(listener)
	if err != nil {
		return closeTLSListenerOnError(listener, err)
	}
	return s.tlsEnabled(tlsListener, addr)
}

func closeTLSListenerOnError(listener net.Listener, err error) (net.Listener, string, error) {
	listener.Close()
	return nil, "", err
}

func (s *Server) tlsEnabled(listener net.Listener, addr string) (net.Listener, string, error) {
	s.log.Info("TLS enabled")
	return listener, addr, nil
}

func (s *Server) tlsListener(listener net.Listener) (net.Listener, error) {
	cert, err := tls.LoadX509KeyPair(s.cfg.TLSCert, s.cfg.TLSKey)
	if err != nil {
		return nil, fmt.Errorf("tls: %w", err)
	}
	return tls.NewListener(listener, serverTLSConfig(cert)), nil
}

func serverTLSConfig(cert tls.Certificate) *tls.Config {
	return &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
}

// RunOnListener serves on a pre-existing listener. Used by tests so
// they can pick a free port and bypass the TLS/host config.
func (s *Server) RunOnListener(ctx context.Context, listener net.Listener) error {
	return s.serveLogged(ctx, listener, listener.Addr())
}

func (s *Server) serveLogged(ctx context.Context, listener net.Listener, addr any) error {
	s.log.Info("listening", "addr", addr)
	return s.serve(ctx, listener)
}

func (s *Server) serve(ctx context.Context, listener net.Listener) error {
	st := s.newServeState(ctx)
	defer st.stop()
	return s.runServe(listener, st)
}

type serveState struct {
	ctx     context.Context
	stop    context.CancelFunc
	wg      sync.WaitGroup
	tracker *ipTracker
}

func (s *Server) newServeState(ctx context.Context) *serveState {
	serveCtx, stop := context.WithCancel(ctx)
	return &serveState{ctx: serveCtx, stop: stop, tracker: newIPTracker(s.cfg.MaxConnectionsPerIP)}
}

func (s *Server) runServe(listener net.Listener, st *serveState) error {
	s.startBackgroundLoops(st.ctx, &st.wg)
	closeOnCancel(st.ctx, listener)
	return s.acceptLoop(st.ctx, st.stop, listener, st.tracker, &st.wg)
}

// startBackgroundLoops spawns the lock-manager sweeper goroutines.
// They exit when serveCtx is cancelled.
func (s *Server) startBackgroundLoops(serveCtx context.Context, wg *sync.WaitGroup) {
	wg.Add(2)
	go func() { defer wg.Done(); s.lm.LeaseExpiryLoop(serveCtx) }()
	go func() { defer wg.Done(); s.lm.GCLoop(serveCtx) }()
}

// closeOnCancel closes listener once ctx is cancelled. Used to make
// the Accept call return promptly on shutdown.
func closeOnCancel(ctx context.Context, listener net.Listener) {
	go func() {
		<-ctx.Done()
		listener.Close()
	}()
}

// acceptLoop is the body of serve: accept, gate, dispatch, loop. On
// fatal accept errors it cancels stop, drains the wait group, and
// returns the error.
func (s *Server) acceptLoop(serveCtx context.Context, stop context.CancelFunc, listener net.Listener, tracker *ipTracker, wg *sync.WaitGroup) error {
	return s.acceptLoopWithBackoff(serveCtx, stop, listener, tracker, newBackoff(), wg)
}

func (s *Server) acceptLoopWithBackoff(serveCtx context.Context, stop context.CancelFunc, listener net.Listener, tracker *ipTracker, bo *backoff, wg *sync.WaitGroup) error {
	for {
		if done, err := s.acceptStep(serveCtx, stop, listener, tracker, bo, wg); done {
			return err
		}
	}
}

func (s *Server) acceptStep(serveCtx context.Context, stop context.CancelFunc, listener net.Listener, tracker *ipTracker, bo *backoff, wg *sync.WaitGroup) (bool, error) {
	conn, err := listener.Accept()
	if err != nil {
		return s.handleAcceptError(serveCtx, stop, err, bo, wg)
	}
	return s.acceptedConnStep(serveCtx, conn, tracker, bo, wg)
}

func (s *Server) acceptedConnStep(serveCtx context.Context, conn net.Conn, tracker *ipTracker, bo *backoff, wg *sync.WaitGroup) (bool, error) {
	bo.reset()
	s.startConn(serveCtx, conn, tracker, wg)
	return false, nil
}

// handleAcceptError categorises an accept failure. Returns done=true
// when the loop should exit; the second value is the error to
// propagate.
func (s *Server) handleAcceptError(ctx context.Context, stop context.CancelFunc, err error, bo *backoff, wg *sync.WaitGroup) (bool, error) {
	if ctx.Err() != nil {
		return s.acceptShutdown(wg)
	}
	return s.handleLiveAcceptError(ctx, stop, err, bo, wg)
}

func (s *Server) handleLiveAcceptError(ctx context.Context, stop context.CancelFunc, err error, bo *backoff, wg *sync.WaitGroup) (bool, error) {
	if !isTemporaryNetErr(err) {
		return s.acceptFatal(stop, err, wg)
	}
	return s.backoffAndContinue(ctx, err, bo, wg)
}

func (s *Server) acceptShutdown(wg *sync.WaitGroup) (bool, error) {
	s.drain(wg)
	return true, nil
}

func (s *Server) acceptFatal(stop context.CancelFunc, err error, wg *sync.WaitGroup) (bool, error) {
	stop()
	s.drain(wg)
	return true, fmt.Errorf("accept: %w", err)
}

// backoffAndContinue logs the transient error and sleeps for bo's
// next interval. Returns done=true if shutdown is requested during
// the sleep.
func (s *Server) backoffAndContinue(ctx context.Context, err error, bo *backoff, wg *sync.WaitGroup) (bool, error) {
	d := bo.next()
	s.log.Error("accept error, backing off", "err", err, "backoff", d)
	return s.sleepBackoff(ctx, d, wg)
}

func (s *Server) sleepBackoff(ctx context.Context, d time.Duration, wg *sync.WaitGroup) (bool, error) {
	if waitForTimer(ctx.Done(), time.After(d)) {
		return false, nil
	}
	return s.acceptShutdown(wg)
}

// startConn applies global+per-IP caps, registers the connection,
// and spawns the per-connection handler. Returns false if the conn
// was rejected.
func (s *Server) startConn(serveCtx context.Context, conn net.Conn, tracker *ipTracker, wg *sync.WaitGroup) bool {
	ip, ok := s.acceptConn(conn, tracker)
	return s.startAcceptedConn(serveCtx, conn, ip, ok, tracker, wg)
}

func (s *Server) startAcceptedConn(serveCtx context.Context, conn net.Conn, ip string, ok bool, tracker *ipTracker, wg *sync.WaitGroup) bool {
	if !ok {
		return false
	}
	s.spawnConnHandler(serveCtx, conn, ip, tracker, wg)
	return true
}

func (s *Server) acceptConn(conn net.Conn, tracker *ipTracker) (string, bool) {
	if !s.acceptUnderGlobalCap(conn) {
		return "", false
	}
	return s.acceptUnderIPCap(conn, tracker)
}

func (s *Server) acceptUnderIPCap(conn net.Conn, tracker *ipTracker) (string, bool) {
	ip, ok := tracker.acquire(conn)
	if !ok {
		return s.rejectIPCapResult(conn, ip)
	}
	return ip, true
}

func (s *Server) rejectIPCapResult(conn net.Conn, ip string) (string, bool) {
	s.rejectIPCap(conn, ip)
	return ip, false
}

func (s *Server) rejectIPCap(conn net.Conn, ip string) {
	s.log.Warn("max connections per IP, rejecting", "ip", ip, "max", s.cfg.MaxConnectionsPerIP)
	conn.Close()
}

// acceptUnderGlobalCap returns false (closing conn) when the
// max-connections cap has been reached.
func (s *Server) acceptUnderGlobalCap(conn net.Conn) bool {
	if !s.globalCapReached() {
		return true
	}
	s.rejectGlobalCap(conn)
	return false
}

func (s *Server) globalCapReached() bool {
	return s.cfg.MaxConnections > 0 && s.connCount.Load() >= int64(s.cfg.MaxConnections)
}

func (s *Server) rejectGlobalCap(conn net.Conn) {
	s.log.Warn("max connections reached, rejecting", "max", s.cfg.MaxConnections)
	conn.Close()
}

// spawnConnHandler bumps accounting, allocates a connID, and runs
// ServeConn in a goroutine.
func (s *Server) spawnConnHandler(serveCtx context.Context, conn net.Conn, ip string, tracker *ipTracker, wg *sync.WaitGroup) {
	connID := s.registerConn(conn)
	wg.Add(1)
	go s.runConnHandler(serveCtx, conn, connID, ip, tracker, wg)
}

func (s *Server) registerConn(conn net.Conn) uint64 {
	s.connCount.Add(1)
	s.conns.Store(conn, struct{}{})
	return s.NextConnID()
}

func (s *Server) runConnHandler(serveCtx context.Context, conn net.Conn, connID uint64, ip string, tracker *ipTracker, wg *sync.WaitGroup) {
	defer wg.Done()
	defer s.unregisterConn(conn, ip, tracker)
	s.ServeConn(serveCtx, conn, connID)
}

func (s *Server) unregisterConn(conn net.Conn, ip string, tracker *ipTracker) {
	s.connCount.Add(-1)
	s.conns.Delete(conn)
	tracker.release(ip)
}

// backoff implements the bounded exponential backoff used for
// transient accept errors.
type backoff struct {
	cur, max time.Duration
}

func newBackoff() *backoff {
	return &backoff{max: time.Second}
}

func (b *backoff) reset() { b.cur = 0 }

// next returns the duration for the next sleep and updates state.
func (b *backoff) next() time.Duration {
	if b.cur == 0 {
		return b.start()
	}
	return b.grow()
}

func (b *backoff) start() time.Duration {
	b.cur = 5 * time.Millisecond
	return b.cur
}

func (b *backoff) grow() time.Duration {
	b.cur = minDuration(b.cur*2, b.max)
	return b.cur
}

func minDuration(a, b time.Duration) time.Duration {
	if a > b {
		return b
	}
	return a
}

// drain blocks until all connection goroutines exit, force-closing
// connections after ShutdownTimeout if needed.
func (s *Server) drain(wg *sync.WaitGroup) {
	s.log.Info("shutting down, draining connections")
	if s.drainNaturally(wg) {
		return
	}
	s.forceDrain(wg)
}

func (s *Server) drainNaturally(wg *sync.WaitGroup) bool {
	if s.cfg.ShutdownTimeout <= 0 {
		wg.Wait()
		return true
	}
	return waitWithTimeout(wg, s.cfg.ShutdownTimeout)
}

func (s *Server) forceDrain(wg *sync.WaitGroup) {
	s.log.Warn("shutdown timeout reached, force-closing connections")
	s.forceCloseAllConns()
	wg.Wait()
}

// waitWithTimeout returns true if wg drained within d, false on timeout.
func waitWithTimeout(wg *sync.WaitGroup, d time.Duration) bool {
	return !waitForTimer(waitGroupDone(wg), time.After(d))
}

func waitGroupDone(wg *sync.WaitGroup) <-chan struct{} {
	done := make(chan struct{})
	go closeWhenWaitGroupDone(wg, done)
	return done
}

func closeWhenWaitGroupDone(wg *sync.WaitGroup, done chan struct{}) {
	wg.Wait()
	close(done)
}

func waitForTimer(done <-chan struct{}, timer <-chan time.Time) bool {
	select {
	case <-timer:
		return true
	case <-done:
		return false
	}
}

// forceCloseAllConns closes every tracked connection.
func (s *Server) forceCloseAllConns() {
	s.conns.Range(closeTrackedConn)
}

func closeTrackedConn(key, _ any) bool {
	if c, ok := key.(net.Conn); ok {
		c.Close()
	}
	return true
}

func isTemporaryNetErr(err error) bool {
	ne, ok := asNetErr(err)
	return ok && netErrTemporary(ne)
}

func asNetErr(err error) (net.Error, bool) {
	var ne net.Error
	ok := errors.As(err, &ne)
	return ne, ok
}

type temporaryNetError interface{ Temporary() bool }

func netErrTemporary(err net.Error) bool {
	te, ok := err.(temporaryNetError)
	return ok && te.Temporary()
}

// ipTracker enforces per-remote-IP connection caps. nil-safe and
// cap=0 means unlimited.
type ipTracker struct {
	max    int
	mu     sync.Mutex
	counts map[string]int
}

func newIPTracker(max int) *ipTracker {
	return &ipTracker{max: max, counts: make(map[string]int)}
}

func (t *ipTracker) acquire(conn net.Conn) (string, bool) {
	if t.unlimited() {
		return "", true
	}
	return t.acquireIP(remoteIP(conn.RemoteAddr().String()))
}

func (t *ipTracker) unlimited() bool { return t.max <= 0 }

func (t *ipTracker) acquireIP(ip string) (string, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return ip, t.incrementIP(ip)
}

func (t *ipTracker) incrementIP(ip string) bool {
	if t.counts[ip] >= t.max {
		return false
	}
	t.counts[ip]++
	return true
}

func (t *ipTracker) release(ip string) {
	if ip == "" {
		return
	}
	t.releaseIP(ip)
}

func (t *ipTracker) releaseIP(ip string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.decrementIP(ip)
}

func (t *ipTracker) decrementIP(ip string) {
	t.counts[ip]--
	if t.counts[ip] <= 0 {
		delete(t.counts, ip)
	}
}

func remoteIP(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err == nil {
		return host
	}
	return addr
}
