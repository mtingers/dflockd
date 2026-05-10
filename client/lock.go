package client

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math"
	"net"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// renewableResource — shared state for Lock and Semaphore
// ---------------------------------------------------------------------------

// renewableResource holds the live connection, the held token, and the
// background-renewal goroutine lifecycle. Lock and Semaphore embed it
// anonymously so Token / Close / stopRenew / connect have one
// implementation.
type renewableResource struct {
	mu          sync.Mutex
	conn        *Conn
	token       string
	lease       int
	cancelRenew context.CancelFunc
	renewDone   chan struct{}
}

// Token returns the current token, or "" if not held.
func (r *renewableResource) Token() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.token
}

// Close stops renewal and closes the connection. Idempotent.
func (r *renewableResource) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stopRenew()
	return r.closeConnLocked()
}

// closeConnLocked closes r.conn and clears r.token / r.lease. Must be
// called with r.mu held. Returns the close error, or nil if there
// was no live conn.
func (r *renewableResource) closeConnLocked() error {
	if r.conn == nil {
		return nil
	}
	err := r.conn.Close()
	r.conn = nil
	r.token = ""
	r.lease = 0
	return err
}

// clearConnIfCurrent zeroes the live state when conn matches r.conn.
func (r *renewableResource) clearConnIfCurrent(conn *Conn) {
	if r.conn == conn {
		r.conn = nil
		r.token = ""
		r.lease = 0
	}
}

// ---------------------------------------------------------------------------
// Renewal loop lifecycle
// ---------------------------------------------------------------------------

// stopRenewGrace caps how long stopRenew waits for the renew goroutine
// before force-closing the conn to interrupt its in-flight Renew.
const stopRenewGrace = 2 * time.Second

// stopRenew cancels the renewal goroutine and waits for it to exit.
// Must be called with r.mu held; temporarily releases the mutex so
// the goroutine can finish a tick that grabs r.mu.
func (r *renewableResource) stopRenew() {
	if r.cancelRenew != nil {
		r.cancelRenew()
		r.cancelRenew = nil
	}
	if r.renewDone == nil {
		return
	}
	r.waitForRenewExit()
}

// waitForRenewExit drops r.mu, waits for the renew goroutine to
// signal done, optionally force-closes a stuck conn, then re-takes
// r.mu before returning.
func (r *renewableResource) waitForRenewExit() {
	done := r.renewDone
	r.renewDone = nil
	conn := r.conn
	r.mu.Unlock()
	defer r.mu.Lock()

	select {
	case <-done:
		return
	case <-time.After(stopRenewGrace):
	}
	if conn != nil {
		_ = conn.Close()
	}
	<-done
}

// ---------------------------------------------------------------------------
// Connection establishment
// ---------------------------------------------------------------------------

// connect dials addr (optionally over TLS) and authenticates. Closes
// any pre-existing connection first. Must be called with r.mu held.
func (r *renewableResource) connect(addr string, tlsCfg *tls.Config, authToken string) error {
	r.closeOldConnLocked()
	conn, err := dialAndAuth(addr, tlsCfg, authToken)
	if err != nil {
		return err
	}
	r.conn = conn
	return nil
}

// closeOldConnLocked drops the existing conn (if any) before connect
// installs a new one. Must be called with r.mu held.
func (r *renewableResource) closeOldConnLocked() {
	if r.conn != nil {
		r.conn.Close()
	}
	r.conn = nil
	r.token = ""
	r.lease = 0
}

// dialAndAuth dials a fresh connection and authenticates if a token
// is configured. Used for the initial connect and the cleanup path.
func dialAndAuth(addr string, tlsCfg *tls.Config, authToken string) (*Conn, error) {
	conn, err := dialNew(addr, tlsCfg)
	if err != nil {
		return nil, err
	}
	if authToken == "" {
		return conn, nil
	}
	if err := Authenticate(conn, authToken); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

func dialNew(addr string, tlsCfg *tls.Config) (*Conn, error) {
	if tlsCfg != nil {
		return DialTLS(addr, tlsCfg)
	}
	return Dial(addr)
}

// ---------------------------------------------------------------------------
// Renewal goroutine
// ---------------------------------------------------------------------------

// renewFn is the protocol-level renew used by the background loop.
type renewFn func(*Conn, string, string, ...Option) (int, error)

// startRenewLoop spawns the background renewal goroutine. Must be
// called with r.mu held.
func (r *renewableResource) startRenewLoop(key string, leaseSec int, ratio, jitter float64, opts []Option, fn renewFn, onErr func(error)) {
	r.stopRenew()
	ctx, cancel := context.WithCancel(context.Background())
	r.cancelRenew = cancel
	done := make(chan struct{})
	r.renewDone = done
	go r.renewLoop(ctx, done, renewLoop{
		key: key, leaseSec: leaseSec, ratio: ratio, jitter: jitter,
		opts: opts, fn: fn, onErr: onErr,
	})
}

// renewLoop bundles the fields the background goroutine needs.
type renewLoop struct {
	key      string
	leaseSec int
	ratio    float64
	jitter   float64
	opts     []Option
	fn       renewFn
	onErr    func(error)
}

func (r *renewableResource) renewLoop(ctx context.Context, done chan<- struct{}, l renewLoop) {
	defer close(done)
	interval := renewInterval(l.leaseSec, l.ratio)
	timer := time.NewTimer(jitteredInterval(interval, l.jitter))
	defer timer.Stop()
	for {
		if !r.renewTick(ctx, timer.C, interval, l) {
			return
		}
		timer.Reset(jitteredInterval(interval, l.jitter))
	}
}

// renewTick blocks for the next tick or ctx cancellation; returns
// true to keep looping, false to exit. Errors from the protocol fn
// surface via l.onErr and exit the loop.
func (r *renewableResource) renewTick(ctx context.Context, tick <-chan time.Time, interval time.Duration, l renewLoop) bool {
	select {
	case <-ctx.Done():
		return false
	case <-tick:
	}
	conn, tok, ok := r.snapshotForRenew()
	if !ok {
		return false
	}
	if _, err := l.fn(conn, l.key, tok, l.opts...); err != nil {
		notifyRenewErr(ctx, err, l.onErr)
		return false
	}
	return true
}

// snapshotForRenew returns the live conn+token under r.mu, or
// ok=false if the resource is no longer held.
func (r *renewableResource) snapshotForRenew() (*Conn, string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.conn == nil || r.token == "" {
		return nil, "", false
	}
	return r.conn, r.token, true
}

func notifyRenewErr(ctx context.Context, err error, onErr func(error)) {
	if ctx.Err() != nil || onErr == nil {
		return
	}
	onErr(err)
}

const minRenewInterval = time.Millisecond

func renewInterval(leaseSec int, ratio float64) time.Duration {
	dur := time.Duration(leaseSec) * time.Second
	interval := time.Duration(float64(dur) * ratio)
	if interval <= 0 {
		return minRenewInterval
	}
	return interval
}

// ---------------------------------------------------------------------------
// Defaults and validation
// ---------------------------------------------------------------------------

const defaultRenewJitter = 0.10

func defaultAcquireTimeout(t time.Duration) time.Duration {
	if t > 0 {
		return t
	}
	return 10 * time.Second
}

func defaultRenewRatio(r float64) float64 {
	if r > 0 {
		return r
	}
	return 0.5
}

func defaultRenewJitterValue(j float64) float64 {
	if j > 0 {
		return j
	}
	return defaultRenewJitter
}

// validateRenewConfig rejects values that would silently produce
// broken runtime behaviour.
func validateRenewConfig(leaseTTL int, renewRatio, renewJitter float64) error {
	if err := validateLeaseTTLRange(leaseTTL); err != nil {
		return err
	}
	if err := validateUnitFraction("RenewRatio", renewRatio); err != nil {
		return err
	}
	return validateUnitFraction("RenewJitter", renewJitter)
}

func validateLeaseTTLRange(leaseTTL int) error {
	if leaseTTL < 0 {
		return fmt.Errorf("dflockd: LeaseTTL must be >= 0 (got %d)", leaseTTL)
	}
	if int64(leaseTTL) > maxProtocolSeconds {
		return fmt.Errorf("dflockd: LeaseTTL too large (max %d)", maxProtocolSeconds)
	}
	return nil
}

func validateUnitFraction(name string, value float64) error {
	if math.IsNaN(value) || value < 0 || value >= 1 {
		return fmt.Errorf("dflockd: %s must be in [0, 1) (got %v)", name, value)
	}
	return nil
}

// buildOpts constructs the Option slice from a lease TTL value.
func buildOpts(leaseTTL int) []Option {
	if leaseTTL > 0 {
		return []Option{WithLeaseTTL(leaseTTL)}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Connection-watch and abandoned-grant cleanup
// ---------------------------------------------------------------------------

// closeConnOnContextDone closes conn if ctx is cancelled before
// stop() is called. stop waits for the watcher to exit.
func closeConnOnContextDone(ctx context.Context, conn interface{ Close() error }) func() {
	done := make(chan struct{})
	exited := make(chan struct{})
	go watchAndClose(ctx, conn, done, exited)
	return func() {
		close(done)
		<-exited
	}
}

func watchAndClose(ctx context.Context, conn interface{ Close() error }, done, exited chan struct{}) {
	defer close(exited)
	select {
	case <-ctx.Done():
		_ = conn.Close()
	case <-done:
	}
}

// abandonedGrantCleanupTimeout caps best-effort cleanup of a token
// granted just as the caller cancelled.
const abandonedGrantCleanupTimeout = 2 * time.Second

type releaseFn func(*Conn, string, string) error

// tryReleaseWithDeadline runs releaseFn under a fixed deadline.
func tryReleaseWithDeadline(c *Conn, key, token string, fn releaseFn) error {
	if c == nil {
		return net.ErrClosed
	}
	_ = c.conn.SetDeadline(time.Now().Add(abandonedGrantCleanupTimeout))
	err := fn(c, key, token)
	_ = c.conn.SetDeadline(time.Time{})
	return err
}

// cleanupAbandonedGrant releases (key, token) on conn; if that
// fails, dial a fresh conn and retry once. Best-effort.
func cleanupAbandonedGrant(conn *Conn, addr string, tlsCfg *tls.Config, authToken, key, token string, fn releaseFn) {
	if token == "" {
		return
	}
	if tryReleaseWithDeadline(conn, key, token, fn) == nil {
		return
	}
	cleanupViaFreshConn(addr, tlsCfg, authToken, key, token, fn)
}

func cleanupViaFreshConn(addr string, tlsCfg *tls.Config, authToken, key, token string, fn releaseFn) {
	c, err := dialAndAuth(addr, tlsCfg, authToken)
	if err != nil {
		return
	}
	defer c.Close()
	_ = tryReleaseWithDeadline(c, key, token, fn)
}

// releaseWithContext runs fn with cancellation watching.
func releaseWithContext(ctx context.Context, conn *Conn, key, token string, fn releaseFn) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	stop := closeConnOnContextDone(ctx, conn)
	err := fn(conn, key, token)
	stop()
	if err != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// ---------------------------------------------------------------------------
// Shared acquire/enqueue/wait flow
// ---------------------------------------------------------------------------

// resourceOps captures the per-resource (Lock vs Semaphore) protocol
// hooks. Lock and Semaphore each populate this once and pass it into
// the shared flow methods.
type resourceOps struct {
	acquire func(*Conn, string, time.Duration, ...Option) (string, int, error)
	enqueue func(*Conn, string, ...Option) (string, string, int, error)
	wait    func(*Conn, string, time.Duration) (string, int, error)
	release releaseFn
	renew   renewFn
}

// flowParams bundles every static parameter the runAcquire/run* methods
// need so the public Lock/Semaphore methods are 1-2 line dispatchers.
type flowParams struct {
	key        string
	addr       string
	tlsCfg     *tls.Config
	authToken  string
	timeout    time.Duration
	opts       []Option
	ratio      float64
	jitter     float64
	onRenewErr func(error)
	ops        resourceOps
}

// renewLoopFor returns a renewLoop populated for the given lease.
func (p flowParams) renewLoopFor(lease int) renewLoop {
	return renewLoop{
		key: p.key, leaseSec: lease, ratio: p.ratio, jitter: p.jitter,
		opts: p.opts, fn: p.ops.renew, onErr: p.onRenewErr,
	}
}

// runAcquire is the shared single-phase flow used by Lock.Acquire and
// Semaphore.Acquire. The protocol differences are entirely in p.ops.
func (r *renewableResource) runAcquire(ctx context.Context, p flowParams) (bool, error) {
	conn, err := r.openFresh(p)
	if err != nil {
		return false, err
	}
	token, lease, callErr := callWithCancel(ctx, conn, func() (string, int, error) {
		return p.ops.acquire(conn, p.key, p.timeout, p.opts...)
	})
	r.cleanupOnCanceledGrant(ctx, conn, p, token, callErr)
	return r.finishAcquire(ctx, conn, p, token, lease, callErr)
}

// runEnqueue is the shared two-phase phase 1.
func (r *renewableResource) runEnqueue(ctx context.Context, p flowParams) (string, error) {
	conn, err := r.openFresh(p)
	if err != nil {
		return "", err
	}
	status, token, lease, callErr := callWithCancel4(ctx, conn, func() (string, string, int, error) {
		return p.ops.enqueue(conn, p.key, p.opts...)
	})
	r.cleanupOnCanceledEnqueue(ctx, conn, p, status, token, callErr)
	return r.finishEnqueue(ctx, conn, p, status, token, lease, callErr)
}

// runWait is the shared two-phase phase 2. Reuses the existing conn
// from a prior runEnqueue.
func (r *renewableResource) runWait(ctx context.Context, p flowParams, timeout time.Duration) (bool, error) {
	conn, ok := r.snapshotConn()
	if !ok {
		return false, ErrNotQueued
	}
	token, lease, callErr := callWithCancel(ctx, conn, func() (string, int, error) {
		return p.ops.wait(conn, p.key, timeout)
	})
	r.cleanupOnCanceledGrant(ctx, conn, p, token, callErr)
	return r.finishAcquire(ctx, conn, p, token, lease, callErr)
}

// snapshotConn returns the live conn under r.mu.
func (r *renewableResource) snapshotConn() (*Conn, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.conn == nil {
		return nil, false
	}
	return r.conn, true
}

// openFresh dials a fresh conn, replacing any current one. Returns
// the new conn (also stored in r.conn) under r.mu released.
func (r *renewableResource) openFresh(p flowParams) (*Conn, error) {
	r.mu.Lock()
	r.stopRenew()
	if err := r.connect(p.addr, p.tlsCfg, p.authToken); err != nil {
		r.mu.Unlock()
		return nil, err
	}
	conn := r.conn
	r.mu.Unlock()
	return conn, nil
}

// callWithCancel runs fn while watching ctx; if ctx fires, conn is
// closed which interrupts fn's I/O.
func callWithCancel[A, B any](ctx context.Context, conn *Conn, fn func() (A, B, error)) (A, B, error) {
	stop := closeConnOnContextDone(ctx, conn)
	a, b, err := fn()
	stop()
	return a, b, err
}

// callWithCancel4 is callWithCancel for 4-return functions (enqueue).
func callWithCancel4[A, B, C any](ctx context.Context, conn *Conn, fn func() (A, B, C, error)) (A, B, C, error) {
	stop := closeConnOnContextDone(ctx, conn)
	a, b, c, err := fn()
	stop()
	return a, b, c, err
}

// cleanupOnCanceledGrant fires a best-effort release if a token was
// granted just as ctx was cancelled.
func (r *renewableResource) cleanupOnCanceledGrant(ctx context.Context, conn *Conn, p flowParams, token string, err error) {
	if err == nil && ctx.Err() != nil {
		cleanupAbandonedGrant(conn, p.addr, p.tlsCfg, p.authToken, p.key, token, p.ops.release)
	}
}

// cleanupOnCanceledEnqueue is the enqueue equivalent: only the
// "acquired" status produced a token to release.
func (r *renewableResource) cleanupOnCanceledEnqueue(ctx context.Context, conn *Conn, p flowParams, status, token string, err error) {
	if err == nil && ctx.Err() != nil && status == "acquired" {
		cleanupAbandonedGrant(conn, p.addr, p.tlsCfg, p.authToken, p.key, token, p.ops.release)
	}
}

// ---------------------------------------------------------------------------
// finish* — re-acquire r.mu and decide install vs error vs cancel.
// ---------------------------------------------------------------------------

// finishAcquire is shared by runAcquire (Lock+Semaphore) and runWait.
func (r *renewableResource) finishAcquire(ctx context.Context, conn *Conn, p flowParams, token string, lease int, err error) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err != nil {
		return r.acquireErrPath(ctx, conn, err)
	}
	if ctx.Err() != nil {
		return r.cancelAfterSuccess(conn, ctx)
	}
	if r.conn != conn {
		return r.orphanedConnPath(conn, p, token)
	}
	r.installAndStartRenew(p, token, lease)
	return true, nil
}

func (r *renewableResource) acquireErrPath(ctx context.Context, conn *Conn, err error) (bool, error) {
	conn.Close()
	r.clearConnIfCurrent(conn)
	if errors.Is(err, ErrTimeout) {
		return false, nil
	}
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	return false, err
}

func (r *renewableResource) cancelAfterSuccess(conn *Conn, ctx context.Context) (bool, error) {
	conn.Close()
	r.clearConnIfCurrent(conn)
	return false, ctx.Err()
}

func (r *renewableResource) orphanedConnPath(conn *Conn, p flowParams, token string) (bool, error) {
	cleanupAbandonedGrant(conn, p.addr, p.tlsCfg, p.authToken, p.key, token, p.ops.release)
	return false, net.ErrClosed
}

func (r *renewableResource) installAndStartRenew(p flowParams, token string, lease int) {
	r.token = token
	r.lease = lease
	loop := p.renewLoopFor(lease)
	r.startRenewLoop(loop.key, loop.leaseSec, loop.ratio, loop.jitter, loop.opts, loop.fn, loop.onErr)
}

// finishEnqueue is the enqueue analogue of finishAcquire. Status may
// be "acquired" (treat like a successful acquire) or "queued" (just
// hold the conn for the upcoming Wait).
func (r *renewableResource) finishEnqueue(ctx context.Context, conn *Conn, p flowParams, status, token string, lease int, err error) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err != nil {
		return "", r.enqueueErrPath(ctx, conn, err)
	}
	if ctx.Err() != nil {
		return "", r.cancelAfterEnqueue(conn, ctx)
	}
	if r.conn != conn {
		return "", r.orphanedEnqueuePath(conn, p, status, token)
	}
	if status == "acquired" {
		r.installAndStartRenew(p, token, lease)
	}
	return status, nil
}

func (r *renewableResource) enqueueErrPath(ctx context.Context, conn *Conn, err error) error {
	conn.Close()
	r.clearConnIfCurrent(conn)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

func (r *renewableResource) cancelAfterEnqueue(conn *Conn, ctx context.Context) error {
	conn.Close()
	r.clearConnIfCurrent(conn)
	return ctx.Err()
}

func (r *renewableResource) orphanedEnqueuePath(conn *Conn, p flowParams, status, token string) error {
	if status == "acquired" {
		cleanupAbandonedGrant(conn, p.addr, p.tlsCfg, p.authToken, p.key, token, p.ops.release)
	} else {
		_ = conn.Close()
	}
	return net.ErrClosed
}

// ---------------------------------------------------------------------------
// Shared release flow
// ---------------------------------------------------------------------------

// runRelease stops renewal, releases the held token (if any), and
// closes the conn. Used by Lock.Release and Semaphore.Release.
func (r *renewableResource) runRelease(ctx context.Context, key string, fn releaseFn) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.stopRenew()
	if r.conn == nil {
		return nil
	}
	err := r.releaseHeldToken(ctx, key, fn)
	_ = r.closeConnLocked()
	return err
}

func (r *renewableResource) releaseHeldToken(ctx context.Context, key string, fn releaseFn) error {
	if r.token == "" {
		return nil
	}
	return releaseWithContext(ctx, r.conn, key, r.token, fn)
}

// ---------------------------------------------------------------------------
// Lock — high-level distributed lock
// ---------------------------------------------------------------------------

// Lock owns a single distributed lock.
type Lock struct {
	Key string
	// AcquireTimeout bounds how long Acquire/Wait blocks. The zero
	// value means 10s, NOT a non-blocking poll; the wire is
	// second-granular, so sub-second values round up to 1s. For a
	// true zero-timeout attempt use the low-level client.Acquire.
	AcquireTimeout time.Duration
	LeaseTTL       int
	Servers        []string
	ShardFunc      ShardFunc
	RenewRatio     float64
	RenewJitter    float64
	TLSConfig      *tls.Config
	AuthToken      string
	OnRenewError   func(err error)

	renewableResource
}

func (l *Lock) Acquire(ctx context.Context) (bool, error) {
	p, err := l.flowParams()
	if err != nil {
		return false, err
	}
	return l.runAcquire(ctx, p)
}

func (l *Lock) Enqueue(ctx context.Context) (string, error) {
	p, err := l.flowParams()
	if err != nil {
		return "", err
	}
	return l.runEnqueue(ctx, p)
}

func (l *Lock) Wait(ctx context.Context, timeout time.Duration) (bool, error) {
	p, err := l.flowParams()
	if err != nil {
		return false, err
	}
	return l.runWait(ctx, p, timeout)
}

func (l *Lock) Release(ctx context.Context) error {
	return l.runRelease(ctx, l.Key, Release)
}

// flowParams builds the per-call flowParams from a Lock's fields.
func (l *Lock) flowParams() (flowParams, error) {
	if err := l.preflight(); err != nil {
		return flowParams{}, err
	}
	addr, err := resolveServerAddr(l.Key, l.Servers, l.ShardFunc)
	if err != nil {
		return flowParams{}, err
	}
	return flowParams{
		key: l.Key, addr: addr, tlsCfg: l.TLSConfig, authToken: l.AuthToken,
		timeout: defaultAcquireTimeout(l.AcquireTimeout), opts: buildOpts(l.LeaseTTL),
		ratio: defaultRenewRatio(l.RenewRatio), jitter: defaultRenewJitterValue(l.RenewJitter),
		onRenewErr: l.OnRenewError, ops: lockOps,
	}, nil
}

func (l *Lock) preflight() error {
	if err := validateRenewConfig(l.LeaseTTL, l.RenewRatio, l.RenewJitter); err != nil {
		return err
	}
	return validateKey(l.Key)
}

// lockOps wires the package-level Acquire/Enqueue/Wait/Release/Renew
// into a resourceOps record for Lock.
var lockOps = resourceOps{
	acquire: Acquire,
	enqueue: Enqueue,
	wait:    Wait,
	release: Release,
	renew:   Renew,
}

// ---------------------------------------------------------------------------
// Semaphore — high-level distributed semaphore
// ---------------------------------------------------------------------------

// Semaphore is the multi-slot equivalent of Lock.
type Semaphore struct {
	Key   string
	Limit int
	// AcquireTimeout bounds how long Acquire/Wait blocks. The zero
	// value means 10s, NOT a non-blocking poll; sub-second values
	// round up to 1s. For a true zero-timeout attempt use the
	// low-level client.SemAcquire.
	AcquireTimeout time.Duration
	LeaseTTL       int
	Servers        []string
	ShardFunc      ShardFunc
	RenewRatio     float64
	RenewJitter    float64
	TLSConfig      *tls.Config
	AuthToken      string
	OnRenewError   func(err error)

	renewableResource
}

func (s *Semaphore) Acquire(ctx context.Context) (bool, error) {
	p, err := s.flowParams()
	if err != nil {
		return false, err
	}
	return s.runAcquire(ctx, p)
}

func (s *Semaphore) Enqueue(ctx context.Context) (string, error) {
	p, err := s.flowParams()
	if err != nil {
		return "", err
	}
	return s.runEnqueue(ctx, p)
}

func (s *Semaphore) Wait(ctx context.Context, timeout time.Duration) (bool, error) {
	p, err := s.flowParams()
	if err != nil {
		return false, err
	}
	return s.runWait(ctx, p, timeout)
}

func (s *Semaphore) Release(ctx context.Context) error {
	return s.runRelease(ctx, s.Key, SemRelease)
}

// flowParams builds the per-call flowParams from a Semaphore's fields.
func (s *Semaphore) flowParams() (flowParams, error) {
	if err := s.preflight(); err != nil {
		return flowParams{}, err
	}
	addr, err := resolveServerAddr(s.Key, s.Servers, s.ShardFunc)
	if err != nil {
		return flowParams{}, err
	}
	return flowParams{
		key: s.Key, addr: addr, tlsCfg: s.TLSConfig, authToken: s.AuthToken,
		timeout: defaultAcquireTimeout(s.AcquireTimeout), opts: buildOpts(s.LeaseTTL),
		ratio: defaultRenewRatio(s.RenewRatio), jitter: defaultRenewJitterValue(s.RenewJitter),
		onRenewErr: s.OnRenewError, ops: s.ops(),
	}, nil
}

func (s *Semaphore) preflight() error {
	if err := validateRenewConfig(s.LeaseTTL, s.RenewRatio, s.RenewJitter); err != nil {
		return err
	}
	if err := validateSemaphoreLimit(s.Limit); err != nil {
		return err
	}
	return validateKey(s.Key)
}

// ops returns a resourceOps with the limit baked into acquire/enqueue.
func (s *Semaphore) ops() resourceOps {
	limit := s.Limit
	return resourceOps{
		acquire: func(c *Conn, k string, t time.Duration, opts ...Option) (string, int, error) {
			return SemAcquire(c, k, t, limit, opts...)
		},
		enqueue: func(c *Conn, k string, opts ...Option) (string, string, int, error) {
			return SemEnqueue(c, k, limit, opts...)
		},
		wait:    SemWait,
		release: SemRelease,
		renew:   SemRenew,
	}
}
