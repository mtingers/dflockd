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
// implementation. Fields are unexported to keep the surface clean.
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

// Close stops the renewal goroutine and closes the connection. The
// server will auto-release any held slot when AutoReleaseOnDisconnect
// is enabled. Idempotent.
func (r *renewableResource) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stopRenew()
	if r.conn == nil {
		return nil
	}
	err := r.conn.Close()
	r.conn = nil
	r.token = ""
	r.lease = 0
	return err
}

// clearConnIfCurrent zeroes the live state when conn matches r.conn,
// used after a network error.
func (r *renewableResource) clearConnIfCurrent(conn *Conn) {
	if r.conn == conn {
		r.conn = nil
		r.token = ""
		r.lease = 0
	}
}

// stopRenewGrace bounds how long stopRenew waits for the renewal
// goroutine to notice cancellation before force-closing the conn. A
// hung server's Renew shouldn't wedge Release().
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
	// Goroutine still alive — force-close the conn so its in-flight
	// Renew I/O errors out.
	if conn != nil {
		_ = conn.Close()
	}
	<-done
}

// connect dials addr (optionally over TLS) and authenticates. Closes
// any pre-existing connection first. Must be called with r.mu held.
func (r *renewableResource) connect(addr string, tlsCfg *tls.Config, authToken string) error {
	if r.conn != nil {
		r.conn.Close()
	}
	r.conn = nil
	r.token = ""
	r.lease = 0
	var (
		conn *Conn
		err  error
	)
	if tlsCfg != nil {
		conn, err = DialTLS(addr, tlsCfg)
	} else {
		conn, err = Dial(addr)
	}
	if err != nil {
		return err
	}
	if authToken != "" {
		if err := Authenticate(conn, authToken); err != nil {
			conn.Close()
			return err
		}
	}
	r.conn = conn
	return nil
}

// renewFn is the protocol-level renew used by the background loop:
// either Renew (locks) or SemRenew (semaphores).
type renewFn func(*Conn, string, string, ...Option) (int, error)

// startRenewLoop spawns the background renewal goroutine. Must be
// called with r.mu held.
func (r *renewableResource) startRenewLoop(key string, leaseSec int, ratio, jitter float64, opts []Option, fn renewFn, onErr func(error)) {
	r.stopRenew()
	ctx, cancel := context.WithCancel(context.Background())
	r.cancelRenew = cancel
	done := make(chan struct{})
	r.renewDone = done

	interval := renewInterval(leaseSec, ratio)

	go func() {
		defer close(done)
		timer := time.NewTimer(jitteredInterval(interval, jitter))
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				r.mu.Lock()
				if r.conn == nil || r.token == "" {
					r.mu.Unlock()
					return
				}
				conn := r.conn
				tok := r.token
				r.mu.Unlock()

				if _, err := fn(conn, key, tok, opts...); err != nil {
					if ctx.Err() != nil {
						return
					}
					if onErr != nil {
						onErr(err)
					}
					return
				}
				timer.Reset(jitteredInterval(interval, jitter))
			}
		}
	}()
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

// validateRenewConfig rejects values that would silently produce broken
// runtime behaviour: a ratio >= 1.0 schedules the first renewal at or
// past lease expiry, so the lock would always be lost before renewal.
func validateRenewConfig(leaseTTL int, renewRatio, renewJitter float64) error {
	if leaseTTL < 0 {
		return fmt.Errorf("dflockd: LeaseTTL must be >= 0 (got %d)", leaseTTL)
	}
	if int64(leaseTTL) > maxProtocolSeconds {
		return fmt.Errorf("dflockd: LeaseTTL too large (max %d)", maxProtocolSeconds)
	}
	if math.IsNaN(renewRatio) || renewRatio < 0 || renewRatio >= 1 {
		return fmt.Errorf("dflockd: RenewRatio must be in [0, 1) (got %v)", renewRatio)
	}
	if math.IsNaN(renewJitter) || renewJitter < 0 || renewJitter >= 1 {
		return fmt.Errorf("dflockd: RenewJitter must be in [0, 1) (got %v)", renewJitter)
	}
	return nil
}

// buildOpts constructs the Option slice from a lease TTL value.
// nil when leaseTTL is 0 so the server's default applies.
func buildOpts(leaseTTL int) []Option {
	if leaseTTL > 0 {
		return []Option{WithLeaseTTL(leaseTTL)}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Connection-watch and abandoned-grant cleanup helpers
// ---------------------------------------------------------------------------

// closeConnOnContextDone closes conn if ctx is cancelled before stop()
// is called. The returned stop function waits for the watcher to exit
// so a later cancellation can't race us into closing a connection
// after the operation already returned.
func closeConnOnContextDone(ctx context.Context, conn interface{ Close() error }) func() {
	done := make(chan struct{})
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-done:
		}
	}()
	return func() {
		close(done)
		<-exited
	}
}

// abandonedGrantCleanupTimeout caps best-effort cleanup of a token
// granted just as the caller cancelled.
const abandonedGrantCleanupTimeout = 2 * time.Second

// releaseFn is the protocol-level release used by cleanup.
type releaseFn func(*Conn, string, string) error

// tryReleaseWithDeadline runs releaseFn under a fixed deadline so
// cleanup never wedges a cancelled caller.
func tryReleaseWithDeadline(c *Conn, key, token string, fn releaseFn) error {
	if c == nil {
		return net.ErrClosed
	}
	_ = c.conn.SetDeadline(time.Now().Add(abandonedGrantCleanupTimeout))
	err := fn(c, key, token)
	_ = c.conn.SetDeadline(time.Time{})
	return err
}

// dialCleanupConn opens a fresh connection for cleanup when the
// original conn is unusable.
func dialCleanupConn(addr string, tlsCfg *tls.Config, authToken string) (*Conn, error) {
	var (
		conn *Conn
		err  error
	)
	if tlsCfg != nil {
		conn, err = DialTLS(addr, tlsCfg)
	} else {
		conn, err = Dial(addr)
	}
	if err != nil {
		return nil, err
	}
	if authToken != "" {
		if err := Authenticate(conn, authToken); err != nil {
			conn.Close()
			return nil, err
		}
	}
	return conn, nil
}

// cleanupAbandonedGrant releases (key, token) on conn; if that fails
// (e.g. conn was already torn down), it dials a fresh connection and
// retries once. Best-effort.
func cleanupAbandonedGrant(conn *Conn, addr string, tlsCfg *tls.Config, authToken, key, token string, fn releaseFn) {
	if token == "" {
		return
	}
	if tryReleaseWithDeadline(conn, key, token, fn) == nil {
		return
	}
	cleanupConn, err := dialCleanupConn(addr, tlsCfg, authToken)
	if err != nil {
		return
	}
	defer cleanupConn.Close()
	_ = tryReleaseWithDeadline(cleanupConn, key, token, fn)
}

// releaseWithContext runs fn with cancellation watching: if ctx fires
// while we're blocked in the network call, the conn is closed so the
// I/O errors out promptly.
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
// Lock — high-level distributed lock
// ---------------------------------------------------------------------------

// Lock owns a single distributed lock: it dials the right server,
// performs the acquire, runs background renewal, and releases on
// teardown.
type Lock struct {
	Key            string
	AcquireTimeout time.Duration   // default 10s
	LeaseTTL       int             // custom lease TTL in seconds; 0 = server default
	Servers        []string        // e.g. ["127.0.0.1:6388"]
	ShardFunc      ShardFunc       // defaults to CRC32Shard
	RenewRatio     float64         // fraction of lease at which to renew; default 0.5
	RenewJitter    float64         // early-only jitter; default 0.10
	TLSConfig      *tls.Config     // if non-nil, connect via TLS
	AuthToken      string          // if non-empty, authenticate on connect
	OnRenewError   func(err error) // optional callback when background renewal fails

	renewableResource
}

func (l *Lock) acqTimeout() time.Duration   { return defaultAcquireTimeout(l.AcquireTimeout) }
func (l *Lock) renewRatio() float64         { return defaultRenewRatio(l.RenewRatio) }
func (l *Lock) renewJitter() float64        { return defaultRenewJitterValue(l.RenewJitter) }
func (l *Lock) serverAddr() (string, error) { return resolveServerAddr(l.Key, l.Servers, l.ShardFunc) }
func (l *Lock) opts() []Option              { return buildOpts(l.LeaseTTL) }

// Acquire connects, runs single-phase acquire, and starts the renewal
// loop. Returns false (with nil err) on timeout. ctx cancellation
// closes the connection to unblock the server-side wait.
func (l *Lock) Acquire(ctx context.Context) (bool, error) {
	if err := validateRenewConfig(l.LeaseTTL, l.RenewRatio, l.RenewJitter); err != nil {
		return false, err
	}
	if err := validateKey(l.Key); err != nil {
		return false, err
	}
	l.mu.Lock()
	l.stopRenew()
	addr, err := l.serverAddr()
	if err != nil {
		l.mu.Unlock()
		return false, err
	}
	if err := l.connect(addr, l.TLSConfig, l.AuthToken); err != nil {
		l.mu.Unlock()
		return false, err
	}
	conn := l.conn
	l.mu.Unlock()

	stop := closeConnOnContextDone(ctx, conn)
	token, lease, err := Acquire(conn, l.Key, l.acqTimeout(), l.opts()...)
	stop()
	if err == nil && ctx.Err() != nil {
		cleanupAbandonedGrant(conn, addr, l.TLSConfig, l.AuthToken, l.Key, token, Release)
	}

	l.mu.Lock()
	if err != nil {
		if errors.Is(err, ErrTimeout) {
			conn.Close()
			l.clearConnIfCurrent(conn)
			l.mu.Unlock()
			return false, nil
		}
		// Always close to avoid FD leak. net.Conn.Close is idempotent.
		conn.Close()
		l.clearConnIfCurrent(conn)
		l.mu.Unlock()
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		return false, err
	}
	if ctx.Err() != nil {
		conn.Close()
		l.clearConnIfCurrent(conn)
		l.mu.Unlock()
		return false, ctx.Err()
	}
	if l.conn != conn {
		// Concurrent re-Acquire stole this conn; release the orphan grant.
		l.mu.Unlock()
		cleanupAbandonedGrant(conn, addr, l.TLSConfig, l.AuthToken, l.Key, token, Release)
		return false, net.ErrClosed
	}
	l.token = token
	l.lease = lease
	l.startRenewLoop(l.Key, l.lease, l.renewRatio(), l.renewJitter(), l.opts(), Renew, l.OnRenewError)
	l.mu.Unlock()
	return true, nil
}

// Enqueue performs phase 1 of two-phase locking. Returns "acquired"
// (with renewal already started) or "queued" (call Wait next).
func (l *Lock) Enqueue(ctx context.Context) (string, error) {
	if err := validateRenewConfig(l.LeaseTTL, l.RenewRatio, l.RenewJitter); err != nil {
		return "", err
	}
	if err := validateKey(l.Key); err != nil {
		return "", err
	}
	l.mu.Lock()
	l.stopRenew()
	addr, err := l.serverAddr()
	if err != nil {
		l.mu.Unlock()
		return "", err
	}
	if err := l.connect(addr, l.TLSConfig, l.AuthToken); err != nil {
		l.mu.Unlock()
		return "", err
	}
	conn := l.conn
	l.mu.Unlock()

	stop := closeConnOnContextDone(ctx, conn)
	status, token, lease, err := Enqueue(conn, l.Key, l.opts()...)
	stop()
	if err == nil && ctx.Err() != nil && status == "acquired" {
		cleanupAbandonedGrant(conn, addr, l.TLSConfig, l.AuthToken, l.Key, token, Release)
	}

	l.mu.Lock()
	if err != nil {
		conn.Close()
		l.clearConnIfCurrent(conn)
		l.mu.Unlock()
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", err
	}
	if ctx.Err() != nil {
		conn.Close()
		l.clearConnIfCurrent(conn)
		l.mu.Unlock()
		return "", ctx.Err()
	}
	if l.conn != conn {
		l.mu.Unlock()
		if status == "acquired" {
			cleanupAbandonedGrant(conn, addr, l.TLSConfig, l.AuthToken, l.Key, token, Release)
		} else {
			_ = conn.Close()
		}
		return "", net.ErrClosed
	}
	if status == "acquired" {
		l.token = token
		l.lease = lease
		l.startRenewLoop(l.Key, l.lease, l.renewRatio(), l.renewJitter(), l.opts(), Renew, l.OnRenewError)
	}
	l.mu.Unlock()
	return status, nil
}

// Wait performs phase 2 of two-phase locking. Must be called after
// Enqueue returned "queued". Returns false on timeout (and closes the
// connection — the caller must Enqueue again to re-queue).
func (l *Lock) Wait(ctx context.Context, timeout time.Duration) (bool, error) {
	if err := validateRenewConfig(l.LeaseTTL, l.RenewRatio, l.RenewJitter); err != nil {
		return false, err
	}
	if err := validateKey(l.Key); err != nil {
		return false, err
	}
	l.mu.Lock()
	if l.conn == nil {
		l.mu.Unlock()
		return false, ErrNotQueued
	}
	conn := l.conn
	addr, err := l.serverAddr()
	if err != nil {
		l.mu.Unlock()
		return false, err
	}
	l.mu.Unlock()

	stop := closeConnOnContextDone(ctx, conn)
	token, lease, err := Wait(conn, l.Key, timeout)
	stop()
	if err == nil && ctx.Err() != nil {
		cleanupAbandonedGrant(conn, addr, l.TLSConfig, l.AuthToken, l.Key, token, Release)
	}

	l.mu.Lock()
	if err != nil {
		if errors.Is(err, ErrTimeout) {
			conn.Close()
			l.clearConnIfCurrent(conn)
			l.mu.Unlock()
			return false, nil
		}
		conn.Close()
		l.clearConnIfCurrent(conn)
		l.mu.Unlock()
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		return false, err
	}
	if ctx.Err() != nil {
		conn.Close()
		l.clearConnIfCurrent(conn)
		l.mu.Unlock()
		return false, ctx.Err()
	}
	if l.conn != conn {
		l.mu.Unlock()
		cleanupAbandonedGrant(conn, addr, l.TLSConfig, l.AuthToken, l.Key, token, Release)
		return false, net.ErrClosed
	}
	l.token = token
	l.lease = lease
	l.startRenewLoop(l.Key, l.lease, l.renewRatio(), l.renewJitter(), l.opts(), Renew, l.OnRenewError)
	l.mu.Unlock()
	return true, nil
}

// Release stops renewal, releases the lock on the server, and closes
// the connection. If the caller is queued (Enqueue returned "queued"
// but Wait hasn't granted), there's no token to release; closing the
// conn is the protocol-level signal to abandon the waiter, and Release
// returns nil rather than surfacing a misleading "empty value" error.
func (l *Lock) Release(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.stopRenew()
	if l.conn == nil {
		return nil
	}
	var err error
	if l.token != "" {
		err = releaseWithContext(ctx, l.conn, l.Key, l.token, Release)
	}
	l.conn.Close()
	l.conn = nil
	l.token = ""
	l.lease = 0
	return err
}

// ---------------------------------------------------------------------------
// Semaphore — high-level distributed semaphore
// ---------------------------------------------------------------------------

// Semaphore is the multi-slot equivalent of Lock.
type Semaphore struct {
	Key            string
	Limit          int
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

func (s *Semaphore) acqTimeout() time.Duration { return defaultAcquireTimeout(s.AcquireTimeout) }
func (s *Semaphore) renewRatio() float64       { return defaultRenewRatio(s.RenewRatio) }
func (s *Semaphore) renewJitter() float64      { return defaultRenewJitterValue(s.RenewJitter) }
func (s *Semaphore) serverAddr() (string, error) {
	return resolveServerAddr(s.Key, s.Servers, s.ShardFunc)
}
func (s *Semaphore) opts() []Option { return buildOpts(s.LeaseTTL) }

// Acquire connects and runs single-phase semaphore acquire.
func (s *Semaphore) Acquire(ctx context.Context) (bool, error) {
	if err := validateRenewConfig(s.LeaseTTL, s.RenewRatio, s.RenewJitter); err != nil {
		return false, err
	}
	if err := validateSemaphoreLimit(s.Limit); err != nil {
		return false, err
	}
	if err := validateKey(s.Key); err != nil {
		return false, err
	}
	s.mu.Lock()
	s.stopRenew()
	addr, err := s.serverAddr()
	if err != nil {
		s.mu.Unlock()
		return false, err
	}
	if err := s.connect(addr, s.TLSConfig, s.AuthToken); err != nil {
		s.mu.Unlock()
		return false, err
	}
	conn := s.conn
	s.mu.Unlock()

	stop := closeConnOnContextDone(ctx, conn)
	token, lease, err := SemAcquire(conn, s.Key, s.acqTimeout(), s.Limit, s.opts()...)
	stop()
	if err == nil && ctx.Err() != nil {
		cleanupAbandonedGrant(conn, addr, s.TLSConfig, s.AuthToken, s.Key, token, SemRelease)
	}

	s.mu.Lock()
	if err != nil {
		if errors.Is(err, ErrTimeout) {
			conn.Close()
			s.clearConnIfCurrent(conn)
			s.mu.Unlock()
			return false, nil
		}
		conn.Close()
		s.clearConnIfCurrent(conn)
		s.mu.Unlock()
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		return false, err
	}
	if ctx.Err() != nil {
		conn.Close()
		s.clearConnIfCurrent(conn)
		s.mu.Unlock()
		return false, ctx.Err()
	}
	if s.conn != conn {
		s.mu.Unlock()
		cleanupAbandonedGrant(conn, addr, s.TLSConfig, s.AuthToken, s.Key, token, SemRelease)
		return false, net.ErrClosed
	}
	s.token = token
	s.lease = lease
	s.startRenewLoop(s.Key, s.lease, s.renewRatio(), s.renewJitter(), s.opts(), SemRenew, s.OnRenewError)
	s.mu.Unlock()
	return true, nil
}

// Enqueue performs phase 1 of two-phase semaphore acquire.
func (s *Semaphore) Enqueue(ctx context.Context) (string, error) {
	if err := validateRenewConfig(s.LeaseTTL, s.RenewRatio, s.RenewJitter); err != nil {
		return "", err
	}
	if err := validateSemaphoreLimit(s.Limit); err != nil {
		return "", err
	}
	if err := validateKey(s.Key); err != nil {
		return "", err
	}
	s.mu.Lock()
	s.stopRenew()
	addr, err := s.serverAddr()
	if err != nil {
		s.mu.Unlock()
		return "", err
	}
	if err := s.connect(addr, s.TLSConfig, s.AuthToken); err != nil {
		s.mu.Unlock()
		return "", err
	}
	conn := s.conn
	s.mu.Unlock()

	stop := closeConnOnContextDone(ctx, conn)
	status, token, lease, err := SemEnqueue(conn, s.Key, s.Limit, s.opts()...)
	stop()
	if err == nil && ctx.Err() != nil && status == "acquired" {
		cleanupAbandonedGrant(conn, addr, s.TLSConfig, s.AuthToken, s.Key, token, SemRelease)
	}

	s.mu.Lock()
	if err != nil {
		conn.Close()
		s.clearConnIfCurrent(conn)
		s.mu.Unlock()
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", err
	}
	if ctx.Err() != nil {
		conn.Close()
		s.clearConnIfCurrent(conn)
		s.mu.Unlock()
		return "", ctx.Err()
	}
	if s.conn != conn {
		s.mu.Unlock()
		if status == "acquired" {
			cleanupAbandonedGrant(conn, addr, s.TLSConfig, s.AuthToken, s.Key, token, SemRelease)
		} else {
			_ = conn.Close()
		}
		return "", net.ErrClosed
	}
	if status == "acquired" {
		s.token = token
		s.lease = lease
		s.startRenewLoop(s.Key, s.lease, s.renewRatio(), s.renewJitter(), s.opts(), SemRenew, s.OnRenewError)
	}
	s.mu.Unlock()
	return status, nil
}

// Wait performs phase 2 of two-phase semaphore acquire.
func (s *Semaphore) Wait(ctx context.Context, timeout time.Duration) (bool, error) {
	if err := validateRenewConfig(s.LeaseTTL, s.RenewRatio, s.RenewJitter); err != nil {
		return false, err
	}
	if err := validateKey(s.Key); err != nil {
		return false, err
	}
	s.mu.Lock()
	if s.conn == nil {
		s.mu.Unlock()
		return false, ErrNotQueued
	}
	conn := s.conn
	addr, err := s.serverAddr()
	if err != nil {
		s.mu.Unlock()
		return false, err
	}
	s.mu.Unlock()

	stop := closeConnOnContextDone(ctx, conn)
	token, lease, err := SemWait(conn, s.Key, timeout)
	stop()
	if err == nil && ctx.Err() != nil {
		cleanupAbandonedGrant(conn, addr, s.TLSConfig, s.AuthToken, s.Key, token, SemRelease)
	}

	s.mu.Lock()
	if err != nil {
		if errors.Is(err, ErrTimeout) {
			conn.Close()
			s.clearConnIfCurrent(conn)
			s.mu.Unlock()
			return false, nil
		}
		conn.Close()
		s.clearConnIfCurrent(conn)
		s.mu.Unlock()
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		return false, err
	}
	if ctx.Err() != nil {
		conn.Close()
		s.clearConnIfCurrent(conn)
		s.mu.Unlock()
		return false, ctx.Err()
	}
	if s.conn != conn {
		s.mu.Unlock()
		cleanupAbandonedGrant(conn, addr, s.TLSConfig, s.AuthToken, s.Key, token, SemRelease)
		return false, net.ErrClosed
	}
	s.token = token
	s.lease = lease
	s.startRenewLoop(s.Key, s.lease, s.renewRatio(), s.renewJitter(), s.opts(), SemRenew, s.OnRenewError)
	s.mu.Unlock()
	return true, nil
}

// Release stops renewal, releases the slot, and closes the connection.
func (s *Semaphore) Release(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.stopRenew()
	if s.conn == nil {
		return nil
	}
	var err error
	if s.token != "" {
		err = releaseWithContext(ctx, s.conn, s.Key, s.token, SemRelease)
	}
	s.conn.Close()
	s.conn = nil
	s.token = ""
	s.lease = 0
	return err
}
