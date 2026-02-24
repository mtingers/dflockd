// Package client provides a Go client for the dflockd distributed lock server.
package client

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"hash/crc32"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Sentinel errors returned by protocol operations.
var (
	ErrTimeout       = errors.New("dflockd: timeout")
	ErrMaxLocks      = errors.New("dflockd: max locks reached")
	ErrMaxWaiters    = errors.New("dflockd: max waiters reached")
	ErrServer        = errors.New("dflockd: server error")
	ErrNotQueued     = errors.New("dflockd: not enqueued")
	ErrLimitMismatch = errors.New("dflockd: limit mismatch")
	ErrAuth          = errors.New("dflockd: authentication failed")
)

// Option configures optional parameters for protocol commands.
type Option func(*options)

type options struct {
	leaseTTL int // seconds; 0 means use server default
}

// WithLeaseTTL sets a custom lease TTL (in seconds) for an Acquire or Enqueue call.
func WithLeaseTTL(seconds int) Option {
	return func(o *options) { o.leaseTTL = seconds }
}

// ---------------------------------------------------------------------------
// Conn — thin wrapper around net.Conn with a buffered reader
// ---------------------------------------------------------------------------

// Conn wraps a TCP connection to a dflockd server, providing a buffered reader
// for line-oriented protocol communication. Conn is safe for concurrent use;
// a mutex serializes request/response pairs to prevent interleaved I/O.
type Conn struct {
	mu     sync.Mutex
	conn   net.Conn
	reader *bufio.Reader
}

// Dial connects to a dflockd server at the given address (host:port).
func Dial(addr string) (*Conn, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Conn{conn: conn, reader: bufio.NewReader(conn)}, nil
}

// DialTLS connects to a dflockd server at the given address using TLS.
func DialTLS(addr string, cfg *tls.Config) (*Conn, error) {
	conn, err := tls.Dial("tcp", addr, cfg)
	if err != nil {
		return nil, err
	}
	return &Conn{conn: conn, reader: bufio.NewReader(conn)}, nil
}

// Close closes the underlying TCP connection.
func (c *Conn) Close() error {
	return c.conn.Close()
}

// maxResponseBytes is the maximum length of a single server response line.
// Server responses are short (status + token + lease), so 512 bytes is generous.
const maxResponseBytes = 512

// sendRecv sends a 3-line protocol command and reads one response line.
// The mutex ensures that concurrent callers (e.g. a renewal goroutine)
// cannot interleave their request/response bytes.
func (c *Conn) sendRecv(cmd, key, arg string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	msg := fmt.Sprintf("%s\n%s\n%s\n", cmd, key, arg)
	if _, err := c.conn.Write([]byte(msg)); err != nil {
		return "", err
	}
	return c.readLine()
}

// readLine reads a single newline-terminated line, enforcing maxResponseBytes
// to prevent unbounded memory allocation from a malicious server.
// Must be called with c.mu held.
func (c *Conn) readLine() (string, error) {
	var buf [maxResponseBytes]byte
	n := 0
	for {
		b, err := c.reader.ReadByte()
		if err != nil {
			return "", err
		}
		if b == '\n' {
			break
		}
		if n >= maxResponseBytes {
			return "", fmt.Errorf("dflockd: server response too long")
		}
		buf[n] = b
		n++
	}
	return strings.TrimRight(string(buf[:n]), "\r"), nil
}

// ---------------------------------------------------------------------------
// Authentication
// ---------------------------------------------------------------------------

// Authenticate sends an auth command with the given token. Returns nil on
// success, ErrAuth if the server rejects the token.
func Authenticate(c *Conn, token string) error {
	resp, err := c.sendRecv("auth", "_", token)
	if err != nil {
		return err
	}
	if resp != "ok" {
		return ErrAuth
	}
	return nil
}

// ---------------------------------------------------------------------------
// Low-level protocol functions
// ---------------------------------------------------------------------------

// Acquire sends a lock ("l") command. It blocks on the server side until the
// lock is acquired or acquireTimeout expires. Returns the token, lease TTL in
// seconds, and any error. Returns ErrTimeout if the server reports a timeout.
func Acquire(c *Conn, key string, acquireTimeout time.Duration, opts ...Option) (token string, leaseTTL int, err error) {
	var o options
	for _, fn := range opts {
		fn(&o)
	}
	arg := strconv.Itoa(int(acquireTimeout.Seconds()))
	if o.leaseTTL > 0 {
		arg += " " + strconv.Itoa(o.leaseTTL)
	}

	resp, err := c.sendRecv("l", key, arg)
	if err != nil {
		return "", 0, err
	}
	return parseAcquireResponse(resp)
}

// Release sends a release ("r") command for the given key and token.
func Release(c *Conn, key, token string) error {
	resp, err := c.sendRecv("r", key, token)
	if err != nil {
		return err
	}
	if resp != "ok" {
		return fmt.Errorf("%w: release: %s", ErrServer, resp)
	}
	return nil
}

// Renew sends a renew ("n") command and returns the remaining lease seconds.
func Renew(c *Conn, key, token string, opts ...Option) (remaining int, err error) {
	var o options
	for _, fn := range opts {
		fn(&o)
	}
	arg := token
	if o.leaseTTL > 0 {
		arg += " " + strconv.Itoa(o.leaseTTL)
	}

	resp, err := c.sendRecv("n", key, arg)
	if err != nil {
		return 0, err
	}

	parts := strings.Fields(resp)
	if len(parts) == 2 && parts[0] == "ok" {
		r, err := strconv.Atoi(parts[1])
		if err != nil {
			return 0, fmt.Errorf("%w: renew: bad remaining %q", ErrServer, parts[1])
		}
		return r, nil
	}
	return 0, fmt.Errorf("%w: renew: %s", ErrServer, resp)
}

// Enqueue sends an enqueue ("e") command. Returns the status ("acquired" or
// "queued"), and if acquired, the token and lease TTL.
func Enqueue(c *Conn, key string, opts ...Option) (status, token string, leaseTTL int, err error) {
	var o options
	for _, fn := range opts {
		fn(&o)
	}
	arg := ""
	if o.leaseTTL > 0 {
		arg = strconv.Itoa(o.leaseTTL)
	}

	resp, err := c.sendRecv("e", key, arg)
	if err != nil {
		return "", "", 0, err
	}

	if resp == "queued" {
		return "queued", "", 0, nil
	}
	if resp == "error_max_locks" {
		return "", "", 0, ErrMaxLocks
	}
	if resp == "error_max_waiters" {
		return "", "", 0, ErrMaxWaiters
	}

	parts := strings.Fields(resp)
	if len(parts) == 3 && parts[0] == "acquired" {
		ttl, err := strconv.Atoi(parts[2])
		if err != nil {
			return "", "", 0, fmt.Errorf("%w: enqueue: bad lease %q", ErrServer, parts[2])
		}
		return "acquired", parts[1], ttl, nil
	}
	return "", "", 0, fmt.Errorf("%w: enqueue: %s", ErrServer, resp)
}

// Wait sends a wait ("w") command after a prior Enqueue. It blocks until the
// lock is granted or waitTimeout expires. Returns the token, lease TTL, and
// any error. Returns ErrTimeout on timeout, ErrNotQueued if not enqueued.
func Wait(c *Conn, key string, waitTimeout time.Duration) (token string, leaseTTL int, err error) {
	arg := strconv.Itoa(int(waitTimeout.Seconds()))
	resp, err := c.sendRecv("w", key, arg)
	if err != nil {
		return "", 0, err
	}
	if resp == "timeout" {
		return "", 0, ErrTimeout
	}
	if resp == "error" {
		return "", 0, ErrNotQueued
	}
	return parseOKTokenLease(resp, "wait")
}

// ---------------------------------------------------------------------------
// Low-level semaphore protocol functions
// ---------------------------------------------------------------------------

// SemAcquire sends a semaphore acquire ("sl") command. Returns the token,
// lease TTL in seconds, and any error. Returns ErrTimeout on timeout,
// ErrLimitMismatch if the limit doesn't match the existing semaphore.
func SemAcquire(c *Conn, key string, acquireTimeout time.Duration, limit int, opts ...Option) (token string, leaseTTL int, err error) {
	var o options
	for _, fn := range opts {
		fn(&o)
	}
	arg := strconv.Itoa(int(acquireTimeout.Seconds())) + " " + strconv.Itoa(limit)
	if o.leaseTTL > 0 {
		arg += " " + strconv.Itoa(o.leaseTTL)
	}

	resp, err := c.sendRecv("sl", key, arg)
	if err != nil {
		return "", 0, err
	}
	return parseSemAcquireResponse(resp)
}

// SemRelease sends a semaphore release ("sr") command for the given key and token.
func SemRelease(c *Conn, key, token string) error {
	resp, err := c.sendRecv("sr", key, token)
	if err != nil {
		return err
	}
	if resp != "ok" {
		return fmt.Errorf("%w: sem_release: %s", ErrServer, resp)
	}
	return nil
}

// SemRenew sends a semaphore renew ("sn") command and returns the remaining lease seconds.
func SemRenew(c *Conn, key, token string, opts ...Option) (remaining int, err error) {
	var o options
	for _, fn := range opts {
		fn(&o)
	}
	arg := token
	if o.leaseTTL > 0 {
		arg += " " + strconv.Itoa(o.leaseTTL)
	}

	resp, err := c.sendRecv("sn", key, arg)
	if err != nil {
		return 0, err
	}

	parts := strings.Fields(resp)
	if len(parts) == 2 && parts[0] == "ok" {
		r, err := strconv.Atoi(parts[1])
		if err != nil {
			return 0, fmt.Errorf("%w: sem_renew: bad remaining %q", ErrServer, parts[1])
		}
		return r, nil
	}
	return 0, fmt.Errorf("%w: sem_renew: %s", ErrServer, resp)
}

// SemEnqueue sends a semaphore enqueue ("se") command. Returns the status
// ("acquired" or "queued"), and if acquired, the token and lease TTL.
func SemEnqueue(c *Conn, key string, limit int, opts ...Option) (status, token string, leaseTTL int, err error) {
	var o options
	for _, fn := range opts {
		fn(&o)
	}
	arg := strconv.Itoa(limit)
	if o.leaseTTL > 0 {
		arg += " " + strconv.Itoa(o.leaseTTL)
	}

	resp, err := c.sendRecv("se", key, arg)
	if err != nil {
		return "", "", 0, err
	}

	if resp == "queued" {
		return "queued", "", 0, nil
	}
	if resp == "error_max_locks" {
		return "", "", 0, ErrMaxLocks
	}
	if resp == "error_max_waiters" {
		return "", "", 0, ErrMaxWaiters
	}
	if resp == "error_limit_mismatch" {
		return "", "", 0, ErrLimitMismatch
	}

	parts := strings.Fields(resp)
	if len(parts) == 3 && parts[0] == "acquired" {
		ttl, err := strconv.Atoi(parts[2])
		if err != nil {
			return "", "", 0, fmt.Errorf("%w: sem_enqueue: bad lease %q", ErrServer, parts[2])
		}
		return "acquired", parts[1], ttl, nil
	}
	return "", "", 0, fmt.Errorf("%w: sem_enqueue: %s", ErrServer, resp)
}

// SemWait sends a semaphore wait ("sw") command after a prior SemEnqueue.
func SemWait(c *Conn, key string, waitTimeout time.Duration) (token string, leaseTTL int, err error) {
	arg := strconv.Itoa(int(waitTimeout.Seconds()))
	resp, err := c.sendRecv("sw", key, arg)
	if err != nil {
		return "", 0, err
	}
	if resp == "timeout" {
		return "", 0, ErrTimeout
	}
	if resp == "error" {
		return "", 0, ErrNotQueued
	}
	return parseOKTokenLease(resp, "sem_wait")
}

// ---------------------------------------------------------------------------
// Response parsing helpers
// ---------------------------------------------------------------------------

func parseSemAcquireResponse(resp string) (string, int, error) {
	if resp == "timeout" {
		return "", 0, ErrTimeout
	}
	if resp == "error_max_locks" {
		return "", 0, ErrMaxLocks
	}
	if resp == "error_max_waiters" {
		return "", 0, ErrMaxWaiters
	}
	if resp == "error_limit_mismatch" {
		return "", 0, ErrLimitMismatch
	}
	return parseOKTokenLease(resp, "sem_acquire")
}

func parseAcquireResponse(resp string) (string, int, error) {
	if resp == "timeout" {
		return "", 0, ErrTimeout
	}
	if resp == "error_max_locks" {
		return "", 0, ErrMaxLocks
	}
	if resp == "error_max_waiters" {
		return "", 0, ErrMaxWaiters
	}
	return parseOKTokenLease(resp, "acquire")
}

func parseOKTokenLease(resp, cmd string) (string, int, error) {
	parts := strings.Fields(resp)
	if len(parts) == 3 && parts[0] == "ok" {
		ttl, err := strconv.Atoi(parts[2])
		if err != nil {
			return "", 0, fmt.Errorf("%w: %s: bad lease %q", ErrServer, cmd, parts[2])
		}
		return parts[1], ttl, nil
	}
	return "", 0, fmt.Errorf("%w: %s: %s", ErrServer, cmd, resp)
}

// ---------------------------------------------------------------------------
// Sharding
// ---------------------------------------------------------------------------

// ShardFunc maps a key to a server index given the number of servers.
type ShardFunc func(key string, numServers int) int

// CRC32Shard returns a shard index using CRC-32 (IEEE). This matches the
// Python client's zlib.crc32-based stable_hash_shard.
// Returns 0 if numServers <= 0.
func CRC32Shard(key string, numServers int) int {
	if numServers <= 0 {
		return 0
	}
	h := crc32.ChecksumIEEE([]byte(key))
	return int(h % uint32(numServers))
}

// ---------------------------------------------------------------------------
// Lock — high-level distributed lock
// ---------------------------------------------------------------------------

// Lock provides a high-level interface for acquiring, holding, and releasing
// a distributed lock, including automatic lease renewal in the background.
type Lock struct {
	Key            string
	AcquireTimeout time.Duration // default 10s
	LeaseTTL       int           // custom lease TTL in seconds; 0 = server default
	Servers        []string      // e.g. ["127.0.0.1:6388"]
	ShardFunc      ShardFunc     // defaults to CRC32Shard
	RenewRatio     float64       // fraction of lease at which to renew; default 0.5
	TLSConfig      *tls.Config   // if non-nil, connect using TLS
	AuthToken      string        // if non-empty, authenticate after connecting

	mu          sync.Mutex
	conn        *Conn
	token       string
	lease       int
	cancelRenew context.CancelFunc
	renewDone   chan struct{} // closed when the renew goroutine exits
}

func (l *Lock) shardFunc() ShardFunc {
	if l.ShardFunc != nil {
		return l.ShardFunc
	}
	return CRC32Shard
}

func (l *Lock) acquireTimeout() time.Duration {
	if l.AcquireTimeout > 0 {
		return l.AcquireTimeout
	}
	return 10 * time.Second
}

func (l *Lock) renewRatio() float64 {
	if l.RenewRatio > 0 {
		return l.RenewRatio
	}
	return 0.5
}

func (l *Lock) serverAddr() string {
	servers := l.Servers
	if len(servers) == 0 {
		servers = []string{"127.0.0.1:6388"}
	}
	idx := l.shardFunc()(l.Key, len(servers))
	return servers[idx]
}

func (l *Lock) opts() []Option {
	if l.LeaseTTL > 0 {
		return []Option{WithLeaseTTL(l.LeaseTTL)}
	}
	return nil
}

// connect dials the appropriate shard server.
func (l *Lock) connect() error {
	addr := l.serverAddr()
	var conn *Conn
	var err error
	if l.TLSConfig != nil {
		conn, err = DialTLS(addr, l.TLSConfig)
	} else {
		conn, err = Dial(addr)
	}
	if err != nil {
		return err
	}
	if l.AuthToken != "" {
		if err := Authenticate(conn, l.AuthToken); err != nil {
			conn.Close()
			return err
		}
	}
	l.conn = conn
	return nil
}

// Acquire connects to the server, acquires the lock, and starts a background
// goroutine to renew the lease. Returns false (with nil error) on timeout.
// The provided context controls cancellation; if it is cancelled, the
// connection is closed which unblocks the server-side wait.
func (l *Lock) Acquire(ctx context.Context) (bool, error) {
	l.mu.Lock()
	if err := l.connect(); err != nil {
		l.mu.Unlock()
		return false, err
	}
	conn := l.conn
	l.mu.Unlock()

	// If context is cancellable, close the connection on cancel to unblock.
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			conn.Close()
		case <-done:
		}
	}()

	token, lease, err := Acquire(conn, l.Key, l.acquireTimeout(), l.opts()...)
	close(done)

	l.mu.Lock()
	defer l.mu.Unlock()

	if err != nil {
		if errors.Is(err, ErrTimeout) {
			l.conn.Close()
			l.conn = nil
			return false, nil
		}
		// If context was cancelled, the conn.Close may have caused an I/O error.
		if ctx.Err() != nil {
			l.conn = nil
			return false, ctx.Err()
		}
		l.conn.Close()
		l.conn = nil
		return false, err
	}

	l.token = token
	l.lease = lease
	l.startRenew()
	return true, nil
}

// Enqueue performs the first phase of two-phase locking. Returns "acquired" or
// "queued". If acquired, a renewal goroutine is started automatically.
// The provided context controls cancellation; if cancelled, the connection
// is closed which unblocks any in-progress server I/O.
func (l *Lock) Enqueue(ctx context.Context) (string, error) {
	l.mu.Lock()
	if err := l.connect(); err != nil {
		l.mu.Unlock()
		return "", err
	}
	conn := l.conn
	l.mu.Unlock()

	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			conn.Close()
		case <-done:
		}
	}()

	status, token, lease, err := Enqueue(conn, l.Key, l.opts()...)
	close(done)

	l.mu.Lock()
	defer l.mu.Unlock()

	if err != nil {
		if ctx.Err() != nil {
			l.conn = nil
			return "", ctx.Err()
		}
		l.conn.Close()
		l.conn = nil
		return "", err
	}

	if status == "acquired" {
		l.token = token
		l.lease = lease
		l.startRenew()
	}
	return status, nil
}

// Wait performs the second phase of two-phase locking. Must be called after
// Enqueue returned "queued". Returns false (with nil error) on timeout.
func (l *Lock) Wait(ctx context.Context, timeout time.Duration) (bool, error) {
	l.mu.Lock()
	if l.conn == nil {
		l.mu.Unlock()
		return false, ErrNotQueued
	}
	conn := l.conn
	l.mu.Unlock()

	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			conn.Close()
		case <-done:
		}
	}()

	token, lease, err := Wait(conn, l.Key, timeout)
	close(done)

	l.mu.Lock()
	defer l.mu.Unlock()

	if err != nil {
		if errors.Is(err, ErrTimeout) {
			return false, nil
		}
		if ctx.Err() != nil {
			l.conn = nil
			return false, ctx.Err()
		}
		l.conn.Close()
		l.conn = nil
		return false, err
	}

	l.token = token
	l.lease = lease
	l.startRenew()
	return true, nil
}

// stopRenew cancels the renewal goroutine and waits for it to exit.
// Must be called with l.mu held; temporarily releases the lock to wait.
func (l *Lock) stopRenew() {
	if l.cancelRenew != nil {
		l.cancelRenew()
		l.cancelRenew = nil
	}
	if l.renewDone != nil {
		done := l.renewDone
		l.renewDone = nil
		// Release the mutex while waiting so the goroutine can finish
		// its in-progress Renew (which also grabs l.mu via Conn.sendRecv).
		l.mu.Unlock()
		<-done
		l.mu.Lock()
	}
}

// Release stops the renewal goroutine, releases the lock on the server, and
// closes the connection. The ctx parameter is reserved for future use.
func (l *Lock) Release(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.stopRenew()

	if l.conn == nil {
		return nil
	}

	err := Release(l.conn, l.Key, l.token)
	l.conn.Close()
	l.conn = nil
	l.token = ""
	return err
}

// Close stops renewal and closes the connection without explicitly releasing
// the lock. The server will auto-release if configured to do so.
func (l *Lock) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.stopRenew()

	if l.conn == nil {
		return nil
	}

	err := l.conn.Close()
	l.conn = nil
	l.token = ""
	return err
}

// Token returns the current lock token, or "" if not acquired.
func (l *Lock) Token() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.token
}

// startRenew launches a background goroutine that renews the lease at
// renewRatio * leaseTTL intervals. Must be called with l.mu held.
func (l *Lock) startRenew() {
	l.stopRenew()
	ctx, cancel := context.WithCancel(context.Background())
	l.cancelRenew = cancel

	done := make(chan struct{})
	l.renewDone = done

	interval := time.Duration(float64(l.lease)*l.renewRatio()) * time.Second
	if interval < 1*time.Second {
		interval = 1 * time.Second
	}

	conn := l.conn
	key := l.Key
	token := l.token
	opts := l.opts()

	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				l.mu.Lock()
				if l.conn == nil || l.token == "" {
					l.mu.Unlock()
					return
				}
				// Re-read in case of reconnection (future-proofing).
				conn = l.conn
				key = l.Key
				token = l.token
				l.mu.Unlock()

				_, _ = Renew(conn, key, token, opts...)
			}
		}
	}()
}

// ---------------------------------------------------------------------------
// Semaphore — high-level distributed semaphore
// ---------------------------------------------------------------------------

// Semaphore provides a high-level interface for acquiring, holding, and
// releasing a distributed semaphore slot, including automatic lease renewal.
type Semaphore struct {
	Key            string
	Limit          int
	AcquireTimeout time.Duration // default 10s
	LeaseTTL       int           // custom lease TTL in seconds; 0 = server default
	Servers        []string      // e.g. ["127.0.0.1:6388"]
	ShardFunc      ShardFunc     // defaults to CRC32Shard
	RenewRatio     float64       // fraction of lease at which to renew; default 0.5
	TLSConfig      *tls.Config   // if non-nil, connect using TLS
	AuthToken      string        // if non-empty, authenticate after connecting

	mu          sync.Mutex
	conn        *Conn
	token       string
	lease       int
	cancelRenew context.CancelFunc
	renewDone   chan struct{}
}

func (s *Semaphore) shardFunc() ShardFunc {
	if s.ShardFunc != nil {
		return s.ShardFunc
	}
	return CRC32Shard
}

func (s *Semaphore) acquireTimeout() time.Duration {
	if s.AcquireTimeout > 0 {
		return s.AcquireTimeout
	}
	return 10 * time.Second
}

func (s *Semaphore) renewRatio() float64 {
	if s.RenewRatio > 0 {
		return s.RenewRatio
	}
	return 0.5
}

func (s *Semaphore) serverAddr() string {
	servers := s.Servers
	if len(servers) == 0 {
		servers = []string{"127.0.0.1:6388"}
	}
	idx := s.shardFunc()(s.Key, len(servers))
	return servers[idx]
}

func (s *Semaphore) opts() []Option {
	if s.LeaseTTL > 0 {
		return []Option{WithLeaseTTL(s.LeaseTTL)}
	}
	return nil
}

func (s *Semaphore) connect() error {
	addr := s.serverAddr()
	var conn *Conn
	var err error
	if s.TLSConfig != nil {
		conn, err = DialTLS(addr, s.TLSConfig)
	} else {
		conn, err = Dial(addr)
	}
	if err != nil {
		return err
	}
	if s.AuthToken != "" {
		if err := Authenticate(conn, s.AuthToken); err != nil {
			conn.Close()
			return err
		}
	}
	s.conn = conn
	return nil
}

// Acquire connects to the server, acquires a semaphore slot, and starts
// background lease renewal. Returns false (with nil error) on timeout.
func (s *Semaphore) Acquire(ctx context.Context) (bool, error) {
	s.mu.Lock()
	if err := s.connect(); err != nil {
		s.mu.Unlock()
		return false, err
	}
	conn := s.conn
	s.mu.Unlock()

	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			conn.Close()
		case <-done:
		}
	}()

	token, lease, err := SemAcquire(conn, s.Key, s.acquireTimeout(), s.Limit, s.opts()...)
	close(done)

	s.mu.Lock()
	defer s.mu.Unlock()

	if err != nil {
		if errors.Is(err, ErrTimeout) {
			s.conn.Close()
			s.conn = nil
			return false, nil
		}
		if ctx.Err() != nil {
			s.conn = nil
			return false, ctx.Err()
		}
		s.conn.Close()
		s.conn = nil
		return false, err
	}

	s.token = token
	s.lease = lease
	s.startRenew()
	return true, nil
}

// Enqueue performs the first phase of two-phase semaphore acquire.
// The provided context controls cancellation; if cancelled, the connection
// is closed which unblocks any in-progress server I/O.
func (s *Semaphore) Enqueue(ctx context.Context) (string, error) {
	s.mu.Lock()
	if err := s.connect(); err != nil {
		s.mu.Unlock()
		return "", err
	}
	conn := s.conn
	s.mu.Unlock()

	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			conn.Close()
		case <-done:
		}
	}()

	status, token, lease, err := SemEnqueue(conn, s.Key, s.Limit, s.opts()...)
	close(done)

	s.mu.Lock()
	defer s.mu.Unlock()

	if err != nil {
		if ctx.Err() != nil {
			s.conn = nil
			return "", ctx.Err()
		}
		s.conn.Close()
		s.conn = nil
		return "", err
	}

	if status == "acquired" {
		s.token = token
		s.lease = lease
		s.startRenew()
	}
	return status, nil
}

// Wait performs the second phase of two-phase semaphore acquire.
func (s *Semaphore) Wait(ctx context.Context, timeout time.Duration) (bool, error) {
	s.mu.Lock()
	if s.conn == nil {
		s.mu.Unlock()
		return false, ErrNotQueued
	}
	conn := s.conn
	s.mu.Unlock()

	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			conn.Close()
		case <-done:
		}
	}()

	token, lease, err := SemWait(conn, s.Key, timeout)
	close(done)

	s.mu.Lock()
	defer s.mu.Unlock()

	if err != nil {
		if errors.Is(err, ErrTimeout) {
			return false, nil
		}
		if ctx.Err() != nil {
			s.conn = nil
			return false, ctx.Err()
		}
		s.conn.Close()
		s.conn = nil
		return false, err
	}

	s.token = token
	s.lease = lease
	s.startRenew()
	return true, nil
}

func (s *Semaphore) stopRenew() {
	if s.cancelRenew != nil {
		s.cancelRenew()
		s.cancelRenew = nil
	}
	if s.renewDone != nil {
		done := s.renewDone
		s.renewDone = nil
		s.mu.Unlock()
		<-done
		s.mu.Lock()
	}
}

// Release stops renewal, releases the semaphore slot, and closes the connection.
// The ctx parameter is reserved for future use.
func (s *Semaphore) Release(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.stopRenew()

	if s.conn == nil {
		return nil
	}

	err := SemRelease(s.conn, s.Key, s.token)
	s.conn.Close()
	s.conn = nil
	s.token = ""
	return err
}

// Close stops renewal and closes the connection without explicitly releasing.
func (s *Semaphore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.stopRenew()

	if s.conn == nil {
		return nil
	}

	err := s.conn.Close()
	s.conn = nil
	s.token = ""
	return err
}

// Token returns the current semaphore slot token, or "" if not acquired.
func (s *Semaphore) Token() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.token
}

func (s *Semaphore) startRenew() {
	s.stopRenew()
	ctx, cancel := context.WithCancel(context.Background())
	s.cancelRenew = cancel

	done := make(chan struct{})
	s.renewDone = done

	interval := time.Duration(float64(s.lease)*s.renewRatio()) * time.Second
	if interval < 1*time.Second {
		interval = 1 * time.Second
	}

	conn := s.conn
	key := s.Key
	token := s.token
	opts := s.opts()

	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.mu.Lock()
				if s.conn == nil || s.token == "" {
					s.mu.Unlock()
					return
				}
				conn = s.conn
				key = s.Key
				token = s.token
				s.mu.Unlock()

				_, _ = SemRenew(conn, key, token, opts...)
			}
		}
	}()
}
