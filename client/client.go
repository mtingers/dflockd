// Package client provides a Go client for the dflockd distributed lock server.
package client

import (
	"bufio"
	"context"
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
	ErrTimeout   = errors.New("dflockd: timeout")
	ErrMaxLocks  = errors.New("dflockd: max locks reached")
	ErrServer    = errors.New("dflockd: server error")
	ErrNotQueued = errors.New("dflockd: not enqueued")
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
// for line-oriented protocol communication.
type Conn struct {
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

// Close closes the underlying TCP connection.
func (c *Conn) Close() error {
	return c.conn.Close()
}

// sendRecv sends a 3-line protocol command and reads one response line.
func (c *Conn) sendRecv(cmd, key, arg string) (string, error) {
	msg := fmt.Sprintf("%s\n%s\n%s\n", cmd, key, arg)
	if _, err := c.conn.Write([]byte(msg)); err != nil {
		return "", err
	}
	line, err := c.reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
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
// Response parsing helpers
// ---------------------------------------------------------------------------

func parseAcquireResponse(resp string) (string, int, error) {
	if resp == "timeout" {
		return "", 0, ErrTimeout
	}
	if resp == "error_max_locks" {
		return "", 0, ErrMaxLocks
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
func CRC32Shard(key string, numServers int) int {
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
	conn, err := Dial(l.serverAddr())
	if err != nil {
		return err
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
	defer l.mu.Unlock()

	if err := l.connect(); err != nil {
		return false, err
	}

	// If context is cancellable, close the connection on cancel to unblock.
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			l.conn.Close()
		case <-done:
		}
	}()

	token, lease, err := Acquire(l.conn, l.Key, l.acquireTimeout(), l.opts()...)
	close(done)

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
func (l *Lock) Enqueue(ctx context.Context) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.connect(); err != nil {
		return "", err
	}

	status, token, lease, err := Enqueue(l.conn, l.Key, l.opts()...)
	if err != nil {
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
	defer l.mu.Unlock()

	if l.conn == nil {
		return false, ErrNotQueued
	}

	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			l.conn.Close()
		case <-done:
		}
	}()

	token, lease, err := Wait(l.conn, l.Key, timeout)
	close(done)

	if err != nil {
		if errors.Is(err, ErrTimeout) {
			return false, nil
		}
		if ctx.Err() != nil {
			l.conn = nil
			return false, ctx.Err()
		}
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
		// its in-progress Renew (which also grabs l.mu).
		l.mu.Unlock()
		<-done
		l.mu.Lock()
	}
}

// Release stops the renewal goroutine, releases the lock on the server, and
// closes the connection.
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
	if l.cancelRenew != nil {
		l.cancelRenew()
	}
	ctx, cancel := context.WithCancel(context.Background())
	l.cancelRenew = cancel

	done := make(chan struct{})
	l.renewDone = done

	interval := time.Duration(float64(l.lease)*l.renewRatio()) * time.Second
	if interval < 1*time.Second {
		interval = 1 * time.Second
	}

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
				conn := l.conn
				key := l.Key
				token := l.token
				l.mu.Unlock()

				_, _ = Renew(conn, key, token, l.opts()...)
			}
		}
	}()
}
