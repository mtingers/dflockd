// Package client provides a Go client for the dflockd distributed lock server.
package client

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mtingers/dflockd/internal/protocol"
)

// Sentinel errors returned by protocol operations.
var (
	ErrTimeout       = errors.New("dflockd: timeout")
	ErrMaxLocks      = errors.New("dflockd: max locks reached")
	ErrMaxWaiters    = errors.New("dflockd: max waiters reached")
	ErrServer        = errors.New("dflockd: server error")
	ErrNotQueued     = errors.New("dflockd: not enqueued")
	ErrAlreadyQueued = errors.New("dflockd: already enqueued")
	ErrLimitMismatch = errors.New("dflockd: limit mismatch")
	ErrLeaseExpired  = errors.New("dflockd: lease expired")
	ErrAuth          = errors.New("dflockd: authentication failed")
	ErrDraining      = errors.New("dflockd: server draining")
)

// Option configures optional parameters for protocol commands.
type Option func(*options)

type options struct {
	leaseTTL int // seconds; 0 means use server default
}

const maxProtocolSeconds = int64(math.MaxInt64) / int64(time.Second)

// WithLeaseTTL sets a custom lease TTL (in seconds) for an Acquire or Enqueue call.
func WithLeaseTTL(seconds int) Option {
	return func(o *options) { o.leaseTTL = seconds }
}

// parseOptions applies and validates Option values. Previously each caller
// inlined the apply loop and there was no validation: a negative leaseTTL
// was silently treated as "server default" rather than surfacing the
// programmer error. Callers expecting their custom lease often didn't
// notice until the lock behaved unexpectedly.
func parseOptions(opts []Option) (options, error) {
	var o options
	for _, fn := range opts {
		fn(&o)
	}
	if o.leaseTTL < 0 {
		return o, fmt.Errorf("dflockd: lease TTL must be >= 0 (got %d)", o.leaseTTL)
	}
	if int64(o.leaseTTL) > maxProtocolSeconds {
		return o, fmt.Errorf("dflockd: lease TTL too large (max %d)", maxProtocolSeconds)
	}
	return o, nil
}

// ---------------------------------------------------------------------------
// Conn — thin wrapper around net.Conn with a buffered reader
// ---------------------------------------------------------------------------

// DefaultDialTimeout is the default timeout for establishing a TCP connection.
const DefaultDialTimeout = 10 * time.Second

// defaultKeepAlive is the interval between TCP keepalive probes.
const defaultKeepAlive = 30 * time.Second

// Conn wraps a TCP connection to a dflockd server, providing a buffered reader
// for line-oriented protocol communication. Conn is safe for concurrent use;
// a mutex serializes request/response pairs to prevent interleaved I/O.
type Conn struct {
	mu     sync.Mutex
	conn   net.Conn
	reader *bufio.Reader
}

// Dial connects to a dflockd server at the given address (host:port).
// Uses DefaultDialTimeout and enables TCP keepalive.
func Dial(addr string) (*Conn, error) {
	dialer := &net.Dialer{
		Timeout:   DefaultDialTimeout,
		KeepAlive: defaultKeepAlive,
	}
	conn, err := dialer.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Conn{conn: conn, reader: bufio.NewReader(conn)}, nil
}

// DialTLS connects to a dflockd server at the given address using TLS.
// Uses DefaultDialTimeout and enables TCP keepalive.
func DialTLS(addr string, cfg *tls.Config) (*Conn, error) {
	dialer := &net.Dialer{
		Timeout:   DefaultDialTimeout,
		KeepAlive: defaultKeepAlive,
	}
	conn, err := tls.DialWithDialer(dialer, "tcp", addr, cfg)
	if err != nil {
		return nil, err
	}
	return &Conn{conn: conn, reader: bufio.NewReader(conn)}, nil
}

// Close closes the underlying TCP connection. It is safe to call
// concurrently with sendRecv; the underlying net.Conn.Close is
// goroutine-safe and will unblock any pending I/O.
func (c *Conn) Close() error {
	return c.conn.Close()
}

// maxResponseBytes is the maximum length of a single server response line.
// Most responses are short (status + token + lease), but the stats command
// returns JSON that can grow with the number of active locks.
const maxResponseBytes = 65536

// sendRecv sends a 3-line protocol command and reads one response line.
// The mutex ensures that concurrent callers (e.g. a renewal goroutine)
// cannot interleave their request/response bytes.
func (c *Conn) sendRecv(cmd, key, arg string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Build the 3-line frame directly in one []byte rather than
	// fmt.Sprintf + []byte cast (two allocations per request on the hot
	// path). Pre-sized exactly, so append never reallocates.
	buf := make([]byte, 0, len(cmd)+len(key)+len(arg)+3)
	buf = append(buf, cmd...)
	buf = append(buf, '\n')
	buf = append(buf, key...)
	buf = append(buf, '\n')
	buf = append(buf, arg...)
	buf = append(buf, '\n')
	if _, err := c.conn.Write(buf); err != nil {
		return "", err
	}
	return c.readLine()
}

// readLine reads a single newline-terminated line, enforcing maxResponseBytes
// to prevent unbounded memory allocation from a malicious server.
// Must be called with c.mu held.
func (c *Conn) readLine() (string, error) {
	var buf []byte
	for {
		b, err := c.reader.ReadByte()
		if err != nil {
			return "", err
		}
		if b == '\n' {
			break
		}
		if len(buf) >= maxResponseBytes {
			// Drain the rest of the oversized line to keep the
			// reader in a consistent state for subsequent reads.
			for {
				d, err := c.reader.ReadByte()
				if err != nil || d == '\n' {
					break
				}
			}
			return "", fmt.Errorf("dflockd: server response too long")
		}
		buf = append(buf, b)
	}
	return strings.TrimRight(string(buf), "\r"), nil
}

// ---------------------------------------------------------------------------
// Authentication
// ---------------------------------------------------------------------------

// Authenticate sends an auth command with the given token. Returns nil on
// success, ErrAuth if the server rejects the token.
func Authenticate(c *Conn, token string) error {
	if strings.ContainsAny(token, "\n\r") {
		return fmt.Errorf("dflockd: auth token contains newline")
	}
	resp, err := c.sendRecv("auth", "_", token)
	if err != nil {
		return err
	}
	if resp != "ok" {
		if resp == "error_draining" {
			return ErrDraining
		}
		return ErrAuth
	}
	return nil
}

// validateKey checks that a key is non-empty and contains no whitespace.
// This mirrors the server-side validation and gives immediate feedback
// instead of a protocol-level rejection.
func validateKey(key string) error {
	if key == "" {
		return fmt.Errorf("dflockd: empty key")
	}
	if len(key) > protocol.MaxLineBytes {
		return fmt.Errorf("dflockd: key too long (max %d bytes)", protocol.MaxLineBytes)
	}
	for _, c := range key {
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			return fmt.Errorf("dflockd: key contains whitespace")
		}
	}
	return nil
}

func validateProtocolLineLength(name, value string) error {
	if len(value) > protocol.MaxLineBytes {
		return fmt.Errorf("dflockd: %s too long (max %d bytes)", name, protocol.MaxLineBytes)
	}
	return nil
}

func validateSemaphoreLimit(limit int) error {
	if limit <= 0 {
		return fmt.Errorf("dflockd: semaphore limit must be > 0 (got %d)", limit)
	}
	return nil
}

// secondsCeil converts a duration to whole seconds, rounding up so that
// sub-second durations are not silently truncated to zero.
func secondsCeil(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	s := int64(d / time.Second)
	if d%time.Second != 0 {
		s++
	}
	return s
}

func timeoutArg(d time.Duration) (string, error) {
	seconds := secondsCeil(d)
	if seconds > maxProtocolSeconds {
		return "", fmt.Errorf("dflockd: timeout too large (max %d)", maxProtocolSeconds)
	}
	return strconv.FormatInt(seconds, 10), nil
}

// ---------------------------------------------------------------------------
// Low-level protocol functions
// ---------------------------------------------------------------------------

// Acquire sends a lock ("l") command. It blocks on the server side until the
// lock is acquired or acquireTimeout expires. Returns the token, lease TTL in
// seconds, and any error. Returns ErrTimeout if the server reports a timeout.
func Acquire(c *Conn, key string, acquireTimeout time.Duration, opts ...Option) (token string, leaseTTL int, err error) {
	if err := validateKey(key); err != nil {
		return "", 0, err
	}
	o, err := parseOptions(opts)
	if err != nil {
		return "", 0, err
	}
	arg, err := timeoutArg(acquireTimeout)
	if err != nil {
		return "", 0, err
	}
	if o.leaseTTL > 0 {
		arg += " " + strconv.Itoa(o.leaseTTL)
	}
	if err := validateProtocolLineLength("acquire argument", arg); err != nil {
		return "", 0, err
	}

	resp, err := c.sendRecv("l", key, arg)
	if err != nil {
		return "", 0, err
	}
	return parseAcquireResponse(resp)
}

// Release sends a release ("r") command for the given key and token.
func Release(c *Conn, key, token string) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if err := validateValue(token); err != nil {
		return err
	}
	if err := validateProtocolLineLength("token", token); err != nil {
		return err
	}
	resp, err := c.sendRecv("r", key, token)
	if err != nil {
		return err
	}
	if resp != "ok" {
		if resp == "error_draining" {
			return ErrDraining
		}
		return fmt.Errorf("%w: release: %s", ErrServer, resp)
	}
	return nil
}

// Renew sends a renew ("n") command and returns the remaining lease seconds.
func Renew(c *Conn, key, token string, opts ...Option) (remaining int, err error) {
	if err := validateKey(key); err != nil {
		return 0, err
	}
	if err := validateValue(token); err != nil {
		return 0, err
	}
	o, err := parseOptions(opts)
	if err != nil {
		return 0, err
	}
	arg := token
	if o.leaseTTL > 0 {
		arg += " " + strconv.Itoa(o.leaseTTL)
	}
	if err := validateProtocolLineLength("renew argument", arg); err != nil {
		return 0, err
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
	if resp == "error_draining" {
		return 0, ErrDraining
	}
	return 0, fmt.Errorf("%w: renew: %s", ErrServer, resp)
}

// Enqueue sends an enqueue ("e") command. Returns the status ("acquired" or
// "queued"), and if acquired, the token and lease TTL.
func Enqueue(c *Conn, key string, opts ...Option) (status, token string, leaseTTL int, err error) {
	if err := validateKey(key); err != nil {
		return "", "", 0, err
	}
	o, err := parseOptions(opts)
	if err != nil {
		return "", "", 0, err
	}
	arg := ""
	if o.leaseTTL > 0 {
		arg = strconv.Itoa(o.leaseTTL)
	}
	if err := validateProtocolLineLength("enqueue argument", arg); err != nil {
		return "", "", 0, err
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
	if resp == "error_already_enqueued" {
		return "", "", 0, ErrAlreadyQueued
	}
	if resp == "error_limit_mismatch" {
		return "", "", 0, ErrLimitMismatch
	}
	if resp == "error_draining" {
		return "", "", 0, ErrDraining
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
	if err := validateKey(key); err != nil {
		return "", 0, err
	}
	arg, err := timeoutArg(waitTimeout)
	if err != nil {
		return "", 0, err
	}
	if err := validateProtocolLineLength("wait argument", arg); err != nil {
		return "", 0, err
	}
	resp, err := c.sendRecv("w", key, arg)
	if err != nil {
		return "", 0, err
	}
	if resp == "timeout" {
		return "", 0, ErrTimeout
	}
	if resp == "error_not_enqueued" {
		return "", 0, ErrNotQueued
	}
	if resp == "error_lease_expired" {
		return "", 0, ErrLeaseExpired
	}
	if resp == "error" {
		return "", 0, ErrServer
	}
	if resp == "error_draining" {
		return "", 0, ErrDraining
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
	if err := validateKey(key); err != nil {
		return "", 0, err
	}
	if err := validateSemaphoreLimit(limit); err != nil {
		return "", 0, err
	}
	o, err := parseOptions(opts)
	if err != nil {
		return "", 0, err
	}
	timeout, err := timeoutArg(acquireTimeout)
	if err != nil {
		return "", 0, err
	}
	arg := timeout + " " + strconv.Itoa(limit)
	if o.leaseTTL > 0 {
		arg += " " + strconv.Itoa(o.leaseTTL)
	}
	if err := validateProtocolLineLength("semaphore acquire argument", arg); err != nil {
		return "", 0, err
	}

	resp, err := c.sendRecv("sl", key, arg)
	if err != nil {
		return "", 0, err
	}
	return parseSemAcquireResponse(resp)
}

// SemRelease sends a semaphore release ("sr") command for the given key and token.
func SemRelease(c *Conn, key, token string) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if err := validateValue(token); err != nil {
		return err
	}
	if err := validateProtocolLineLength("token", token); err != nil {
		return err
	}
	resp, err := c.sendRecv("sr", key, token)
	if err != nil {
		return err
	}
	if resp != "ok" {
		if resp == "error_draining" {
			return ErrDraining
		}
		return fmt.Errorf("%w: sem_release: %s", ErrServer, resp)
	}
	return nil
}

// SemRenew sends a semaphore renew ("sn") command and returns the remaining lease seconds.
func SemRenew(c *Conn, key, token string, opts ...Option) (remaining int, err error) {
	if err := validateKey(key); err != nil {
		return 0, err
	}
	if err := validateValue(token); err != nil {
		return 0, err
	}
	o, err := parseOptions(opts)
	if err != nil {
		return 0, err
	}
	arg := token
	if o.leaseTTL > 0 {
		arg += " " + strconv.Itoa(o.leaseTTL)
	}
	if err := validateProtocolLineLength("semaphore renew argument", arg); err != nil {
		return 0, err
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
	if resp == "error_draining" {
		return 0, ErrDraining
	}
	return 0, fmt.Errorf("%w: sem_renew: %s", ErrServer, resp)
}

// SemEnqueue sends a semaphore enqueue ("se") command. Returns the status
// ("acquired" or "queued"), and if acquired, the token and lease TTL.
func SemEnqueue(c *Conn, key string, limit int, opts ...Option) (status, token string, leaseTTL int, err error) {
	if err := validateKey(key); err != nil {
		return "", "", 0, err
	}
	if err := validateSemaphoreLimit(limit); err != nil {
		return "", "", 0, err
	}
	o, err := parseOptions(opts)
	if err != nil {
		return "", "", 0, err
	}
	arg := strconv.Itoa(limit)
	if o.leaseTTL > 0 {
		arg += " " + strconv.Itoa(o.leaseTTL)
	}
	if err := validateProtocolLineLength("semaphore enqueue argument", arg); err != nil {
		return "", "", 0, err
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
	if resp == "error_already_enqueued" {
		return "", "", 0, ErrAlreadyQueued
	}
	if resp == "error_draining" {
		return "", "", 0, ErrDraining
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
	if err := validateKey(key); err != nil {
		return "", 0, err
	}
	arg, err := timeoutArg(waitTimeout)
	if err != nil {
		return "", 0, err
	}
	if err := validateProtocolLineLength("semaphore wait argument", arg); err != nil {
		return "", 0, err
	}
	resp, err := c.sendRecv("sw", key, arg)
	if err != nil {
		return "", 0, err
	}
	if resp == "timeout" {
		return "", 0, ErrTimeout
	}
	if resp == "error_not_enqueued" {
		return "", 0, ErrNotQueued
	}
	if resp == "error_lease_expired" {
		return "", 0, ErrLeaseExpired
	}
	if resp == "error" {
		return "", 0, ErrServer
	}
	if resp == "error_draining" {
		return "", 0, ErrDraining
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
	if resp == "error_lease_expired" {
		return "", 0, ErrLeaseExpired
	}
	if resp == "error_draining" {
		return "", 0, ErrDraining
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
	if resp == "error_limit_mismatch" {
		return "", 0, ErrLimitMismatch
	}
	if resp == "error_lease_expired" {
		return "", 0, ErrLeaseExpired
	}
	if resp == "error_draining" {
		return "", 0, ErrDraining
	}
	return parseOKTokenLease(resp, "acquire")
}

func parseOKTokenLease(resp, cmd string) (string, int, error) {
	if resp == "error_draining" {
		return "", 0, ErrDraining
	}
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
// Shared state + helpers used by both Lock and Semaphore
// ---------------------------------------------------------------------------

// renewableResource is the runtime state shared by Lock and Semaphore:
// the live connection, the held token, and the renewal goroutine
// lifecycle. Both public types embed this anonymously so that
// Token(), Close(), stopRenew(), and connect() have one implementation.
// Fields are unexported so they don't leak into the public API surface.
type renewableResource struct {
	mu          sync.Mutex
	conn        *Conn
	token       string
	lease       int
	cancelRenew context.CancelFunc
	renewDone   chan struct{} // closed when the renew goroutine exits
}

// Token returns the current lock/semaphore token, or "" if not held.
func (r *renewableResource) Token() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.token
}

// Close ends the renewal goroutine and closes the connection without
// sending a release. The server will auto-release if configured to do
// so. Promoted onto Lock and Semaphore via embedding.
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

func (r *renewableResource) clearConnIfCurrent(conn *Conn) {
	if r.conn == conn {
		r.conn = nil
		r.token = ""
		r.lease = 0
	}
}

// stopRenew cancels the renewal goroutine and waits for it to exit.
// Must be called with r.mu held. If the renewal goroutine is blocked
// inside a network Renew call (server slow/hung), ctx cancellation
// alone won't unblock it; after stopRenewGrace we force-close the conn
// so its I/O errors out.
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
	conn := r.conn // snapshot for force-close; conn.Close is idempotent
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

// connect dials the given address, optionally over TLS, and
// authenticates. Closes any pre-existing connection first. Must be
// called with r.mu held.
func (r *renewableResource) connect(addr string, tlsCfg *tls.Config, authToken string) error {
	if r.conn != nil {
		r.conn.Close()
	}
	r.conn = nil
	r.token = ""
	r.lease = 0
	var conn *Conn
	var err error
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

// abandonedGrantCleanupTimeout bounds best-effort cleanup for a token that was
// granted just as the caller's context was cancelled. Release is a normal
// protocol round trip, so do not let cleanup wedge the cancelled caller.
const abandonedGrantCleanupTimeout = 2 * time.Second

func tryReleaseWithDeadline(c *Conn, key, token string, releaseFn func(*Conn, string, string) error) error {
	if c == nil {
		return net.ErrClosed
	}
	_ = c.conn.SetDeadline(time.Now().Add(abandonedGrantCleanupTimeout))
	err := releaseFn(c, key, token)
	_ = c.conn.SetDeadline(time.Time{})
	return err
}

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

func cleanupAbandonedGrant(conn *Conn, addr string, tlsCfg *tls.Config, authToken, key, token string, releaseFn func(*Conn, string, string) error) {
	if token == "" {
		return
	}
	if tryReleaseWithDeadline(conn, key, token, releaseFn) == nil {
		return
	}
	cleanupConn, err := dialCleanupConn(addr, tlsCfg, authToken)
	if err != nil {
		return
	}
	defer cleanupConn.Close()
	_ = tryReleaseWithDeadline(cleanupConn, key, token, releaseFn)
}

func releaseWithContext(ctx context.Context, conn *Conn, key, token string, releaseFn func(*Conn, string, string) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	stopCancelWatch := closeConnOnContextDone(ctx, conn)
	err := releaseFn(conn, key, token)
	stopCancelWatch()
	if err != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// defaultAcquireTimeout returns the given value, or 10s if unset.
func defaultAcquireTimeout(t time.Duration) time.Duration {
	if t > 0 {
		return t
	}
	return 10 * time.Second
}

const defaultRenewJitter = 0.10

// validateRenewConfig rejects Lock/Semaphore field values that would
// silently produce broken runtime behavior. Negative LeaseTTL is otherwise
// dropped by buildOpts and the server default is used without warning;
// RenewRatio >= 1.0 schedules the first renewal at-or-past the lease
// expiry, so the lock is lost before the renewal fires.
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

// defaultRenewRatio returns the given value, or 0.5 if unset.
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

const minRenewInterval = time.Millisecond

func renewInterval(leaseSec int, ratio float64) time.Duration {
	leaseDur := time.Duration(leaseSec) * time.Second
	interval := time.Duration(float64(leaseDur) * ratio)
	if interval <= 0 {
		return minRenewInterval
	}
	return interval
}

// defaultShardFunc returns the given ShardFunc, or CRC32Shard if unset.
func defaultShardFunc(f ShardFunc) ShardFunc {
	if f != nil {
		return f
	}
	return CRC32Shard
}

// resolveServerAddr picks the server for key based on the sharding
// function. Defaults to 127.0.0.1:6388 if no servers are provided.
func resolveServerAddr(key string, servers []string, f ShardFunc) string {
	if len(servers) == 0 {
		servers = []string{"127.0.0.1:6388"}
	}
	idx := defaultShardFunc(f)(key, len(servers))
	return servers[idx]
}

// buildOpts constructs the Option slice from a lease TTL value. Returns
// nil when leaseTTL is 0 so the server's default is used.
func buildOpts(leaseTTL int) []Option {
	if leaseTTL > 0 {
		return []Option{WithLeaseTTL(leaseTTL)}
	}
	return nil
}

// startRenewLoop launches a background goroutine that renews a lease at
// ratio * leaseSec intervals. renewFn is either Renew (locks) or
// SemRenew (semaphores). Must be called with r.mu held.
func (r *renewableResource) startRenewLoop(key string, leaseSec int, ratio, jitter float64, opts []Option, renewFn func(*Conn, string, string, ...Option) (int, error), onErr func(error)) {
	r.stopRenew()
	ctx, cancel := context.WithCancel(context.Background())
	r.cancelRenew = cancel

	done := make(chan struct{})
	r.renewDone = done

	interval := renewInterval(leaseSec, ratio)

	go func() {
		defer close(done)
		timer := time.NewTimer(jitteredRenewInterval(interval, jitter))
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

				_, err := renewFn(conn, key, tok, opts...)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					if onErr != nil {
						onErr(err)
					}
					return
				}
				timer.Reset(jitteredRenewInterval(interval, jitter))
			}
		}
	}()
}

func jitteredRenewInterval(interval time.Duration, jitter float64) time.Duration {
	if interval <= 0 || jitter <= 0 {
		return interval
	}
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return interval
	}
	// Use the top 53 bits so the integer-to-float conversion has full
	// float64 precision. The factor is [1-jitter, 1], so jitter only moves
	// renewals earlier and never risks renewing after the configured ratio.
	const denom = float64(uint64(1) << 53)
	x := float64(binary.BigEndian.Uint64(buf[:])>>11) / denom
	factor := 1 - x*jitter
	d := time.Duration(float64(interval) * factor)
	if d <= 0 {
		return interval
	}
	return d
}

// ---------------------------------------------------------------------------
// Lock — high-level distributed lock
// ---------------------------------------------------------------------------

// Lock provides a high-level interface for acquiring, holding, and releasing
// a distributed lock, including automatic lease renewal in the background.
type Lock struct {
	Key            string
	AcquireTimeout time.Duration   // default 10s
	LeaseTTL       int             // custom lease TTL in seconds; 0 = server default
	Servers        []string        // e.g. ["127.0.0.1:6388"]
	ShardFunc      ShardFunc       // defaults to CRC32Shard
	RenewRatio     float64         // fraction of lease at which to renew; default 0.5
	RenewJitter    float64         // early-only jitter fraction for renewals; default 0.10
	TLSConfig      *tls.Config     // if non-nil, connect using TLS
	AuthToken      string          // if non-empty, authenticate after connecting
	OnRenewError   func(err error) // optional; called when background lease renewal fails

	renewableResource
}

func (l *Lock) acquireTimeoutVal() time.Duration { return defaultAcquireTimeout(l.AcquireTimeout) }
func (l *Lock) renewRatioVal() float64           { return defaultRenewRatio(l.RenewRatio) }
func (l *Lock) renewJitterVal() float64          { return defaultRenewJitterValue(l.RenewJitter) }
func (l *Lock) serverAddr() string               { return resolveServerAddr(l.Key, l.Servers, l.ShardFunc) }
func (l *Lock) opts() []Option                   { return buildOpts(l.LeaseTTL) }

// closeConnOnContextDone closes conn if ctx is cancelled before the returned
// stop function is called. stop waits for the watcher goroutine to exit so a
// later context cancellation cannot close a connection after an operation has
// already returned success.
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

// Acquire connects to the server, acquires the lock, and starts a background
// goroutine to renew the lease. Returns false (with nil error) on timeout.
// The provided context controls cancellation; if it is cancelled, the
// connection is closed which unblocks the server-side wait.
func (l *Lock) Acquire(ctx context.Context) (bool, error) {
	if err := validateRenewConfig(l.LeaseTTL, l.RenewRatio, l.RenewJitter); err != nil {
		return false, err
	}
	l.mu.Lock()
	l.stopRenew()
	addr := l.serverAddr()
	if err := l.connect(addr, l.TLSConfig, l.AuthToken); err != nil {
		l.mu.Unlock()
		return false, err
	}
	conn := l.conn
	l.mu.Unlock()

	stopCancelWatch := closeConnOnContextDone(ctx, conn)
	token, lease, err := Acquire(conn, l.Key, l.acquireTimeoutVal(), l.opts()...)
	stopCancelWatch()
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
		// If context was cancelled, the conn.Close in the cancellation
		// goroutine may have caused this I/O error — but the goroutine
		// is not guaranteed to have run (select is non-deterministic when
		// both done and ctx.Done() are ready). Always close to avoid a
		// leaked FD; double-close on net.Conn is harmless.
		if ctx.Err() != nil {
			conn.Close()
			l.clearConnIfCurrent(conn)
			l.mu.Unlock()
			return false, ctx.Err()
		}
		conn.Close()
		l.clearConnIfCurrent(conn)
		l.mu.Unlock()
		return false, err
	}

	// Guard against the cancellation goroutine closing the connection
	// after the operation succeeded (race between close(done) and ctx.Done()).
	// The token was already explicitly released above. Closing the conn still
	// tears down this abandoned high-level resource and is harmless if the
	// cancellation watcher already closed it.
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
	l.startRenew()
	l.mu.Unlock()
	return true, nil
}

// Enqueue performs the first phase of two-phase locking. Returns "acquired" or
// "queued". If acquired, a renewal goroutine is started automatically.
// The provided context controls cancellation; if cancelled, the connection
// is closed which unblocks any in-progress server I/O.
func (l *Lock) Enqueue(ctx context.Context) (string, error) {
	if err := validateRenewConfig(l.LeaseTTL, l.RenewRatio, l.RenewJitter); err != nil {
		return "", err
	}
	l.mu.Lock()
	l.stopRenew()
	addr := l.serverAddr()
	if err := l.connect(addr, l.TLSConfig, l.AuthToken); err != nil {
		l.mu.Unlock()
		return "", err
	}
	conn := l.conn
	l.mu.Unlock()

	stopCancelWatch := closeConnOnContextDone(ctx, conn)
	status, token, lease, err := Enqueue(conn, l.Key, l.opts()...)
	stopCancelWatch()
	if err == nil && ctx.Err() != nil && status == "acquired" {
		cleanupAbandonedGrant(conn, addr, l.TLSConfig, l.AuthToken, l.Key, token, Release)
	}

	l.mu.Lock()

	if err != nil {
		if ctx.Err() != nil {
			conn.Close()
			l.clearConnIfCurrent(conn)
			l.mu.Unlock()
			return "", ctx.Err()
		}
		conn.Close()
		l.clearConnIfCurrent(conn)
		l.mu.Unlock()
		return "", err
	}

	// If enqueue acquired immediately, the token was explicitly released above.
	// If it only queued, closing the conn cancels the pending waiter.
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
		l.startRenew()
	}
	l.mu.Unlock()
	return status, nil
}

// Wait performs the second phase of two-phase locking. Must be called after
// Enqueue returned "queued". Returns false (with nil error) on timeout.
// On timeout the connection is closed; the caller must call Enqueue again
// to re-enter the queue.
func (l *Lock) Wait(ctx context.Context, timeout time.Duration) (bool, error) {
	if err := validateRenewConfig(l.LeaseTTL, l.RenewRatio, l.RenewJitter); err != nil {
		return false, err
	}
	l.mu.Lock()
	if l.conn == nil {
		l.mu.Unlock()
		return false, ErrNotQueued
	}
	conn := l.conn
	addr := l.serverAddr()
	l.mu.Unlock()

	stopCancelWatch := closeConnOnContextDone(ctx, conn)
	token, lease, err := Wait(conn, l.Key, timeout)
	stopCancelWatch()
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
		if ctx.Err() != nil {
			conn.Close()
			l.clearConnIfCurrent(conn)
			l.mu.Unlock()
			return false, ctx.Err()
		}
		conn.Close()
		l.clearConnIfCurrent(conn)
		l.mu.Unlock()
		return false, err
	}

	// The token was already explicitly released above. Closing the conn still
	// tears down this abandoned high-level resource.
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
	l.startRenew()
	l.mu.Unlock()
	return true, nil
}

// stopRenewGrace bounds how long stopRenew waits for the renewal goroutine
// to notice ctx cancellation before it force-closes the connection. 2s is
// long enough for a responsive server to complete an in-flight Renew, but
// short enough that a hung server doesn't wedge Release() forever.
const stopRenewGrace = 2 * time.Second

// stopRenew cancels the renewal goroutine and waits for it to exit.
// Must be called with l.mu held; temporarily releases the mutex so
// the renewal goroutine can complete its tick (which grabs l.mu).
//
// If the goroutine is stuck inside a Renew network call (server hung or
// network slow), ctx cancellation alone can't unblock it. After a grace
// period we force-close the underlying conn, which interrupts the Renew
// I/O with an error; the goroutine then exits normally.
// Release stops the renewal goroutine, releases the lock on the server, and
// closes the connection. Cancelling ctx closes the connection to unblock a
// release round trip that is stuck in network I/O.
//
// If the caller is queued (Enqueue returned "queued" but Wait has not yet
// granted a token), there is no token to release; closing the connection is
// the protocol-level signal to abandon the waiter, and Release returns nil
// rather than surfacing a misleading "empty value" error from the wire-level
// validator.
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

// Close (promoted from renewableResource) and Token (also promoted)
// are defined on the embedded renewableResource type; they are
// promoted into the public API of Lock and Semaphore via the
// anonymous embed.

func (l *Lock) startRenew() {
	l.startRenewLoop(l.Key, l.lease, l.renewRatioVal(), l.renewJitterVal(), l.opts(), Renew, l.OnRenewError)
}

// ---------------------------------------------------------------------------
// Semaphore — high-level distributed semaphore
// ---------------------------------------------------------------------------

// Semaphore provides a high-level interface for acquiring, holding, and
// releasing a distributed semaphore slot, including automatic lease renewal.
type Semaphore struct {
	Key            string
	Limit          int
	AcquireTimeout time.Duration   // default 10s
	LeaseTTL       int             // custom lease TTL in seconds; 0 = server default
	Servers        []string        // e.g. ["127.0.0.1:6388"]
	ShardFunc      ShardFunc       // defaults to CRC32Shard
	RenewRatio     float64         // fraction of lease at which to renew; default 0.5
	RenewJitter    float64         // early-only jitter fraction for renewals; default 0.10
	TLSConfig      *tls.Config     // if non-nil, connect using TLS
	AuthToken      string          // if non-empty, authenticate after connecting
	OnRenewError   func(err error) // optional; called when background lease renewal fails

	renewableResource
}

func (s *Semaphore) acquireTimeoutVal() time.Duration { return defaultAcquireTimeout(s.AcquireTimeout) }
func (s *Semaphore) renewRatioVal() float64           { return defaultRenewRatio(s.RenewRatio) }
func (s *Semaphore) renewJitterVal() float64          { return defaultRenewJitterValue(s.RenewJitter) }
func (s *Semaphore) serverAddr() string               { return resolveServerAddr(s.Key, s.Servers, s.ShardFunc) }
func (s *Semaphore) opts() []Option                   { return buildOpts(s.LeaseTTL) }

// Acquire connects to the server, acquires a semaphore slot, and starts
// background lease renewal. Returns false (with nil error) on timeout.
func (s *Semaphore) Acquire(ctx context.Context) (bool, error) {
	if err := validateRenewConfig(s.LeaseTTL, s.RenewRatio, s.RenewJitter); err != nil {
		return false, err
	}
	if err := validateSemaphoreLimit(s.Limit); err != nil {
		return false, err
	}
	s.mu.Lock()
	s.stopRenew()
	addr := s.serverAddr()
	if err := s.connect(addr, s.TLSConfig, s.AuthToken); err != nil {
		s.mu.Unlock()
		return false, err
	}
	conn := s.conn
	s.mu.Unlock()

	stopCancelWatch := closeConnOnContextDone(ctx, conn)
	token, lease, err := SemAcquire(conn, s.Key, s.acquireTimeoutVal(), s.Limit, s.opts()...)
	stopCancelWatch()
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
		if ctx.Err() != nil {
			conn.Close()
			s.clearConnIfCurrent(conn)
			s.mu.Unlock()
			return false, ctx.Err()
		}
		conn.Close()
		s.clearConnIfCurrent(conn)
		s.mu.Unlock()
		return false, err
	}

	// The token was already explicitly released above. Closing the conn still
	// tears down this abandoned high-level resource.
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
	s.startRenew()
	s.mu.Unlock()
	return true, nil
}

// Enqueue performs the first phase of two-phase semaphore acquire.
// The provided context controls cancellation; if cancelled, the connection
// is closed which unblocks any in-progress server I/O.
func (s *Semaphore) Enqueue(ctx context.Context) (string, error) {
	if err := validateRenewConfig(s.LeaseTTL, s.RenewRatio, s.RenewJitter); err != nil {
		return "", err
	}
	if err := validateSemaphoreLimit(s.Limit); err != nil {
		return "", err
	}
	s.mu.Lock()
	s.stopRenew()
	addr := s.serverAddr()
	if err := s.connect(addr, s.TLSConfig, s.AuthToken); err != nil {
		s.mu.Unlock()
		return "", err
	}
	conn := s.conn
	s.mu.Unlock()

	stopCancelWatch := closeConnOnContextDone(ctx, conn)
	status, token, lease, err := SemEnqueue(conn, s.Key, s.Limit, s.opts()...)
	stopCancelWatch()
	if err == nil && ctx.Err() != nil && status == "acquired" {
		cleanupAbandonedGrant(conn, addr, s.TLSConfig, s.AuthToken, s.Key, token, SemRelease)
	}

	s.mu.Lock()

	if err != nil {
		if ctx.Err() != nil {
			conn.Close()
			s.clearConnIfCurrent(conn)
			s.mu.Unlock()
			return "", ctx.Err()
		}
		conn.Close()
		s.clearConnIfCurrent(conn)
		s.mu.Unlock()
		return "", err
	}

	// If enqueue acquired immediately, the token was explicitly released above.
	// If it only queued, closing the conn cancels the pending waiter.
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
		s.startRenew()
	}
	s.mu.Unlock()
	return status, nil
}

// Wait performs the second phase of two-phase semaphore acquire.
// Returns false (with nil error) on timeout. On timeout the connection is
// closed; the caller must call Enqueue again to re-enter the queue.
func (s *Semaphore) Wait(ctx context.Context, timeout time.Duration) (bool, error) {
	if err := validateRenewConfig(s.LeaseTTL, s.RenewRatio, s.RenewJitter); err != nil {
		return false, err
	}
	s.mu.Lock()
	if s.conn == nil {
		s.mu.Unlock()
		return false, ErrNotQueued
	}
	conn := s.conn
	addr := s.serverAddr()
	s.mu.Unlock()

	stopCancelWatch := closeConnOnContextDone(ctx, conn)
	token, lease, err := SemWait(conn, s.Key, timeout)
	stopCancelWatch()
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
		if ctx.Err() != nil {
			conn.Close()
			s.clearConnIfCurrent(conn)
			s.mu.Unlock()
			return false, ctx.Err()
		}
		conn.Close()
		s.clearConnIfCurrent(conn)
		s.mu.Unlock()
		return false, err
	}

	// The token was already explicitly released above. Closing the conn still
	// tears down this abandoned high-level resource.
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
	s.startRenew()
	s.mu.Unlock()
	return true, nil
}

// Release stops renewal, releases the semaphore slot, and closes the connection.
// Cancelling ctx closes the connection to unblock a release round trip that is
// stuck in network I/O.
//
// If the caller is queued (Enqueue returned "queued" but Wait has not yet
// granted a token), there is no token to release; closing the connection is
// the protocol-level signal to abandon the waiter, and Release returns nil
// rather than surfacing a misleading "empty value" error from the wire-level
// validator.
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

// Close and Token are promoted from the embedded renewableResource.

// ---------------------------------------------------------------------------
// Signaling
// ---------------------------------------------------------------------------

// Signal represents a received signal from a channel.
type Signal struct {
	Channel string
	Payload string
}

// DefaultHeartbeatInterval is the default interval between ping commands
// sent by SignalConn to keep the server from timing out idle connections.
const DefaultHeartbeatInterval = 15 * time.Second

// signalChanBuffer is the buffer size for the client's sigCh. Matches
// the server-side push-writer buffer (internal/server.writeChBuffer)
// and the bridge-side buffer (internal/httpapi.sigChBuffer) so none of
// the three pipeline stages bottlenecks independently.
const signalChanBuffer = 64

// SignalConnOption configures optional parameters for NewSignalConn.
type SignalConnOption func(*SignalConn)

// WithHeartbeatInterval sets the interval between heartbeat ping commands.
// Set to 0 to disable heartbeats.
func WithHeartbeatInterval(d time.Duration) SignalConnOption {
	return func(sc *SignalConn) { sc.heartbeatInterval = d }
}

// SignalConn wraps a Conn for signal operations, providing a background
// reader that separates push signals from command responses.
//
// The sigCh buffer (64) is a soft cap: if the consumer can't keep up and
// the channel fills, incoming signals are dropped silently and the drop
// count is exposed via DroppedSignals(). Consumers that must not miss
// signals should monitor that counter and either scale up consumers or
// drop the connection (the server will also evict slow consumers via
// CancelConn when its own WriteCh buffer overflows).
type SignalConn struct {
	conn              *Conn
	sigCh             chan Signal
	respCh            chan string
	done              chan struct{}
	closeCh           chan struct{}
	closeOnce         sync.Once
	heartbeatInterval time.Duration
	dropped           atomic.Uint64
}

// NewSignalConn creates a SignalConn from an existing Conn.
// It starts a background goroutine that reads lines from the connection
// and routes "sig ..." push messages to sigCh and command responses to respCh.
// A heartbeat goroutine sends periodic ping commands to prevent the server
// from timing out the connection (default: every 15s). Use
// WithHeartbeatInterval(0) to disable.
func NewSignalConn(c *Conn, opts ...SignalConnOption) *SignalConn {
	sc := &SignalConn{
		conn:              c,
		sigCh:             make(chan Signal, signalChanBuffer),
		respCh:            make(chan string, 1),
		done:              make(chan struct{}),
		closeCh:           make(chan struct{}),
		heartbeatInterval: DefaultHeartbeatInterval,
	}
	for _, o := range opts {
		o(sc)
	}
	go sc.readLoop()
	if sc.heartbeatInterval > 0 {
		go sc.heartbeatLoop()
	}
	return sc
}

func (sc *SignalConn) readLoop() {
	// Defers run LIFO: respCh closes first, then sigCh, then done. Closing
	// respCh on exit lets a sendCmd that's parked in <-sc.respCh observe
	// (zero, false) and return cleanly. Closing sigCh is the canonical
	// "no more signals" signal for Signals() consumers. done is the last
	// to close so Close()'s `<-sc.done` rendezvous is the final wait.
	defer close(sc.done)
	defer close(sc.sigCh)
	defer close(sc.respCh)
	for {
		line, err := sc.conn.readLine()
		if err != nil {
			return
		}
		if strings.HasPrefix(line, "sig ") {
			rest := line[4:]
			idx := strings.Index(rest, " ")
			if idx < 0 {
				continue
			}
			sig := Signal{
				Channel: rest[:idx],
				Payload: rest[idx+1:],
			}
			select {
			case sc.sigCh <- sig:
			default:
				// Slow consumer — drop rather than block, matching the
				// server's own slow-consumer policy. Count is exposed via
				// DroppedSignals() so callers can observe lossy delivery.
				sc.dropped.Add(1)
			}
		} else {
			// Command responses must not be dropped — sendCmd serializes
			// commands so under normal use respCh drains promptly. But if a
			// misbehaving server emits an extra response no caller is
			// waiting for, respCh's size-1 buffer fills and a second push
			// blocks here. Watch closeCh so Close() can unblock us instead
			// of deadlocking on `<-sc.done`.
			select {
			case sc.respCh <- line:
			case <-sc.closeCh:
				return
			}
		}
	}
}

func (sc *SignalConn) sendCmd(cmd, key, arg string) (string, error) {
	sc.conn.mu.Lock()
	defer sc.conn.mu.Unlock()
	// Drain any stale response.
	select {
	case <-sc.respCh:
	default:
	}
	// Direct build — see Conn.sendRecv for rationale.
	buf := make([]byte, 0, len(cmd)+len(key)+len(arg)+3)
	buf = append(buf, cmd...)
	buf = append(buf, '\n')
	buf = append(buf, key...)
	buf = append(buf, '\n')
	buf = append(buf, arg...)
	buf = append(buf, '\n')
	if _, err := sc.conn.conn.Write(buf); err != nil {
		return "", err
	}
	select {
	case resp, ok := <-sc.respCh:
		if !ok {
			return "", fmt.Errorf("dflockd: connection closed")
		}
		return resp, nil
	case <-sc.done:
		select {
		case resp, ok := <-sc.respCh:
			if ok {
				return resp, nil
			}
		default:
		}
		return "", fmt.Errorf("dflockd: connection closed")
	}
}

// ListenOption configures optional parameters for Listen/Unlisten.
type ListenOption func(*listenOptions)

type listenOptions struct {
	group string
}

// WithGroup sets the queue group for a Listen or Unlisten call.
// Within a group, only one member receives each signal via round-robin.
func WithGroup(group string) ListenOption {
	return func(o *listenOptions) { o.group = group }
}

func validateValue(value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("dflockd: empty value")
	}
	if strings.ContainsAny(value, "\n\r") {
		return fmt.Errorf("dflockd: value contains newline")
	}
	return nil
}

func validateArg(name, value string) error {
	if strings.ContainsAny(value, "\n\r") {
		return fmt.Errorf("dflockd: %s contains newline", name)
	}
	if err := validateProtocolLineLength(name, value); err != nil {
		return err
	}
	return nil
}

// Listen subscribes to signals matching the given pattern.
func (sc *SignalConn) Listen(pattern string, opts ...ListenOption) error {
	if err := validateKey(pattern); err != nil {
		return fmt.Errorf("dflockd: invalid pattern: %w", err)
	}
	var lo listenOptions
	for _, o := range opts {
		o(&lo)
	}
	if err := validateArg("group", lo.group); err != nil {
		return err
	}
	resp, err := sc.sendCmd("listen", pattern, lo.group)
	if err != nil {
		return err
	}
	if resp != "ok" {
		return fmt.Errorf("%w: listen: %s", ErrServer, resp)
	}
	return nil
}

// Unlisten unsubscribes from signals matching the given pattern.
func (sc *SignalConn) Unlisten(pattern string, opts ...ListenOption) error {
	if err := validateKey(pattern); err != nil {
		return fmt.Errorf("dflockd: invalid pattern: %w", err)
	}
	var lo listenOptions
	for _, o := range opts {
		o(&lo)
	}
	if err := validateArg("group", lo.group); err != nil {
		return err
	}
	resp, err := sc.sendCmd("unlisten", pattern, lo.group)
	if err != nil {
		return err
	}
	if resp != "ok" {
		return fmt.Errorf("%w: unlisten: %s", ErrServer, resp)
	}
	return nil
}

// Emit sends a signal on a channel (must be literal, no wildcards).
// Returns the number of listeners that received the signal.
func (sc *SignalConn) Emit(channel, payload string) (int, error) {
	if err := validateKey(channel); err != nil {
		return 0, fmt.Errorf("dflockd: invalid channel: %w", err)
	}
	if err := validateValue(payload); err != nil {
		return 0, err
	}
	if maxPayload := protocol.MaxSignalPayloadBytes(channel); maxPayload < 0 || len(payload) > maxPayload {
		if maxPayload < 0 {
			maxPayload = 0
		}
		return 0, fmt.Errorf("dflockd: payload too large (max %d bytes)", maxPayload)
	}
	resp, err := sc.sendCmd("signal", channel, payload)
	if err != nil {
		return 0, err
	}
	return parseOKInt(resp, "signal")
}

// Signals returns a read-only channel that receives signals pushed by the server.
func (sc *SignalConn) Signals() <-chan Signal {
	return sc.sigCh
}

// DroppedSignals returns the total number of signals that were dropped
// because the Signals() channel was full when they arrived. Monotonically
// increasing; use it to detect slow-consumer conditions. Zero in the
// common case.
func (sc *SignalConn) DroppedSignals() uint64 {
	return sc.dropped.Load()
}

// Close closes the underlying connection and waits for the read loop to exit.
func (sc *SignalConn) Close() error {
	sc.closeOnce.Do(func() { close(sc.closeCh) })
	err := sc.conn.Close()
	<-sc.done
	return err
}

// heartbeatLoop sends periodic ping commands to keep the connection alive.
func (sc *SignalConn) heartbeatLoop() {
	ticker := time.NewTicker(sc.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-sc.closeCh:
			return
		case <-sc.done:
			return
		case <-ticker.C:
			if _, err := sc.sendCmd("ping", "_", ""); err != nil {
				return
			}
		}
	}
}

// Emit sends a signal on a channel using a regular (non-SignalConn) connection.
// Returns the number of listeners that received the signal.
func Emit(c *Conn, channel, payload string) (int, error) {
	if err := validateKey(channel); err != nil {
		return 0, err
	}
	if err := validateValue(payload); err != nil {
		return 0, err
	}
	if maxPayload := protocol.MaxSignalPayloadBytes(channel); maxPayload < 0 || len(payload) > maxPayload {
		if maxPayload < 0 {
			maxPayload = 0
		}
		return 0, fmt.Errorf("dflockd: payload too large (max %d bytes)", maxPayload)
	}
	resp, err := c.sendRecv("signal", channel, payload)
	if err != nil {
		return 0, err
	}
	return parseOKInt(resp, "signal")
}

func parseOKInt(resp, cmd string) (int, error) {
	parts := strings.Fields(resp)
	if len(parts) == 2 && parts[0] == "ok" {
		n, err := strconv.Atoi(parts[1])
		if err != nil {
			return 0, fmt.Errorf("%w: %s: bad value %q", ErrServer, cmd, parts[1])
		}
		return n, nil
	}
	return 0, fmt.Errorf("%w: %s: %s", ErrServer, cmd, resp)
}

func (s *Semaphore) startRenew() {
	s.startRenewLoop(s.Key, s.lease, s.renewRatioVal(), s.renewJitterVal(), s.opts(), SemRenew, s.OnRenewError)
}
