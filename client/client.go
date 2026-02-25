// Package client provides a Go client for the dflockd distributed lock server.
package client

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
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
	ErrTimeout        = errors.New("dflockd: timeout")
	ErrMaxLocks       = errors.New("dflockd: max locks reached")
	ErrMaxWaiters     = errors.New("dflockd: max waiters reached")
	ErrServer         = errors.New("dflockd: server error")
	ErrNotQueued      = errors.New("dflockd: not enqueued")
	ErrAlreadyQueued  = errors.New("dflockd: already enqueued")
	ErrLimitMismatch  = errors.New("dflockd: limit mismatch")
	ErrLeaseExpired   = errors.New("dflockd: lease expired")
	ErrAuth           = errors.New("dflockd: authentication failed")
	ErrMaxKeys        = errors.New("dflockd: max keys reached")
	ErrNotFound       = errors.New("dflockd: not found")
	ErrListFull       = errors.New("dflockd: list full")
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
	resp, err := c.sendRecv("auth", "_", token)
	if err != nil {
		return err
	}
	if resp != "ok" {
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
	for _, c := range key {
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			return fmt.Errorf("dflockd: key contains whitespace")
		}
	}
	return nil
}

func validateValue(value string) error {
	if strings.ContainsAny(value, "\n\r") {
		return fmt.Errorf("dflockd: value contains newline")
	}
	return nil
}

// secondsCeil converts a duration to whole seconds, rounding up so that
// sub-second durations are not silently truncated to zero.
func secondsCeil(d time.Duration) int {
	if d <= 0 {
		return 0
	}
	s := int(d / time.Second)
	if d%time.Second != 0 {
		s++
	}
	return s
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
	var o options
	for _, fn := range opts {
		fn(&o)
	}
	arg := strconv.Itoa(secondsCeil(acquireTimeout))
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
	if err := validateKey(key); err != nil {
		return err
	}
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
	if err := validateKey(key); err != nil {
		return 0, err
	}
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
	if err := validateKey(key); err != nil {
		return "", "", 0, err
	}
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
	if resp == "error_already_enqueued" {
		return "", "", 0, ErrAlreadyQueued
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
	arg := strconv.Itoa(secondsCeil(waitTimeout))
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
	var o options
	for _, fn := range opts {
		fn(&o)
	}
	arg := strconv.Itoa(secondsCeil(acquireTimeout)) + " " + strconv.Itoa(limit)
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
	if err := validateKey(key); err != nil {
		return err
	}
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
	if err := validateKey(key); err != nil {
		return 0, err
	}
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
	if err := validateKey(key); err != nil {
		return "", "", 0, err
	}
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
	if resp == "error_already_enqueued" {
		return "", "", 0, ErrAlreadyQueued
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
	arg := strconv.Itoa(secondsCeil(waitTimeout))
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
// Response parsing helpers (new types)
// ---------------------------------------------------------------------------

func parseOKInt64(resp, cmd string) (int64, error) {
	parts := strings.Fields(resp)
	if len(parts) == 2 && parts[0] == "ok" {
		n, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("%w: %s: bad value %q", ErrServer, cmd, parts[1])
		}
		return n, nil
	}
	if resp == "error_max_keys" {
		return 0, ErrMaxKeys
	}
	return 0, fmt.Errorf("%w: %s: %s", ErrServer, cmd, resp)
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
	if resp == "error_max_keys" {
		return 0, ErrMaxKeys
	}
	if resp == "error_list_full" {
		return 0, ErrListFull
	}
	return 0, fmt.Errorf("%w: %s: %s", ErrServer, cmd, resp)
}

// ---------------------------------------------------------------------------
// Atomic Counters
// ---------------------------------------------------------------------------

// Incr increments a counter by delta and returns the new value.
func Incr(c *Conn, key string, delta int64) (int64, error) {
	if err := validateKey(key); err != nil {
		return 0, err
	}
	resp, err := c.sendRecv("incr", key, strconv.FormatInt(delta, 10))
	if err != nil {
		return 0, err
	}
	return parseOKInt64(resp, "incr")
}

// Decr decrements a counter by delta and returns the new value.
func Decr(c *Conn, key string, delta int64) (int64, error) {
	if err := validateKey(key); err != nil {
		return 0, err
	}
	resp, err := c.sendRecv("decr", key, strconv.FormatInt(delta, 10))
	if err != nil {
		return 0, err
	}
	return parseOKInt64(resp, "decr")
}

// GetCounter returns the current value of a counter (0 if nonexistent).
func GetCounter(c *Conn, key string) (int64, error) {
	if err := validateKey(key); err != nil {
		return 0, err
	}
	resp, err := c.sendRecv("get", key, "")
	if err != nil {
		return 0, err
	}
	return parseOKInt64(resp, "get")
}

// SetCounter sets a counter to a specific value.
func SetCounter(c *Conn, key string, value int64) error {
	if err := validateKey(key); err != nil {
		return err
	}
	resp, err := c.sendRecv("cset", key, strconv.FormatInt(value, 10))
	if err != nil {
		return err
	}
	if resp == "error_max_keys" {
		return ErrMaxKeys
	}
	if resp != "ok" {
		return fmt.Errorf("%w: cset: %s", ErrServer, resp)
	}
	return nil
}

// ---------------------------------------------------------------------------
// KV Store
// ---------------------------------------------------------------------------

// KVSet sets a key-value pair with an optional TTL in seconds (0 = no expiry).
func KVSet(c *Conn, key, value string, ttlSeconds int) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if err := validateValue(value); err != nil {
		return err
	}
	if ttlSeconds < 0 {
		ttlSeconds = 0
	}
	// Always send TTL explicitly to avoid ambiguity with numeric values.
	arg := value + " " + strconv.Itoa(ttlSeconds)
	resp, err := c.sendRecv("kset", key, arg)
	if err != nil {
		return err
	}
	if resp == "error_max_keys" {
		return ErrMaxKeys
	}
	if resp != "ok" {
		return fmt.Errorf("%w: kset: %s", ErrServer, resp)
	}
	return nil
}

// KVGet retrieves a value by key. Returns ErrNotFound if the key doesn't exist.
func KVGet(c *Conn, key string) (string, error) {
	if err := validateKey(key); err != nil {
		return "", err
	}
	resp, err := c.sendRecv("kget", key, "")
	if err != nil {
		return "", err
	}
	if resp == "nil" {
		return "", ErrNotFound
	}
	if strings.HasPrefix(resp, "ok ") {
		return resp[3:], nil
	}
	return "", fmt.Errorf("%w: kget: %s", ErrServer, resp)
}

// KVDel deletes a key-value pair.
func KVDel(c *Conn, key string) error {
	if err := validateKey(key); err != nil {
		return err
	}
	resp, err := c.sendRecv("kdel", key, "")
	if err != nil {
		return err
	}
	if resp != "ok" {
		return fmt.Errorf("%w: kdel: %s", ErrServer, resp)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Signaling
// ---------------------------------------------------------------------------

// Signal represents a received signal from a channel.
type Signal struct {
	Channel string
	Payload string
}

// SignalConn wraps a Conn for signal operations, providing a background
// reader that separates push signals from command responses.
type SignalConn struct {
	conn   *Conn
	sigCh  chan Signal
	respCh chan string
	done   chan struct{}
}

// NewSignalConn creates a SignalConn from an existing Conn.
// The Conn must not be used directly after this call.
func NewSignalConn(c *Conn) *SignalConn {
	sc := &SignalConn{
		conn:   c,
		sigCh:  make(chan Signal, 64),
		respCh: make(chan string, 1),
		done:   make(chan struct{}),
	}
	go sc.readLoop()
	return sc
}

func (sc *SignalConn) readLoop() {
	defer close(sc.done)
	for {
		line, err := sc.conn.readLine()
		if err != nil {
			close(sc.sigCh)
			return
		}
		if strings.HasPrefix(line, "sig ") {
			// Parse: "sig <channel> <payload>"
			rest := line[4:]
			idx := strings.Index(rest, " ")
			if idx >= 0 {
				sig := Signal{
					Channel: rest[:idx],
					Payload: rest[idx+1:],
				}
				select {
				case sc.sigCh <- sig:
				default:
				}
			}
		} else {
			sc.respCh <- line
		}
	}
}

// sendCmd sends a command and waits for the response (from respCh).
// The mutex is held across both write and response read to prevent
// concurrent callers from cross-matching responses.
func (sc *SignalConn) sendCmd(cmd, key, arg string) (string, error) {
	sc.conn.mu.Lock()
	defer sc.conn.mu.Unlock()
	msg := fmt.Sprintf("%s\n%s\n%s\n", cmd, key, arg)
	if _, err := sc.conn.conn.Write([]byte(msg)); err != nil {
		return "", err
	}

	select {
	case resp, ok := <-sc.respCh:
		if !ok {
			return "", fmt.Errorf("dflockd: connection closed")
		}
		return resp, nil
	case <-sc.done:
		return "", fmt.Errorf("dflockd: connection closed")
	}
}

// ListenOption configures optional parameters for Listen/Unlisten.
type ListenOption func(*listenOptions)

type listenOptions struct {
	group string
}

// WithGroup sets the queue group name for a Listen or Unlisten call.
// Within a group, only one member receives each signal via round-robin.
func WithGroup(group string) ListenOption {
	return func(o *listenOptions) { o.group = group }
}

// Listen subscribes to a pattern (supports * and > wildcards).
func (sc *SignalConn) Listen(pattern string, opts ...ListenOption) error {
	var lo listenOptions
	for _, o := range opts {
		o(&lo)
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

// Unlisten unsubscribes from a pattern.
func (sc *SignalConn) Unlisten(pattern string, opts ...ListenOption) error {
	var lo listenOptions
	for _, o := range opts {
		o(&lo)
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
// Returns the number of receivers.
func (sc *SignalConn) Emit(channel, payload string) (int, error) {
	if err := validateValue(payload); err != nil {
		return 0, err
	}
	resp, err := sc.sendCmd("signal", channel, payload)
	if err != nil {
		return 0, err
	}
	return parseOKInt(resp, "signal")
}

// Signals returns the channel on which received signals are delivered.
func (sc *SignalConn) Signals() <-chan Signal {
	return sc.sigCh
}

// Close closes the underlying connection.
func (sc *SignalConn) Close() error {
	err := sc.conn.Close()
	<-sc.done
	return err
}

// Emit sends a signal on a channel using a regular (non-listening) connection.
// Returns the number of receivers.
func Emit(c *Conn, channel, payload string) (int, error) {
	if err := validateKey(channel); err != nil {
		return 0, err
	}
	if err := validateValue(payload); err != nil {
		return 0, err
	}
	resp, err := c.sendRecv("signal", channel, payload)
	if err != nil {
		return 0, err
	}
	return parseOKInt(resp, "signal")
}

// ---------------------------------------------------------------------------
// Lists/Queues
// ---------------------------------------------------------------------------

// LPush prepends a value to a list and returns the new length.
func LPush(c *Conn, key, value string) (int, error) {
	if err := validateKey(key); err != nil {
		return 0, err
	}
	if err := validateValue(value); err != nil {
		return 0, err
	}
	resp, err := c.sendRecv("lpush", key, value)
	if err != nil {
		return 0, err
	}
	return parseOKInt(resp, "lpush")
}

// RPush appends a value to a list and returns the new length.
func RPush(c *Conn, key, value string) (int, error) {
	if err := validateKey(key); err != nil {
		return 0, err
	}
	if err := validateValue(value); err != nil {
		return 0, err
	}
	resp, err := c.sendRecv("rpush", key, value)
	if err != nil {
		return 0, err
	}
	return parseOKInt(resp, "rpush")
}

// LPop removes and returns the first element. Returns ErrNotFound if empty.
func LPop(c *Conn, key string) (string, error) {
	if err := validateKey(key); err != nil {
		return "", err
	}
	resp, err := c.sendRecv("lpop", key, "")
	if err != nil {
		return "", err
	}
	if resp == "nil" {
		return "", ErrNotFound
	}
	if strings.HasPrefix(resp, "ok ") {
		return resp[3:], nil
	}
	return "", fmt.Errorf("%w: lpop: %s", ErrServer, resp)
}

// RPop removes and returns the last element. Returns ErrNotFound if empty.
func RPop(c *Conn, key string) (string, error) {
	if err := validateKey(key); err != nil {
		return "", err
	}
	resp, err := c.sendRecv("rpop", key, "")
	if err != nil {
		return "", err
	}
	if resp == "nil" {
		return "", ErrNotFound
	}
	if strings.HasPrefix(resp, "ok ") {
		return resp[3:], nil
	}
	return "", fmt.Errorf("%w: rpop: %s", ErrServer, resp)
}

// LLen returns the length of a list (0 if nonexistent).
func LLen(c *Conn, key string) (int, error) {
	if err := validateKey(key); err != nil {
		return 0, err
	}
	resp, err := c.sendRecv("llen", key, "")
	if err != nil {
		return 0, err
	}
	return parseOKInt(resp, "llen")
}

// LRange returns a range of elements from a list.
// Uses Redis-like index semantics (0-based, negative from end, inclusive).
func LRange(c *Conn, key string, start, stop int) ([]string, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	arg := strconv.Itoa(start) + " " + strconv.Itoa(stop)
	resp, err := c.sendRecv("lrange", key, arg)
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(resp, "ok ") {
		return nil, fmt.Errorf("%w: lrange: %s", ErrServer, resp)
	}
	var items []string
	if err := json.Unmarshal([]byte(resp[3:]), &items); err != nil {
		return nil, fmt.Errorf("%w: lrange: bad json: %v", ErrServer, err)
	}
	return items, nil
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
	OnRenewError   func(err error) // optional; called when background lease renewal fails

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

// connect dials the appropriate shard server. If there is an existing
// connection it is closed first to prevent resource leaks.
func (l *Lock) connect() error {
	if l.conn != nil {
		l.conn.Close()
		l.conn = nil
	}
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
	l.stopRenew()
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
		// If context was cancelled, the conn.Close in the cancellation
		// goroutine may have caused this I/O error — but the goroutine
		// is not guaranteed to have run (select is non-deterministic when
		// both done and ctx.Done() are ready). Always close to avoid a
		// leaked FD; double-close on net.Conn is harmless.
		if ctx.Err() != nil {
			l.conn.Close()
			l.conn = nil
			return false, ctx.Err()
		}
		l.conn.Close()
		l.conn = nil
		return false, err
	}

	// Guard against the cancellation goroutine closing the connection
	// after the operation succeeded (race between close(done) and ctx.Done()).
	// Don't attempt Release — the cancellation goroutine may have already
	// closed the conn, and closing it below triggers server-side auto-release.
	if ctx.Err() != nil {
		l.conn.Close()
		l.conn = nil
		return false, ctx.Err()
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
	l.stopRenew()
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
			l.conn.Close()
			l.conn = nil
			return "", ctx.Err()
		}
		l.conn.Close()
		l.conn = nil
		return "", err
	}

	// Don't attempt Release — the cancellation goroutine may have already
	// closed the conn, and closing it below triggers server-side auto-release.
	if ctx.Err() != nil {
		l.conn.Close()
		l.conn = nil
		return "", ctx.Err()
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
// On timeout the connection is closed; the caller must call Enqueue again
// to re-enter the queue.
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
			l.conn.Close()
			l.conn = nil
			return false, nil
		}
		if ctx.Err() != nil {
			l.conn.Close()
			l.conn = nil
			return false, ctx.Err()
		}
		l.conn.Close()
		l.conn = nil
		return false, err
	}

	// Don't attempt Release — the cancellation goroutine may have already
	// closed the conn, and closing it below triggers server-side auto-release.
	if ctx.Err() != nil {
		l.conn.Close()
		l.conn = nil
		return false, ctx.Err()
	}

	l.token = token
	l.lease = lease
	l.startRenew()
	return true, nil
}

// stopRenew cancels the renewal goroutine and waits for it to exit.
// Must be called with l.mu held; temporarily releases the mutex so
// the renewal goroutine can complete its tick (which grabs l.mu).
func (l *Lock) stopRenew() {
	if l.cancelRenew != nil {
		l.cancelRenew()
		l.cancelRenew = nil
	}
	if l.renewDone != nil {
		done := l.renewDone
		l.renewDone = nil
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
	l.lease = 0
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
	l.lease = 0
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
// If renewal fails, the OnRenewError callback is invoked (if set)
// and the goroutine exits.
func (l *Lock) startRenew() {
	l.stopRenew()
	ctx, cancel := context.WithCancel(context.Background())
	l.cancelRenew = cancel

	done := make(chan struct{})
	l.renewDone = done

	leaseDur := time.Duration(l.lease) * time.Second
	interval := time.Duration(float64(leaseDur) * l.renewRatio())
	if interval < 1*time.Second {
		interval = 1 * time.Second
	}

	conn := l.conn
	key := l.Key
	token := l.token
	opts := l.opts()
	onErr := l.OnRenewError

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

				_, err := Renew(conn, key, token, opts...)
				if err != nil {
					if ctx.Err() != nil {
						return // Deliberately stopped; not a real failure.
					}
					if onErr != nil {
						onErr(err)
					}
					return
				}
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
	OnRenewError   func(err error) // optional; called when background lease renewal fails

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

// connect dials the appropriate shard server. If there is an existing
// connection it is closed first to prevent resource leaks.
func (s *Semaphore) connect() error {
	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}
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
	s.stopRenew()
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
			s.conn.Close()
			s.conn = nil
			return false, ctx.Err()
		}
		s.conn.Close()
		s.conn = nil
		return false, err
	}

	// Don't attempt SemRelease — the cancellation goroutine may have already
	// closed the conn, and closing it below triggers server-side auto-release.
	if ctx.Err() != nil {
		s.conn.Close()
		s.conn = nil
		return false, ctx.Err()
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
	s.stopRenew()
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
			s.conn.Close()
			s.conn = nil
			return "", ctx.Err()
		}
		s.conn.Close()
		s.conn = nil
		return "", err
	}

	// Don't attempt SemRelease — the cancellation goroutine may have already
	// closed the conn, and closing it below triggers server-side auto-release.
	if ctx.Err() != nil {
		s.conn.Close()
		s.conn = nil
		return "", ctx.Err()
	}

	if status == "acquired" {
		s.token = token
		s.lease = lease
		s.startRenew()
	}
	return status, nil
}

// Wait performs the second phase of two-phase semaphore acquire.
// Returns false (with nil error) on timeout. On timeout the connection is
// closed; the caller must call Enqueue again to re-enter the queue.
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
			s.conn.Close()
			s.conn = nil
			return false, nil
		}
		if ctx.Err() != nil {
			s.conn.Close()
			s.conn = nil
			return false, ctx.Err()
		}
		s.conn.Close()
		s.conn = nil
		return false, err
	}

	// Don't attempt SemRelease — the cancellation goroutine may have already
	// closed the conn, and closing it below triggers server-side auto-release.
	if ctx.Err() != nil {
		s.conn.Close()
		s.conn = nil
		return false, ctx.Err()
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
	s.lease = 0
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
	s.lease = 0
	return err
}

// Token returns the current semaphore slot token, or "" if not acquired.
func (s *Semaphore) Token() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.token
}

// startRenew launches a background goroutine that renews the semaphore lease.
// If renewal fails, the OnRenewError callback is invoked (if set)
// and the goroutine exits.
func (s *Semaphore) startRenew() {
	s.stopRenew()
	ctx, cancel := context.WithCancel(context.Background())
	s.cancelRenew = cancel

	done := make(chan struct{})
	s.renewDone = done

	leaseDur := time.Duration(s.lease) * time.Second
	interval := time.Duration(float64(leaseDur) * s.renewRatio())
	if interval < 1*time.Second {
		interval = 1 * time.Second
	}

	conn := s.conn
	key := s.Key
	token := s.token
	opts := s.opts()
	onErr := s.OnRenewError

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

				_, err := SemRenew(conn, key, token, opts...)
				if err != nil {
					if ctx.Err() != nil {
						return // Deliberately stopped; not a real failure.
					}
					if onErr != nil {
						onErr(err)
					}
					return
				}
			}
		}
	}()
}
