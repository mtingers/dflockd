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
	ErrCASConflict    = errors.New("dflockd: cas conflict")
	ErrBarrierCountMismatch = errors.New("dflockd: barrier count mismatch")
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
	if token == "" {
		return fmt.Errorf("dflockd: empty auth token")
	}
	if err := validateArg("token", token); err != nil {
		return err
	}
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

// validateArg checks that a protocol argument does not contain newlines,
// which would inject extra protocol lines.
func validateArg(name, value string) error {
	if strings.ContainsAny(value, "\n\r") {
		return fmt.Errorf("dflockd: %s contains newline", name)
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
	if err := validateArg("token", token); err != nil {
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
	if err := validateArg("token", token); err != nil {
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

	if resp == "error_lease_expired" {
		return 0, ErrLeaseExpired
	}
	parts := strings.Fields(resp)
	if (len(parts) == 2 || len(parts) == 3) && parts[0] == "ok" {
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
	if resp == "error_type_mismatch" {
		return "", "", 0, ErrTypeMismatch
	}
	if resp == "error_limit_mismatch" {
		return "", "", 0, ErrLimitMismatch
	}

	parts := strings.Fields(resp)
	if (len(parts) == 3 || len(parts) == 4) && parts[0] == "acquired" {
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
	if err := validateArg("token", token); err != nil {
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
	if err := validateArg("token", token); err != nil {
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

	if resp == "error_lease_expired" {
		return 0, ErrLeaseExpired
	}
	parts := strings.Fields(resp)
	if (len(parts) == 2 || len(parts) == 3) && parts[0] == "ok" {
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
	if resp == "error_type_mismatch" {
		return "", "", 0, ErrTypeMismatch
	}
	if resp == "error_already_enqueued" {
		return "", "", 0, ErrAlreadyQueued
	}

	parts := strings.Fields(resp)
	if (len(parts) == 3 || len(parts) == 4) && parts[0] == "acquired" {
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
	if resp == "error_type_mismatch" {
		return "", 0, ErrTypeMismatch
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
	if resp == "error_type_mismatch" {
		return "", 0, ErrTypeMismatch
	}
	if resp == "error_limit_mismatch" {
		return "", 0, ErrLimitMismatch
	}
	return parseOKTokenLease(resp, "acquire")
}

func parseOKTokenLease(resp, cmd string) (string, int, error) {
	parts := strings.Fields(resp)
	// Accept 3 or 4 fields (4th is fence, ignored here)
	if (len(parts) == 3 || len(parts) == 4) && parts[0] == "ok" {
		ttl, err := strconv.Atoi(parts[2])
		if err != nil {
			return "", 0, fmt.Errorf("%w: %s: bad lease %q", ErrServer, cmd, parts[2])
		}
		return parts[1], ttl, nil
	}
	return "", 0, fmt.Errorf("%w: %s: %s", ErrServer, cmd, resp)
}

// parseOKTokenLeaseFence parses "ok <token> <lease> <fence>" or "acquired <token> <lease> <fence>".
func parseOKTokenLeaseFence(resp, cmd string) (string, int, uint64, error) {
	parts := strings.Fields(resp)
	if (len(parts) == 3 || len(parts) == 4) && (parts[0] == "ok" || parts[0] == "acquired") {
		ttl, err := strconv.Atoi(parts[2])
		if err != nil {
			return "", 0, 0, fmt.Errorf("%w: %s: bad lease %q", ErrServer, cmd, parts[2])
		}
		var fence uint64
		if len(parts) == 4 {
			fence, err = strconv.ParseUint(parts[3], 10, 64)
			if err != nil {
				return "", 0, 0, fmt.Errorf("%w: %s: bad fence %q", ErrServer, cmd, parts[3])
			}
		}
		return parts[1], ttl, fence, nil
	}
	return "", 0, 0, fmt.Errorf("%w: %s: %s", ErrServer, cmd, resp)
}

// parseRenewWithFence parses "ok <remaining> <fence>".
func parseRenewWithFence(resp, cmd string) (int, uint64, error) {
	if resp == "error_lease_expired" {
		return 0, 0, ErrLeaseExpired
	}
	parts := strings.Fields(resp)
	if (len(parts) == 2 || len(parts) == 3) && parts[0] == "ok" {
		r, err := strconv.Atoi(parts[1])
		if err != nil {
			return 0, 0, fmt.Errorf("%w: %s: bad remaining %q", ErrServer, cmd, parts[1])
		}
		var fence uint64
		if len(parts) == 3 {
			fence, err = strconv.ParseUint(parts[2], 10, 64)
			if err != nil {
				return 0, 0, fmt.Errorf("%w: %s: bad fence %q", ErrServer, cmd, parts[2])
			}
		}
		return r, fence, nil
	}
	return 0, 0, fmt.Errorf("%w: %s: %s", ErrServer, cmd, resp)
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
	if strings.Contains(value, "\t") {
		return fmt.Errorf("dflockd: value contains tab")
	}
	if ttlSeconds < 0 {
		ttlSeconds = 0
	}
	// Tab separates value from TTL to avoid ambiguity with numeric values.
	arg := value + "\t" + strconv.Itoa(ttlSeconds)
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
	if resp == "ok" {
		return "", nil
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

// KVCAS performs an atomic compare-and-swap on a key. If oldValue matches the
// current value, it is replaced with newValue. If the key doesn't exist and
// oldValue is "", the key is created. Returns (true, nil) on success,
// (false, nil) on mismatch.
func KVCAS(c *Conn, key, oldValue, newValue string, ttlSeconds int) (bool, error) {
	if err := validateKey(key); err != nil {
		return false, err
	}
	if err := validateValue(oldValue); err != nil {
		return false, fmt.Errorf("dflockd: oldValue: %w", err)
	}
	if strings.Contains(oldValue, "\t") {
		return false, fmt.Errorf("dflockd: oldValue contains tab")
	}
	if newValue == "" {
		return false, fmt.Errorf("dflockd: newValue must not be empty")
	}
	if err := validateValue(newValue); err != nil {
		return false, err
	}
	if strings.Contains(newValue, "\t") {
		return false, fmt.Errorf("dflockd: newValue contains tab")
	}
	if ttlSeconds < 0 {
		ttlSeconds = 0
	}
	arg := oldValue + "\t" + newValue + "\t" + strconv.Itoa(ttlSeconds)
	resp, err := c.sendRecv("kcas", key, arg)
	if err != nil {
		return false, err
	}
	if resp == "ok" {
		return true, nil
	}
	if resp == "cas_conflict" {
		return false, nil
	}
	if resp == "error_max_keys" {
		return false, ErrMaxKeys
	}
	return false, fmt.Errorf("%w: kcas: %s", ErrServer, resp)
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
			if idx < 0 {
				continue // malformed push — drop
			}
			sig := Signal{
				Channel: rest[:idx],
				Payload: rest[idx+1:],
			}
			select {
			case sc.sigCh <- sig:
			default:
			}
		} else {
			select {
			case sc.respCh <- line:
			default:
				// Drop unexpected response to prevent readLoop from blocking.
			}
		}
	}
}

// sendCmd sends a command and waits for the response (from respCh).
// The mutex is held across both write and response read to prevent
// concurrent callers from cross-matching responses.
func (sc *SignalConn) sendCmd(cmd, key, arg string) (string, error) {
	sc.conn.mu.Lock()
	defer sc.conn.mu.Unlock()
	// Drain any stale response left from a previous race to prevent desync.
	select {
	case <-sc.respCh:
	default:
	}
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
		// Check respCh once more: the response may have arrived just
		// before readLoop closed done.
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

// WithGroup sets the queue group name for a Listen or Unlisten call.
// Within a group, only one member receives each signal via round-robin.
func WithGroup(group string) ListenOption {
	return func(o *listenOptions) { o.group = group }
}

// Listen subscribes to a pattern (supports * and > wildcards).
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

// Unlisten unsubscribes from a pattern.
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
// Returns the number of receivers.
func (sc *SignalConn) Emit(channel, payload string) (int, error) {
	if err := validateKey(channel); err != nil {
		return 0, fmt.Errorf("dflockd: invalid channel: %w", err)
	}
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
	if resp == "ok" {
		return "", nil
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
	if resp == "ok" {
		return "", nil
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
	fence       uint64
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

// closeConn closes the connection if non-nil and sets it to nil.
// Must be called with l.mu held.
func (l *Lock) closeConn() {
	if l.conn != nil {
		l.conn.Close()
		l.conn = nil
	}
}

// connect dials the appropriate shard server. If there is an existing
// connection it is closed first to prevent resource leaks.
func (l *Lock) connect() error {
	l.closeConn()
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
	l.token = ""
	l.fence = 0
	l.lease = 0
	if err := l.connect(); err != nil {
		l.mu.Unlock()
		return false, err
	}
	conn := l.conn
	l.mu.Unlock()

	// If context is cancellable, close the connection on cancel to unblock.
	// The done channel prevents the goroutine from closing the connection
	// after the RPC completes, so we can still perform cleanup (release).
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			conn.Close()
		case <-done:
		}
	}()

	token, lease, fence, err := AcquireWithFence(conn, l.Key, l.acquireTimeout(), l.opts()...)

	l.mu.Lock()
	defer l.mu.Unlock()

	if err != nil {
		close(done)
		if errors.Is(err, ErrTimeout) {
			l.closeConn()
			return false, nil
		}
		if ctx.Err() != nil {
			l.closeConn()
			return false, ctx.Err()
		}
		l.closeConn()
		return false, err
	}

	if ctx.Err() != nil {
		// Lock acquired but context cancelled — release BEFORE signalling
		// done so the cancellation goroutine cannot close the connection
		// out from under us.
		Release(conn, l.Key, token)
		close(done)
		l.closeConn()
		return false, ctx.Err()
	}
	close(done)

	l.token = token
	l.lease = lease
	l.fence = fence
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
	l.token = ""
	l.fence = 0
	l.lease = 0
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

	status, token, lease, fence, err := EnqueueWithFence(conn, l.Key, l.opts()...)

	l.mu.Lock()
	defer l.mu.Unlock()

	if err != nil {
		close(done)
		if ctx.Err() != nil {
			l.closeConn()
			return "", ctx.Err()
		}
		l.closeConn()
		return "", err
	}

	if ctx.Err() != nil {
		if status == "acquired" {
			Release(conn, l.Key, token)
		}
		close(done)
		l.closeConn()
		return "", ctx.Err()
	}
	close(done)

	if status == "acquired" {
		l.token = token
		l.lease = lease
		l.fence = fence
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

	token, lease, fence, err := WaitWithFence(conn, l.Key, timeout)

	l.mu.Lock()
	defer l.mu.Unlock()

	if err != nil {
		close(done)
		if errors.Is(err, ErrTimeout) {
			l.closeConn()
			return false, nil
		}
		if ctx.Err() != nil {
			l.closeConn()
			return false, ctx.Err()
		}
		l.closeConn()
		return false, err
	}

	if ctx.Err() != nil {
		Release(conn, l.Key, token)
		close(done)
		l.closeConn()
		return false, ctx.Err()
	}
	close(done)

	l.token = token
	l.lease = lease
	l.fence = fence
	l.startRenew()
	return true, nil
}

// stopRenew cancels the renewal goroutine and waits for it to exit.
// Must be called with l.mu held; temporarily releases the mutex so
// the renewal goroutine can complete its tick (which grabs l.mu).
// If the goroutine doesn't exit promptly (blocked in I/O), the connection
// is closed to force-unblock it.
func (l *Lock) stopRenew() {
	if l.cancelRenew != nil {
		l.cancelRenew()
		l.cancelRenew = nil
	}
	if l.renewDone != nil {
		done := l.renewDone
		l.renewDone = nil
		conn := l.conn
		forceClosed := false
		l.mu.Unlock()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			if conn != nil {
				conn.Close()
				forceClosed = true
			}
			<-done
		}
		l.mu.Lock()
		// If we force-closed the connection, nil it out so callers
		// (Release, Close) don't try to use the dead connection.
		if forceClosed && l.conn == conn {
			l.conn = nil
		}
	}
}

// Release stops the renewal goroutine, releases the lock on the server, and
// closes the connection. The ctx parameter is reserved for future use.
func (l *Lock) Release(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	savedToken := l.token
	l.stopRenew()

	if l.conn == nil {
		// Connection was force-closed by stopRenew. The server will
		// release the lock when the disconnect is detected or lease expires.
		l.token = ""
		l.fence = 0
		l.lease = 0
		if savedToken != "" {
			return fmt.Errorf("dflockd: connection lost, server-side release skipped (lease will expire)")
		}
		return nil
	}

	// Use savedToken in case the renew goroutine cleared l.token
	// between our ctx.Err() check and mu acquisition.
	token := l.token
	if token == "" {
		token = savedToken
	}
	var err error
	if token != "" {
		err = Release(l.conn, l.Key, token)
	}
	l.closeConn()
	l.token = ""
	l.fence = 0
	l.lease = 0
	return err
}

// Close stops renewal and closes the connection without explicitly releasing
// the lock. The server will auto-release if configured to do so.
func (l *Lock) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.stopRenew()
	l.closeConn()
	l.token = ""
	l.fence = 0
	l.lease = 0
	return nil
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
	// Callers must have already called stopRenew() before setting state.
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

				_, newFence, err := RenewWithFence(conn, key, token, opts...)
				if err != nil {
					if ctx.Err() != nil {
						return // Deliberately stopped; not a real failure.
					}
					// Clear token to prevent stale lock usage.
					// Re-check ctx after acquiring mutex: stopRenew may
					// have cancelled while we were waiting for the lock.
					l.mu.Lock()
					if ctx.Err() != nil {
						l.mu.Unlock()
						return
					}
					l.token = ""
					l.fence = 0
					l.lease = 0
					l.mu.Unlock()
					if onErr != nil {
						go onErr(err)
					}
					return
				}
				if newFence > 0 {
					l.mu.Lock()
					l.fence = newFence
					l.mu.Unlock()
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
	fence       uint64
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

// closeConn closes the connection if non-nil and sets it to nil.
// Must be called with s.mu held.
func (s *Semaphore) closeConn() {
	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}
}

// connect dials the appropriate shard server. If there is an existing
// connection it is closed first to prevent resource leaks.
func (s *Semaphore) connect() error {
	s.closeConn()
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
	s.token = ""
	s.fence = 0
	s.lease = 0
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

	token, lease, fence, err := SemAcquireWithFence(conn, s.Key, s.acquireTimeout(), s.Limit, s.opts()...)

	s.mu.Lock()
	defer s.mu.Unlock()

	if err != nil {
		close(done)
		if errors.Is(err, ErrTimeout) {
			s.closeConn()
			return false, nil
		}
		if ctx.Err() != nil {
			s.closeConn()
			return false, ctx.Err()
		}
		s.closeConn()
		return false, err
	}

	if ctx.Err() != nil {
		SemRelease(conn, s.Key, token)
		close(done)
		s.closeConn()
		return false, ctx.Err()
	}
	close(done)

	s.token = token
	s.lease = lease
	s.fence = fence
	s.startRenew()
	return true, nil
}

// Enqueue performs the first phase of two-phase semaphore acquire.
// The provided context controls cancellation; if cancelled, the connection
// is closed which unblocks any in-progress server I/O.
func (s *Semaphore) Enqueue(ctx context.Context) (string, error) {
	s.mu.Lock()
	s.stopRenew()
	s.token = ""
	s.fence = 0
	s.lease = 0
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

	status, token, lease, fence, err := SemEnqueueWithFence(conn, s.Key, s.Limit, s.opts()...)

	s.mu.Lock()
	defer s.mu.Unlock()

	if err != nil {
		close(done)
		if ctx.Err() != nil {
			s.closeConn()
			return "", ctx.Err()
		}
		s.closeConn()
		return "", err
	}

	if ctx.Err() != nil {
		if status == "acquired" {
			SemRelease(conn, s.Key, token)
		}
		close(done)
		s.closeConn()
		return "", ctx.Err()
	}
	close(done)

	if status == "acquired" {
		s.token = token
		s.lease = lease
		s.fence = fence
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

	token, lease, fence, err := SemWaitWithFence(conn, s.Key, timeout)

	s.mu.Lock()
	defer s.mu.Unlock()

	if err != nil {
		close(done)
		if errors.Is(err, ErrTimeout) {
			s.closeConn()
			return false, nil
		}
		if ctx.Err() != nil {
			s.closeConn()
			return false, ctx.Err()
		}
		s.closeConn()
		return false, err
	}

	if ctx.Err() != nil {
		SemRelease(conn, s.Key, token)
		close(done)
		s.closeConn()
		return false, ctx.Err()
	}
	close(done)

	s.token = token
	s.lease = lease
	s.fence = fence
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
		conn := s.conn
		forceClosed := false
		s.mu.Unlock()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			if conn != nil {
				conn.Close()
				forceClosed = true
			}
			<-done
		}
		s.mu.Lock()
		if forceClosed && s.conn == conn {
			s.conn = nil
		}
	}
}

// Release stops renewal, releases the semaphore slot, and closes the connection.
// The ctx parameter is reserved for future use.
func (s *Semaphore) Release(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	savedToken := s.token
	s.stopRenew()

	if s.conn == nil {
		// Connection was force-closed by stopRenew. The server will
		// release the slot when the disconnect is detected or lease expires.
		s.token = ""
		s.fence = 0
		s.lease = 0
		if savedToken != "" {
			return fmt.Errorf("dflockd: connection lost, server-side release skipped (lease will expire)")
		}
		return nil
	}

	// Use savedToken in case the renew goroutine cleared s.token.
	token := s.token
	if token == "" {
		token = savedToken
	}
	var err error
	if token != "" {
		err = SemRelease(s.conn, s.Key, token)
	}
	s.closeConn()
	s.token = ""
	s.fence = 0
	s.lease = 0
	return err
}

// Close stops renewal and closes the connection without explicitly releasing.
func (s *Semaphore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.stopRenew()
	s.closeConn()
	s.token = ""
	s.fence = 0
	s.lease = 0
	return nil
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
	// Callers must have already called stopRenew() before setting state.
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

				_, newFence, err := SemRenewWithFence(conn, key, token, opts...)
				if err != nil {
					if ctx.Err() != nil {
						return // Deliberately stopped; not a real failure.
					}
					// Clear token to prevent stale semaphore usage.
					// Re-check ctx after acquiring mutex: stopRenew may
					// have cancelled while we were waiting for the lock.
					s.mu.Lock()
					if ctx.Err() != nil {
						s.mu.Unlock()
						return
					}
					s.token = ""
					s.fence = 0
					s.lease = 0
					s.mu.Unlock()
					if onErr != nil {
						go onErr(err)
					}
					return
				}
				if newFence > 0 {
					s.mu.Lock()
					s.fence = newFence
					s.mu.Unlock()
				}
			}
		}
	}()
}

// ---------------------------------------------------------------------------
// WithFence variants — low-level protocol functions
// ---------------------------------------------------------------------------

var ErrTypeMismatch = errors.New("dflockd: type mismatch")

// AcquireWithFence sends a lock ("l") command and returns the fencing token.
func AcquireWithFence(c *Conn, key string, acquireTimeout time.Duration, opts ...Option) (token string, leaseTTL int, fence uint64, err error) {
	if err := validateKey(key); err != nil {
		return "", 0, 0, err
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
		return "", 0, 0, err
	}
	if resp == "timeout" {
		return "", 0, 0, ErrTimeout
	}
	if resp == "error_max_locks" {
		return "", 0, 0, ErrMaxLocks
	}
	if resp == "error_max_waiters" {
		return "", 0, 0, ErrMaxWaiters
	}
	if resp == "error_type_mismatch" {
		return "", 0, 0, ErrTypeMismatch
	}
	if resp == "error_limit_mismatch" {
		return "", 0, 0, ErrLimitMismatch
	}
	return parseOKTokenLeaseFence(resp, "acquire")
}

// SemAcquireWithFence sends a semaphore acquire ("sl") command and returns the fencing token.
func SemAcquireWithFence(c *Conn, key string, acquireTimeout time.Duration, limit int, opts ...Option) (token string, leaseTTL int, fence uint64, err error) {
	if err := validateKey(key); err != nil {
		return "", 0, 0, err
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
		return "", 0, 0, err
	}
	if resp == "timeout" {
		return "", 0, 0, ErrTimeout
	}
	if resp == "error_max_locks" {
		return "", 0, 0, ErrMaxLocks
	}
	if resp == "error_max_waiters" {
		return "", 0, 0, ErrMaxWaiters
	}
	if resp == "error_limit_mismatch" {
		return "", 0, 0, ErrLimitMismatch
	}
	if resp == "error_type_mismatch" {
		return "", 0, 0, ErrTypeMismatch
	}
	return parseOKTokenLeaseFence(resp, "sem_acquire")
}

// EnqueueWithFence sends an enqueue ("e") command and returns the fencing token.
func EnqueueWithFence(c *Conn, key string, opts ...Option) (status, token string, leaseTTL int, fence uint64, err error) {
	if err := validateKey(key); err != nil {
		return "", "", 0, 0, err
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
		return "", "", 0, 0, err
	}
	if resp == "queued" {
		return "queued", "", 0, 0, nil
	}
	if resp == "error_max_locks" {
		return "", "", 0, 0, ErrMaxLocks
	}
	if resp == "error_max_waiters" {
		return "", "", 0, 0, ErrMaxWaiters
	}
	if resp == "error_already_enqueued" {
		return "", "", 0, 0, ErrAlreadyQueued
	}
	if resp == "error_type_mismatch" {
		return "", "", 0, 0, ErrTypeMismatch
	}
	if resp == "error_limit_mismatch" {
		return "", "", 0, 0, ErrLimitMismatch
	}
	parts := strings.Fields(resp)
	if (len(parts) == 3 || len(parts) == 4) && parts[0] == "acquired" {
		ttl, err := strconv.Atoi(parts[2])
		if err != nil {
			return "", "", 0, 0, fmt.Errorf("%w: enqueue: bad lease %q", ErrServer, parts[2])
		}
		var fence uint64
		if len(parts) == 4 {
			fence, err = strconv.ParseUint(parts[3], 10, 64)
			if err != nil {
				return "", "", 0, 0, fmt.Errorf("%w: enqueue: bad fence %q", ErrServer, parts[3])
			}
		}
		return "acquired", parts[1], ttl, fence, nil
	}
	return "", "", 0, 0, fmt.Errorf("%w: enqueue: %s", ErrServer, resp)
}

// SemEnqueueWithFence sends a semaphore enqueue ("se") and returns the fencing token.
func SemEnqueueWithFence(c *Conn, key string, limit int, opts ...Option) (status, token string, leaseTTL int, fence uint64, err error) {
	if err := validateKey(key); err != nil {
		return "", "", 0, 0, err
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
		return "", "", 0, 0, err
	}
	if resp == "queued" {
		return "queued", "", 0, 0, nil
	}
	if resp == "error_max_locks" {
		return "", "", 0, 0, ErrMaxLocks
	}
	if resp == "error_max_waiters" {
		return "", "", 0, 0, ErrMaxWaiters
	}
	if resp == "error_limit_mismatch" {
		return "", "", 0, 0, ErrLimitMismatch
	}
	if resp == "error_type_mismatch" {
		return "", "", 0, 0, ErrTypeMismatch
	}
	if resp == "error_already_enqueued" {
		return "", "", 0, 0, ErrAlreadyQueued
	}
	parts := strings.Fields(resp)
	if (len(parts) == 3 || len(parts) == 4) && parts[0] == "acquired" {
		ttl, err := strconv.Atoi(parts[2])
		if err != nil {
			return "", "", 0, 0, fmt.Errorf("%w: sem_enqueue: bad lease %q", ErrServer, parts[2])
		}
		var fence uint64
		if len(parts) == 4 {
			fence, err = strconv.ParseUint(parts[3], 10, 64)
			if err != nil {
				return "", "", 0, 0, fmt.Errorf("%w: sem_enqueue: bad fence %q", ErrServer, parts[3])
			}
		}
		return "acquired", parts[1], ttl, fence, nil
	}
	return "", "", 0, 0, fmt.Errorf("%w: sem_enqueue: %s", ErrServer, resp)
}

// WaitWithFence sends a wait ("w") command and returns the fencing token.
func WaitWithFence(c *Conn, key string, waitTimeout time.Duration) (token string, leaseTTL int, fence uint64, err error) {
	if err := validateKey(key); err != nil {
		return "", 0, 0, err
	}
	resp, err := c.sendRecv("w", key, strconv.Itoa(secondsCeil(waitTimeout)))
	if err != nil {
		return "", 0, 0, err
	}
	if resp == "timeout" {
		return "", 0, 0, ErrTimeout
	}
	if resp == "error_not_enqueued" {
		return "", 0, 0, ErrNotQueued
	}
	if resp == "error_lease_expired" {
		return "", 0, 0, ErrLeaseExpired
	}
	if resp == "error" {
		return "", 0, 0, ErrServer
	}
	return parseOKTokenLeaseFence(resp, "wait")
}

// SemWaitWithFence sends a semaphore wait ("sw") command and returns the fencing token.
func SemWaitWithFence(c *Conn, key string, waitTimeout time.Duration) (token string, leaseTTL int, fence uint64, err error) {
	if err := validateKey(key); err != nil {
		return "", 0, 0, err
	}
	resp, err := c.sendRecv("sw", key, strconv.Itoa(secondsCeil(waitTimeout)))
	if err != nil {
		return "", 0, 0, err
	}
	if resp == "timeout" {
		return "", 0, 0, ErrTimeout
	}
	if resp == "error_not_enqueued" {
		return "", 0, 0, ErrNotQueued
	}
	if resp == "error_lease_expired" {
		return "", 0, 0, ErrLeaseExpired
	}
	if resp == "error" {
		return "", 0, 0, ErrServer
	}
	return parseOKTokenLeaseFence(resp, "sem_wait")
}

// RenewWithFence sends a renew ("n") command and returns the fencing token.
func RenewWithFence(c *Conn, key, token string, opts ...Option) (remaining int, fence uint64, err error) {
	if err := validateKey(key); err != nil {
		return 0, 0, err
	}
	if err := validateArg("token", token); err != nil {
		return 0, 0, err
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
		return 0, 0, err
	}
	return parseRenewWithFence(resp, "renew")
}

// SemRenewWithFence sends a semaphore renew ("sn") and returns the fencing token.
func SemRenewWithFence(c *Conn, key, token string, opts ...Option) (remaining int, fence uint64, err error) {
	if err := validateKey(key); err != nil {
		return 0, 0, err
	}
	if err := validateArg("token", token); err != nil {
		return 0, 0, err
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
		return 0, 0, err
	}
	return parseRenewWithFence(resp, "sem_renew")
}

// ---------------------------------------------------------------------------
// Blocking List Pop
// ---------------------------------------------------------------------------

// BLPop blocks until an item is available at the left of the list, or timeout.
// Returns ("", ErrTimeout) on timeout.
func BLPop(c *Conn, key string, timeout time.Duration) (string, error) {
	if err := validateKey(key); err != nil {
		return "", err
	}
	resp, err := c.sendRecv("blpop", key, strconv.Itoa(secondsCeil(timeout)))
	if err != nil {
		return "", err
	}
	if resp == "nil" {
		return "", ErrTimeout
	}
	if resp == "ok" {
		return "", nil
	}
	if strings.HasPrefix(resp, "ok ") {
		return resp[3:], nil
	}
	if resp == "error_max_keys" {
		return "", ErrMaxKeys
	}
	return "", fmt.Errorf("%w: blpop: %s", ErrServer, resp)
}

// BRPop blocks until an item is available at the right of the list, or timeout.
// Returns ("", ErrTimeout) on timeout.
func BRPop(c *Conn, key string, timeout time.Duration) (string, error) {
	if err := validateKey(key); err != nil {
		return "", err
	}
	resp, err := c.sendRecv("brpop", key, strconv.Itoa(secondsCeil(timeout)))
	if err != nil {
		return "", err
	}
	if resp == "nil" {
		return "", ErrTimeout
	}
	if resp == "ok" {
		return "", nil
	}
	if strings.HasPrefix(resp, "ok ") {
		return resp[3:], nil
	}
	if resp == "error_max_keys" {
		return "", ErrMaxKeys
	}
	return "", fmt.Errorf("%w: brpop: %s", ErrServer, resp)
}

// ---------------------------------------------------------------------------
// Read-Write Lock — low-level protocol functions
// ---------------------------------------------------------------------------

// RLock sends a read-lock ("rl") command.
func RLock(c *Conn, key string, timeout time.Duration, opts ...Option) (token string, leaseTTL int, fence uint64, err error) {
	return rwLock(c, "rl", key, timeout, opts...)
}

// WLock sends a write-lock ("wl") command.
func WLock(c *Conn, key string, timeout time.Duration, opts ...Option) (token string, leaseTTL int, fence uint64, err error) {
	return rwLock(c, "wl", key, timeout, opts...)
}

func rwLock(c *Conn, cmd, key string, timeout time.Duration, opts ...Option) (string, int, uint64, error) {
	if err := validateKey(key); err != nil {
		return "", 0, 0, err
	}
	var o options
	for _, fn := range opts {
		fn(&o)
	}
	arg := strconv.Itoa(secondsCeil(timeout))
	if o.leaseTTL > 0 {
		arg += " " + strconv.Itoa(o.leaseTTL)
	}
	resp, err := c.sendRecv(cmd, key, arg)
	if err != nil {
		return "", 0, 0, err
	}
	if resp == "timeout" {
		return "", 0, 0, ErrTimeout
	}
	if resp == "error_max_locks" {
		return "", 0, 0, ErrMaxLocks
	}
	if resp == "error_max_waiters" {
		return "", 0, 0, ErrMaxWaiters
	}
	if resp == "error_type_mismatch" {
		return "", 0, 0, ErrTypeMismatch
	}
	if resp == "error_lease_expired" {
		return "", 0, 0, ErrLeaseExpired
	}
	return parseOKTokenLeaseFence(resp, cmd)
}

// RUnlock sends a read-unlock ("rr") command.
func RUnlock(c *Conn, key, token string) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if err := validateArg("token", token); err != nil {
		return err
	}
	resp, err := c.sendRecv("rr", key, token)
	if err != nil {
		return err
	}
	if resp != "ok" {
		return fmt.Errorf("%w: rr: %s", ErrServer, resp)
	}
	return nil
}

// WUnlock sends a write-unlock ("wr") command.
func WUnlock(c *Conn, key, token string) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if err := validateArg("token", token); err != nil {
		return err
	}
	resp, err := c.sendRecv("wr", key, token)
	if err != nil {
		return err
	}
	if resp != "ok" {
		return fmt.Errorf("%w: wr: %s", ErrServer, resp)
	}
	return nil
}

// RRenew sends a read-renew ("rn") command.
func RRenew(c *Conn, key, token string, opts ...Option) (remaining int, fence uint64, err error) {
	return rwRenew(c, "rn", key, token, opts...)
}

// WRenew sends a write-renew ("wn") command.
func WRenew(c *Conn, key, token string, opts ...Option) (remaining int, fence uint64, err error) {
	return rwRenew(c, "wn", key, token, opts...)
}

func rwRenew(c *Conn, cmd, key, token string, opts ...Option) (int, uint64, error) {
	if err := validateKey(key); err != nil {
		return 0, 0, err
	}
	if err := validateArg("token", token); err != nil {
		return 0, 0, err
	}
	var o options
	for _, fn := range opts {
		fn(&o)
	}
	arg := token
	if o.leaseTTL > 0 {
		arg += " " + strconv.Itoa(o.leaseTTL)
	}
	resp, err := c.sendRecv(cmd, key, arg)
	if err != nil {
		return 0, 0, err
	}
	return parseRenewWithFence(resp, cmd)
}

// REnqueue sends a read-enqueue ("re") command.
func REnqueue(c *Conn, key string, opts ...Option) (status, token string, leaseTTL int, fence uint64, err error) {
	return rwEnqueue(c, "re", key, opts...)
}

// WEnqueue sends a write-enqueue ("we") command.
func WEnqueue(c *Conn, key string, opts ...Option) (status, token string, leaseTTL int, fence uint64, err error) {
	return rwEnqueue(c, "we", key, opts...)
}

func rwEnqueue(c *Conn, cmd, key string, opts ...Option) (string, string, int, uint64, error) {
	if err := validateKey(key); err != nil {
		return "", "", 0, 0, err
	}
	var o options
	for _, fn := range opts {
		fn(&o)
	}
	arg := ""
	if o.leaseTTL > 0 {
		arg = strconv.Itoa(o.leaseTTL)
	}
	resp, err := c.sendRecv(cmd, key, arg)
	if err != nil {
		return "", "", 0, 0, err
	}
	if resp == "queued" {
		return "queued", "", 0, 0, nil
	}
	if resp == "error_max_locks" {
		return "", "", 0, 0, ErrMaxLocks
	}
	if resp == "error_max_waiters" {
		return "", "", 0, 0, ErrMaxWaiters
	}
	if resp == "error_type_mismatch" {
		return "", "", 0, 0, ErrTypeMismatch
	}
	if resp == "error_already_enqueued" {
		return "", "", 0, 0, ErrAlreadyQueued
	}
	parts := strings.Fields(resp)
	if (len(parts) == 3 || len(parts) == 4) && parts[0] == "acquired" {
		ttl, err := strconv.Atoi(parts[2])
		if err != nil {
			return "", "", 0, 0, fmt.Errorf("%w: %s: bad lease %q", ErrServer, cmd, parts[2])
		}
		var fence uint64
		if len(parts) == 4 {
			fence, err = strconv.ParseUint(parts[3], 10, 64)
			if err != nil {
				return "", "", 0, 0, fmt.Errorf("%w: %s: bad fence %q", ErrServer, cmd, parts[3])
			}
		}
		return "acquired", parts[1], ttl, fence, nil
	}
	return "", "", 0, 0, fmt.Errorf("%w: %s: %s", ErrServer, cmd, resp)
}

// RWait sends a read-wait ("rw") command.
func RWait(c *Conn, key string, timeout time.Duration) (token string, leaseTTL int, fence uint64, err error) {
	return rwWait(c, "rw", key, timeout)
}

// WWait sends a write-wait ("ww") command.
func WWait(c *Conn, key string, timeout time.Duration) (token string, leaseTTL int, fence uint64, err error) {
	return rwWait(c, "ww", key, timeout)
}

func rwWait(c *Conn, cmd, key string, timeout time.Duration) (string, int, uint64, error) {
	if err := validateKey(key); err != nil {
		return "", 0, 0, err
	}
	resp, err := c.sendRecv(cmd, key, strconv.Itoa(secondsCeil(timeout)))
	if err != nil {
		return "", 0, 0, err
	}
	if resp == "timeout" {
		return "", 0, 0, ErrTimeout
	}
	if resp == "error_not_enqueued" {
		return "", 0, 0, ErrNotQueued
	}
	if resp == "error_lease_expired" {
		return "", 0, 0, ErrLeaseExpired
	}
	if resp == "error" {
		return "", 0, 0, ErrServer
	}
	return parseOKTokenLeaseFence(resp, cmd)
}

// ---------------------------------------------------------------------------
// RWLock — high-level distributed read-write lock
// ---------------------------------------------------------------------------

// RWLock provides a high-level interface for acquiring, holding, and releasing
// a distributed read-write lock with automatic lease renewal.
type RWLock struct {
	Key            string
	AcquireTimeout time.Duration
	LeaseTTL       int
	Servers        []string
	ShardFunc      ShardFunc
	RenewRatio     float64
	TLSConfig      *tls.Config
	AuthToken      string
	OnRenewError   func(err error)

	mu          sync.Mutex
	conn        *Conn
	token       string
	fence       uint64
	lease       int
	mode        string // "rl" or "wl" — tracks acquire mode for correct unlock
	cancelRenew context.CancelFunc
	renewDone   chan struct{}
}

func (rw *RWLock) shardFunc() ShardFunc {
	if rw.ShardFunc != nil {
		return rw.ShardFunc
	}
	return CRC32Shard
}

func (rw *RWLock) acquireTimeout() time.Duration {
	if rw.AcquireTimeout > 0 {
		return rw.AcquireTimeout
	}
	return 10 * time.Second
}

func (rw *RWLock) renewRatio() float64 {
	if rw.RenewRatio > 0 {
		return rw.RenewRatio
	}
	return 0.5
}

func (rw *RWLock) serverAddr() string {
	servers := rw.Servers
	if len(servers) == 0 {
		servers = []string{"127.0.0.1:6388"}
	}
	idx := rw.shardFunc()(rw.Key, len(servers))
	return servers[idx]
}

func (rw *RWLock) opts() []Option {
	if rw.LeaseTTL > 0 {
		return []Option{WithLeaseTTL(rw.LeaseTTL)}
	}
	return nil
}

// closeConn closes the connection if non-nil and sets it to nil.
// Must be called with rw.mu held.
func (rw *RWLock) closeConn() {
	if rw.conn != nil {
		rw.conn.Close()
		rw.conn = nil
	}
}

func (rw *RWLock) connect() error {
	rw.closeConn()
	addr := rw.serverAddr()
	var conn *Conn
	var err error
	if rw.TLSConfig != nil {
		conn, err = DialTLS(addr, rw.TLSConfig)
	} else {
		conn, err = Dial(addr)
	}
	if err != nil {
		return err
	}
	if rw.AuthToken != "" {
		if err := Authenticate(conn, rw.AuthToken); err != nil {
			conn.Close()
			return err
		}
	}
	rw.conn = conn
	return nil
}

// RLock acquires a read lock. Returns false (with nil error) on timeout.
func (rw *RWLock) RLock(ctx context.Context) (bool, error) {
	return rw.acquire(ctx, "rl")
}

// WLock acquires a write lock. Returns false (with nil error) on timeout.
func (rw *RWLock) WLock(ctx context.Context) (bool, error) {
	return rw.acquire(ctx, "wl")
}

func (rw *RWLock) acquire(ctx context.Context, cmd string) (bool, error) {
	rw.mu.Lock()
	rw.stopRenew()
	rw.token = ""
	rw.fence = 0
	rw.lease = 0
	rw.mode = ""
	if err := rw.connect(); err != nil {
		rw.mu.Unlock()
		return false, err
	}
	conn := rw.conn
	rw.mu.Unlock()

	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			conn.Close()
		case <-done:
		}
	}()

	token, lease, fence, err := rwLock(conn, cmd, rw.Key, rw.acquireTimeout(), rw.opts()...)

	rw.mu.Lock()
	defer rw.mu.Unlock()

	if err != nil {
		close(done)
		if errors.Is(err, ErrTimeout) {
			rw.closeConn()
			return false, nil
		}
		if ctx.Err() != nil {
			rw.closeConn()
			return false, ctx.Err()
		}
		rw.closeConn()
		return false, err
	}

	if ctx.Err() != nil {
		// Release the acquired lock to avoid holding it until lease expiry.
		if cmd == "wl" {
			WUnlock(conn, rw.Key, token)
		} else {
			RUnlock(conn, rw.Key, token)
		}
		close(done)
		rw.closeConn()
		return false, ctx.Err()
	}
	close(done)

	rw.token = token
	rw.fence = fence
	rw.lease = lease
	rw.mode = cmd
	rw.startRenew(cmd)
	return true, nil
}

// Unlock releases the lock (works for both read and write).
func (rw *RWLock) Unlock(ctx context.Context) error {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	savedToken := rw.token
	rw.stopRenew()

	if rw.conn == nil {
		// Connection was force-closed by stopRenew. The server will
		// release the lock when the disconnect is detected or lease expires.
		rw.token = ""
		rw.fence = 0
		rw.lease = 0
		rw.mode = ""
		if savedToken != "" {
			return fmt.Errorf("dflockd: connection lost, server-side unlock skipped (lease will expire)")
		}
		return nil
	}

	// Use savedToken in case the renew goroutine cleared rw.token.
	token := rw.token
	if token == "" {
		token = savedToken
	}
	var err error
	if token != "" {
		if rw.mode == "wl" {
			err = WUnlock(rw.conn, rw.Key, token)
		} else {
			err = RUnlock(rw.conn, rw.Key, token)
		}
	}
	rw.closeConn()
	rw.token = ""
	rw.fence = 0
	rw.lease = 0
	rw.mode = ""
	return err
}

// Close stops renewal and closes the connection.
func (rw *RWLock) Close() error {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	rw.stopRenew()
	rw.closeConn()
	rw.token = ""
	rw.fence = 0
	rw.lease = 0
	rw.mode = ""
	return nil
}

// Token returns the current lock token.
func (rw *RWLock) Token() string {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	return rw.token
}

// Fence returns the fencing token from the last acquire.
func (rw *RWLock) Fence() uint64 {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	return rw.fence
}

func (rw *RWLock) stopRenew() {
	if rw.cancelRenew != nil {
		rw.cancelRenew()
		rw.cancelRenew = nil
	}
	if rw.renewDone != nil {
		done := rw.renewDone
		rw.renewDone = nil
		conn := rw.conn
		forceClosed := false
		rw.mu.Unlock()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			if conn != nil {
				conn.Close()
				forceClosed = true
			}
			<-done
		}
		rw.mu.Lock()
		if forceClosed && rw.conn == conn {
			rw.conn = nil
		}
	}
}

func (rw *RWLock) startRenew(cmd string) {
	// Callers must have already called stopRenew() before setting state.
	ctx, cancel := context.WithCancel(context.Background())
	rw.cancelRenew = cancel

	done := make(chan struct{})
	rw.renewDone = done

	leaseDur := time.Duration(rw.lease) * time.Second
	interval := time.Duration(float64(leaseDur) * rw.renewRatio())
	if interval < 1*time.Second {
		interval = 1 * time.Second
	}

	conn := rw.conn
	key := rw.Key
	token := rw.token
	opts := rw.opts()
	onErr := rw.OnRenewError
	renewCmd := "rn"
	if cmd == "wl" {
		renewCmd = "wn"
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
				rw.mu.Lock()
				if rw.conn == nil || rw.token == "" {
					rw.mu.Unlock()
					return
				}
				conn = rw.conn
				key = rw.Key
				token = rw.token
				rw.mu.Unlock()

				_, newFence, err := rwRenew(conn, renewCmd, key, token, opts...)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					// Clear token state to prevent stale lock usage.
					// Preserve rw.mode so Unlock sends the correct command.
					// Re-check ctx after acquiring mutex: stopRenew may
					// have cancelled while we were waiting for the lock.
					rw.mu.Lock()
					if ctx.Err() != nil {
						rw.mu.Unlock()
						return
					}
					rw.token = ""
					rw.fence = 0
					rw.lease = 0
					rw.mu.Unlock()
					if onErr != nil {
						go onErr(err)
					}
					return
				}
				if newFence > 0 {
					rw.mu.Lock()
					rw.fence = newFence
					rw.mu.Unlock()
				}
			}
		}
	}()
}

// Fence returns the fencing token from the last Lock.Acquire.
func (l *Lock) Fence() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.fence
}

// Fence returns the fencing token from the last Semaphore.Acquire.
func (s *Semaphore) Fence() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fence
}

// ---------------------------------------------------------------------------
// Watch/Notify
// ---------------------------------------------------------------------------

// WatchEvent represents a key change notification.
type WatchEvent struct {
	Type string // e.g. "kset", "kdel", "acquire", "release", etc.
	Key  string
}

// WatchConn wraps a Conn for watch operations, providing a background
// reader that separates push watch events from command responses.
type WatchConn struct {
	conn    *Conn
	eventCh chan WatchEvent
	respCh  chan string
	done    chan struct{}
}

// NewWatchConn creates a WatchConn from an existing Conn.
// The Conn must not be used directly after this call.
func NewWatchConn(c *Conn) *WatchConn {
	wc := &WatchConn{
		conn:    c,
		eventCh: make(chan WatchEvent, 64),
		respCh:  make(chan string, 1),
		done:    make(chan struct{}),
	}
	go wc.readLoop()
	return wc
}

func (wc *WatchConn) readLoop() {
	defer close(wc.done)
	for {
		line, err := wc.conn.readLine()
		if err != nil {
			close(wc.eventCh)
			return
		}
		if strings.HasPrefix(line, "watch ") {
			// Parse: "watch <event_type> <key>"
			rest := line[6:]
			idx := strings.Index(rest, " ")
			if idx < 0 {
				continue // malformed push — drop
			}
			ev := WatchEvent{
				Type: rest[:idx],
				Key:  rest[idx+1:],
			}
			select {
			case wc.eventCh <- ev:
			default:
			}
		} else {
			select {
			case wc.respCh <- line:
			default:
			}
		}
	}
}

func (wc *WatchConn) sendCmd(cmd, key, arg string) (string, error) {
	wc.conn.mu.Lock()
	defer wc.conn.mu.Unlock()
	select {
	case <-wc.respCh:
	default:
	}
	msg := fmt.Sprintf("%s\n%s\n%s\n", cmd, key, arg)
	if _, err := wc.conn.conn.Write([]byte(msg)); err != nil {
		return "", err
	}
	select {
	case resp, ok := <-wc.respCh:
		if !ok {
			return "", fmt.Errorf("dflockd: connection closed")
		}
		return resp, nil
	case <-wc.done:
		// Check respCh once more: the response may have arrived just
		// before readLoop closed done.
		select {
		case resp, ok := <-wc.respCh:
			if ok {
				return resp, nil
			}
		default:
		}
		return "", fmt.Errorf("dflockd: connection closed")
	}
}

// Watch subscribes to changes on a key or pattern.
func (wc *WatchConn) Watch(pattern string) error {
	if err := validateKey(pattern); err != nil {
		return fmt.Errorf("dflockd: invalid pattern: %w", err)
	}
	resp, err := wc.sendCmd("watch", pattern, "")
	if err != nil {
		return err
	}
	if resp != "ok" {
		return fmt.Errorf("%w: watch: %s", ErrServer, resp)
	}
	return nil
}

// Unwatch unsubscribes from a key or pattern.
func (wc *WatchConn) Unwatch(pattern string) error {
	if err := validateKey(pattern); err != nil {
		return fmt.Errorf("dflockd: invalid pattern: %w", err)
	}
	resp, err := wc.sendCmd("unwatch", pattern, "")
	if err != nil {
		return err
	}
	if resp != "ok" {
		return fmt.Errorf("%w: unwatch: %s", ErrServer, resp)
	}
	return nil
}

// Events returns the channel on which watch events are delivered.
func (wc *WatchConn) Events() <-chan WatchEvent {
	return wc.eventCh
}

// Close closes the underlying connection.
func (wc *WatchConn) Close() error {
	err := wc.conn.Close()
	<-wc.done
	return err
}

// ---------------------------------------------------------------------------
// Barriers
// ---------------------------------------------------------------------------

// BarrierWait blocks until 'count' participants arrive at the barrier,
// or the timeout expires. Returns true if the barrier tripped.
func BarrierWait(c *Conn, key string, count int, timeout time.Duration) (bool, error) {
	if err := validateKey(key); err != nil {
		return false, err
	}
	arg := strconv.Itoa(count) + " " + strconv.Itoa(secondsCeil(timeout))
	resp, err := c.sendRecv("bwait", key, arg)
	if err != nil {
		return false, err
	}
	if resp == "ok" {
		return true, nil
	}
	if resp == "timeout" {
		return false, nil
	}
	if resp == "error_barrier_count_mismatch" {
		return false, ErrBarrierCountMismatch
	}
	if resp == "error_max_keys" {
		return false, ErrMaxKeys
	}
	return false, fmt.Errorf("%w: bwait: %s", ErrServer, resp)
}

// ---------------------------------------------------------------------------
// Leader Election
// ---------------------------------------------------------------------------

// LeaderEvent represents a leader change notification.
type LeaderEvent struct {
	Type string // "elected", "resigned", "failover"
	Key  string
}

// LeaderConn wraps a Conn for leader election operations, providing a
// background reader that separates push leader events from command responses.
type LeaderConn struct {
	conn    *Conn
	eventCh chan LeaderEvent
	respCh  chan string
	done    chan struct{}
}

// NewLeaderConn creates a LeaderConn from an existing Conn.
// The Conn must not be used directly after this call.
func NewLeaderConn(c *Conn) *LeaderConn {
	lc := &LeaderConn{
		conn:    c,
		eventCh: make(chan LeaderEvent, 64),
		respCh:  make(chan string, 1),
		done:    make(chan struct{}),
	}
	go lc.readLoop()
	return lc
}

func (lc *LeaderConn) readLoop() {
	defer close(lc.done)
	for {
		line, err := lc.conn.readLine()
		if err != nil {
			close(lc.eventCh)
			return
		}
		if strings.HasPrefix(line, "leader ") {
			// Parse: "leader <event> <key>"
			rest := line[7:]
			idx := strings.Index(rest, " ")
			if idx < 0 {
				continue // malformed push — drop
			}
			ev := LeaderEvent{
				Type: rest[:idx],
				Key:  rest[idx+1:],
			}
			select {
			case lc.eventCh <- ev:
			default:
			}
		} else {
			select {
			case lc.respCh <- line:
			default:
			}
		}
	}
}

func (lc *LeaderConn) sendCmd(cmd, key, arg string) (string, error) {
	lc.conn.mu.Lock()
	defer lc.conn.mu.Unlock()
	select {
	case <-lc.respCh:
	default:
	}
	msg := fmt.Sprintf("%s\n%s\n%s\n", cmd, key, arg)
	if _, err := lc.conn.conn.Write([]byte(msg)); err != nil {
		return "", err
	}
	select {
	case resp, ok := <-lc.respCh:
		if !ok {
			return "", fmt.Errorf("dflockd: connection closed")
		}
		return resp, nil
	case <-lc.done:
		// Check respCh once more: the response may have arrived just
		// before readLoop closed done.
		select {
		case resp, ok := <-lc.respCh:
			if ok {
				return resp, nil
			}
		default:
		}
		return "", fmt.Errorf("dflockd: connection closed")
	}
}

// Elect attempts to become leader for the given key. Returns the token,
// lease TTL, fence, and any error.
func (lc *LeaderConn) Elect(key string, timeout time.Duration, opts ...Option) (token string, leaseTTL int, fence uint64, err error) {
	if err := validateKey(key); err != nil {
		return "", 0, 0, err
	}
	var o options
	for _, fn := range opts {
		fn(&o)
	}
	arg := strconv.Itoa(secondsCeil(timeout))
	if o.leaseTTL > 0 {
		arg += " " + strconv.Itoa(o.leaseTTL)
	}
	resp, err := lc.sendCmd("elect", key, arg)
	if err != nil {
		return "", 0, 0, err
	}
	if resp == "timeout" {
		return "", 0, 0, ErrTimeout
	}
	if resp == "error_max_locks" {
		return "", 0, 0, ErrMaxLocks
	}
	if resp == "error_max_waiters" {
		return "", 0, 0, ErrMaxWaiters
	}
	if resp == "error_type_mismatch" {
		return "", 0, 0, ErrTypeMismatch
	}
	if resp == "error_limit_mismatch" {
		return "", 0, 0, ErrLimitMismatch
	}
	return parseOKTokenLeaseFence(resp, "elect")
}

// Resign gives up leadership for the given key.
func (lc *LeaderConn) Resign(key, token string) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if err := validateArg("token", token); err != nil {
		return err
	}
	resp, err := lc.sendCmd("resign", key, token)
	if err != nil {
		return err
	}
	if resp != "ok" {
		return fmt.Errorf("%w: resign: %s", ErrServer, resp)
	}
	return nil
}

// Renew renews the leader lease. Returns the remaining seconds and fence.
func (lc *LeaderConn) Renew(key, token string, opts ...Option) (int, uint64, error) {
	if err := validateKey(key); err != nil {
		return 0, 0, err
	}
	if err := validateArg("token", token); err != nil {
		return 0, 0, err
	}
	var o options
	for _, fn := range opts {
		fn(&o)
	}
	arg := token
	if o.leaseTTL > 0 {
		arg += " " + strconv.Itoa(o.leaseTTL)
	}
	resp, err := lc.sendCmd("n", key, arg)
	if err != nil {
		return 0, 0, err
	}
	return parseRenewWithFence(resp, "renew")
}

// Observe subscribes to leader change events for a key.
func (lc *LeaderConn) Observe(key string) error {
	if err := validateKey(key); err != nil {
		return err
	}
	resp, err := lc.sendCmd("observe", key, "")
	if err != nil {
		return err
	}
	if resp != "ok" {
		return fmt.Errorf("%w: observe: %s", ErrServer, resp)
	}
	return nil
}

// Unobserve unsubscribes from leader change events for a key.
func (lc *LeaderConn) Unobserve(key string) error {
	if err := validateKey(key); err != nil {
		return err
	}
	resp, err := lc.sendCmd("unobserve", key, "")
	if err != nil {
		return err
	}
	if resp != "ok" {
		return fmt.Errorf("%w: unobserve: %s", ErrServer, resp)
	}
	return nil
}

// Events returns the channel on which leader events are delivered.
func (lc *LeaderConn) Events() <-chan LeaderEvent {
	return lc.eventCh
}

// Close closes the underlying connection.
func (lc *LeaderConn) Close() error {
	err := lc.conn.Close()
	<-lc.done
	return err
}

// Election provides a high-level interface for participating in leader election,
// including automatic lease renewal.
type Election struct {
	Key            string
	AcquireTimeout time.Duration
	LeaseTTL       int
	Servers        []string
	ShardFunc      ShardFunc
	TLSConfig      *tls.Config
	AuthToken      string
	RenewRatio     float64
	OnElected      func()
	OnResigned     func()
	OnRenewError   func(error)

	mu          sync.Mutex
	lc          *LeaderConn
	token       string
	fence       uint64
	lease       int
	isLeader    bool
	cancelRenew context.CancelFunc
	renewDone   chan struct{}
}

func (e *Election) shardFunc() ShardFunc {
	if e.ShardFunc != nil {
		return e.ShardFunc
	}
	return CRC32Shard
}

func (e *Election) acquireTimeout() time.Duration {
	if e.AcquireTimeout > 0 {
		return e.AcquireTimeout
	}
	return 10 * time.Second
}

func (e *Election) renewRatio() float64 {
	if e.RenewRatio > 0 {
		return e.RenewRatio
	}
	return 0.5
}

func (e *Election) serverAddr() string {
	servers := e.Servers
	if len(servers) == 0 {
		servers = []string{"127.0.0.1:6388"}
	}
	idx := e.shardFunc()(e.Key, len(servers))
	return servers[idx]
}

func (e *Election) opts() []Option {
	if e.LeaseTTL > 0 {
		return []Option{WithLeaseTTL(e.LeaseTTL)}
	}
	return nil
}

// closeLC closes the LeaderConn. Closes the underlying TCP connection
// then waits for readLoop to exit.
func (e *Election) closeLC() {
	if e.lc == nil {
		return
	}
	e.lc.conn.Close()
	<-e.lc.done
	e.lc = nil
}

func (e *Election) connect() error {
	e.closeLC()
	addr := e.serverAddr()
	var conn *Conn
	var err error
	if e.TLSConfig != nil {
		conn, err = DialTLS(addr, e.TLSConfig)
	} else {
		conn, err = Dial(addr)
	}
	if err != nil {
		return err
	}
	if e.AuthToken != "" {
		if err := Authenticate(conn, e.AuthToken); err != nil {
			conn.Close()
			return err
		}
	}
	e.lc = NewLeaderConn(conn)
	return nil
}

// Campaign blocks until elected or the context is cancelled.
// Returns true if elected, false if context was cancelled.
func (e *Election) Campaign(ctx context.Context) (bool, error) {
	e.mu.Lock()
	e.stopRenew()
	e.isLeader = false
	e.token = ""
	e.fence = 0
	e.lease = 0
	if err := e.connect(); err != nil {
		e.mu.Unlock()
		return false, err
	}
	lc := e.lc
	e.mu.Unlock()

	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			lc.conn.Close()
		case <-done:
		}
	}()

	token, lease, fence, err := lc.Elect(e.Key, e.acquireTimeout(), e.opts()...)

	e.mu.Lock()
	defer e.mu.Unlock()

	if err != nil {
		close(done)
		if errors.Is(err, ErrTimeout) {
			e.closeLC()
			return false, nil
		}
		if ctx.Err() != nil {
			e.closeLC()
			return false, ctx.Err()
		}
		e.closeLC()
		return false, err
	}

	if ctx.Err() != nil {
		lc.Resign(e.Key, token)
		close(done)
		e.closeLC()
		return false, ctx.Err()
	}

	close(done)

	e.token = token
	e.lease = lease
	e.fence = fence
	e.isLeader = true
	e.startRenew()

	if e.OnElected != nil {
		go e.OnElected()
	}

	return true, nil
}

// Resign gives up leadership.
func (e *Election) Resign(ctx context.Context) error {
	e.mu.Lock()

	savedToken := e.token
	wasLeader := e.isLeader
	e.stopRenew()

	if e.lc == nil {
		// Connection was force-closed by stopRenew. The server still
		// considers us the leader until lease expiry. Clear local state
		// and report the error to the caller.
		e.isLeader = false
		e.token = ""
		e.fence = 0
		e.lease = 0
		e.mu.Unlock()
		if wasLeader && e.OnResigned != nil {
			go e.OnResigned()
		}
		if wasLeader {
			return fmt.Errorf("dflockd: connection lost, server-side resign skipped (lease will expire)")
		}
		return nil
	}

	// Use savedToken in case the renew goroutine cleared e.token.
	token := e.token
	if token == "" {
		token = savedToken
	}
	lc := e.lc
	key := e.Key

	// Drop the lock during the blocking network call so that accessors
	// (IsLeader, Token, Fence) are not blocked.
	e.mu.Unlock()

	var err error
	if token != "" {
		err = lc.Resign(key, token)
	} else if wasLeader {
		// Leadership was lost by renewal failure before Resign was called.
		err = fmt.Errorf("dflockd: not leader (leadership lost before resign)")
	}

	e.mu.Lock()
	e.isLeader = false
	e.closeLC()
	e.token = ""
	e.fence = 0
	e.lease = 0
	e.mu.Unlock()

	if wasLeader && e.OnResigned != nil {
		go e.OnResigned()
	}

	return err
}

// IsLeader returns true if currently elected as leader.
func (e *Election) IsLeader() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.isLeader
}

// Token returns the current lock token.
func (e *Election) Token() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.token
}

// Fence returns the current fencing token.
func (e *Election) Fence() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.fence
}

// Close stops renewal and closes the connection.
func (e *Election) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.stopRenew()
	e.closeLC()
	e.token = ""
	e.fence = 0
	e.lease = 0
	e.isLeader = false
	return nil
}

func (e *Election) stopRenew() {
	if e.cancelRenew != nil {
		e.cancelRenew()
		e.cancelRenew = nil
	}
	if e.renewDone != nil {
		done := e.renewDone
		e.renewDone = nil
		lc := e.lc
		forceClosed := false
		e.mu.Unlock()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			if lc != nil {
				lc.conn.Close()
				forceClosed = true
			}
			<-done
		}
		e.mu.Lock()
		if forceClosed && e.lc == lc {
			<-lc.done // wait for readLoop goroutine to exit
			e.lc = nil
		}
	}
}

func (e *Election) startRenew() {
	// Callers must have already called stopRenew() before setting state.
	ctx, cancel := context.WithCancel(context.Background())
	e.cancelRenew = cancel

	done := make(chan struct{})
	e.renewDone = done

	leaseDur := time.Duration(e.lease) * time.Second
	interval := time.Duration(float64(leaseDur) * e.renewRatio())
	if interval < 1*time.Second {
		interval = 1 * time.Second
	}

	lc := e.lc
	key := e.Key
	token := e.token
	opts := e.opts()
	onErr := e.OnRenewError
	onResigned := e.OnResigned

	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				e.mu.Lock()
				if e.lc == nil || e.token == "" {
					e.mu.Unlock()
					return
				}
				lc = e.lc
				key = e.Key
				token = e.token
				e.mu.Unlock()

				_, newFence, err := lc.Renew(key, token, opts...)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					// Clear leadership state to prevent split-brain.
					// Re-check ctx after acquiring mutex: stopRenew may
					// have cancelled while we were waiting for the lock.
					e.mu.Lock()
					if ctx.Err() != nil {
						e.mu.Unlock()
						return
					}
					wasLeader := e.isLeader
					e.isLeader = false
					e.token = ""
					e.fence = 0
					e.lease = 0
					e.mu.Unlock()
					if onErr != nil {
						go onErr(err)
					}
					if wasLeader && onResigned != nil {
						go onResigned()
					}
					return
				}
				if newFence > 0 {
					e.mu.Lock()
					e.fence = newFence
					e.mu.Unlock()
				}
			}
		}
	}()
}
