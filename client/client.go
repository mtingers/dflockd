// Package client provides a Go client for the dflockd distributed FIFO
// lock server.
//
// Three layers from low to high:
//
//   - Conn: a single TCP (or TLS) connection with a buffered reader.
//     Used for the imperative command functions (Acquire, Release, etc.)
//     when you want to manage the connection yourself.
//
//   - Lock and Semaphore: high-level types that own a connection,
//     run automatic lease renewal in the background, and re-encode the
//     two-phase API into Acquire/Wait/Release.
//
//   - CRC32Shard / ShardFunc: client-side key sharding so a small
//     fleet of servers can be addressed without external routing.
package client

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mtingers/dflockd/internal/protocol"
)

// Sentinel errors returned by protocol operations. Wrapped errors
// always wrap one of these, so callers can use errors.Is.
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

const maxProtocolSeconds = int64(math.MaxInt64) / int64(time.Second)

// Option configures optional parameters for protocol commands.
type Option func(*options)

type options struct {
	leaseTTL int // seconds; 0 means use server default
}

// WithLeaseTTL sets a custom lease TTL (in seconds) for an Acquire or
// Enqueue. 0 (the default) means "use the server-configured default".
func WithLeaseTTL(seconds int) Option {
	return func(o *options) { o.leaseTTL = seconds }
}

// parseOptions applies and validates Option values. A negative TTL is
// a programmer error and surfaces immediately rather than silently
// becoming "server default".
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
// Conn
// ---------------------------------------------------------------------------

// DefaultDialTimeout caps the TCP handshake.
const DefaultDialTimeout = 10 * time.Second

// defaultKeepAlive is the interval between TCP keepalive probes.
const defaultKeepAlive = 30 * time.Second

// Conn wraps a TCP/TLS connection to a dflockd server with a buffered
// reader. Conn is safe for concurrent use; an internal mutex serialises
// request/response pairs so writes don't interleave on the wire.
type Conn struct {
	mu     sync.Mutex
	conn   net.Conn
	reader *bufio.Reader
}

// Dial connects to a dflockd server (host:port).
func Dial(addr string) (*Conn, error) {
	return dialContext(context.Background(), addr)
}

// dialContext is Dial with a caller-controlled connection context.
func dialContext(ctx context.Context, addr string) (*Conn, error) {
	dialer := &net.Dialer{Timeout: DefaultDialTimeout, KeepAlive: defaultKeepAlive}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Conn{conn: conn, reader: bufio.NewReader(conn)}, nil
}

func dialTLSContext(ctx context.Context, addr string, cfg *tls.Config) (*Conn, error) {
	dialer := &tls.Dialer{
		NetDialer: &net.Dialer{Timeout: DefaultDialTimeout, KeepAlive: defaultKeepAlive},
		Config:    cfg,
	}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Conn{conn: conn, reader: bufio.NewReader(conn)}, nil
}

// DialTLS connects to a dflockd server using TLS.
func DialTLS(addr string, cfg *tls.Config) (*Conn, error) {
	dialer := &net.Dialer{Timeout: DefaultDialTimeout, KeepAlive: defaultKeepAlive}
	conn, err := tls.DialWithDialer(dialer, "tcp", addr, cfg)
	if err != nil {
		return nil, err
	}
	return &Conn{conn: conn, reader: bufio.NewReader(conn)}, nil
}

// Close closes the underlying connection. net.Conn.Close is goroutine-
// safe and will unblock any pending I/O.
func (c *Conn) Close() error { return c.conn.Close() }

// maxResponseBytes caps a single server response line. Stats responses
// can grow with the active resource count, so the limit is generous.
const maxResponseBytes = 65536

// sendRecv writes one 3-line frame (cmd, key, arg) and reads exactly
// one response line. Holding c.mu means concurrent callers cannot
// interleave their bytes on the wire. A server-side "error_not_leader"
// response is converted into a *NotLeaderError so every caller can
// react uniformly (typically: reconnect to the named leader and retry).
func (c *Conn) sendRecv(cmd, key, arg string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Build the frame in one allocation.
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
	resp, err := c.readLine()
	if err != nil {
		return "", err
	}
	if nle := notLeaderFromResp(resp); nle != nil {
		return "", nle
	}
	return resp, nil
}

// readLine reads a single newline-terminated line, capped at
// maxResponseBytes to defend against malicious or buggy servers.
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
			// Drain the rest so subsequent reads stay framed.
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

// Authenticate sends an "auth" command. Returns ErrAuth if the server
// rejects the token.
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

// ---------------------------------------------------------------------------
// Validation helpers (mirror the server-side checks for fast feedback)
// ---------------------------------------------------------------------------

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

func validateLineLength(name, value string) error {
	if len(value) > protocol.MaxLineBytes {
		return fmt.Errorf("dflockd: %s too long (max %d bytes)", name, protocol.MaxLineBytes)
	}
	return nil
}

func validateToken(token string) error {
	if strings.TrimSpace(token) == "" {
		return fmt.Errorf("dflockd: empty value")
	}
	if strings.ContainsAny(token, "\n\r") {
		return fmt.Errorf("dflockd: value contains newline")
	}
	return nil
}

func validateSemaphoreLimit(limit int) error {
	if limit <= 0 {
		return fmt.Errorf("dflockd: semaphore limit must be > 0 (got %d)", limit)
	}
	return nil
}

// FenceFromToken returns the 64-bit monotonic prefix encoded in the
// first 16 hex chars of a server-issued token. The prefix strictly
// increases on every grant from a single dflockd server (including
// across restarts on a non-regressing wall clock), making it safe to
// use as a fencing token: a downstream resource can store the most
// recent fence it has observed for a given key and reject any write
// whose fence compares less. Comparison is per-key — fences from
// different keys aren't meaningfully ordered relative to one another.
func FenceFromToken(token string) (uint64, error) {
	prefix, err := decodeFencePrefix(token)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(prefix[:]), nil
}

// decodeFencePrefix validates the token's length and full hex shape,
// then returns the first 8 raw bytes.
func decodeFencePrefix(token string) ([8]byte, error) {
	var prefix [8]byte
	if len(token) != 32 {
		return prefix, fmt.Errorf("dflockd: token must be 32 hex chars, got %d", len(token))
	}
	var raw [16]byte
	_, err := hex.Decode(raw[:], []byte(token))
	copy(prefix[:], raw[:8])
	return prefix, wrapHexErr(err)
}

func wrapHexErr(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("dflockd: token is not hex: %w", err)
}

// secondsCeil converts a Duration to whole seconds, rounding up so a
// sub-second timeout is never silently truncated to zero.
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
// Low-level lock protocol
// ---------------------------------------------------------------------------

// Acquire sends an "l" command. Returns ErrTimeout on server-side
// timeout. Otherwise returns the granted token + lease seconds.
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
	if err := validateLineLength("acquire argument", arg); err != nil {
		return "", 0, err
	}
	resp, err := c.sendRecv("l", key, arg)
	if err != nil {
		return "", 0, err
	}
	return parseAcquireGrant(resp, "acquire")
}

// Release sends an "r" command for (key, token).
func Release(c *Conn, key, token string) error {
	return doRelease(c, "r", key, token)
}

// Barrier sends a "barrier" command and returns once the server
// acknowledges it. In cluster mode this is a linearizable-read barrier:
// when the call returns nil, every preceding write that committed
// against the connected leader is reflected in state the same
// connection's subsequent read will see. In single-node mode it
// returns immediately. On a follower the error is a *NotLeaderError —
// reconnect to the named leader and retry.
func Barrier(c *Conn) error {
	resp, err := c.sendRecv("barrier", "_", "")
	if err != nil {
		return err
	}
	if resp != "ok" {
		return fmt.Errorf("dflockd: barrier: %s", resp)
	}
	return nil
}

// SetStableRef sends a "stable-ref <ref>" command to the server,
// pinning the connection's caller identity. After this call, any
// acquire / enqueue / wait on this connection uses ref instead of
// the server's connID-derived value. Used by failover-aware callers:
// on a reconnect after a leader kill, a fresh connection with the
// same stable ref re-attaches to the original FSM waiter/holder slot
// (preserving FIFO order) instead of starting from the back of the
// queue.
//
// Returns once per connection — a second call returns an error. The
// ref is bounded to 64 ASCII bytes; longer or non-printable values
// are rejected.
func SetStableRef(c *Conn, ref string) error {
	if ref == "" {
		return fmt.Errorf("dflockd: stable-ref: empty")
	}
	resp, err := c.sendRecv("stable-ref", ref, "")
	if err != nil {
		return err
	}
	if resp != "ok" {
		return fmt.Errorf("dflockd: stable-ref: %s", resp)
	}
	return nil
}

// Renew sends an "n" command and returns the remaining lease seconds.
func Renew(c *Conn, key, token string, opts ...Option) (remaining int, err error) {
	return doRenew(c, "n", key, token, opts)
}

// Enqueue sends an "e" command (phase 1 of two-phase). Returns
// ("acquired", token, leaseTTL, nil) on the fast-path grant or
// ("queued", "", 0, nil) if the caller should call Wait next.
func Enqueue(c *Conn, key string, opts ...Option) (status, token string, leaseTTL int, err error) {
	return doEnqueue(c, "e", key, 0, opts)
}

// Wait sends a "w" command (phase 2 of two-phase). Returns ErrTimeout
// on timeout or ErrNotQueued if the caller didn't first Enqueue.
func Wait(c *Conn, key string, waitTimeout time.Duration) (token string, leaseTTL int, err error) {
	return doWait(c, "w", key, waitTimeout)
}

// SemAcquire sends an "sl" command.
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
	if err := validateLineLength("semaphore acquire argument", arg); err != nil {
		return "", 0, err
	}
	resp, err := c.sendRecv("sl", key, arg)
	if err != nil {
		return "", 0, err
	}
	return parseAcquireGrant(resp, "sem_acquire")
}

// SemRelease sends an "sr" command for (key, token).
func SemRelease(c *Conn, key, token string) error {
	return doRelease(c, "sr", key, token)
}

// SemRenew sends an "sn" command and returns the remaining lease seconds.
func SemRenew(c *Conn, key, token string, opts ...Option) (remaining int, err error) {
	return doRenew(c, "sn", key, token, opts)
}

// SemEnqueue sends an "se" command (phase 1).
func SemEnqueue(c *Conn, key string, limit int, opts ...Option) (status, token string, leaseTTL int, err error) {
	if err := validateSemaphoreLimit(limit); err != nil {
		return "", "", 0, err
	}
	return doEnqueue(c, "se", key, limit, opts)
}

// SemWait sends an "sw" command (phase 2).
func SemWait(c *Conn, key string, waitTimeout time.Duration) (token string, leaseTTL int, err error) {
	return doWait(c, "sw", key, waitTimeout)
}

// statusErrors maps every wire error code that any client command
// can receive to its sentinel error. Per-command parsers consult
// this table; codes not listed here surface as ErrServer (or as a
// command-specific wrapper).
var statusErrors = map[string]error{
	"timeout":                ErrTimeout,
	"error_max_locks":        ErrMaxLocks,
	"error_max_waiters":      ErrMaxWaiters,
	"error_limit_mismatch":   ErrLimitMismatch,
	"error_already_enqueued": ErrAlreadyQueued,
	"error_not_enqueued":     ErrNotQueued,
	"error_lease_expired":    ErrLeaseExpired,
	"error_draining":         ErrDraining,
}

// commandOp pairs the wire command with its operation label, used in
// error messages so the caller knows which method failed.
type commandOp struct {
	cmd, op string
}

var (
	releaseOp = map[string]commandOp{
		"r":  {"r", "release"},
		"sr": {"sr", "sem_release"},
	}
	renewOp = map[string]commandOp{
		"n":  {"n", "renew"},
		"sn": {"sn", "sem_renew"},
	}
	enqueueOp = map[string]commandOp{
		"e":  {"e", "enqueue"},
		"se": {"se", "sem_enqueue"},
	}
	waitOp = map[string]commandOp{
		"w":  {"w", "wait"},
		"sw": {"sw", "sem_wait"},
	}
)

// doRelease is the common path for r and sr.
func doRelease(c *Conn, cmd, key, token string) error {
	if err := validateReleaseArgs(key, token); err != nil {
		return err
	}
	resp, err := c.sendRecv(cmd, key, token)
	if err != nil {
		return err
	}
	return parseReleaseResp(resp, releaseOp[cmd].op)
}

func validateReleaseArgs(key, token string) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if err := validateToken(token); err != nil {
		return err
	}
	return validateLineLength("token", token)
}

func parseReleaseResp(resp, op string) error {
	if resp == "ok" {
		return nil
	}
	if e, ok := statusErrors[resp]; ok {
		return e
	}
	return fmt.Errorf("%w: %s: %s", ErrServer, op, resp)
}

// doRenew is the common path for n and sn.
func doRenew(c *Conn, cmd, key, token string, opts []Option) (int, error) {
	arg, err := buildRenewArg(key, token, opts)
	if err != nil {
		return 0, err
	}
	resp, err := c.sendRecv(cmd, key, arg)
	if err != nil {
		return 0, err
	}
	return parseRenewResp(resp, renewOp[cmd].op)
}

func buildRenewArg(key, token string, opts []Option) (string, error) {
	if err := validateKey(key); err != nil {
		return "", err
	}
	if err := validateToken(token); err != nil {
		return "", err
	}
	o, err := parseOptions(opts)
	if err != nil {
		return "", err
	}
	return formatRenewArg(token, o.leaseTTL)
}

func formatRenewArg(token string, leaseTTL int) (string, error) {
	arg := token
	if leaseTTL > 0 {
		arg += " " + strconv.Itoa(leaseTTL)
	}
	if err := validateLineLength("renew argument", arg); err != nil {
		return "", err
	}
	return arg, nil
}

func parseRenewResp(resp, op string) (int, error) {
	if n, ok := parseRenewOK(resp); ok {
		return n, nil
	}
	if e, ok := statusErrors[resp]; ok {
		return 0, e
	}
	if isBadRenewOK(resp) {
		return 0, fmt.Errorf("%w: renew: bad remaining %q", ErrServer, strings.Fields(resp)[1])
	}
	return 0, fmt.Errorf("%w: %s: %s", ErrServer, op, resp)
}

func parseRenewOK(resp string) (int, bool) {
	parts := strings.Fields(resp)
	if len(parts) != 2 || parts[0] != "ok" {
		return 0, false
	}
	n, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, false
	}
	return n, true
}

func isBadRenewOK(resp string) bool {
	parts := strings.Fields(resp)
	return len(parts) == 2 && parts[0] == "ok"
}

// doEnqueue is the common path for e and se. limit is unused for e.
func doEnqueue(c *Conn, cmd, key string, limit int, opts []Option) (string, string, int, error) {
	arg, err := buildEnqueueArg(cmd, key, limit, opts)
	if err != nil {
		return "", "", 0, err
	}
	resp, err := c.sendRecv(cmd, key, arg)
	if err != nil {
		return "", "", 0, err
	}
	return parseEnqueueResp(resp, enqueueOp[cmd].op)
}

func buildEnqueueArg(cmd, key string, limit int, opts []Option) (string, error) {
	if err := validateKey(key); err != nil {
		return "", err
	}
	o, err := parseOptions(opts)
	if err != nil {
		return "", err
	}
	arg := formatEnqueueArg(cmd, limit, o.leaseTTL)
	if err := validateLineLength("enqueue argument", arg); err != nil {
		return "", err
	}
	return arg, nil
}

func formatEnqueueArg(cmd string, limit, leaseTTL int) string {
	arg := ""
	if cmd == "se" {
		arg = strconv.Itoa(limit)
	}
	if leaseTTL == 0 {
		return arg
	}
	if arg != "" {
		arg += " "
	}
	return arg + strconv.Itoa(leaseTTL)
}

func parseEnqueueResp(resp, op string) (string, string, int, error) {
	if resp == "queued" {
		return "queued", "", 0, nil
	}
	if e, ok := statusErrors[resp]; ok {
		return "", "", 0, e
	}
	if tok, ttl, ok := parseAcquiredGrant(resp); ok {
		return "acquired", tok, ttl, nil
	}
	return "", "", 0, fmt.Errorf("%w: %s: %s", ErrServer, op, resp)
}

func parseAcquiredGrant(resp string) (string, int, bool) {
	parts := strings.Fields(resp)
	if len(parts) != 3 || parts[0] != "acquired" {
		return "", 0, false
	}
	ttl, err := strconv.Atoi(parts[2])
	if err != nil {
		return "", 0, false
	}
	return parts[1], ttl, true
}

// doWait is the common path for w and sw.
func doWait(c *Conn, cmd, key string, waitTimeout time.Duration) (string, int, error) {
	arg, err := buildWaitArg(key, waitTimeout)
	if err != nil {
		return "", 0, err
	}
	resp, err := c.sendRecv(cmd, key, arg)
	if err != nil {
		return "", 0, err
	}
	return parseWaitResp(resp, waitOp[cmd].op)
}

func buildWaitArg(key string, waitTimeout time.Duration) (string, error) {
	if err := validateKey(key); err != nil {
		return "", err
	}
	arg, err := timeoutArg(waitTimeout)
	if err != nil {
		return "", err
	}
	if err := validateLineLength("wait argument", arg); err != nil {
		return "", err
	}
	return arg, nil
}

func parseWaitResp(resp, op string) (string, int, error) {
	if e, ok := statusErrors[resp]; ok {
		return "", 0, e
	}
	if resp == "error" {
		return "", 0, ErrServer
	}
	return parseGrantResponse(resp, op)
}

// parseAcquireGrant decodes a single-phase acquire response.
func parseAcquireGrant(resp, op string) (string, int, error) {
	if e, ok := statusErrors[resp]; ok {
		return "", 0, e
	}
	return parseGrantResponse(resp, op)
}

// parseGrantResponse parses an "ok <token> <lease>" line.
func parseGrantResponse(resp, op string) (string, int, error) {
	parts := strings.Fields(resp)
	if len(parts) == 3 && parts[0] == "ok" {
		ttl, err := strconv.Atoi(parts[2])
		if err != nil {
			return "", 0, fmt.Errorf("%w: %s: bad lease %q", ErrServer, op, parts[2])
		}
		return parts[1], ttl, nil
	}
	return "", 0, fmt.Errorf("%w: %s: %s", ErrServer, op, resp)
}

// ---------------------------------------------------------------------------
// Sharding
// ---------------------------------------------------------------------------

// ShardFunc maps a key to a server index given the number of servers.
// It must return a value in [0, numServers); high-level types return
// an error if a custom function returns an out-of-range index.
type ShardFunc func(key string, numServers int) int

// CRC32Shard returns a CRC-32 (IEEE) shard index. Matches the Python
// client's stable_hash_shard so a heterogeneous client fleet picks the
// same server for a given key.
func CRC32Shard(key string, numServers int) int {
	if numServers <= 0 {
		return 0
	}
	return int(crc32.ChecksumIEEE([]byte(key)) % uint32(numServers))
}

func defaultShardFunc(f ShardFunc) ShardFunc {
	if f != nil {
		return f
	}
	return CRC32Shard
}

// resolveServerAddr picks the server addr for key based on the given
// servers + shard function. Defaults to 127.0.0.1:6388 when servers is
// empty.
func resolveServerAddr(key string, servers []string, f ShardFunc) (string, error) {
	if len(servers) == 0 {
		servers = []string{"127.0.0.1:6388"}
	}
	idx := defaultShardFunc(f)(key, len(servers))
	if idx < 0 || idx >= len(servers) {
		return "", fmt.Errorf("dflockd: shard function returned index %d for %d servers", idx, len(servers))
	}
	return servers[idx], nil
}

// jitteredInterval applies an early-only jitter factor in [1-jitter, 1]
// to interval. crypto/rand is used so unrelated processes don't
// synchronise their renewals onto the same scheduler tick.
func jitteredInterval(interval time.Duration, jitter float64) time.Duration {
	if interval <= 0 || jitter <= 0 {
		return interval
	}
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return interval
	}
	const denom = float64(uint64(1) << 53)
	x := float64(binary.BigEndian.Uint64(buf[:])>>11) / denom
	factor := 1 - x*jitter
	d := time.Duration(float64(interval) * factor)
	if d <= 0 {
		return interval
	}
	return d
}
