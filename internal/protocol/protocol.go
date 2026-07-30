// Package protocol implements the line-based wire protocol of dflockd.
//
// Frame: every request is 3 newline-terminated lines:
//
//	<cmd>\n
//	<key>\n
//	<arg>\n
//
// A response is a single newline-terminated line. The first whitespace-
// separated token is the status; remaining tokens (when present) carry
// status-specific data such as the token + lease seconds for an
// acquire grant.
package protocol

import (
	"bufio"
	"errors"
	"fmt"
	"math"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"
)

// MaxLineBytes caps the cmd, key, and most arg lines.
const MaxLineBytes = 256

// MaxSemaphoreLimit caps a semaphore's slot count. It's far larger than
// any sane use and exists mainly so the value fits in the fixed-width
// fields of the cluster snapshot encoding (a uint32) without truncating,
// which would diverge a snapshot-restored replica from a log-replayed one.
const MaxSemaphoreLimit = 1 << 20

// MaxAuthTokenBytes caps the auth token line. Tokens may legitimately be
// large, so we accept up to 64 KiB while keeping the tight cap on the
// rest of the protocol surface.
const MaxAuthTokenBytes = 64 * 1024

// maxSecondsValue is the largest seconds value that can be multiplied by
// time.Second without overflowing int64.
const maxSecondsValue = int64(math.MaxInt64) / int64(time.Second)

// Commands.
const (
	CmdAcquire    = "l"  // lock acquire (single-phase)
	CmdRelease    = "r"  // lock release
	CmdRenew      = "n"  // lock renew
	CmdEnqueue    = "e"  // lock enqueue (two-phase, phase 1)
	CmdWait       = "w"  // lock wait (two-phase, phase 2)
	CmdSemAcquire = "sl" // semaphore acquire (single-phase)
	CmdSemRelease = "sr" // semaphore release
	CmdSemRenew   = "sn" // semaphore renew
	CmdSemEnqueue = "se" // semaphore enqueue (two-phase, phase 1)
	CmdSemWait    = "sw" // semaphore wait (two-phase, phase 2)
	CmdPing       = "ping"
	CmdStats      = "stats"
	CmdAuth       = "auth"
	// CmdBarrier proposes a no-op through Raft and waits for it to apply.
	// In cluster mode it's a linearizable-read barrier (every preceding
	// committed write is visible after the call returns ok). In single-
	// node mode it returns ok immediately. On a follower it returns
	// error_not_leader.
	CmdBarrier = "barrier"
	// CmdStableRef sets a per-connection opaque identifier so the client
	// can re-attach to its existing FSM slots (waiters / holders with the
	// matching ref) after a leader failover + reconnect. Wire form:
	//   stable-ref\n<ref>\n_\n
	// where <ref> is any non-empty ASCII string ≤ 64 bytes. Subsequent
	// acquire/enqueue/wait on this connection use the stable ref instead
	// of the connID-derived ref. Sending it twice on the same connection
	// returns error_invalid (the ref is locked in on first use).
	CmdStableRef = "stable-ref"
)

// MaxStableRefLen bounds the stable ref string a client may send. Refs
// flow through the wire frame size (capped at ~64 KiB), but a stable
// ref is just a session-life-of-the-client identifier — 64 bytes is
// more than enough for a UUID + a small prefix.
const MaxStableRefLen = 64

// ValidateStableRef applies the format shared by the TCP stable-ref
// command and transport-specific adapters such as the HTTP API.
func ValidateStableRef(ref string) error {
	if ref == "" {
		return errors.New("empty")
	}
	if len(ref) > MaxStableRefLen {
		return fmt.Errorf("too long (%d > %d)", len(ref), MaxStableRefLen)
	}
	for _, r := range ref {
		if r < 0x20 || r > 0x7e {
			return errors.New("non-printable byte")
		}
	}
	return nil
}

// Status values returned in responses.
const (
	StatusOK                   = "ok"
	StatusAcquired             = "acquired" // two-phase enqueue fast-path
	StatusQueued               = "queued"
	StatusTimeout              = "timeout"
	StatusError                = "error"
	StatusErrorAuth            = "error_auth"
	StatusErrorMaxLocks        = "error_max_locks"
	StatusErrorMaxWaiters      = "error_max_waiters"
	StatusErrorLimitMismatch   = "error_limit_mismatch"
	StatusErrorNotEnqueued     = "error_not_enqueued"
	StatusErrorAlreadyEnqueued = "error_already_enqueued"
	StatusErrorLeaseExpired    = "error_lease_expired"
	StatusErrorDraining        = "error_draining"
	// StatusErrorNotLeader is returned by a cluster-mode follower for
	// any mutating command. The trailing whitespace-separated token, if
	// non-empty, is the leader's client-facing host:port; the client
	// retries against it (or round-robins members if empty).
	StatusErrorNotLeader = "error_not_leader"
)

// Pre-encoded response bytes for the common (no-payload) statuses. Avoids
// allocating on every response.
var (
	respOK                   = []byte("ok\n")
	respQueued               = []byte("queued\n")
	respTimeout              = []byte("timeout\n")
	respError                = []byte("error\n")
	respErrorAuth            = []byte("error_auth\n")
	respErrorMaxLocks        = []byte("error_max_locks\n")
	respErrorMaxWaiters      = []byte("error_max_waiters\n")
	respErrorLimitMismatch   = []byte("error_limit_mismatch\n")
	respErrorNotEnqueued     = []byte("error_not_enqueued\n")
	respErrorAlreadyEnqueued = []byte("error_already_enqueued\n")
	respErrorLeaseExpired    = []byte("error_lease_expired\n")
	respErrorDraining        = []byte("error_draining\n")

	prefixOK       = []byte("ok ")
	prefixAcquired = []byte("acquired ")
)

// Error codes for ProtocolError.
const (
	ErrCodeReadTimeout    = 10
	ErrCodeDisconnect     = 11
	ErrCodeLineTooLong    = 12
	ErrCodeInvalidCmd     = 3
	ErrCodeInvalidInt     = 4
	ErrCodeInvalidKey     = 5
	ErrCodeInvalidTimeout = 6
	ErrCodeEmptyToken     = 7
	ErrCodeInvalidArg     = 8
	ErrCodeInvalidLease   = 9
	ErrCodeInvalidLimit   = 13
)

// ProtocolError carries a numeric error code and a human-readable message.
// Code values are stable across the wire so client log analysis stays
// consistent.
type ProtocolError struct {
	Code    int
	Message string
}

// Error implements the error interface.
func (e *ProtocolError) Error() string {
	return fmt.Sprintf("protocol error %d: %s", e.Code, e.Message)
}

// Request is a fully parsed client request.
type Request struct {
	Cmd            string
	Key            string
	Token          string        // for r/n/sr/sn
	AcquireTimeout time.Duration // for l/sl/w/sw
	LeaseTTL       time.Duration // for l/sl/n/sn/e/se
	Limit          int           // for sl/se
	AuthToken      string        // for auth
	StableRef      string        // for stable-ref
}

// Ack is a fully formed server response.
type Ack struct {
	Status   string
	Token    string // grants populate this
	LeaseTTL int    // seconds; grants populate this
	Extra    string // free-form trailing text (e.g. stats JSON, renew remaining)
}

// ReadRequest reads exactly three protocol lines from r and parses them.
// timeout sets the per-read deadline on conn.  defaultLeaseTTL is applied
// when a command's lease argument is omitted.
func ReadRequest(r *bufio.Reader, timeout time.Duration, conn net.Conn, defaultLeaseTTL time.Duration) (*Request, error) {
	frame, err := readRequestFrame(r, timeout, conn)
	if err != nil {
		return nil, err
	}
	return parseRequest(frame.cmd, frame.key, frame.arg, defaultLeaseTTL)
}

// ReadRequestAfterIdle waits without a deadline for the first byte of the
// next request, then applies timeout independently to each protocol line.
// Established sessions may therefore remain idle while holding a lease,
// without weakening the slow-frame deadline once a request begins.
func ReadRequestAfterIdle(r *bufio.Reader, timeout time.Duration, conn net.Conn, defaultLeaseTTL time.Duration) (*Request, error) {
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		return nil, readDeadlineErr()
	}
	if _, err := r.Peek(1); err != nil {
		return nil, readByteErr(err)
	}
	return ReadRequest(r, timeout, conn, defaultLeaseTTL)
}

type requestFrame struct {
	cmd, key, arg string
}

func readRequestFrame(r *bufio.Reader, timeout time.Duration, conn net.Conn) (requestFrame, error) {
	cmd, err := readLine(r, timeout, conn, MaxLineBytes)
	if err != nil {
		return requestFrame{}, err
	}
	return readFrameAfterCmd(r, timeout, conn, cmd)
}

func readFrameAfterCmd(r *bufio.Reader, timeout time.Duration, conn net.Conn, cmd string) (requestFrame, error) {
	key, err := readLine(r, timeout, conn, MaxLineBytes)
	if err != nil {
		return requestFrame{}, err
	}
	return readFrameArg(r, timeout, conn, cmd, key)
}

func readFrameArg(r *bufio.Reader, timeout time.Duration, conn net.Conn, cmd, key string) (requestFrame, error) {
	arg, err := readLine(r, timeout, conn, argMaxBytes(cmd))
	if err != nil {
		return requestFrame{}, err
	}
	return requestFrame{cmd: cmd, key: key, arg: arg}, nil
}

func argMaxBytes(cmd string) int {
	if cmd == CmdAuth {
		return MaxAuthTokenBytes
	}
	return MaxLineBytes
}

// parseRequest dispatches to the per-command parser. Pure function for
// easy testing without a network connection.
func parseRequest(cmd, key, arg string, defaultLeaseTTL time.Duration) (*Request, error) {
	parser, ok := requestParsers[cmd]
	if !ok {
		return nil, invalidCmdErr(cmd)
	}
	return parser(cmd, key, arg, defaultLeaseTTL)
}

type requestParser func(cmd, key, arg string, defaultLeaseTTL time.Duration) (*Request, error)

var requestParsers = map[string]requestParser{
	CmdPing: parseNoKeyCommand, CmdStats: parseNoKeyCommand, CmdBarrier: parseNoKeyCommand,
	CmdAuth:      parseAuthCommand,
	CmdStableRef: parseStableRefCommand,
	CmdAcquire:   parseKeyedCommand(parseAcquire), CmdSemAcquire: parseKeyedCommand(parseAcquire),
	CmdRelease: parseKeyedCommand(parseReleaseCommand), CmdSemRelease: parseKeyedCommand(parseReleaseCommand),
	CmdRenew: parseKeyedCommand(parseRenew), CmdSemRenew: parseKeyedCommand(parseRenew),
	CmdEnqueue: parseKeyedCommand(parseEnqueue), CmdSemEnqueue: parseKeyedCommand(parseEnqueue),
	CmdWait: parseKeyedCommand(parseWaitCommand), CmdSemWait: parseKeyedCommand(parseWaitCommand),
}

func parseNoKeyCommand(cmd, _, _ string, _ time.Duration) (*Request, error) {
	return &Request{Cmd: cmd}, nil
}

func parseAuthCommand(cmd, _, arg string, _ time.Duration) (*Request, error) {
	return &Request{Cmd: cmd, AuthToken: strings.TrimSpace(arg)}, nil
}

// parseStableRefCommand reads the ref from the key line (wire form:
// "stable-ref\n<ref>\n_\n"). Refs are bounded to MaxStableRefLen and
// must be non-empty and printable ASCII.
func parseStableRefCommand(cmd, key, _ string, _ time.Duration) (*Request, error) {
	ref := strings.TrimSpace(key)
	if err := ValidateStableRef(ref); err != nil {
		return nil, &ProtocolError{Code: ErrCodeInvalidArg, Message: "stable-ref: " + err.Error()}
	}
	return &Request{Cmd: cmd, StableRef: ref}, nil
}

func parseKeyedCommand(parser requestParser) requestParser {
	return func(cmd, key, arg string, ttl time.Duration) (*Request, error) {
		return parseValidatedKeyCommand(parser, cmd, key, arg, ttl)
	}
}

func parseValidatedKeyCommand(parser requestParser, cmd, key, arg string, ttl time.Duration) (*Request, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	return parser(cmd, key, arg, ttl)
}

func invalidCmdErr(cmd string) error {
	return &ProtocolError{Code: ErrCodeInvalidCmd, Message: fmt.Sprintf("invalid cmd %q", cmd)}
}

// parseAcquire parses "l" / "sl" arguments:
//
//	l  arg: <timeout> [<lease_ttl>]
//	sl arg: <timeout> <limit> [<lease_ttl>]
func parseAcquire(cmd, key, arg string, defaultLeaseTTL time.Duration) (*Request, error) {
	parts := strings.Fields(arg)
	if err := requireArgCount(parts, acquireShape(cmd)); err != nil {
		return nil, err
	}
	return buildAcquire(cmd, key, parts, defaultLeaseTTL)
}

type argShape struct {
	counts []int
	msg    string
}

var (
	lockAcquireShape = argShape{[]int{1, 2}, "lock arg must be: <timeout> [<lease_ttl>]"}
	semAcquireShape  = argShape{[]int{2, 3}, "sl arg must be: <timeout> <limit> [<lease_ttl>]"}
	renewShape       = argShape{[]int{1, 2}, "renew arg must be: <token> [<lease_ttl>]"}
	lockEnqueueShape = argShape{[]int{0, 1}, "e arg must be: [<lease_ttl>]"}
	semEnqueueShape  = argShape{[]int{1, 2}, "se arg must be: <limit> [<lease_ttl>]"}
)

func acquireShape(cmd string) argShape {
	if cmd == CmdSemAcquire {
		return semAcquireShape
	}
	return lockAcquireShape
}

func requireArgCount(parts []string, shape argShape) error {
	if slices.Contains(shape.counts, len(parts)) {
		return nil
	}
	return argErr(shape.msg)
}

func buildAcquire(cmd, key string, parts []string, defaultLeaseTTL time.Duration) (*Request, error) {
	req, err := baseAcquire(cmd, key, parts[0], defaultLeaseTTL)
	if err != nil {
		return nil, err
	}
	return finishAcquire(req, parts)
}

func baseAcquire(cmd, key, timeout string, defaultLeaseTTL time.Duration) (*Request, error) {
	timeoutDur, err := parseTimeout(timeout)
	if err != nil {
		return nil, err
	}
	return &Request{Cmd: cmd, Key: key, AcquireTimeout: timeoutDur, LeaseTTL: defaultLeaseTTL}, nil
}

func finishAcquire(req *Request, parts []string) (*Request, error) {
	idx, err := applyAcquireLimit(req, parts)
	if err != nil {
		return nil, err
	}
	return applyOptionalLease(req, parts, idx)
}

func applyAcquireLimit(req *Request, parts []string) (int, error) {
	if req.Cmd != CmdSemAcquire {
		return 1, nil
	}
	return applySemLimit(req, parts[1], 2)
}

func applySemLimit(req *Request, raw string, next int) (int, error) {
	limit, err := parseLimit(raw)
	return applyParsedSemLimit(req, limit, err, next)
}

func applyParsedSemLimit(req *Request, limit int, err error, next int) (int, error) {
	if err != nil {
		return 0, err
	}
	req.Limit = limit
	return next, nil
}

func applyOptionalLease(req *Request, parts []string, idx int) (*Request, error) {
	if len(parts) <= idx {
		return req, nil
	}
	return applyLease(req, parts[idx])
}

func applyLease(req *Request, raw string) (*Request, error) {
	lease, err := parseLease(raw)
	return applyParsedLease(req, lease, err)
}

func applyParsedLease(req *Request, lease time.Duration, err error) (*Request, error) {
	if err != nil {
		return nil, err
	}
	req.LeaseTTL = lease
	return req, nil
}

// parseRelease parses "r" / "sr" arguments: <token>
func parseRelease(cmd, key, arg string) (*Request, error) {
	token := strings.TrimSpace(arg)
	if token == "" {
		return nil, emptyTokenErr()
	}
	return &Request{Cmd: cmd, Key: key, Token: token}, nil
}

func parseReleaseCommand(cmd, key, arg string, _ time.Duration) (*Request, error) {
	return parseRelease(cmd, key, arg)
}

// parseRenew parses "n" / "sn" arguments: <token> [<lease_ttl>]
func parseRenew(cmd, key, arg string, defaultLeaseTTL time.Duration) (*Request, error) {
	parts := strings.Fields(arg)
	if err := requireArgCount(parts, renewShape); err != nil {
		return nil, err
	}
	return buildRenew(cmd, key, parts, defaultLeaseTTL)
}

func buildRenew(cmd, key string, parts []string, defaultLeaseTTL time.Duration) (*Request, error) {
	req := &Request{Cmd: cmd, Key: key, Token: strings.TrimSpace(parts[0]), LeaseTTL: defaultLeaseTTL}
	if req.Token == "" {
		return nil, emptyTokenErr()
	}
	return applyOptionalLease(req, parts, 1)
}

// parseEnqueue parses "e" / "se" arguments:
//
//	e  arg: [<lease_ttl>]
//	se arg: <limit> [<lease_ttl>]
func parseEnqueue(cmd, key, arg string, defaultLeaseTTL time.Duration) (*Request, error) {
	parts := strings.Fields(arg)
	if err := requireArgCount(parts, enqueueShape(cmd)); err != nil {
		return nil, err
	}
	return buildEnqueue(cmd, key, parts, defaultLeaseTTL)
}

func enqueueShape(cmd string) argShape {
	if cmd == CmdSemEnqueue {
		return semEnqueueShape
	}
	return lockEnqueueShape
}

func buildEnqueue(cmd, key string, parts []string, defaultLeaseTTL time.Duration) (*Request, error) {
	req := &Request{Cmd: cmd, Key: key, LeaseTTL: defaultLeaseTTL}
	idx, err := applyEnqueueLimit(req, parts)
	return finishEnqueue(req, parts, idx, err)
}

func finishEnqueue(req *Request, parts []string, idx int, err error) (*Request, error) {
	if err != nil {
		return nil, err
	}
	return applyOptionalLease(req, parts, idx)
}

func applyEnqueueLimit(req *Request, parts []string) (int, error) {
	if req.Cmd != CmdSemEnqueue {
		return 0, nil
	}
	return applySemLimit(req, parts[0], 1)
}

// parseWait parses "w" / "sw" arguments: <timeout>
func parseWait(cmd, key, arg string) (*Request, error) {
	stripped := strings.TrimSpace(arg)
	if stripped == "" {
		return nil, argErr("wait arg must be: <timeout>")
	}
	return buildWait(cmd, key, stripped)
}

func parseWaitCommand(cmd, key, arg string, _ time.Duration) (*Request, error) {
	return parseWait(cmd, key, arg)
}

func buildWait(cmd, key, timeout string) (*Request, error) {
	timeoutDur, err := parseTimeout(timeout)
	if err != nil {
		return nil, err
	}
	return &Request{Cmd: cmd, Key: key, AcquireTimeout: timeoutDur}, nil
}

// FormatResponse encodes ack into the wire format. Returns a fresh byte
// slice the caller may write directly.
func FormatResponse(ack *Ack, defaultLeaseTTLSec int) []byte {
	if ack.Status == StatusErrorNotLeader {
		return formatNotLeader(ack.Extra)
	}
	if responseMayCarryPayload(ack.Status) {
		return formatGrantOrPlain(ack, defaultLeaseTTLSec)
	}
	return formatPlainStatus(ack.Status)
}

// formatNotLeader emits "error_not_leader [<addr>]\n". An empty addr is
// allowed — the client falls back to round-robin against the configured
// members until one becomes leader.
func formatNotLeader(addr string) []byte {
	if addr == "" {
		return []byte(StatusErrorNotLeader + "\n")
	}
	return []byte(StatusErrorNotLeader + " " + addr + "\n")
}

func responseMayCarryPayload(status string) bool {
	return status == StatusOK || status == StatusAcquired
}

func formatPlainStatus(status string) []byte {
	if resp, ok := plainResponses[status]; ok {
		return resp
	}
	return []byte(status + "\n")
}

var plainResponses = map[string][]byte{
	StatusQueued: respQueued, StatusTimeout: respTimeout,
	StatusError: respError, StatusErrorAuth: respErrorAuth,
	StatusErrorMaxLocks: respErrorMaxLocks, StatusErrorMaxWaiters: respErrorMaxWaiters,
	StatusErrorLimitMismatch: respErrorLimitMismatch, StatusErrorNotEnqueued: respErrorNotEnqueued,
	StatusErrorAlreadyEnqueued: respErrorAlreadyEnqueued, StatusErrorLeaseExpired: respErrorLeaseExpired,
	StatusErrorDraining: respErrorDraining,
}

// formatGrantOrPlain handles the two flavours of "ok" / "acquired":
//
//	<status> <token> <lease>\n   when ack.Token != ""
//	<status> <extra>\n           when ack.Extra != ""
//	<status>\n                   otherwise
func formatGrantOrPlain(ack *Ack, defaultLeaseTTLSec int) []byte {
	if ack.Token != "" {
		return formatGrant(ack, defaultLeaseTTLSec)
	}
	return formatGrantWithoutToken(ack)
}

func formatGrantWithoutToken(ack *Ack) []byte {
	if ack.Extra != "" {
		return formatExtra(ack)
	}
	return plainGrantStatus(ack.Status)
}

func formatGrant(ack *Ack, defaultLeaseTTLSec int) []byte {
	lease := responseLease(ack.LeaseTTL, defaultLeaseTTLSec)
	buf := make([]byte, 0, len(statusPrefix(ack.Status))+len(ack.Token)+12)
	return appendGrant(buf, ack, lease)
}

func appendGrant(buf []byte, ack *Ack, lease int) []byte {
	buf = append(buf, statusPrefix(ack.Status)...)
	buf = append(buf, ack.Token...)
	buf = append(buf, ' ')
	return appendLine(strconv.AppendInt(buf, int64(lease), 10))
}

func responseLease(lease, defaultLease int) int {
	if lease == 0 {
		return defaultLease
	}
	return lease
}

func formatExtra(ack *Ack) []byte {
	buf := make([]byte, 0, len(statusPrefix(ack.Status))+len(ack.Extra)+1)
	buf = append(buf, statusPrefix(ack.Status)...)
	buf = append(buf, ack.Extra...)
	return appendLine(buf)
}

func plainGrantStatus(status string) []byte {
	if status == StatusOK {
		return respOK
	}
	return []byte(status + "\n")
}

func statusPrefix(status string) []byte {
	if status == StatusAcquired {
		return prefixAcquired
	}
	return prefixOK
}

func appendLine(buf []byte) []byte {
	return append(buf, '\n')
}

// readLine reads one newline-terminated line from r, enforcing max as the
// payload cap. The returned string has trailing CR stripped.
//
// Hot-path note: lines that fit in MaxLineBytes are accumulated in a
// stack-allocated backing array, so the only per-line heap allocation is
// the final string conversion — the byte buffer itself never escapes.
func readLine(r *bufio.Reader, timeout time.Duration, conn net.Conn, max int) (string, error) {
	if err := setReadDeadline(conn, timeout); err != nil {
		return "", err
	}
	var stackBuf [MaxLineBytes]byte
	return readLineWithBuffer(r, lineBuffer(max, stackBuf[:0]), max)
}

func setReadDeadline(conn net.Conn, timeout time.Duration) error {
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return readDeadlineErr()
	}
	return nil
}

func lineBuffer(max int, stack []byte) []byte {
	if max <= MaxLineBytes {
		return stack
	}
	return make([]byte, 0, 256)
}

func readLineWithBuffer(r *bufio.Reader, buf []byte, max int) (string, error) {
	lr := &lineReader{r: r, buf: buf, max: max}
	return lr.read()
}

type lineReader struct {
	r    *bufio.Reader
	buf  []byte
	max  int
	step lineStep
}

func (lr *lineReader) read() (string, error) {
	for {
		if !lr.advance() {
			return finishReadLine(lr.step)
		}
	}
}

func (lr *lineReader) advance() bool {
	lr.step = readLineStep(lr.r, lr.buf, lr.max)
	return lr.keepReading()
}

func (lr *lineReader) keepReading() bool {
	if lr.step.done || lr.step.err != nil {
		return false
	}
	lr.buf = lr.step.buf
	return true
}

type lineStep struct {
	done bool
	buf  []byte
	err  error
}

func readLineStep(r *bufio.Reader, buf []byte, max int) lineStep {
	b, err := r.ReadByte()
	if err != nil {
		return failedLine(buf, readByteErr(err))
	}
	return readLineByte(r, b, buf, max)
}

func readLineByte(r *bufio.Reader, b byte, buf []byte, max int) lineStep {
	if b == '\n' {
		return completedLine(buf)
	}
	return readPayloadByte(r, b, buf, max)
}

func readPayloadByte(r *bufio.Reader, b byte, buf []byte, max int) lineStep {
	if len(buf) >= max {
		return failedLine(buf, lineTooLongAfterDrain(r))
	}
	return pendingLine(append(buf, b))
}

func finishReadLine(step lineStep) (string, error) {
	if step.err != nil {
		return "", step.err
	}
	return cleanLine(step.buf), nil
}

func completedLine(buf []byte) lineStep {
	return lineStep{done: true, buf: buf}
}

func pendingLine(buf []byte) lineStep {
	return lineStep{buf: buf}
}

func failedLine(buf []byte, err error) lineStep {
	return lineStep{buf: buf, err: err}
}

func cleanLine(buf []byte) string {
	return strings.TrimRight(string(buf), "\r")
}

func readByteErr(err error) error {
	if isTimeoutErr(err) {
		return readTimeoutErr()
	}
	return disconnectErr()
}

func isTimeoutErr(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

func lineTooLongAfterDrain(r *bufio.Reader) error {
	drainLine(r)
	return lineTooLongErr()
}

// drainLine consumes the rest of an oversized line so subsequent reads
// remain framed.
func drainLine(r *bufio.Reader) {
	for {
		if drainLineDone(r) {
			return
		}
	}
}

func drainLineDone(r *bufio.Reader) bool {
	c, err := r.ReadByte()
	return err != nil || c == '\n'
}

// validateKey rejects keys that are empty or contain whitespace. Whitespace
// would desynchronise the line-oriented framing on the receiver.
func validateKey(key string) error {
	if key == "" {
		return invalidKeyErr("empty key")
	}
	return validateNonEmptyKey(key)
}

func validateNonEmptyKey(key string) error {
	if strings.ContainsAny(key, " \t\n\r") {
		return invalidKeyErr("key contains whitespace")
	}
	return nil
}

// parseSeconds parses an integer seconds value into a time.Duration,
// rejecting overflow and out-of-range values. minSec sets the inclusive
// lower bound (0 for timeouts, 1 for lease).
func parseSeconds(s, what string, minSec, code int) (time.Duration, error) {
	n, err := parseSecondsInt(s, what)
	return secondsDuration(n, err, what, minSec, code)
}

func secondsDuration(n int64, err error, what string, minSec, code int) (time.Duration, error) {
	if err != nil {
		return 0, err
	}
	return checkedSecondsDuration(n, what, minSec, code)
}

func checkedSecondsDuration(n int64, what string, minSec, code int) (time.Duration, error) {
	if err := validateSeconds(n, what, minSec, code); err != nil {
		return 0, err
	}
	return time.Duration(n) * time.Second, nil
}

func parseTimeout(s string) (time.Duration, error) {
	return parseSeconds(s, "timeout", 0, ErrCodeInvalidTimeout)
}

func parseLease(s string) (time.Duration, error) {
	return parseSeconds(s, "lease_ttl", 1, ErrCodeInvalidLease)
}

func parseSecondsInt(s, what string) (int64, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, invalidIntErr(what, s)
	}
	return n, nil
}

func validateSeconds(n int64, what string, minSec, code int) error {
	if err := validateSecondsMin(n, what, minSec, code); err != nil {
		return err
	}
	return validateSecondsMax(n, what, code)
}

func validateSecondsMin(n int64, what string, minSec, code int) error {
	if n < int64(minSec) {
		return secondsMinErr(what, minSec, code)
	}
	return nil
}

func secondsMinErr(what string, minSec, code int) error {
	if minSec == 0 {
		return protocolErr(code, fmt.Sprintf("%s must be >= 0", what))
	}
	return protocolErr(code, fmt.Sprintf("%s must be > 0", what))
}

func validateSecondsMax(n int64, what string, code int) error {
	if n > maxSecondsValue {
		return protocolErr(code, fmt.Sprintf("%s too large (max %d)", what, maxSecondsValue))
	}
	return nil
}

// parseLimit parses a positive semaphore limit.
func parseLimit(s string) (int, error) {
	n, err := parseLimitInt(s)
	if err != nil {
		return 0, err
	}
	return validateLimit(n)
}

func parseLimitInt(s string) (int, error) {
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, invalidIntErr("limit", s)
	}
	return n, nil
}

func validateLimit(n int) (int, error) {
	if n <= 0 {
		return 0, protocolErr(ErrCodeInvalidLimit, "limit must be > 0")
	}
	if n > MaxSemaphoreLimit {
		return 0, protocolErr(ErrCodeInvalidLimit, "limit too large")
	}
	return n, nil
}

func argErr(msg string) error {
	return protocolErr(ErrCodeInvalidArg, msg)
}

func emptyTokenErr() error {
	return protocolErr(ErrCodeEmptyToken, "empty token")
}

func invalidKeyErr(msg string) error {
	return protocolErr(ErrCodeInvalidKey, msg)
}

func invalidIntErr(what, raw string) error {
	return protocolErr(ErrCodeInvalidInt, fmt.Sprintf("invalid %s: %q", what, raw))
}

func readDeadlineErr() error {
	return protocolErr(ErrCodeReadTimeout, "failed to set deadline")
}

func readTimeoutErr() error {
	return protocolErr(ErrCodeReadTimeout, "read timeout")
}

func disconnectErr() error {
	return protocolErr(ErrCodeDisconnect, "client disconnected")
}

func lineTooLongErr() error {
	return protocolErr(ErrCodeLineTooLong, "line too long")
}

func protocolErr(code int, msg string) error {
	return &ProtocolError{Code: code, Message: msg}
}
