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
	"strconv"
	"strings"
	"time"
)

// MaxLineBytes caps the cmd, key, and most arg lines.
const MaxLineBytes = 256

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
)

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
	cmd, err := readLine(r, timeout, conn, MaxLineBytes)
	if err != nil {
		return nil, err
	}
	key, err := readLine(r, timeout, conn, MaxLineBytes)
	if err != nil {
		return nil, err
	}
	argMax := MaxLineBytes
	if cmd == CmdAuth {
		argMax = MaxAuthTokenBytes
	}
	arg, err := readLine(r, timeout, conn, argMax)
	if err != nil {
		return nil, err
	}
	return parseRequest(cmd, key, arg, defaultLeaseTTL)
}

// parseRequest dispatches to the per-command parser. Pure function for
// easy testing without a network connection.
func parseRequest(cmd, key, arg string, defaultLeaseTTL time.Duration) (*Request, error) {
	switch cmd {
	case CmdPing:
		return &Request{Cmd: cmd}, nil
	case CmdStats:
		return &Request{Cmd: cmd}, nil
	case CmdAuth:
		return &Request{Cmd: cmd, AuthToken: strings.TrimSpace(arg)}, nil
	}

	if err := validateKey(key); err != nil {
		return nil, err
	}

	switch cmd {
	case CmdAcquire, CmdSemAcquire:
		return parseAcquire(cmd, key, arg, defaultLeaseTTL)
	case CmdRelease, CmdSemRelease:
		return parseRelease(cmd, key, arg)
	case CmdRenew, CmdSemRenew:
		return parseRenew(cmd, key, arg, defaultLeaseTTL)
	case CmdEnqueue, CmdSemEnqueue:
		return parseEnqueue(cmd, key, arg, defaultLeaseTTL)
	case CmdWait, CmdSemWait:
		return parseWait(cmd, key, arg)
	}
	return nil, &ProtocolError{Code: ErrCodeInvalidCmd, Message: fmt.Sprintf("invalid cmd %q", cmd)}
}

// parseAcquire parses "l" / "sl" arguments:
//
//	l  arg: <timeout> [<lease_ttl>]
//	sl arg: <timeout> <limit> [<lease_ttl>]
func parseAcquire(cmd, key, arg string, defaultLeaseTTL time.Duration) (*Request, error) {
	parts := strings.Fields(arg)
	isSem := cmd == CmdSemAcquire

	want := []int{1, 2}
	if isSem {
		want = []int{2, 3}
	}
	if !containsInt(want, len(parts)) {
		if isSem {
			return nil, argErr("sl arg must be: <timeout> <limit> [<lease_ttl>]")
		}
		return nil, argErr("lock arg must be: <timeout> [<lease_ttl>]")
	}
	timeoutDur, err := parseSeconds(parts[0], "timeout", 0, ErrCodeInvalidTimeout)
	if err != nil {
		return nil, err
	}
	req := &Request{Cmd: cmd, Key: key, AcquireTimeout: timeoutDur, LeaseTTL: defaultLeaseTTL}
	idx := 1
	if isSem {
		limit, err := parseLimit(parts[1])
		if err != nil {
			return nil, err
		}
		req.Limit = limit
		idx = 2
	}
	if len(parts) > idx {
		lease, err := parseSeconds(parts[idx], "lease_ttl", 1, ErrCodeInvalidLease)
		if err != nil {
			return nil, err
		}
		req.LeaseTTL = lease
	}
	return req, nil
}

// parseRelease parses "r" / "sr" arguments: <token>
func parseRelease(cmd, key, arg string) (*Request, error) {
	token := strings.TrimSpace(arg)
	if token == "" {
		return nil, &ProtocolError{Code: ErrCodeEmptyToken, Message: "empty token"}
	}
	return &Request{Cmd: cmd, Key: key, Token: token}, nil
}

// parseRenew parses "n" / "sn" arguments: <token> [<lease_ttl>]
func parseRenew(cmd, key, arg string, defaultLeaseTTL time.Duration) (*Request, error) {
	parts := strings.Fields(arg)
	if len(parts) != 1 && len(parts) != 2 {
		return nil, argErr("renew arg must be: <token> [<lease_ttl>]")
	}
	token := strings.TrimSpace(parts[0])
	if token == "" {
		return nil, &ProtocolError{Code: ErrCodeEmptyToken, Message: "empty token"}
	}
	leaseTTL := defaultLeaseTTL
	if len(parts) == 2 {
		var err error
		leaseTTL, err = parseSeconds(parts[1], "lease_ttl", 1, ErrCodeInvalidLease)
		if err != nil {
			return nil, err
		}
	}
	return &Request{Cmd: cmd, Key: key, Token: token, LeaseTTL: leaseTTL}, nil
}

// parseEnqueue parses "e" / "se" arguments:
//
//	e  arg: [<lease_ttl>]
//	se arg: <limit> [<lease_ttl>]
func parseEnqueue(cmd, key, arg string, defaultLeaseTTL time.Duration) (*Request, error) {
	parts := strings.Fields(arg)
	isSem := cmd == CmdSemEnqueue
	if isSem {
		if len(parts) != 1 && len(parts) != 2 {
			return nil, argErr("se arg must be: <limit> [<lease_ttl>]")
		}
	} else {
		// e accepts at most one field. Reject extras so a typo like
		// "30 junk" doesn't silently parse the lease and drop the rest.
		if len(parts) > 1 {
			return nil, argErr("e arg must be: [<lease_ttl>]")
		}
	}

	req := &Request{Cmd: cmd, Key: key, LeaseTTL: defaultLeaseTTL}
	idx := 0
	if isSem {
		limit, err := parseLimit(parts[0])
		if err != nil {
			return nil, err
		}
		req.Limit = limit
		idx = 1
	}
	if len(parts) > idx {
		lease, err := parseSeconds(parts[idx], "lease_ttl", 1, ErrCodeInvalidLease)
		if err != nil {
			return nil, err
		}
		req.LeaseTTL = lease
	}
	return req, nil
}

// parseWait parses "w" / "sw" arguments: <timeout>
func parseWait(cmd, key, arg string) (*Request, error) {
	stripped := strings.TrimSpace(arg)
	if stripped == "" {
		return nil, argErr("wait arg must be: <timeout>")
	}
	timeoutDur, err := parseSeconds(stripped, "timeout", 0, ErrCodeInvalidTimeout)
	if err != nil {
		return nil, err
	}
	return &Request{Cmd: cmd, Key: key, AcquireTimeout: timeoutDur}, nil
}

// FormatResponse encodes ack into the wire format. Returns a fresh byte
// slice the caller may write directly.
func FormatResponse(ack *Ack, defaultLeaseTTLSec int) []byte {
	switch ack.Status {
	case StatusOK, StatusAcquired:
		return formatGrantOrPlain(ack, defaultLeaseTTLSec)
	case StatusQueued:
		return respQueued
	case StatusTimeout:
		return respTimeout
	case StatusError:
		return respError
	case StatusErrorAuth:
		return respErrorAuth
	case StatusErrorMaxLocks:
		return respErrorMaxLocks
	case StatusErrorMaxWaiters:
		return respErrorMaxWaiters
	case StatusErrorLimitMismatch:
		return respErrorLimitMismatch
	case StatusErrorNotEnqueued:
		return respErrorNotEnqueued
	case StatusErrorAlreadyEnqueued:
		return respErrorAlreadyEnqueued
	case StatusErrorLeaseExpired:
		return respErrorLeaseExpired
	case StatusErrorDraining:
		return respErrorDraining
	}
	// Fallback for any custom status the caller injects (e.g. tests).
	return []byte(ack.Status + "\n")
}

// formatGrantOrPlain handles the two flavours of "ok" / "acquired":
//
//	<status> <token> <lease>\n   when ack.Token != ""
//	<status> <extra>\n           when ack.Extra != ""
//	<status>\n                   otherwise
func formatGrantOrPlain(ack *Ack, defaultLeaseTTLSec int) []byte {
	prefix := prefixOK
	if ack.Status == StatusAcquired {
		prefix = prefixAcquired
	}
	if ack.Token != "" {
		lease := ack.LeaseTTL
		if lease == 0 {
			lease = defaultLeaseTTLSec
		}
		buf := make([]byte, 0, len(prefix)+len(ack.Token)+12)
		buf = append(buf, prefix...)
		buf = append(buf, ack.Token...)
		buf = append(buf, ' ')
		buf = strconv.AppendInt(buf, int64(lease), 10)
		buf = append(buf, '\n')
		return buf
	}
	if ack.Extra != "" {
		buf := make([]byte, 0, len(prefix)+len(ack.Extra)+1)
		buf = append(buf, prefix...)
		buf = append(buf, ack.Extra...)
		buf = append(buf, '\n')
		return buf
	}
	if ack.Status == StatusOK {
		return respOK
	}
	return []byte(ack.Status + "\n")
}

// readLine reads one newline-terminated line from r, enforcing max as the
// payload cap. The returned string has trailing CR stripped.
//
// Hot-path note: most lines fit in MaxLineBytes; we use a stack-allocated
// backing array in that case so the common request triple causes zero
// heap allocation.
func readLine(r *bufio.Reader, timeout time.Duration, conn net.Conn, max int) (string, error) {
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return "", &ProtocolError{Code: ErrCodeReadTimeout, Message: "failed to set deadline"}
	}

	var stackBuf [MaxLineBytes]byte
	var buf []byte
	if max <= MaxLineBytes {
		buf = stackBuf[:0]
	} else {
		buf = make([]byte, 0, 256)
	}

	for {
		b, err := r.ReadByte()
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				return "", &ProtocolError{Code: ErrCodeReadTimeout, Message: "read timeout"}
			}
			return "", &ProtocolError{Code: ErrCodeDisconnect, Message: "client disconnected"}
		}
		if b == '\n' {
			break
		}
		if len(buf) >= max {
			drainLine(r)
			return "", &ProtocolError{Code: ErrCodeLineTooLong, Message: "line too long"}
		}
		buf = append(buf, b)
	}
	return strings.TrimRight(string(buf), "\r"), nil
}

// drainLine consumes the rest of an oversized line so subsequent reads
// remain framed.
func drainLine(r *bufio.Reader) {
	for {
		c, err := r.ReadByte()
		if err != nil || c == '\n' {
			return
		}
	}
}

// validateKey rejects keys that are empty or contain whitespace. Whitespace
// would desynchronise the line-oriented framing on the receiver.
func validateKey(key string) error {
	if key == "" {
		return &ProtocolError{Code: ErrCodeInvalidKey, Message: "empty key"}
	}
	for _, c := range key {
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			return &ProtocolError{Code: ErrCodeInvalidKey, Message: "key contains whitespace"}
		}
	}
	return nil
}

// parseSeconds parses an integer seconds value into a time.Duration,
// rejecting overflow and out-of-range values. minSec sets the inclusive
// lower bound (0 for timeouts, 1 for lease).
func parseSeconds(s, what string, minSec, code int) (time.Duration, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, &ProtocolError{Code: ErrCodeInvalidInt, Message: fmt.Sprintf("invalid %s: %q", what, s)}
	}
	if n < int64(minSec) {
		if minSec == 0 {
			return 0, &ProtocolError{Code: code, Message: fmt.Sprintf("%s must be >= 0", what)}
		}
		return 0, &ProtocolError{Code: code, Message: fmt.Sprintf("%s must be > 0", what)}
	}
	if n > maxSecondsValue {
		return 0, &ProtocolError{Code: code, Message: fmt.Sprintf("%s too large (max %d)", what, maxSecondsValue)}
	}
	return time.Duration(n) * time.Second, nil
}

// parseLimit parses a positive semaphore limit.
func parseLimit(s string) (int, error) {
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, &ProtocolError{Code: ErrCodeInvalidInt, Message: fmt.Sprintf("invalid limit: %q", s)}
	}
	if n <= 0 {
		return 0, &ProtocolError{Code: ErrCodeInvalidLimit, Message: "limit must be > 0"}
	}
	return n, nil
}

func argErr(msg string) error {
	return &ProtocolError{Code: ErrCodeInvalidArg, Message: msg}
}

func containsInt(xs []int, v int) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}
