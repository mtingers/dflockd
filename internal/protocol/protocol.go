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

// maxSecondsValue is the largest seconds value that can be multiplied by
// time.Second without overflowing int64. time.Duration tops out at
// ~9.22e9 seconds (~292 years); values beyond that would wrap to a
// negative duration and silently corrupt timeouts.
const maxSecondsValue = int64(math.MaxInt64) / int64(time.Second)

// Pre-computed response prefixes to avoid allocations on the hot path.
var (
	respOK                   = []byte("ok\n")
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
	respQueued               = []byte("queued\n")

	prefixOK       = []byte("ok ")
	prefixAcquired = []byte("acquired ")
)

// MaxLineBytes caps command, key, and most arg lines. 256 is plenty for
// the three-line request framing on every command except `signal` and
// `auth` — a keyed hex token and integer args comfortably fit.
const MaxLineBytes = 256

// MaxPayloadBytes caps the `signal` payload line and the `auth` token
// line — both of which can realistically carry larger values (JSON
// events, long secrets). 64 KiB matches the client-side response cap
// and is well under bufio.Reader's default 4 KiB buffer only for the
// payload allocation, not the buffer itself: oversized payloads still
// stream through the reader via repeated ReadByte calls.
const MaxPayloadBytes = 64 * 1024

// MaxSignalPayloadBytes returns the largest payload that can be delivered on
// channel without exceeding the 64 KiB line cap used by TCP clients for pushed
// "sig <channel> <payload>" frames.
func MaxSignalPayloadBytes(channel string) int {
	return MaxPayloadBytes - len("sig ") - len(channel) - len(" ")
}

type ProtocolError struct {
	Code    int
	Message string
}

func (e *ProtocolError) Error() string {
	return fmt.Sprintf("protocol error %d: %s", e.Code, e.Message)
}

type Request struct {
	Cmd            string
	Key            string
	AcquireTimeout time.Duration
	LeaseTTL       time.Duration
	Token          string
	Limit          int
	Value          string // signal payload
	Group          string // listen/unlisten: queue group name
}

type Ack struct {
	Status   string // "ok", "acquired", "queued", "timeout", "error", "error_auth", "error_max_locks", "error_max_waiters", "error_limit_mismatch", "error_not_enqueued", "error_already_enqueued", "error_draining"
	Token    string
	LeaseTTL int // seconds; 0 means not set
	Extra    string
}

// ReadLine reads a newline-terminated line from the buffered reader using
// the default MaxLineBytes cap. Thin shim over readLineN for command and
// key lines.
func ReadLine(r *bufio.Reader, timeout time.Duration, conn net.Conn) (string, error) {
	return readLineN(r, timeout, conn, MaxLineBytes)
}

// readLineN is the underlying implementation, parameterised by max.
// Signal payloads and auth tokens go through this with MaxPayloadBytes.
//
// Hot-path note: for the common case (max <= MaxLineBytes, which covers
// every command/key line and most arg lines), we use a stack-allocated
// backing array to avoid heap allocation on every read. Only when the
// caller asks for the larger MaxPayloadBytes cap do we spill to the heap.
// Previously a flat `make([]byte, 0, 256)` allocated on every call,
// regressing throughput at high ops/s.
func readLineN(r *bufio.Reader, timeout time.Duration, conn net.Conn, max int) (string, error) {
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return "", &ProtocolError{Code: 10, Message: "failed to set deadline"}
	}

	var stackBuf [MaxLineBytes]byte
	var buf []byte
	if max <= MaxLineBytes {
		buf = stackBuf[:0]
	} else {
		// Heap spill for the long-payload case. Seed with 256B so the
		// common sub-256 payload doesn't grow.
		buf = make([]byte, 0, 256)
	}

	for {
		b, err := r.ReadByte()
		if err != nil {
			// errors.As rather than a direct type assertion: wrapped
			// errors (fmt.Errorf "%w" chains, crypto/tls layering)
			// should still classify as timeouts. Matches the pattern
			// used by server.isTimeoutErr.
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				return "", &ProtocolError{Code: 10, Message: "read timeout"}
			}
			return "", &ProtocolError{Code: 11, Message: "client disconnected"}
		}
		if b == '\n' {
			break
		}
		if len(buf) >= max {
			// Drain the rest of the oversized line before reporting error
			// to keep the reader in a consistent state.
			for {
				c, err := r.ReadByte()
				if err != nil || c == '\n' {
					break
				}
			}
			return "", &ProtocolError{Code: 12, Message: "line too long"}
		}
		buf = append(buf, b)
	}
	return strings.TrimRight(string(buf), "\r"), nil
}

func parseInt(s string, what string) (int, error) {
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, &ProtocolError{Code: 4, Message: fmt.Sprintf("invalid %s: %q", what, s)}
	}
	return n, nil
}

// parseSecondsArg parses an integer seconds value and converts it to a
// time.Duration, rejecting values that would overflow on multiplication
// with time.Second. minSec bounds the low end (0 for timeouts, 1 for
// lease_ttl). code selects the error code so existing callers keep their
// distinct protocol codes (6 for timeouts, 9 for lease_ttl).
func parseSecondsArg(s, what string, minSec, code int) (time.Duration, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, &ProtocolError{Code: 4, Message: fmt.Sprintf("invalid %s: %q", what, s)}
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

// validateKey rejects keys that are empty or contain whitespace (which would
// cause protocol-level confusion since the wire format is line-oriented).
func validateKey(key string) error {
	if key == "" {
		return &ProtocolError{Code: 5, Message: "empty key"}
	}
	for _, c := range key {
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			return &ProtocolError{Code: 5, Message: "key contains whitespace"}
		}
	}
	return nil
}

func ReadRequest(r *bufio.Reader, timeout time.Duration, conn net.Conn, defaultLeaseTTL time.Duration) (*Request, error) {
	cmd, err := ReadLine(r, timeout, conn)
	if err != nil {
		return nil, err
	}
	key, err := ReadLine(r, timeout, conn)
	if err != nil {
		return nil, err
	}
	// The third line is the arg. For commands that carry a payload or
	// secret (signal payload, auth token), accept up to MaxPayloadBytes
	// so realistic JSON events and long tokens aren't artificially
	// rejected. All other commands keep the tight MaxLineBytes cap.
	argMax := MaxLineBytes
	switch cmd {
	case "signal", "auth":
		argMax = MaxPayloadBytes
	}
	arg, err := readLineN(r, timeout, conn, argMax)
	if err != nil {
		return nil, err
	}

	switch cmd {
	case "l", "r", "n", "e", "w", "sl", "sr", "sn", "se", "sw":
	case "listen", "unlisten", "signal":
	case "auth":
		argStr := strings.TrimSpace(arg)
		return &Request{Cmd: "auth", Token: argStr}, nil
	case "ping":
		return &Request{Cmd: "ping"}, nil
	case "stats":
		return &Request{Cmd: "stats"}, nil
	default:
		return nil, &ProtocolError{Code: 3, Message: fmt.Sprintf("invalid cmd %q", cmd)}
	}

	if err := validateKey(key); err != nil {
		return nil, err
	}

	parts := strings.Fields(arg)

	switch cmd {
	case "l":
		if len(parts) != 1 && len(parts) != 2 {
			return nil, &ProtocolError{Code: 8, Message: "lock arg must be: <timeout> [<lease_ttl>]"}
		}
		timeoutDur, err := parseSecondsArg(parts[0], "timeout", 0, 6)
		if err != nil {
			return nil, err
		}
		leaseTTL := defaultLeaseTTL
		if len(parts) == 2 {
			leaseTTL, err = parseSecondsArg(parts[1], "lease_ttl", 1, 9)
			if err != nil {
				return nil, err
			}
		}
		return &Request{
			Cmd:            cmd,
			Key:            key,
			AcquireTimeout: timeoutDur,
			LeaseTTL:       leaseTTL,
		}, nil

	case "r":
		token := strings.TrimSpace(arg)
		if token == "" {
			return nil, &ProtocolError{Code: 7, Message: "empty token"}
		}
		return &Request{Cmd: cmd, Key: key, Token: token}, nil

	case "n":
		if len(parts) != 1 && len(parts) != 2 {
			return nil, &ProtocolError{Code: 8, Message: "renew arg must be: <token> [<lease_ttl>]"}
		}
		token := strings.TrimSpace(parts[0])
		if token == "" {
			return nil, &ProtocolError{Code: 7, Message: "empty token"}
		}
		leaseTTL := defaultLeaseTTL
		if len(parts) == 2 {
			var err error
			leaseTTL, err = parseSecondsArg(parts[1], "lease_ttl", 1, 9)
			if err != nil {
				return nil, err
			}
		}
		return &Request{Cmd: cmd, Key: key, Token: token, LeaseTTL: leaseTTL}, nil

	case "e":
		stripped := strings.TrimSpace(arg)
		leaseTTL := defaultLeaseTTL
		if stripped != "" {
			var err error
			leaseTTL, err = parseSecondsArg(stripped, "lease_ttl", 1, 9)
			if err != nil {
				return nil, err
			}
		}
		return &Request{Cmd: cmd, Key: key, LeaseTTL: leaseTTL}, nil

	case "w":
		stripped := strings.TrimSpace(arg)
		if stripped == "" {
			return nil, &ProtocolError{Code: 8, Message: "wait arg must be: <timeout>"}
		}
		timeoutDur, err := parseSecondsArg(stripped, "timeout", 0, 6)
		if err != nil {
			return nil, err
		}
		return &Request{
			Cmd:            cmd,
			Key:            key,
			AcquireTimeout: timeoutDur,
		}, nil

	case "sl":
		// sl arg: <timeout> <limit> [<lease_ttl>]
		if len(parts) != 2 && len(parts) != 3 {
			return nil, &ProtocolError{Code: 8, Message: "sl arg must be: <timeout> <limit> [<lease_ttl>]"}
		}
		timeoutDur, err := parseSecondsArg(parts[0], "timeout", 0, 6)
		if err != nil {
			return nil, err
		}
		limit, err := parseInt(parts[1], "limit")
		if err != nil {
			return nil, err
		}
		if limit <= 0 {
			return nil, &ProtocolError{Code: 13, Message: "limit must be > 0"}
		}
		leaseTTL := defaultLeaseTTL
		if len(parts) == 3 {
			leaseTTL, err = parseSecondsArg(parts[2], "lease_ttl", 1, 9)
			if err != nil {
				return nil, err
			}
		}
		return &Request{
			Cmd:            cmd,
			Key:            key,
			AcquireTimeout: timeoutDur,
			LeaseTTL:       leaseTTL,
			Limit:          limit,
		}, nil

	case "sr":
		// sr arg: <token> (same as r)
		token := strings.TrimSpace(arg)
		if token == "" {
			return nil, &ProtocolError{Code: 7, Message: "empty token"}
		}
		return &Request{Cmd: cmd, Key: key, Token: token}, nil

	case "sn":
		// sn arg: <token> [<lease_ttl>] (same as n)
		if len(parts) != 1 && len(parts) != 2 {
			return nil, &ProtocolError{Code: 8, Message: "sn arg must be: <token> [<lease_ttl>]"}
		}
		token := strings.TrimSpace(parts[0])
		if token == "" {
			return nil, &ProtocolError{Code: 7, Message: "empty token"}
		}
		leaseTTL := defaultLeaseTTL
		if len(parts) == 2 {
			var err error
			leaseTTL, err = parseSecondsArg(parts[1], "lease_ttl", 1, 9)
			if err != nil {
				return nil, err
			}
		}
		return &Request{Cmd: cmd, Key: key, Token: token, LeaseTTL: leaseTTL}, nil

	case "se":
		// se arg: <limit> [<lease_ttl>]
		if len(parts) != 1 && len(parts) != 2 {
			return nil, &ProtocolError{Code: 8, Message: "se arg must be: <limit> [<lease_ttl>]"}
		}
		limit, err := parseInt(parts[0], "limit")
		if err != nil {
			return nil, err
		}
		if limit <= 0 {
			return nil, &ProtocolError{Code: 13, Message: "limit must be > 0"}
		}
		leaseTTL := defaultLeaseTTL
		if len(parts) == 2 {
			leaseTTL, err = parseSecondsArg(parts[1], "lease_ttl", 1, 9)
			if err != nil {
				return nil, err
			}
		}
		return &Request{
			Cmd:      cmd,
			Key:      key,
			LeaseTTL: leaseTTL,
			Limit:    limit,
		}, nil

	case "sw":
		// sw arg: <timeout> (same as w)
		stripped := strings.TrimSpace(arg)
		if stripped == "" {
			return nil, &ProtocolError{Code: 8, Message: "sw arg must be: <timeout>"}
		}
		timeoutDur, err := parseSecondsArg(stripped, "timeout", 0, 6)
		if err != nil {
			return nil, err
		}
		return &Request{
			Cmd:            cmd,
			Key:            key,
			AcquireTimeout: timeoutDur,
		}, nil

	case "listen":
		group := strings.TrimSpace(arg)
		return &Request{Cmd: cmd, Key: key, Group: group}, nil

	case "unlisten":
		group := strings.TrimSpace(arg)
		return &Request{Cmd: cmd, Key: key, Group: group}, nil

	case "signal":
		if strings.TrimSpace(arg) == "" {
			return nil, &ProtocolError{Code: 8, Message: "signal arg must be: <payload>"}
		}
		if strings.Contains(key, "*") || strings.Contains(key, ">") {
			return nil, &ProtocolError{Code: 5, Message: "signal channel must not contain wildcards"}
		}
		if maxPayload := MaxSignalPayloadBytes(key); maxPayload < 0 || len(arg) > maxPayload {
			if maxPayload < 0 {
				maxPayload = 0
			}
			return nil, &ProtocolError{Code: 8, Message: fmt.Sprintf("signal payload too large (max %d bytes)", maxPayload)}
		}
		return &Request{Cmd: cmd, Key: key, Value: arg}, nil
	}

	return nil, &ProtocolError{Code: 3, Message: fmt.Sprintf("invalid cmd %q", cmd)}
}

func FormatResponse(ack *Ack, defaultLeaseTTLSec int) []byte {
	switch ack.Status {
	case "ok", "acquired":
		if ack.Token != "" {
			lease := ack.LeaseTTL
			if lease == 0 {
				lease = defaultLeaseTTLSec
			}
			// Build: "<status> <token> <lease>\n" without fmt.Sprintf
			var prefix []byte
			if ack.Status == "ok" {
				prefix = prefixOK
			} else {
				prefix = prefixAcquired
			}
			buf := make([]byte, 0, len(prefix)+len(ack.Token)+1+10+1) // prefix+token+space+digits+newline
			buf = append(buf, prefix...)
			buf = append(buf, ack.Token...)
			buf = append(buf, ' ')
			buf = strconv.AppendInt(buf, int64(lease), 10)
			buf = append(buf, '\n')
			return buf
		}
		if ack.Extra != "" {
			var prefix []byte
			if ack.Status == "ok" {
				prefix = prefixOK
			} else {
				prefix = prefixAcquired
			}
			buf := make([]byte, 0, len(prefix)+len(ack.Extra)+1)
			buf = append(buf, prefix...)
			buf = append(buf, ack.Extra...)
			buf = append(buf, '\n')
			return buf
		}
		return respOK // "ok\n" — bare ok is the only case without token/extra
	default:
		// Use pre-computed slices for known statuses.
		switch ack.Status {
		case "timeout":
			return respTimeout
		case "error":
			return respError
		case "error_auth":
			return respErrorAuth
		case "error_max_locks":
			return respErrorMaxLocks
		case "error_max_waiters":
			return respErrorMaxWaiters
		case "error_limit_mismatch":
			return respErrorLimitMismatch
		case "error_not_enqueued":
			return respErrorNotEnqueued
		case "error_already_enqueued":
			return respErrorAlreadyEnqueued
		case "error_lease_expired":
			return respErrorLeaseExpired
		case "error_draining":
			return respErrorDraining
		case "queued":
			return respQueued
		default:
			return []byte(ack.Status + "\n")
		}
	}
}
