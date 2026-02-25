package protocol

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

// Pre-computed response prefixes to avoid allocations on the hot path.
var (
	respOK               = []byte("ok\n")
	respTimeout          = []byte("timeout\n")
	respError            = []byte("error\n")
	respErrorAuth        = []byte("error_auth\n")
	respErrorMaxLocks    = []byte("error_max_locks\n")
	respErrorMaxWaiters  = []byte("error_max_waiters\n")
	respErrorLimitMismatch    = []byte("error_limit_mismatch\n")
	respErrorNotEnqueued      = []byte("error_not_enqueued\n")
	respErrorAlreadyEnqueued  = []byte("error_already_enqueued\n")
	respErrorMaxKeys          = []byte("error_max_keys\n")
	respErrorListFull         = []byte("error_list_full\n")
	respErrorLeaseExpired     = []byte("error_lease_expired\n")
	respNil                   = []byte("nil\n")
	respQueued                = []byte("queued\n")

	prefixOK       = []byte("ok ")
	prefixAcquired = []byte("acquired ")
)

const MaxLineBytes = 256

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
	Delta          int64  // incr, decr, cset
	Value          string // kset, signal, lpush, rpush
	Start          int    // lrange
	Stop           int    // lrange
	Group          string // listen, unlisten: queue group name
}

type Ack struct {
	Status   string // "ok", "acquired", "queued", "timeout", "error", "error_auth", "error_max_locks", "error_max_waiters", "error_limit_mismatch", "error_not_enqueued", "error_already_enqueued"
	Token    string
	LeaseTTL int // seconds; 0 means not set
	Extra    string
}

// ReadLine reads a single newline-terminated line from the buffered reader,
// enforcing MaxLineBytes during the read to prevent memory exhaustion from
// oversized input.
func ReadLine(r *bufio.Reader, timeout time.Duration, conn net.Conn) (string, error) {
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return "", &ProtocolError{Code: 10, Message: "failed to set deadline"}
	}

	var buf [MaxLineBytes]byte
	n := 0
	for {
		b, err := r.ReadByte()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				return "", &ProtocolError{Code: 10, Message: "read timeout"}
			}
			return "", &ProtocolError{Code: 11, Message: "client disconnected"}
		}
		if b == '\n' {
			break
		}
		if n >= len(buf) {
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
		buf[n] = b
		n++
	}
	line := string(buf[:n])
	return strings.TrimRight(line, "\r"), nil
}

func parseInt(s string, what string) (int, error) {
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, &ProtocolError{Code: 4, Message: fmt.Sprintf("invalid %s: %q", what, s)}
	}
	return n, nil
}

func parseInt64(s string, what string) (int64, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, &ProtocolError{Code: 4, Message: fmt.Sprintf("invalid %s: %q", what, s)}
	}
	return n, nil
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
	arg, err := ReadLine(r, timeout, conn)
	if err != nil {
		return nil, err
	}

	switch cmd {
	case "l", "r", "n", "e", "w", "sl", "sr", "sn", "se", "sw":
	case "incr", "decr", "get", "cset":
	case "kset", "kget", "kdel":
	case "listen", "unlisten", "signal":
	case "lpush", "rpush", "lpop", "rpop", "llen", "lrange":
	case "auth":
		argStr := strings.TrimSpace(arg)
		return &Request{Cmd: "auth", Token: argStr}, nil
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
		timeout, err := parseInt(parts[0], "timeout")
		if err != nil {
			return nil, err
		}
		if timeout < 0 {
			return nil, &ProtocolError{Code: 6, Message: "timeout must be >= 0"}
		}
		leaseTTL := defaultLeaseTTL
		if len(parts) == 2 {
			lt, err := parseInt(parts[1], "lease_ttl")
			if err != nil {
				return nil, err
			}
			leaseTTL = time.Duration(lt) * time.Second
		}
		if leaseTTL <= 0 {
			return nil, &ProtocolError{Code: 9, Message: "lease_ttl must be > 0"}
		}
		return &Request{
			Cmd:            cmd,
			Key:            key,
			AcquireTimeout: time.Duration(timeout) * time.Second,
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
			lt, err := parseInt(parts[1], "lease_ttl")
			if err != nil {
				return nil, err
			}
			leaseTTL = time.Duration(lt) * time.Second
		}
		if leaseTTL <= 0 {
			return nil, &ProtocolError{Code: 9, Message: "lease_ttl must be > 0"}
		}
		return &Request{Cmd: cmd, Key: key, Token: token, LeaseTTL: leaseTTL}, nil

	case "e":
		stripped := strings.TrimSpace(arg)
		leaseTTL := defaultLeaseTTL
		if stripped != "" {
			lt, err := parseInt(stripped, "lease_ttl")
			if err != nil {
				return nil, err
			}
			if lt <= 0 {
				return nil, &ProtocolError{Code: 9, Message: "lease_ttl must be > 0"}
			}
			leaseTTL = time.Duration(lt) * time.Second
		}
		return &Request{Cmd: cmd, Key: key, LeaseTTL: leaseTTL}, nil

	case "w":
		stripped := strings.TrimSpace(arg)
		if stripped == "" {
			return nil, &ProtocolError{Code: 8, Message: "wait arg must be: <timeout>"}
		}
		timeout, err := parseInt(stripped, "timeout")
		if err != nil {
			return nil, err
		}
		if timeout < 0 {
			return nil, &ProtocolError{Code: 6, Message: "timeout must be >= 0"}
		}
		return &Request{
			Cmd:            cmd,
			Key:            key,
			AcquireTimeout: time.Duration(timeout) * time.Second,
		}, nil

	case "sl":
		// sl arg: <timeout> <limit> [<lease_ttl>]
		if len(parts) != 2 && len(parts) != 3 {
			return nil, &ProtocolError{Code: 8, Message: "sl arg must be: <timeout> <limit> [<lease_ttl>]"}
		}
		timeout, err := parseInt(parts[0], "timeout")
		if err != nil {
			return nil, err
		}
		if timeout < 0 {
			return nil, &ProtocolError{Code: 6, Message: "timeout must be >= 0"}
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
			lt, err := parseInt(parts[2], "lease_ttl")
			if err != nil {
				return nil, err
			}
			leaseTTL = time.Duration(lt) * time.Second
		}
		if leaseTTL <= 0 {
			return nil, &ProtocolError{Code: 9, Message: "lease_ttl must be > 0"}
		}
		return &Request{
			Cmd:            cmd,
			Key:            key,
			AcquireTimeout: time.Duration(timeout) * time.Second,
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
			lt, err := parseInt(parts[1], "lease_ttl")
			if err != nil {
				return nil, err
			}
			leaseTTL = time.Duration(lt) * time.Second
		}
		if leaseTTL <= 0 {
			return nil, &ProtocolError{Code: 9, Message: "lease_ttl must be > 0"}
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
			lt, err := parseInt(parts[1], "lease_ttl")
			if err != nil {
				return nil, err
			}
			leaseTTL = time.Duration(lt) * time.Second
		}
		if leaseTTL <= 0 {
			return nil, &ProtocolError{Code: 9, Message: "lease_ttl must be > 0"}
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
		timeout, err := parseInt(stripped, "timeout")
		if err != nil {
			return nil, err
		}
		if timeout < 0 {
			return nil, &ProtocolError{Code: 6, Message: "timeout must be >= 0"}
		}
		return &Request{
			Cmd:            cmd,
			Key:            key,
			AcquireTimeout: time.Duration(timeout) * time.Second,
		}, nil
	// --- Phase 1: Atomic Counters ---

	case "incr", "decr":
		stripped := strings.TrimSpace(arg)
		if stripped == "" {
			return nil, &ProtocolError{Code: 8, Message: cmd + " arg must be: <delta>"}
		}
		delta, err := parseInt64(stripped, "delta")
		if err != nil {
			return nil, err
		}
		return &Request{Cmd: cmd, Key: key, Delta: delta}, nil

	case "get":
		return &Request{Cmd: cmd, Key: key}, nil

	case "cset":
		stripped := strings.TrimSpace(arg)
		if stripped == "" {
			return nil, &ProtocolError{Code: 8, Message: "cset arg must be: <value>"}
		}
		v, err := parseInt64(stripped, "value")
		if err != nil {
			return nil, err
		}
		return &Request{Cmd: cmd, Key: key, Delta: v}, nil

	// --- Phase 2: KV with TTL ---

	case "kset":
		if arg == "" {
			return nil, &ProtocolError{Code: 8, Message: "kset arg must be: <value> [<ttl_s>]"}
		}
		return &Request{Cmd: cmd, Key: key, Value: arg}, nil

	case "kget":
		return &Request{Cmd: cmd, Key: key}, nil

	case "kdel":
		return &Request{Cmd: cmd, Key: key}, nil

	// --- Phase 3: Signaling ---

	case "listen":
		group := strings.TrimSpace(arg)
		return &Request{Cmd: cmd, Key: key, Group: group}, nil

	case "unlisten":
		group := strings.TrimSpace(arg)
		return &Request{Cmd: cmd, Key: key, Group: group}, nil

	case "signal":
		if arg == "" {
			return nil, &ProtocolError{Code: 8, Message: "signal arg must be: <payload>"}
		}
		// Reject wildcard characters in signal channel names
		if strings.Contains(key, "*") || strings.Contains(key, ">") {
			return nil, &ProtocolError{Code: 5, Message: "signal channel must not contain wildcards"}
		}
		return &Request{Cmd: cmd, Key: key, Value: arg}, nil

	// --- Phase 4: Lists/Queues ---

	case "lpush":
		if arg == "" {
			return nil, &ProtocolError{Code: 8, Message: "lpush arg must be: <value>"}
		}
		return &Request{Cmd: cmd, Key: key, Value: arg}, nil

	case "rpush":
		if arg == "" {
			return nil, &ProtocolError{Code: 8, Message: "rpush arg must be: <value>"}
		}
		return &Request{Cmd: cmd, Key: key, Value: arg}, nil

	case "lpop":
		return &Request{Cmd: cmd, Key: key}, nil

	case "rpop":
		return &Request{Cmd: cmd, Key: key}, nil

	case "llen":
		return &Request{Cmd: cmd, Key: key}, nil

	case "lrange":
		if len(parts) != 2 {
			return nil, &ProtocolError{Code: 8, Message: "lrange arg must be: <start> <stop>"}
		}
		start, err := parseInt(parts[0], "start")
		if err != nil {
			return nil, err
		}
		stop, err := parseInt(parts[1], "stop")
		if err != nil {
			return nil, err
		}
		return &Request{Cmd: cmd, Key: key, Start: start, Stop: stop}, nil
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
		case "queued":
			return respQueued
		case "error_max_keys":
			return respErrorMaxKeys
		case "error_list_full":
			return respErrorListFull
		case "error_lease_expired":
			return respErrorLeaseExpired
		case "nil":
			return respNil
		default:
			return []byte(ack.Status + "\n")
		}
	}
}
