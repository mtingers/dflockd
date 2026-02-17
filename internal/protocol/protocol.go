package protocol

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
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
}

type Ack struct {
	Status   string // "ok", "acquired", "queued", "error", "error_max_locks", "timeout"
	Token    string
	LeaseTTL int // seconds; 0 means not set
	Extra    string
}

func ReadLine(r *bufio.Reader, timeout time.Duration, conn net.Conn) (string, error) {
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return "", &ProtocolError{Code: 10, Message: "failed to set deadline"}
	}
	line, err := r.ReadString('\n')
	if err != nil {
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			return "", &ProtocolError{Code: 10, Message: "read timeout"}
		}
		if line == "" {
			return "", &ProtocolError{Code: 11, Message: "client disconnected"}
		}
		return "", &ProtocolError{Code: 11, Message: "client disconnected"}
	}
	if len(line) > MaxLineBytes+1 { // +1 for the \n
		return "", &ProtocolError{Code: 12, Message: "line too long"}
	}
	return strings.TrimRight(line, "\r\n"), nil
}

func parseInt(s string, what string) (int, error) {
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, &ProtocolError{Code: 4, Message: fmt.Sprintf("invalid %s: %q", what, s)}
	}
	return n, nil
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
	default:
		return nil, &ProtocolError{Code: 3, Message: fmt.Sprintf("invalid cmd %q", cmd)}
	}

	if key == "" {
		return nil, &ProtocolError{Code: 5, Message: "empty key"}
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
	}

	return nil, &ProtocolError{Code: 3, Message: fmt.Sprintf("invalid cmd %q", cmd)}
}

func FormatResponse(ack *Ack, defaultLeaseTTLSec int) []byte {
	switch ack.Status {
	case "ok", "acquired":
		prefix := ack.Status
		if ack.Token != "" {
			lease := ack.LeaseTTL
			if lease == 0 {
				lease = defaultLeaseTTLSec
			}
			return []byte(fmt.Sprintf("%s %s %d\n", prefix, ack.Token, lease))
		}
		if ack.Extra != "" {
			return []byte(fmt.Sprintf("%s %s\n", prefix, ack.Extra))
		}
		return []byte(prefix + "\n")
	default:
		return []byte(ack.Status + "\n")
	}
}
