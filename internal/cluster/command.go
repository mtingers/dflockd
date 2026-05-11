package cluster

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
)

// Kind discriminates Command's payload shape. Stable on the wire — never
// renumber existing values.
type Kind uint8

const (
	KindUnknown Kind = iota
	KindAcquire
	KindEnqueue
	KindRelease
	KindRenew
	KindEvict
	KindCleanupConn
	KindGC
	KindBarrier // no-op for ReadIndex-style fences; produces no FSM mutation
)

func (k Kind) String() string {
	switch k {
	case KindAcquire:
		return "acquire"
	case KindEnqueue:
		return "enqueue"
	case KindRelease:
		return "release"
	case KindRenew:
		return "renew"
	case KindEvict:
		return "evict"
	case KindCleanupConn:
		return "cleanup_conn"
	case KindGC:
		return "gc"
	case KindBarrier:
		return "barrier"
	default:
		return fmt.Sprintf("kind(%d)", uint8(k))
	}
}

// Command is one application-level operation submitted to the cluster.
// The flat layout (one struct, omitempty fields) keeps the JSON small
// and unambiguous to log; the codec is internal so we can swap to a
// binary encoding later without affecting any caller.
type Command struct {
	Kind          Kind   `json:"k"`
	NowNanos      int64  `json:"t"`
	Key           string `json:"key,omitempty"`
	Limit         int    `json:"limit,omitempty"`
	Ref           string `json:"ref,omitempty"`
	ConnID        uint64 `json:"cid,omitempty"`
	LeaseTTLNanos int64  `json:"ttl,omitempty"`
	SaltB64       string `json:"salt,omitempty"` // base64 of [8]byte
	Token         string `json:"tok,omitempty"`
}

// Encode serializes c as JSON. The runtime fields (NowNanos, SaltB64)
// must be set by the caller before Encode — Command is just a value;
// the leader stamps them at propose time.
func (c Command) Encode() ([]byte, error) {
	if c.Kind == KindUnknown {
		return nil, errors.New("cluster: refusing to encode KindUnknown")
	}
	return json.Marshal(c)
}

// Decode parses the JSON bytes back into a Command.
func Decode(data []byte) (Command, error) {
	var c Command
	if err := json.Unmarshal(data, &c); err != nil {
		return Command{}, fmt.Errorf("cluster: decode command: %w", err)
	}
	if c.Kind == KindUnknown {
		return Command{}, errors.New("cluster: decoded KindUnknown")
	}
	return c, nil
}

// EncodeSalt / DecodeSalt: tokens carry an 8-byte salt; we ship it as
// base64 inside the JSON envelope (raw bytes don't round-trip through
// JSON cleanly).
func EncodeSalt(salt [8]byte) string {
	return base64.StdEncoding.EncodeToString(salt[:])
}

func DecodeSalt(s string) ([8]byte, error) {
	var out [8]byte
	if s == "" {
		return out, nil
	}
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return out, fmt.Errorf("cluster: decode salt: %w", err)
	}
	if len(b) != 8 {
		return out, fmt.Errorf("cluster: salt length %d, want 8", len(b))
	}
	copy(out[:], b)
	return out, nil
}
