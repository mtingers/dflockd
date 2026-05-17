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
	KindBarrier      // no-op for ReadIndex-style fences; produces no FSM mutation
	KindEvictExpired // sweep all holders past their lease deadline (leader-driven)
	// Append new kinds here only — values are stable on the wire.
)

var kindNames = [...]string{
	KindAcquire:      "acquire",
	KindEnqueue:      "enqueue",
	KindRelease:      "release",
	KindRenew:        "renew",
	KindEvict:        "evict",
	KindCleanupConn:  "cleanup_conn",
	KindGC:           "gc",
	KindBarrier:      "barrier",
	KindEvictExpired: "evict_expired",
}

// String returns the snake_case name of the kind, or "kind(N)" for an
// unknown value. Used for logs, error messages, and metric labels.
func (k Kind) String() string {
	if int(k) < len(kindNames) && kindNames[k] != "" {
		return kindNames[k]
	}
	return fmt.Sprintf("kind(%d)", uint8(k))
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

// Decode parses the JSON bytes back into a Command and validates it.
func Decode(data []byte) (Command, error) {
	var c Command
	if err := json.Unmarshal(data, &c); err != nil {
		return Command{}, fmt.Errorf("cluster: decode command: %w", err)
	}
	if c.Kind == KindUnknown {
		return Command{}, errors.New("cluster: decoded KindUnknown")
	}
	if err := c.Validate(); err != nil {
		return Command{}, err
	}
	return c, nil
}

const (
	// Defensive bounds on a decoded command. The protocol layer already
	// enforces tighter limits on anything that becomes a Command in normal
	// operation; these guard against a hand-crafted / corrupt log entry
	// producing weird FSM state (or a value that can't be re-encoded into
	// a snapshot's fixed-width fields).
	maxCommandKeyBytes = 512  // a protocol key (≤256) plus its "lock:"/"sem:" prefix, with headroom
	maxCommandRefBytes = 4096 // node-id-prefixed connection ref
	maxCommandLimit    = 1 << 20
)

// Validate rejects a structurally-impossible command. It is purely a
// function of the command's fields, so every replica reaches the same
// verdict.
func (c Command) Validate() error {
	if c.LeaseTTLNanos < 0 {
		return fmt.Errorf("cluster: negative lease TTL %d", c.LeaseTTLNanos)
	}
	if len(c.Key) > maxCommandKeyBytes {
		return fmt.Errorf("cluster: key too long (%d bytes)", len(c.Key))
	}
	if len(c.Ref) > maxCommandRefBytes {
		return fmt.Errorf("cluster: ref too long (%d bytes)", len(c.Ref))
	}
	if c.Limit < 0 || c.Limit > maxCommandLimit {
		return fmt.Errorf("cluster: implausible limit %d", c.Limit)
	}
	switch c.Kind {
	case KindAcquire, KindEnqueue, KindRelease, KindRenew, KindEvict:
		if c.Key == "" {
			return fmt.Errorf("cluster: %s command with empty key", c.Kind)
		}
	}
	return nil
}

// EncodeSalt / DecodeSalt: tokens carry an 8-byte salt; we ship it as
// base64 inside the JSON envelope (raw bytes don't round-trip through
// JSON cleanly).
func EncodeSalt(salt [8]byte) string {
	return base64.StdEncoding.EncodeToString(salt[:])
}

// DecodeSalt reverses EncodeSalt. The empty string decodes to the zero
// salt (a Command with no salt-bearing semantics — e.g. KindBarrier).
// Anything else must base64-decode to exactly 8 bytes.
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
