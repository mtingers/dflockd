// Package replication implements peer-to-peer state replication between a
// primary and a secondary dflockd instance.
//
// Topology and safety model
//
// The cluster is two nodes plus an optional witness:
//
//   - Primary: serves all client traffic. Every state mutation
//     (Acquire/Release/Renew/Enqueue-fast-path/Wait-grant/lease-expiry)
//     is sent to the secondary as an Op frame and acknowledged before
//     the primary acks the client.
//   - Secondary: refuses all client mutation traffic. Applies Op frames
//     as they arrive. Read-only ops (ping, stats) may still be served.
//   - Witness (phase 3, stubbed in v1): a tiny voter consulted only at
//     failover time. With a witness present, the secondary may
//     auto-promote when both it and the witness agree the primary is
//     gone. Without a witness, primary failure requires manual operator
//     intervention.
//
// Safety: the secondary never accepts client mutations, period. Only
// the primary can advance state. When the primary self-promotes past
// max-pause-ms (because the peer is unreachable), it bumps its epoch
// and continues solo. The secondary, when it reconnects, observes the
// new epoch and re-syncs as a follower from the primary's snapshot.
// Split-brain is impossible because the secondary's role itself
// forbids serving — there is no path by which two nodes are
// authoritative simultaneously.
//
// Wire format
//
// Each frame is a 4-byte big-endian length prefix followed by a JSON
// payload. Framing is deliberately simple — the volume of replication
// traffic is bounded by client mutation rate, not by raw bandwidth, so
// we trade a few CPU cycles on JSON encoding for protocol clarity.
package replication

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// MaxFrameBytes is the largest single replication frame the wire layer
// will accept. Snapshots can be large (one frame per resource state),
// so 4 MiB is a comfortable cap.
const MaxFrameBytes = 4 * 1024 * 1024

// FrameType discriminates the JSON payload schemas below.
type FrameType string

const (
	FrameHello        FrameType = "hello"         // initial handshake
	FrameHeartbeat    FrameType = "heartbeat"     // keepalive
	FrameOp           FrameType = "op"            // state mutation to apply
	FrameOpAck        FrameType = "op_ack"        // ack a previously-sent Op by Seq
	FrameSnapshotReq  FrameType = "snap_req"      // secondary asks for full state
	FrameSnapshotPart FrameType = "snap_part"     // one chunk of the snapshot
	FrameSnapshotEnd  FrameType = "snap_end"      // snapshot done; resume live ops
)

// Role identifies the role advertised in the Hello frame. Witness is
// reserved for phase 3 and currently rejected in the handshake.
type Role string

const (
	RolePrimary   Role = "primary"
	RoleSecondary Role = "secondary"

	// RoleWitness is reserved for phase 3 (auto-failover with a tiny
	// arbiter daemon participating in 2-of-3 quorum). The handshake
	// rejects this role today — operators wanting auto-failover
	// should use either an external orchestration layer (Kubernetes
	// liveness probes that promote on primary loss) or wait for
	// phase 3.
	RoleWitness Role = "witness"
)

// OpKind is the discriminator on Op.Kind. Each kind names a state
// mutation the secondary should apply to its lock manager.
type OpKind string

const (
	OpHolderAdd       OpKind = "holder_add"        // new lock or sem holder
	OpHolderRemove    OpKind = "holder_remove"     // released, expired, or evicted
	OpHolderRenew     OpKind = "holder_renew"      // lease extended
	OpEnqueuedAdd     OpKind = "enqueued_add"      // two-phase enqueued state recorded
	OpEnqueuedRemove  OpKind = "enqueued_remove"   // two-phase enqueued state cleared
)

// Frame is the union envelope sent on the peer link. Exactly one
// payload field is non-nil for any given Type.
type Frame struct {
	Type FrameType `json:"type"`

	Hello        *Hello         `json:"hello,omitempty"`
	Heartbeat    *Heartbeat     `json:"heartbeat,omitempty"`
	Op           *Op            `json:"op,omitempty"`
	OpAck        *OpAck         `json:"op_ack,omitempty"`
	SnapshotReq  *SnapshotReq   `json:"snap_req,omitempty"`
	SnapshotPart *SnapshotPart  `json:"snap_part,omitempty"`
	SnapshotEnd  *SnapshotEnd   `json:"snap_end,omitempty"`
}

// Hello is the first frame sent on a new peer connection. The
// receiver validates role compatibility (a Primary must talk to a
// Secondary and vice versa) and epoch consistency.
type Hello struct {
	Role        Role   `json:"role"`
	Epoch       uint64 `json:"epoch"`
	ProtoVer    uint32 `json:"proto_ver"`
	NodeID      string `json:"node_id"`     // stable identifier for logs
	StartedUnix int64  `json:"started_unix"`
}

// Heartbeat is a periodic keepalive from each side. Either side
// transitioning to Paused on its absence is what triggers
// self-promotion (primary) or failure mode (secondary).
type Heartbeat struct {
	Epoch uint64 `json:"epoch"`
	Now   int64  `json:"now_unix_ns"`
}

// Op is a single state mutation captured on the primary, sent to the
// secondary, and applied verbatim. Seq is monotonic per primary epoch
// and is echoed in OpAck. Token, ConnID, LeaseExpiresUnixNS together
// fully identify the resulting state.
type Op struct {
	Seq                uint64 `json:"seq"`
	Epoch              uint64 `json:"epoch"`
	Kind               OpKind `json:"kind"`
	Key                string `json:"key"`             // includes "lock:"/"sem:" prefix
	Token              string `json:"token,omitempty"`
	ConnID             uint64 `json:"conn_id,omitempty"`
	Limit              int    `json:"limit,omitempty"`
	LeaseExpiresUnixNS int64  `json:"lease_expires_unix_ns,omitempty"`
	LeaseTTLNS         int64  `json:"lease_ttl_ns,omitempty"`
}

// OpAck confirms a prior Op was applied. The primary only releases
// the corresponding client response after seeing the ack.
type OpAck struct {
	Seq   uint64 `json:"seq"`
	Epoch uint64 `json:"epoch"`
	Err   string `json:"err,omitempty"` // populated if apply failed (rare)
}

// SnapshotReq asks the primary to send a full state dump. Sent by the
// secondary on reconnect when it is unsure of its position.
type SnapshotReq struct {
	Epoch uint64 `json:"epoch"`
}

// SnapshotPart is one chunk of state during catch-up. We currently
// emit one Part per resource for simplicity; a future optimisation
// can batch many resources per part.
type SnapshotPart struct {
	Epoch    uint64    `json:"epoch"`
	Key      string    `json:"key"`
	Limit    int       `json:"limit"`
	Holders  []Holder  `json:"holders"`
	Enqueued []Enqueued `json:"enqueued"`
}

// Holder is a serialisable holder record for snapshot transfer.
type Holder struct {
	Token              string `json:"token"`
	ConnID             uint64 `json:"conn_id"`
	LeaseExpiresUnixNS int64  `json:"lease_expires_unix_ns"`
}

// Enqueued is a serialisable two-phase enqueued state for snapshot transfer.
// Only the post-grant form ("token already issued") is replicated; pure
// waiters (no token yet) are primary-local because their queue position
// cannot be transferred safely across a failover.
type Enqueued struct {
	ConnID             uint64 `json:"conn_id"`
	Token              string `json:"token,omitempty"`
	LeaseTTLNS         int64  `json:"lease_ttl_ns"`
}

// SnapshotEnd terminates a snapshot. After receiving it, the
// secondary resumes applying live Op frames whose Seq is greater than
// the highest Seq seen in the snapshot.
type SnapshotEnd struct {
	Epoch  uint64 `json:"epoch"`
	LastSeq uint64 `json:"last_seq"`
}

// ---------------------------------------------------------------------------
// Encode / Decode
// ---------------------------------------------------------------------------

// WriteFrame writes a single length-prefixed frame to w.
func WriteFrame(w io.Writer, f *Frame) error {
	body, err := json.Marshal(f)
	if err != nil {
		return fmt.Errorf("marshal frame: %w", err)
	}
	if len(body) > MaxFrameBytes {
		return fmt.Errorf("frame too large: %d > %d", len(body), MaxFrameBytes)
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(body)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.Write(body); err != nil {
		return err
	}
	return nil
}

// ReadFrame reads exactly one frame from r. Returns io.EOF cleanly if
// the connection closes between frames.
func ReadFrame(r io.Reader) (*Frame, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n == 0 || n > MaxFrameBytes {
		return nil, fmt.Errorf("invalid frame length %d", n)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, fmt.Errorf("read frame body: %w", err)
	}
	var f Frame
	if err := json.Unmarshal(body, &f); err != nil {
		return nil, fmt.Errorf("unmarshal frame: %w", err)
	}
	return &f, nil
}

// ---------------------------------------------------------------------------
// Tunables (shared with peer.go and replicator.go)
// ---------------------------------------------------------------------------

// HeartbeatInterval is how often each side emits a Heartbeat frame
// while connected. The peer treats absence-for-2x-this as
// connection-lost and starts the pause timer.
const HeartbeatInterval = 500 * time.Millisecond

// DefaultMaxPause is the default --max-pause-ms when not set.
// Conservative: 5 seconds is well past any reasonable network blip
// and short enough that recovery feels prompt.
const DefaultMaxPause = 5 * time.Second

// ProtoVersion is bumped on incompatible wire-format changes. The
// handshake refuses peers whose ProtoVer disagrees.
const ProtoVersion uint32 = 1
