package raft

import (
	"fmt"
	"io"
	"net"
	"time"
)

// Wire format for the TCP Raft transport:
//
//	u32 totalLen
//	u8  frameKind (hello / auth / secure)
//	payload (kind-specific)
//
// Hello payload:    u16 idLen, id; u16 versionLen, version; nonce[32]; proof[32]
// Auth payload:     proof[32]
// Secure payload:   u64 sequence; AES-GCM ciphertext
// Request payload:  u64 reqID, u8 msgTag, u32 payloadLen, binary payload
// Response payload: u64 reqID, u8 msgTag, u32 payloadLen, binary payload
//
// Request/response bodies only appear inside a secure frame. The msgTag
// picks the concrete binary Message payload codec.

const (
	frameHello    uint8 = 0
	frameRequest  uint8 = 1
	frameResponse uint8 = 2
	frameAuth     uint8 = 3
	frameSecure   uint8 = 4

	// maxTCPFrameBytes bounds one received frame. Snapshots can be large
	// in production but dflockd's FSM is small; 64 MiB is a generous cap.
	maxTCPFrameBytes = 64 << 20

	tcpProtoVersion = "raft.v3"
)

const (
	// handshakeTimeout bounds the initial hello exchange on a fresh conn.
	handshakeTimeout = 5 * time.Second
	// connIdleTimeout recycles a connection that has produced no frame
	// for this long. Heartbeats keep busy conns well under it; a dead or
	// partitioned peer's reader goroutine unblocks within it (and the
	// conn is redialed on the next Send). Idle-by-design conns (e.g. a
	// follower's outbound conn to the leader, which it only uses when it
	// campaigns) are simply recycled — harmless.
	connIdleTimeout = 60 * time.Second
	// writeTimeout bounds a single frame write so a slow-reading peer
	// can't wedge a sender (and everyone queued behind it on writeMu).
	writeTimeout = 10 * time.Second
	// dialBackoff is the minimum spacing between dial attempts to a peer
	// that just failed to dial — avoids a tight redial loop (heartbeats
	// fire continuously) against a downed peer.
	dialBackoff = 250 * time.Millisecond
	// tcpKeepAlivePeriod enables OS-level keepalive so a peer whose host
	// vanished (no RST/FIN) is detected even sooner than connIdleTimeout.
	tcpKeepAlivePeriod = 15 * time.Second
)

const (
	tagRequestVoteReq      uint8 = 1
	tagRequestVoteResp     uint8 = 2
	tagAppendEntriesReq    uint8 = 3
	tagAppendEntriesResp   uint8 = 4
	tagInstallSnapshotReq  uint8 = 5
	tagInstallSnapshotResp uint8 = 6
	tagTimeoutNowReq       uint8 = 7
	tagTimeoutNowResp      uint8 = 8
)

// msgTag returns the wire tag for one Message; ok=false on an unknown type.
func msgTag(m Message) (uint8, bool) {
	switch m := m.(type) {
	case *RequestVoteReq:
		return tagRequestVoteReq, m != nil
	case *RequestVoteResp:
		return tagRequestVoteResp, m != nil
	case *AppendEntriesReq:
		return tagAppendEntriesReq, m != nil
	case *AppendEntriesResp:
		return tagAppendEntriesResp, m != nil
	case *InstallSnapshotReq:
		return tagInstallSnapshotReq, m != nil
	case *InstallSnapshotResp:
		return tagInstallSnapshotResp, m != nil
	case *TimeoutNowReq:
		return tagTimeoutNowReq, m != nil
	case *TimeoutNowResp:
		return tagTimeoutNowResp, m != nil
	}
	return 0, false
}

// writeFrameTo writes one frame to a net.Conn under a write deadline so
// a stalled peer can't block the writer indefinitely.
func writeFrameTo(c net.Conn, body []byte, deadline time.Duration) error {
	if deadline > 0 {
		_ = c.SetWriteDeadline(time.Now().Add(deadline))
	}
	return writeFrame(c, body)
}

// writeFrame builds and writes one frame, fronted by its total length.
func writeFrame(w io.Writer, body []byte) error {
	if len(body) > maxTCPFrameBytes {
		return fmt.Errorf("raft: frame too large (%d > %d)", len(body), maxTCPFrameBytes)
	}
	var hdr [4]byte
	be.PutUint32(hdr[:], uint32(len(body)))
	if _, err := w.Write(hdr[:]); err != nil {
		return fmt.Errorf("write frame length: %w", err)
	}
	if _, err := w.Write(body); err != nil {
		return fmt.Errorf("write frame body: %w", err)
	}
	return nil
}

// readFrame reads exactly one frame from r. If r is a net.Conn, a
// positive deadline becomes a (relative) read deadline; a non-positive
// deadline clears any deadline left over from a prior call (e.g. the
// handshake) — otherwise an absolute deadline set once would make every
// later read on the conn fail at that wall-clock time.
func readFrame(r io.Reader, deadline time.Duration) ([]byte, error) {
	if c, ok := r.(net.Conn); ok {
		if deadline > 0 {
			_ = c.SetReadDeadline(time.Now().Add(deadline))
		} else {
			_ = c.SetReadDeadline(time.Time{})
		}
	}
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n := be.Uint32(hdr[:])
	if n > maxTCPFrameBytes {
		return nil, fmt.Errorf("raft: incoming frame too large: %d", n)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// encodeHello builds the body of a challenge-response hello frame.
func encodeHello(h handshakeHello) []byte {
	body := make([]byte, 0, 1+2+len(h.id)+2+len(tcpProtoVersion)+2*handshakeValueBytes)
	body = append(body, frameHello)
	body = appendString16(body, string(h.id))
	body = appendString16(body, tcpProtoVersion)
	body = append(body, h.nonce[:]...)
	body = append(body, h.proof[:]...)
	return body
}

// decodeHello parses a hello body and rejects missing or trailing fields.
func decodeHello(body []byte) (handshakeHello, error) {
	if len(body) == 0 || body[0] != frameHello {
		return handshakeHello{}, fmt.Errorf("raft: expected hello, got kind %d", first(body))
	}
	id, rest, err := takeString16(body[1:])
	if err != nil {
		return handshakeHello{}, fmt.Errorf("hello id: %w", err)
	}
	ver, rest, err := takeString16(rest)
	if err != nil {
		return handshakeHello{}, fmt.Errorf("hello version: %w", err)
	}
	if ver != tcpProtoVersion {
		return handshakeHello{}, fmt.Errorf("raft: peer proto version %q != %q", ver, tcpProtoVersion)
	}
	if len(rest) != 2*handshakeValueBytes {
		return handshakeHello{}, fmt.Errorf("raft: hello auth data length %d, want %d", len(rest), 2*handshakeValueBytes)
	}
	h := handshakeHello{id: NodeID(id)}
	copy(h.nonce[:], rest[:handshakeValueBytes])
	copy(h.proof[:], rest[handshakeValueBytes:])
	return h, nil
}

func first(b []byte) byte {
	if len(b) == 0 {
		return 0
	}
	return b[0]
}

// encodeRPC builds the body of a request or response frame.
func encodeRPC(kind uint8, reqID uint64, m Message) ([]byte, error) {
	tag, ok := msgTag(m)
	if !ok {
		return nil, fmt.Errorf("raft: unknown message type %T", m)
	}
	payload, err := encodeRPCPayload(m)
	if err != nil {
		return nil, fmt.Errorf("raft: encode %T: %w", m, err)
	}
	if len(payload) > maxTCPFrameBytes-secureFrameOverhead-rpcHeaderBytes {
		return nil, fmt.Errorf("raft: rpc payload too large (%d bytes)", len(payload))
	}
	return assembleRPCBody(kind, reqID, tag, payload), nil
}

const rpcHeaderBytes = 1 + 8 + 1 + 4

func assembleRPCBody(kind uint8, reqID uint64, tag uint8, payload []byte) []byte {
	body := make([]byte, 0, rpcHeaderBytes+len(payload))
	body = append(body, kind)
	body = be.AppendUint64(body, reqID)
	body = append(body, tag)
	body = be.AppendUint32(body, uint32(len(payload)))
	return append(body, payload...)
}

// decodeRPC parses a request or response body.
func decodeRPC(body []byte) (kind uint8, reqID uint64, msg Message, err error) {
	if len(body) < rpcHeaderBytes {
		return 0, 0, nil, fmt.Errorf("raft: rpc body truncated (%d bytes)", len(body))
	}
	kind = body[0]
	reqID = be.Uint64(body[1:9])
	tag := body[9]
	payloadLen := be.Uint32(body[10:14])
	if rpcHeaderBytes+int(payloadLen) != len(body) {
		return 0, 0, nil, fmt.Errorf("raft: rpc payload length mismatch: hdr=%d body=%d", payloadLen, len(body)-rpcHeaderBytes)
	}
	msg, err = decodeRPCPayload(tag, body[rpcHeaderBytes:])
	if err != nil {
		return 0, 0, nil, fmt.Errorf("raft: decode msg tag %d: %w", tag, err)
	}
	return kind, reqID, msg, nil
}
