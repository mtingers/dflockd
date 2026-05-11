package raft

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"time"
)

// Wire format for the TCP Raft transport:
//
//	u32 totalLen
//	u8  frameKind (hello / request / response)
//	payload (kind-specific)
//
// Hello payload:    u16 idLen, id; u32 versionLen, version
// Request payload:  u64 reqID, u8 msgTag, u32 jsonLen, json
// Response payload: u64 reqID, u8 msgTag, u32 jsonLen, json
//
// The msgTag picks the concrete Message struct to JSON-unmarshal into.

const (
	frameHello    uint8 = 0
	frameRequest  uint8 = 1
	frameResponse uint8 = 2

	// maxTCPFrameBytes bounds one received frame. Snapshots can be large
	// in production but dflockd's FSM is small; 64 MiB is a generous cap.
	maxTCPFrameBytes = 64 << 20

	tcpProtoVersion = "raft.v1"
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
	switch m.(type) {
	case *RequestVoteReq:
		return tagRequestVoteReq, true
	case *RequestVoteResp:
		return tagRequestVoteResp, true
	case *AppendEntriesReq:
		return tagAppendEntriesReq, true
	case *AppendEntriesResp:
		return tagAppendEntriesResp, true
	case *InstallSnapshotReq:
		return tagInstallSnapshotReq, true
	case *InstallSnapshotResp:
		return tagInstallSnapshotResp, true
	case *TimeoutNowReq:
		return tagTimeoutNowReq, true
	case *TimeoutNowResp:
		return tagTimeoutNowResp, true
	}
	return 0, false
}

// newMessageOf returns a fresh pointer of the Message type the tag names.
func newMessageOf(tag uint8) (Message, bool) {
	switch tag {
	case tagRequestVoteReq:
		return &RequestVoteReq{}, true
	case tagRequestVoteResp:
		return &RequestVoteResp{}, true
	case tagAppendEntriesReq:
		return &AppendEntriesReq{}, true
	case tagAppendEntriesResp:
		return &AppendEntriesResp{}, true
	case tagInstallSnapshotReq:
		return &InstallSnapshotReq{}, true
	case tagInstallSnapshotResp:
		return &InstallSnapshotResp{}, true
	case tagTimeoutNowReq:
		return &TimeoutNowReq{}, true
	case tagTimeoutNowResp:
		return &TimeoutNowResp{}, true
	}
	return nil, false
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

// encodeHello builds the body of a hello frame.
func encodeHello(id NodeID) []byte {
	body := []byte{frameHello}
	body = appendString16(body, string(id))
	body = appendString16(body, tcpProtoVersion)
	return body
}

// decodeHello parses a hello body and returns the peer's NodeID.
func decodeHello(body []byte) (NodeID, error) {
	if len(body) == 0 || body[0] != frameHello {
		return "", fmt.Errorf("raft: expected hello, got kind %d", first(body))
	}
	id, rest, err := takeString16(body[1:])
	if err != nil {
		return "", fmt.Errorf("hello id: %w", err)
	}
	ver, _, err := takeString16(rest)
	if err != nil {
		return "", fmt.Errorf("hello version: %w", err)
	}
	if ver != tcpProtoVersion {
		return "", fmt.Errorf("raft: peer proto version %q != %q", ver, tcpProtoVersion)
	}
	return NodeID(id), nil
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
	js, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("raft: marshal: %w", err)
	}
	return assembleRPCBody(kind, reqID, tag, js), nil
}

func assembleRPCBody(kind uint8, reqID uint64, tag uint8, js []byte) []byte {
	body := make([]byte, 0, 1+8+1+4+len(js))
	body = append(body, kind)
	body = be.AppendUint64(body, reqID)
	body = append(body, tag)
	body = be.AppendUint32(body, uint32(len(js)))
	return append(body, js...)
}

// decodeRPC parses a request or response body.
func decodeRPC(body []byte) (kind uint8, reqID uint64, msg Message, err error) {
	if len(body) < 1+8+1+4 {
		return 0, 0, nil, fmt.Errorf("raft: rpc body truncated (%d bytes)", len(body))
	}
	kind = body[0]
	reqID = be.Uint64(body[1:9])
	tag := body[9]
	jsLen := be.Uint32(body[10:14])
	if 14+int(jsLen) != len(body) {
		return 0, 0, nil, fmt.Errorf("raft: rpc json length mismatch: hdr=%d body=%d", jsLen, len(body)-14)
	}
	msg, ok := newMessageOf(tag)
	if !ok {
		return 0, 0, nil, fmt.Errorf("raft: unknown msg tag %d", tag)
	}
	if err := json.Unmarshal(body[14:], msg); err != nil {
		return 0, 0, nil, fmt.Errorf("raft: unmarshal: %w", err)
	}
	return kind, reqID, msg, nil
}
