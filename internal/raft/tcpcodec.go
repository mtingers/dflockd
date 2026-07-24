package raft

import "fmt"

const (
	rpcFlagTrue    uint8 = 1 << 0
	rpcFlagPreVote uint8 = 1 << 1
)

// Payload fields follow their Message declarations. Scalars are big-endian,
// IDs are string16, byte slices and collections start with a u32 count, and
// AppendEntries reuses the durable Entry codec:
//
//	RequestVote:     term, candidate, last index, last term, flags
//	AppendEntries:   term, leader, previous index/term, commit, entries
//	InstallSnapshot: term, leader, snapshot index/term, config, FSM bytes
//	TimeoutNow:      term, leader
//
// Responses carry the same fixed-width result fields as their structs.
func encodeRPCPayload(m Message) ([]byte, error) {
	switch m := m.(type) {
	case *RequestVoteReq:
		return encodeRequestVoteReq(m)
	case *RequestVoteResp:
		return encodeRequestVoteResp(m), nil
	case *AppendEntriesReq:
		return encodeAppendEntriesReq(m)
	case *AppendEntriesResp:
		return encodeAppendEntriesResp(m), nil
	default:
		return encodeRPCControlPayload(m)
	}
}

func encodeRPCControlPayload(m Message) ([]byte, error) {
	switch m := m.(type) {
	case *InstallSnapshotReq:
		return encodeInstallSnapshotReq(m)
	case *InstallSnapshotResp:
		return encodeInstallSnapshotResp(m), nil
	case *TimeoutNowReq:
		return encodeTimeoutNowReq(m)
	case *TimeoutNowResp:
		return encodeTimeoutNowResp(m), nil
	default:
		return nil, fmt.Errorf("unknown message type %T", m)
	}
}

func encodeRequestVoteReq(m *RequestVoteReq) ([]byte, error) {
	dst := make([]byte, 0, 8+2+len(m.CandidateID)+8+8+1)
	dst = be.AppendUint64(dst, uint64(m.Term))
	var err error
	if dst, err = appendRPCNodeID(dst, m.CandidateID); err != nil {
		return nil, err
	}
	dst = be.AppendUint64(dst, uint64(m.LastLogIndex))
	dst = be.AppendUint64(dst, uint64(m.LastLogTerm))
	return append(dst, boolFlag(m.PreVote)), nil
}

func encodeRequestVoteResp(m *RequestVoteResp) []byte {
	flags := boolFlag(m.VoteGranted)
	if m.PreVote {
		flags |= rpcFlagPreVote
	}
	return append(be.AppendUint64(nil, uint64(m.Term)), flags)
}

func encodeAppendEntriesReq(m *AppendEntriesReq) ([]byte, error) {
	size := 8 + 2 + len(m.LeaderID) + 8 + 8 + 8 + 4
	for _, entry := range m.Entries {
		if len(entry.Data) > maxEntryDataBytes {
			return nil, fmt.Errorf("entry data length %d exceeds max %d", len(entry.Data), maxEntryDataBytes)
		}
		size += 21 + len(entry.Data)
		if size > maxTCPFrameBytes-rpcHeaderBytes {
			return nil, fmt.Errorf("payload length exceeds max %d", maxTCPFrameBytes-rpcHeaderBytes)
		}
	}
	dst := make([]byte, 0, size)
	dst = be.AppendUint64(dst, uint64(m.Term))
	var err error
	if dst, err = appendRPCNodeID(dst, m.LeaderID); err != nil {
		return nil, err
	}
	dst = be.AppendUint64(dst, uint64(m.PrevLogIndex))
	dst = be.AppendUint64(dst, uint64(m.PrevLogTerm))
	dst = be.AppendUint64(dst, uint64(m.LeaderCommit))
	dst = be.AppendUint32(dst, uint32(len(m.Entries)))
	for _, entry := range m.Entries {
		dst = encodeEntry(dst, entry)
	}
	return dst, nil
}

func encodeAppendEntriesResp(m *AppendEntriesResp) []byte {
	dst := be.AppendUint64(nil, uint64(m.Term))
	dst = append(dst, boolFlag(m.Success))
	dst = be.AppendUint64(dst, uint64(m.MatchIndex))
	dst = be.AppendUint64(dst, uint64(m.ConflictIndex))
	return be.AppendUint64(dst, uint64(m.ConflictTerm))
}

func encodeInstallSnapshotReq(m *InstallSnapshotReq) ([]byte, error) {
	if len(m.LeaderID) >= maxString16 {
		return nil, fmt.Errorf("node ID length %d exceeds max %d", len(m.LeaderID), maxString16-1)
	}
	cfg, err := encodeRPCConfig(m.Meta.Configuration)
	if err != nil {
		return nil, err
	}
	size := 8 + 2 + len(m.LeaderID) + 8 + 8 + 4 + len(cfg) + 4 + len(m.Data)
	if size > maxTCPFrameBytes-rpcHeaderBytes {
		return nil, fmt.Errorf("payload length %d exceeds max %d", size, maxTCPFrameBytes-rpcHeaderBytes)
	}
	dst := make([]byte, 0, size)
	dst = be.AppendUint64(dst, uint64(m.Term))
	dst = appendString16(dst, string(m.LeaderID))
	dst = be.AppendUint64(dst, uint64(m.Meta.LastIncludedIndex))
	dst = be.AppendUint64(dst, uint64(m.Meta.LastIncludedTerm))
	dst = be.AppendUint32(dst, uint32(len(cfg)))
	dst = append(dst, cfg...)
	dst = be.AppendUint32(dst, uint32(len(m.Data)))
	return append(dst, m.Data...), nil
}

func encodeInstallSnapshotResp(m *InstallSnapshotResp) []byte {
	dst := be.AppendUint64(nil, uint64(m.Term))
	return be.AppendUint64(dst, uint64(m.LastIndex))
}

func encodeTimeoutNowReq(m *TimeoutNowReq) ([]byte, error) {
	dst := be.AppendUint64(nil, uint64(m.Term))
	return appendRPCNodeID(dst, m.LeaderID)
}

func encodeTimeoutNowResp(m *TimeoutNowResp) []byte {
	return be.AppendUint64(nil, uint64(m.Term))
}

func appendRPCNodeID(dst []byte, id NodeID) ([]byte, error) {
	if len(id) >= maxString16 {
		return nil, fmt.Errorf("node ID length %d exceeds max %d", len(id), maxString16-1)
	}
	return appendString16(dst, string(id)), nil
}

func encodeRPCConfig(c Configuration) ([]byte, error) {
	for id, addr := range c.Voters {
		if len(id) >= maxString16 {
			return nil, fmt.Errorf("voter ID length %d exceeds max %d", len(id), maxString16-1)
		}
		if len(addr) >= maxString16 {
			return nil, fmt.Errorf("voter address length %d exceeds max %d", len(addr), maxString16-1)
		}
	}
	cfg := encodeConfig(nil, c)
	if len(cfg) > maxConfigBytes {
		return nil, fmt.Errorf("config length %d exceeds max %d", len(cfg), maxConfigBytes)
	}
	return cfg, nil
}

func boolFlag(v bool) uint8 {
	if v {
		return rpcFlagTrue
	}
	return 0
}

func decodeRPCPayload(tag uint8, payload []byte) (Message, error) {
	d := rpcDecoder{b: payload}
	msg := d.message(tag)
	if d.err != nil {
		return nil, d.err
	}
	if len(d.b) != 0 {
		return nil, fmt.Errorf("%d trailing payload bytes", len(d.b))
	}
	return msg, nil
}

type rpcDecoder struct {
	b   []byte
	err error
}

func (d *rpcDecoder) message(tag uint8) Message {
	switch tag {
	case tagRequestVoteReq:
		return d.requestVoteReq()
	case tagRequestVoteResp:
		return d.requestVoteResp()
	case tagAppendEntriesReq:
		return d.appendEntriesReq()
	case tagAppendEntriesResp:
		return d.appendEntriesResp()
	default:
		return d.controlMessage(tag)
	}
}

func (d *rpcDecoder) controlMessage(tag uint8) Message {
	switch tag {
	case tagInstallSnapshotReq:
		return d.installSnapshotReq()
	case tagInstallSnapshotResp:
		return d.installSnapshotResp()
	case tagTimeoutNowReq:
		return d.timeoutNowReq()
	case tagTimeoutNowResp:
		return d.timeoutNowResp()
	default:
		d.failf("unknown message tag %d", tag)
		return nil
	}
}

func (d *rpcDecoder) failf(format string, args ...any) {
	if d.err == nil {
		d.err = fmt.Errorf(format, args...)
	}
}

func (d *rpcDecoder) take(n int) []byte {
	if d.err != nil {
		return nil
	}
	if n < 0 || len(d.b) < n {
		d.failf("payload truncated (have %d, need %d)", len(d.b), n)
		return nil
	}
	out := d.b[:n]
	d.b = d.b[n:]
	return out
}

func (d *rpcDecoder) uint8() uint8 {
	b := d.take(1)
	if d.err != nil {
		return 0
	}
	return b[0]
}

func (d *rpcDecoder) uint32() uint32 {
	b := d.take(4)
	if d.err != nil {
		return 0
	}
	return be.Uint32(b)
}

func (d *rpcDecoder) uint64() uint64 {
	b := d.take(8)
	if d.err != nil {
		return 0
	}
	return be.Uint64(b)
}

func (d *rpcDecoder) nodeID() NodeID {
	if d.err != nil {
		return ""
	}
	s, rest, err := takeString16(d.b)
	if err != nil {
		d.err = err
		return ""
	}
	d.b = rest
	return NodeID(s)
}

func (d *rpcDecoder) flags(allowed uint8) uint8 {
	flags := d.uint8()
	if flags & ^allowed != 0 {
		d.failf("invalid flags 0x%x", flags)
	}
	return flags
}

func (d *rpcDecoder) requestVoteReq() Message {
	term := d.uint64()
	id := d.nodeID()
	index := d.uint64()
	logTerm := d.uint64()
	flags := d.flags(rpcFlagTrue)
	return &RequestVoteReq{
		Term: Term(term), CandidateID: id, LastLogIndex: Index(index),
		LastLogTerm: Term(logTerm), PreVote: flags != 0,
	}
}

func (d *rpcDecoder) requestVoteResp() Message {
	term := d.uint64()
	flags := d.flags(rpcFlagTrue | rpcFlagPreVote)
	return &RequestVoteResp{
		Term: Term(term), VoteGranted: flags&rpcFlagTrue != 0, PreVote: flags&rpcFlagPreVote != 0,
	}
}

func (d *rpcDecoder) appendEntriesReq() Message {
	term := d.uint64()
	id := d.nodeID()
	prevIndex := d.uint64()
	prevTerm := d.uint64()
	commit := d.uint64()
	entries := d.entries()
	return &AppendEntriesReq{
		Term: Term(term), LeaderID: id, PrevLogIndex: Index(prevIndex),
		PrevLogTerm: Term(prevTerm), Entries: entries, LeaderCommit: Index(commit),
	}
}

func (d *rpcDecoder) entries() []Entry {
	count := uint64(d.uint32())
	if d.err != nil {
		return nil
	}
	if count > uint64(len(d.b)/21) {
		d.failf("entry count %d implausible for %d bytes", count, len(d.b))
		return nil
	}
	entries := make([]Entry, 0, int(count))
	for i := 0; i < int(count); i++ {
		entry, n, err := decodeEntry(d.b)
		if err != nil {
			d.err = fmt.Errorf("entry %d: %w", i, err)
			return nil
		}
		d.b = d.b[n:]
		entries = append(entries, entry)
	}
	return entries
}

func (d *rpcDecoder) appendEntriesResp() Message {
	term := d.uint64()
	flags := d.flags(rpcFlagTrue)
	match := d.uint64()
	conflictIndex := d.uint64()
	conflictTerm := d.uint64()
	return &AppendEntriesResp{
		Term: Term(term), Success: flags != 0, MatchIndex: Index(match),
		ConflictIndex: Index(conflictIndex), ConflictTerm: Term(conflictTerm),
	}
}

func (d *rpcDecoder) installSnapshotReq() Message {
	term := d.uint64()
	id := d.nodeID()
	index := d.uint64()
	snapshotTerm := d.uint64()
	cfg := d.config()
	data := d.bytes32()
	return &InstallSnapshotReq{
		Term: Term(term), LeaderID: id,
		Meta: SnapshotMeta{
			LastIncludedIndex: Index(index), LastIncludedTerm: Term(snapshotTerm), Configuration: cfg,
		},
		Data: data,
	}
}

func (d *rpcDecoder) installSnapshotResp() Message {
	return &InstallSnapshotResp{Term: Term(d.uint64()), LastIndex: Index(d.uint64())}
}

func (d *rpcDecoder) timeoutNowReq() Message {
	return &TimeoutNowReq{Term: Term(d.uint64()), LeaderID: d.nodeID()}
}

func (d *rpcDecoder) timeoutNowResp() Message {
	return &TimeoutNowResp{Term: Term(d.uint64())}
}

func (d *rpcDecoder) config() Configuration {
	n := d.uint32()
	if d.err != nil {
		return Configuration{}
	}
	if n > maxConfigBytes {
		d.failf("config length %d exceeds max %d", n, maxConfigBytes)
		return Configuration{}
	}
	raw := d.take(int(n))
	if d.err != nil {
		return Configuration{}
	}
	cfg, err := decodeConfig(raw)
	if err != nil {
		d.err = err
	}
	return cfg
}

func (d *rpcDecoder) bytes32() []byte {
	n := uint64(d.uint32())
	if d.err != nil {
		return nil
	}
	if n > uint64(len(d.b)) {
		d.failf("payload truncated (have %d, need %d)", len(d.b), n)
		return nil
	}
	raw := d.take(int(n))
	return append([]byte(nil), raw...)
}
