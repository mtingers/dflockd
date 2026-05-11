package raft

import (
	"encoding/binary"
	"fmt"
	"hash/crc64"
)

// crcTable is the polynomial used for all on-disk and on-wire integrity
// checks in this package.
var crcTable = crc64.MakeTable(crc64.ISO)

func crc(b []byte) uint64 { return crc64.Checksum(b, crcTable) }

// be is the byte order for every fixed-width field this package encodes.
var be = binary.BigEndian

// ---------------------------------------------------------------------------
// Entry
// ---------------------------------------------------------------------------

// maxEntryDataBytes bounds a single log entry's payload. dflockd commands
// are small (sub-KiB); the limit is generous and exists to reject a
// corrupt length read off disk or the wire before it allocates.
const maxEntryDataBytes = 16 << 20

// encodeEntry appends the wire form of e to dst:
//
//	index:u64  term:u64  type:u8  dataLen:u32  data
func encodeEntry(dst []byte, e Entry) []byte {
	dst = be.AppendUint64(dst, uint64(e.Index))
	dst = be.AppendUint64(dst, uint64(e.Term))
	dst = append(dst, byte(e.Type))
	dst = be.AppendUint32(dst, uint32(len(e.Data)))
	return append(dst, e.Data...)
}

// decodeEntry parses one entry from the front of b and returns it along
// with the number of bytes consumed.
func decodeEntry(b []byte) (Entry, int, error) {
	if len(b) < 21 {
		return Entry{}, 0, fmt.Errorf("raft: entry header truncated (%d bytes)", len(b))
	}
	e := Entry{Index: Index(be.Uint64(b[0:8])), Term: Term(be.Uint64(b[8:16])), Type: EntryType(b[16])}
	dataLen := be.Uint32(b[17:21])
	return finishDecodeEntry(b, e, int(dataLen))
}

func finishDecodeEntry(b []byte, e Entry, dataLen int) (Entry, int, error) {
	if dataLen > maxEntryDataBytes {
		return Entry{}, 0, fmt.Errorf("raft: entry data length %d exceeds max %d", dataLen, maxEntryDataBytes)
	}
	end := 21 + dataLen
	if len(b) < end {
		return Entry{}, 0, fmt.Errorf("raft: entry data truncated (have %d, need %d)", len(b), end)
	}
	if dataLen > 0 {
		e.Data = append([]byte(nil), b[21:end]...)
	}
	return e, end, nil
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

// maxConfigBytes bounds an encoded Configuration.
const maxConfigBytes = 1 << 20

func encodeConfig(dst []byte, c Configuration) []byte {
	dst = be.AppendUint32(dst, uint32(len(c.Voters)))
	for id, addr := range c.Voters {
		dst = appendString16(dst, string(id))
		dst = appendString16(dst, addr)
	}
	return dst
}

func decodeConfig(b []byte) (Configuration, error) {
	if len(b) < 4 {
		return Configuration{}, fmt.Errorf("raft: config truncated")
	}
	n := be.Uint32(b[0:4])
	return decodeConfigVoters(b[4:], int(n))
}

func decodeConfigVoters(b []byte, n int) (Configuration, error) {
	c := Configuration{Voters: make(map[NodeID]string, n)}
	for i := 0; i < n; i++ {
		id, rest, err := takeString16(b)
		if err != nil {
			return Configuration{}, err
		}
		addr, rest2, err := takeString16(rest)
		if err != nil {
			return Configuration{}, err
		}
		c.Voters[NodeID(id)] = addr
		b = rest2
	}
	return c, nil
}

// ---------------------------------------------------------------------------
// length-prefixed strings (u16 length; node ids / addrs are short)
// ---------------------------------------------------------------------------

const maxString16 = 1 << 16

func appendString16(dst []byte, s string) []byte {
	dst = be.AppendUint16(dst, uint16(len(s)))
	return append(dst, s...)
}

func takeString16(b []byte) (string, []byte, error) {
	if len(b) < 2 {
		return "", nil, fmt.Errorf("raft: string16 length truncated")
	}
	n := int(be.Uint16(b[0:2]))
	if len(b) < 2+n {
		return "", nil, fmt.Errorf("raft: string16 body truncated (have %d, need %d)", len(b)-2, n)
	}
	return string(b[2 : 2+n]), b[2+n:], nil
}
