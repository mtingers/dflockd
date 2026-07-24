package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
)

func TestRPCBinaryEncodingShrinksEntryData(t *testing.T) {
	data := make([]byte, 16<<10)
	for i := range data {
		data[i] = byte(i)
	}
	msg := &AppendEntriesReq{
		Term: 4, LeaderID: "leader", PrevLogIndex: 9, PrevLogTerm: 3,
		Entries:      []Entry{{Index: 10, Term: 4, Type: EntryNormal, Data: data}},
		LeaderCommit: 9,
	}
	body, err := encodeRPC(frameRequest, 1, msg)
	if err != nil {
		t.Fatalf("encodeRPC: %v", err)
	}
	legacyJSON, err := legacyEncodeRPCJSON(msg)
	if err != nil {
		t.Fatalf("legacyEncodeRPCJSON: %v", err)
	}
	if len(body)*5 >= len(legacyJSON)*4 {
		t.Fatalf("binary body = %d bytes, legacy JSON = %d; want at least 20%% smaller", len(body), len(legacyJSON))
	}
}

func TestRPCBinaryDecoderRejectsMalformedPayloads(t *testing.T) {
	validResp := mustEncodeRPC(t, &RequestVoteResp{Term: 2, VoteGranted: true})
	invalidFlags := append([]byte(nil), validResp...)
	invalidFlags[len(invalidFlags)-1] = 0x80

	trailing := append(append([]byte(nil), validResp...), 0)
	be.PutUint32(trailing[10:14], be.Uint32(trailing[10:14])+1)

	heartbeat := mustEncodeRPC(t, &AppendEntriesReq{Term: 2, LeaderID: "L"})
	invalidCount := append([]byte(nil), heartbeat...)
	countOffset := rpcHeaderBytes + 8 + 2 + len("L") + 8 + 8 + 8
	be.PutUint32(invalidCount[countOffset:countOffset+4], 1)

	unknownTag := append([]byte(nil), validResp...)
	unknownTag[9] = 0xff

	lengthMismatch := append([]byte(nil), validResp...)
	be.PutUint32(lengthMismatch[10:14], be.Uint32(lengthMismatch[10:14])+1)

	for name, body := range map[string][]byte{
		"truncated header": validResp[:rpcHeaderBytes-1],
		"invalid flags":    invalidFlags,
		"trailing bytes":   trailing,
		"invalid count":    invalidCount,
		"unknown tag":      unknownTag,
		"length mismatch":  lengthMismatch,
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, _, err := decodeRPC(body); err == nil {
				t.Fatal("decodeRPC accepted malformed body")
			}
		})
	}
}

func TestRPCBinaryEncoderRejectsOversizedNodeID(t *testing.T) {
	id := NodeID(string(make([]byte, maxString16)))
	if _, err := encodeRPC(frameRequest, 1, &TimeoutNowReq{Term: 1, LeaderID: id}); err == nil {
		t.Fatal("encodeRPC accepted oversized node ID")
	}
	var nilReq *TimeoutNowReq
	if _, err := encodeRPC(frameRequest, 1, nilReq); err == nil {
		t.Fatal("encodeRPC accepted typed nil message")
	}
}

func TestConfigurationCodecCanonicalAndStrict(t *testing.T) {
	first := Configuration{Voters: map[NodeID]string{"b": "host:2", "a": "host:1"}}
	second := Configuration{Voters: map[NodeID]string{"a": "host:1", "b": "host:2"}}
	a := encodeConfig(nil, first)
	b := encodeConfig(nil, second)
	if !bytes.Equal(a, b) {
		t.Fatalf("configuration encoding is not canonical:\n%x\n%x", a, b)
	}
	if _, err := decodeConfig(append(a, 0)); err == nil {
		t.Fatal("decodeConfig accepted trailing data")
	}

	duplicate := be.AppendUint32(nil, 2)
	duplicate = appendString16(duplicate, "a")
	duplicate = appendString16(duplicate, "host:1")
	duplicate = appendString16(duplicate, "a")
	duplicate = appendString16(duplicate, "host:2")
	if _, err := decodeConfig(duplicate); err == nil {
		t.Fatal("decodeConfig accepted duplicate voter")
	}
}

func TestTCPHelloRejectsLegacyJSONProtocol(t *testing.T) {
	body := []byte{frameHello}
	body = appendString16(body, "peer")
	body = appendString16(body, "raft.v1")
	if _, err := decodeHello(body); err == nil {
		t.Fatal("decodeHello accepted legacy JSON protocol")
	}
}

func mustEncodeRPC(t *testing.T, msg Message) []byte {
	t.Helper()
	body, err := encodeRPC(frameRequest, 1, msg)
	if err != nil {
		t.Fatalf("encodeRPC(%T): %v", msg, err)
	}
	return body
}

func legacyEncodeRPCJSON(msg Message) ([]byte, error) {
	tag, ok := msgTag(msg)
	if !ok {
		return nil, fmt.Errorf("unknown message type %T", msg)
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return assembleRPCBody(frameRequest, 1, tag, payload), nil
}

func BenchmarkRPCCodec(b *testing.B) {
	data := make([]byte, 16<<10)
	msg := &AppendEntriesReq{
		Term: 4, LeaderID: "leader", PrevLogIndex: 9, PrevLogTerm: 3,
		Entries:      []Entry{{Index: 10, Term: 4, Type: EntryNormal, Data: data}},
		LeaderCommit: 9,
	}
	body, err := encodeRPC(frameRequest, 1, msg)
	if err != nil {
		b.Fatal(err)
	}

	b.Run("binary/encode", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := encodeRPC(frameRequest, 1, msg); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("binary/decode", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, _, _, err := decodeRPC(body); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("json/encode", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := legacyEncodeRPCJSON(msg); err != nil {
				b.Fatal(err)
			}
		}
	})
	legacyJSON, err := legacyEncodeRPCJSON(msg)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("json/decode", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var decoded AppendEntriesReq
			if err := json.Unmarshal(legacyJSON[rpcHeaderBytes:], &decoded); err != nil {
				b.Fatal(err)
			}
		}
	})
}
