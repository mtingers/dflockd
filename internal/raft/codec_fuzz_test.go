package raft

import (
	"reflect"
	"testing"
)

// FuzzRaftFrameDecode runs decodeRPC against arbitrary bytes and
// asserts (a) it never panics, (b) if the decode succeeds, re-encoding
// the message under the same (kind, reqID) preserves every decoded field.
//
// Seeds: representative AppendEntries, RequestVote (incl. PreVote
// variant), and InstallSnapshot frames built via encodeRPC.
func FuzzRaftFrameDecode(f *testing.F) {
	seedRaftFrames(f)
	f.Fuzz(func(t *testing.T, body []byte) {
		// Decode must not panic. The codec is permissive about the
		// "kind" byte (the dispatcher in tcptransport.go validates that
		// separately); the codec's only contract is that any frame it
		// produces is decoded back into the same (kind, reqID, msg-type).
		kind, reqID, msg, err := decodeRPC(body)
		if err != nil {
			return
		}
		again, err := encodeRPC(kind, reqID, msg)
		if err != nil {
			t.Fatalf("re-encode failed for decoded frame: %v", err)
		}
		kind2, reqID2, msg2, err := decodeRPC(again)
		if err != nil {
			t.Fatalf("re-decode failed: %v", err)
		}
		if kind != kind2 || reqID != reqID2 {
			t.Fatalf("round-trip drift: (kind, reqID) %d/%d -> %d/%d", kind, reqID, kind2, reqID2)
		}
		if !reflect.DeepEqual(msg, msg2) {
			t.Fatalf("message round-trip drift: %#v -> %#v", msg, msg2)
		}
	})
}

// seedRaftFrames adds representative valid frames to the corpus.
func seedRaftFrames(f *testing.F) {
	for _, msg := range fuzzSeedMessages() {
		for _, kind := range []uint8{frameRequest, frameResponse} {
			body, err := encodeRPC(kind, 0xdeadbeef, msg)
			if err != nil {
				f.Fatalf("seed encode failed: %v", err)
			}
			f.Add(body)
		}
	}
}

// fuzzSeedMessages returns one of each RPC type with non-zero
// fields, so the corpus exercises every encoder path.
func fuzzSeedMessages() []Message {
	return []Message{
		&AppendEntriesReq{Term: 7, LeaderID: NodeID("n1"), PrevLogIndex: 4, PrevLogTerm: 6, LeaderCommit: 4},
		&AppendEntriesResp{Term: 7, Success: true, MatchIndex: 5},
		&RequestVoteReq{Term: 8, CandidateID: NodeID("n2"), LastLogIndex: 3, LastLogTerm: 6},
		&RequestVoteResp{Term: 8, VoteGranted: true},
		&RequestVoteReq{Term: 9, CandidateID: NodeID("n3"), LastLogIndex: 5, LastLogTerm: 8, PreVote: true},
		&RequestVoteResp{Term: 9, VoteGranted: false, PreVote: true},
		&InstallSnapshotReq{Term: 10, LeaderID: NodeID("n4"), Meta: SnapshotMeta{LastIncludedIndex: 100, LastIncludedTerm: 9}, Data: []byte{0x01, 0x02, 0x03}},
		&InstallSnapshotResp{Term: 10, LastIndex: 100},
		&TimeoutNowReq{Term: 11, LeaderID: NodeID("n5")},
		&TimeoutNowResp{Term: 11},
	}
}

func FuzzHandshakeFrameDecode(f *testing.F) {
	client, err := newClientHello([]byte(testClusterSecret), "fuzz-client")
	if err != nil {
		f.Fatal(err)
	}
	f.Add(encodeHello(client))
	f.Add(encodeAuth(client.proof))
	f.Fuzz(func(t *testing.T, body []byte) {
		_, _ = decodeHello(body)
		_, _ = decodeAuth(body)
	})
}

func FuzzSecureFrameOpen(f *testing.F) {
	secret := []byte(testClusterSecret)
	client, err := newClientHello(secret, "fuzz-client")
	if err != nil {
		f.Fatal(err)
	}
	server, err := newServerHello(secret, client, "fuzz-server")
	if err != nil {
		f.Fatal(err)
	}
	clientSession, err := newSecureSession(secret, client, server, true)
	if err != nil {
		f.Fatal(err)
	}
	seed, err := clientSession.seal([]byte("raft-rpc"))
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Fuzz(func(t *testing.T, body []byte) {
		serverSession, err := newSecureSession(secret, client, server, false)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = serverSession.open(body)
	})
}
