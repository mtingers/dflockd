package cluster

import (
	"testing"
)

// FuzzClusterCommandDecode runs Decode against arbitrary bytes and
// asserts (a) it never panics, (b) if the decode succeeds, re-encoding
// and re-decoding round-trips into the same Command.
//
// Seeds: a representative Command for every Kind, so the corpus
// exercises each switch arm in Validate / Encode / Decode.
func FuzzClusterCommandDecode(f *testing.F) {
	seedClusterCommands(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		cmd, err := Decode(data)
		if err != nil {
			return // expected for arbitrary inputs
		}
		again, err := cmd.Encode()
		if err != nil {
			t.Fatalf("re-encode failed for decoded command: %v", err)
		}
		cmd2, err := Decode(again)
		if err != nil {
			t.Fatalf("re-decode failed: %v", err)
		}
		if cmd != cmd2 {
			t.Fatalf("round-trip drift: %+v -> %+v", cmd, cmd2)
		}
	})
}

func seedClusterCommands(f *testing.F) {
	seeds := []Command{
		{Kind: KindAcquire, NowNanos: 1, Key: "k", Limit: 0, Ref: "r", ConnID: 7, LeaseTTLNanos: int64(1e9), SaltB64: "AAAAAAAAAAA="},
		{Kind: KindRelease, NowNanos: 2, Key: "k", Token: "deadbeefdeadbeefdeadbeefdeadbeef"},
		{Kind: KindRenew, NowNanos: 3, Key: "k", Token: "deadbeefdeadbeefdeadbeefdeadbeef", LeaseTTLNanos: int64(2e9)},
		{Kind: KindEnqueue, NowNanos: 4, Key: "k", Ref: "r", ConnID: 9, LeaseTTLNanos: int64(3e9), SaltB64: "AQAAAAAAAAA="},
		{Kind: KindEvict, NowNanos: 5},
		{Kind: KindCleanupConn, NowNanos: 6, ConnID: 11},
		{Kind: KindGC, NowNanos: 7},
	}
	for _, c := range seeds {
		b, err := c.Encode()
		if err != nil {
			f.Fatalf("seed encode failed: %v", err)
		}
		f.Add(b)
	}
}
