package client

import (
	"encoding/binary"
	"encoding/hex"
	"testing"
)

func FuzzFenceFromToken(f *testing.F) {
	for _, seed := range []string{
		"00000000000000017f3c1f2b3e9a8d6e",
		"0001a3f217b3c4d8aaaaaaaaaaaaaaaa",
		"ffffffffffffffff0000000000000000",
		"",
		"tooshort",
		"0000000000000001gggggggggggggggg",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, token string) {
		got, err := FenceFromToken(token)
		if err != nil {
			return
		}
		raw, err := hex.DecodeString(token)
		if err != nil || len(raw) != 16 {
			t.Fatalf("FenceFromToken accepted invalid token %q", token)
		}
		if want := binary.BigEndian.Uint64(raw[:8]); got != want {
			t.Fatalf("FenceFromToken(%q) = %d, want %d", token, got, want)
		}
	})
}

func FuzzParseServerResponse(f *testing.F) {
	for _, seed := range []string{
		"ok",
		"ok 00000000000000017f3c1f2b3e9a8d6e 33",
		"acquired 00000000000000017f3c1f2b3e9a8d6e 33",
		"queued",
		"timeout",
		"error",
		"error_draining",
		"ok nope nope",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, resp string) {
		_ = parseReleaseResp(resp, "fuzz_release")
		_, _ = parseRenewResp(resp, "fuzz_renew")
		_, _, _, _ = parseEnqueueResp(resp, "fuzz_enqueue")
		_, _, _ = parseWaitResp(resp, "fuzz_wait")
		_, _, _ = parseAcquireGrant(resp, "fuzz_acquire")
		_, _, _ = parseGrantResponse(resp, "fuzz_grant")
	})
}
