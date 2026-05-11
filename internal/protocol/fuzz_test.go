package protocol

import (
	"bufio"
	"bytes"
	"strings"
	"testing"
	"time"
)

func FuzzParseRequest(f *testing.F) {
	for _, seed := range []struct {
		cmd, key, arg string
	}{
		{CmdPing, "_", ""},
		{CmdStats, "_", ""},
		{CmdAuth, "_", "secret"},
		{CmdAcquire, "deploy", "10 60"},
		{CmdSemAcquire, "pool", "5 3 60"},
		{CmdRelease, "deploy", "00000000000000017f3c1f2b3e9a8d6e"},
		{CmdRenew, "deploy", "00000000000000017f3c1f2b3e9a8d6e 60"},
		{CmdEnqueue, "deploy", "60"},
		{CmdSemEnqueue, "pool", "3 60"},
		{CmdWait, "deploy", "10"},
		{CmdSemWait, "pool", "10"},
		{"bad", "k", "arg"},
		{CmdAcquire, "bad key", "-1"},
	} {
		f.Add(seed.cmd, seed.key, seed.arg)
	}

	f.Fuzz(func(t *testing.T, cmd, key, arg string) {
		req, err := parseRequest(cmd, key, arg, 33*time.Second)
		if err != nil {
			return
		}
		assertParsedRequestShape(t, req, cmd, key)
	})
}

func assertParsedRequestShape(t *testing.T, req *Request, cmd, key string) {
	t.Helper()
	if req == nil {
		t.Fatal("parseRequest returned nil request with nil error")
	}
	if req.Cmd != cmd {
		t.Fatalf("req.Cmd = %q, want %q", req.Cmd, cmd)
	}
	if isKeyedFuzzCmd(cmd) && req.Key != key {
		t.Fatalf("req.Key = %q, want %q", req.Key, key)
	}
	if req.Key != "" && strings.ContainsAny(req.Key, " \t\n\r") {
		t.Fatalf("successful parse kept invalid key %q", req.Key)
	}
	if req.AcquireTimeout < 0 {
		t.Fatalf("successful parse produced negative timeout %v", req.AcquireTimeout)
	}
	if req.LeaseTTL < 0 {
		t.Fatalf("successful parse produced negative lease %v", req.LeaseTTL)
	}
	if isSemaphoreFuzzCmd(cmd) && req.Limit <= 0 {
		t.Fatalf("successful semaphore parse produced limit %d", req.Limit)
	}
}

func isKeyedFuzzCmd(cmd string) bool {
	return cmd != CmdPing && cmd != CmdStats && cmd != CmdAuth
}

func isSemaphoreFuzzCmd(cmd string) bool {
	switch cmd {
	case CmdSemAcquire, CmdSemEnqueue:
		return true
	default:
		return false
	}
}

func FuzzReadRequest(f *testing.F) {
	for _, seed := range [][]byte{
		[]byte("ping\n_\n\n"),
		[]byte("l\nkey\n10 60\n"),
		[]byte("sl\npool\n5 3 60\n"),
		[]byte("auth\n_\nsecret\n"),
		[]byte("l\r\nkey\r\n10\r\n"),
		[]byte("l\nkey\n"),
		bytes.Repeat([]byte("x"), MaxLineBytes+2),
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, frame []byte) {
		if len(frame) > 2*MaxAuthTokenBytes {
			t.Skip("frame too large for this in-process fuzz target")
		}
		r := bufio.NewReader(bytes.NewReader(frame))
		req, err := ReadRequest(r, time.Second, &fakeConn{}, 33*time.Second)
		if err != nil {
			return
		}
		if req == nil {
			t.Fatal("ReadRequest returned nil request with nil error")
		}
	})
}
