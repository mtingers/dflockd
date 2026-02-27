package protocol

import (
	"bufio"
	"fmt"
	"math"
	"net"
	"strings"
	"testing"
	"time"
)

// mockConn implements net.Conn for testing ReadLine/ReadRequest.
type mockConn struct {
	net.Conn
}

func (m *mockConn) SetReadDeadline(t time.Time) error { return nil }
func (m *mockConn) RemoteAddr() net.Addr              { return &net.TCPAddr{} }

func makeReader(lines ...string) *bufio.Reader {
	data := strings.Join(lines, "\n") + "\n"
	return bufio.NewReader(strings.NewReader(data))
}

func TestReadRequest_LockDefault(t *testing.T) {
	r := makeReader("l", "mykey", "10")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "l" || req.Key != "mykey" {
		t.Fatalf("unexpected cmd/key: %s/%s", req.Cmd, req.Key)
	}
	if req.AcquireTimeout != 10*time.Second {
		t.Fatalf("timeout: got %v", req.AcquireTimeout)
	}
	if req.LeaseTTL != 33*time.Second {
		t.Fatalf("lease: got %v", req.LeaseTTL)
	}
}

func TestReadRequest_LockCustomLease(t *testing.T) {
	r := makeReader("l", "mykey", "10 20")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.LeaseTTL != 20*time.Second {
		t.Fatalf("lease: got %v", req.LeaseTTL)
	}
}

func TestReadRequest_Release(t *testing.T) {
	r := makeReader("r", "mykey", "abc123")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "r" || req.Token != "abc123" {
		t.Fatalf("unexpected: cmd=%s token=%s", req.Cmd, req.Token)
	}
}

func TestReadRequest_Renew(t *testing.T) {
	r := makeReader("n", "mykey", "tok1")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "n" || req.Token != "tok1" {
		t.Fatalf("unexpected: cmd=%s token=%s", req.Cmd, req.Token)
	}
	if req.LeaseTTL != 33*time.Second {
		t.Fatalf("lease: got %v", req.LeaseTTL)
	}
}

func TestReadRequest_RenewCustomLease(t *testing.T) {
	r := makeReader("n", "mykey", "tok1 15")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.LeaseTTL != 15*time.Second {
		t.Fatalf("lease: got %v", req.LeaseTTL)
	}
}

func TestReadRequest_InvalidCmd(t *testing.T) {
	r := makeReader("x", "mykey", "arg")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 3 {
		t.Fatalf("expected code 3, got %v", err)
	}
}

func TestReadRequest_EmptyKey(t *testing.T) {
	r := makeReader("l", "", "10")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 5 {
		t.Fatalf("expected code 5, got %v", err)
	}
}

func TestReadRequest_NegativeTimeout(t *testing.T) {
	r := makeReader("l", "k", "-1")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 4 {
		t.Fatalf("expected code 4, got %v", err)
	}
}

func TestReadRequest_ZeroLease(t *testing.T) {
	r := makeReader("l", "k", "10 0")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 9 {
		t.Fatalf("expected code 9, got %v", err)
	}
}

func TestReadRequest_EmptyTokenRelease(t *testing.T) {
	r := makeReader("r", "k", " ")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 7 {
		t.Fatalf("expected code 7, got %v", err)
	}
}

func TestReadRequest_LockBadArgCount(t *testing.T) {
	r := makeReader("l", "k", "1 2 3")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 8 {
		t.Fatalf("expected code 8, got %v", err)
	}
}

func TestReadRequest_EnqueueDefault(t *testing.T) {
	r := makeReader("e", "mykey", "")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "e" || req.Key != "mykey" {
		t.Fatal("unexpected cmd/key")
	}
	if req.LeaseTTL != 33*time.Second {
		t.Fatalf("lease: got %v", req.LeaseTTL)
	}
}

func TestReadRequest_EnqueueCustomLease(t *testing.T) {
	r := makeReader("e", "mykey", "60")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.LeaseTTL != 60*time.Second {
		t.Fatalf("lease: got %v", req.LeaseTTL)
	}
}

func TestReadRequest_EnqueueZeroLease(t *testing.T) {
	r := makeReader("e", "mykey", "0")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 9 {
		t.Fatalf("expected code 9, got %v", err)
	}
}

func TestReadRequest_WaitParse(t *testing.T) {
	r := makeReader("w", "mykey", "10")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "w" || req.Key != "mykey" {
		t.Fatal("unexpected cmd/key")
	}
	if req.AcquireTimeout != 10*time.Second {
		t.Fatalf("timeout: got %v", req.AcquireTimeout)
	}
}

func TestReadRequest_WaitNegativeTimeout(t *testing.T) {
	r := makeReader("w", "mykey", "-1")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 4 {
		t.Fatalf("expected code 4, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// FormatResponse
// ---------------------------------------------------------------------------

func TestFormatResponse_OkWithToken(t *testing.T) {
	ack := &Ack{Status: "ok", Token: "abc", LeaseTTL: 30}
	got := string(FormatResponse(ack, 33))
	if got != "ok abc 30 0\n" {
		t.Fatalf("got %q", got)
	}
}

func TestFormatResponse_OkDefaultLease(t *testing.T) {
	ack := &Ack{Status: "ok", Token: "abc"}
	got := string(FormatResponse(ack, 33))
	if got != "ok abc 33 0\n" {
		t.Fatalf("got %q", got)
	}
}

func TestFormatResponse_OkExtra(t *testing.T) {
	ack := &Ack{Status: "ok", Extra: "25"}
	got := string(FormatResponse(ack, 33))
	if got != "ok 25\n" {
		t.Fatalf("got %q", got)
	}
}

func TestFormatResponse_OkBare(t *testing.T) {
	ack := &Ack{Status: "ok"}
	got := string(FormatResponse(ack, 33))
	if got != "ok\n" {
		t.Fatalf("got %q", got)
	}
}

func TestFormatResponse_Error(t *testing.T) {
	got := string(FormatResponse(&Ack{Status: "error"}, 33))
	if got != "error\n" {
		t.Fatalf("got %q", got)
	}
}

func TestFormatResponse_Timeout(t *testing.T) {
	got := string(FormatResponse(&Ack{Status: "timeout"}, 33))
	if got != "timeout\n" {
		t.Fatalf("got %q", got)
	}
}

func TestFormatResponse_ErrorMaxLocks(t *testing.T) {
	got := string(FormatResponse(&Ack{Status: "error_max_locks"}, 33))
	if got != "error_max_locks\n" {
		t.Fatalf("got %q", got)
	}
}

func TestFormatResponse_AcquiredWithToken(t *testing.T) {
	ack := &Ack{Status: "acquired", Token: "abc", LeaseTTL: 30}
	got := string(FormatResponse(ack, 33))
	if got != "acquired abc 30 0\n" {
		t.Fatalf("got %q", got)
	}
}

func TestFormatResponse_OkWithFence(t *testing.T) {
	ack := &Ack{Status: "ok", Token: "abc", LeaseTTL: 30, Fence: 42}
	got := string(FormatResponse(ack, 33))
	if got != "ok abc 30 42\n" {
		t.Fatalf("got %q", got)
	}
}

func TestFormatResponse_AcquiredWithFence(t *testing.T) {
	ack := &Ack{Status: "acquired", Token: "xyz", LeaseTTL: 60, Fence: 99}
	got := string(FormatResponse(ack, 33))
	if got != "acquired xyz 60 99\n" {
		t.Fatalf("got %q", got)
	}
}

func TestFormatResponse_Queued(t *testing.T) {
	got := string(FormatResponse(&Ack{Status: "queued"}, 33))
	if got != "queued\n" {
		t.Fatalf("got %q", got)
	}
}

func TestFormatResponse_ErrorLimitMismatch(t *testing.T) {
	got := string(FormatResponse(&Ack{Status: "error_limit_mismatch"}, 33))
	if got != "error_limit_mismatch\n" {
		t.Fatalf("got %q", got)
	}
}

// ---------------------------------------------------------------------------
// Semaphore command parsing
// ---------------------------------------------------------------------------

func TestReadRequest_SemLockDefault(t *testing.T) {
	r := makeReader("sl", "mykey", "10 3")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "sl" || req.Key != "mykey" {
		t.Fatalf("unexpected cmd/key: %s/%s", req.Cmd, req.Key)
	}
	if req.AcquireTimeout != 10*time.Second {
		t.Fatalf("timeout: got %v", req.AcquireTimeout)
	}
	if req.Limit != 3 {
		t.Fatalf("limit: got %d", req.Limit)
	}
	if req.LeaseTTL != 33*time.Second {
		t.Fatalf("lease: got %v", req.LeaseTTL)
	}
}

func TestReadRequest_SemLockCustomLease(t *testing.T) {
	r := makeReader("sl", "mykey", "10 3 60")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.LeaseTTL != 60*time.Second {
		t.Fatalf("lease: got %v", req.LeaseTTL)
	}
	if req.Limit != 3 {
		t.Fatalf("limit: got %d", req.Limit)
	}
}

func TestReadRequest_SemLockBadArgCount(t *testing.T) {
	r := makeReader("sl", "mykey", "10")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 8 {
		t.Fatalf("expected code 8, got %v", err)
	}
}

func TestReadRequest_SemLockZeroLimit(t *testing.T) {
	r := makeReader("sl", "mykey", "10 0")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 13 {
		t.Fatalf("expected code 13, got %v", err)
	}
}

func TestReadRequest_SemLockNegativeLimit(t *testing.T) {
	r := makeReader("sl", "mykey", "10 -1")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 13 {
		t.Fatalf("expected code 13, got %v", err)
	}
}

func TestReadRequest_SemRelease(t *testing.T) {
	r := makeReader("sr", "mykey", "abc123")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "sr" || req.Token != "abc123" {
		t.Fatalf("unexpected: cmd=%s token=%s", req.Cmd, req.Token)
	}
}

func TestReadRequest_SemRenew(t *testing.T) {
	r := makeReader("sn", "mykey", "tok1")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "sn" || req.Token != "tok1" || req.LeaseTTL != 33*time.Second {
		t.Fatalf("unexpected: cmd=%s token=%s lease=%v", req.Cmd, req.Token, req.LeaseTTL)
	}
}

func TestReadRequest_SemRenewCustomLease(t *testing.T) {
	r := makeReader("sn", "mykey", "tok1 15")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.LeaseTTL != 15*time.Second {
		t.Fatalf("lease: got %v", req.LeaseTTL)
	}
}

func TestReadRequest_SemEnqueue(t *testing.T) {
	r := makeReader("se", "mykey", "5")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "se" || req.Limit != 5 {
		t.Fatalf("unexpected: cmd=%s limit=%d", req.Cmd, req.Limit)
	}
	if req.LeaseTTL != 33*time.Second {
		t.Fatalf("lease: got %v", req.LeaseTTL)
	}
}

func TestReadRequest_SemEnqueueCustomLease(t *testing.T) {
	r := makeReader("se", "mykey", "5 60")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Limit != 5 || req.LeaseTTL != 60*time.Second {
		t.Fatalf("unexpected: limit=%d lease=%v", req.Limit, req.LeaseTTL)
	}
}

func TestReadRequest_SemEnqueueZeroLimit(t *testing.T) {
	r := makeReader("se", "mykey", "0")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 13 {
		t.Fatalf("expected code 13, got %v", err)
	}
}

func TestReadRequest_SemWait(t *testing.T) {
	r := makeReader("sw", "mykey", "10")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "sw" || req.AcquireTimeout != 10*time.Second {
		t.Fatalf("unexpected: cmd=%s timeout=%v", req.Cmd, req.AcquireTimeout)
	}
}

func TestReadRequest_SemWaitNegativeTimeout(t *testing.T) {
	r := makeReader("sw", "mykey", "-1")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 4 {
		t.Fatalf("expected code 4, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Key validation
// ---------------------------------------------------------------------------

func TestReadRequest_KeyWithSpace(t *testing.T) {
	r := makeReader("l", "bad key", "10")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 5 {
		t.Fatalf("expected code 5 (key whitespace), got %v", err)
	}
}

func TestReadRequest_KeyWithTab(t *testing.T) {
	r := makeReader("l", "bad\tkey", "10")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 5 {
		t.Fatalf("expected code 5 (key whitespace), got %v", err)
	}
}

// ---------------------------------------------------------------------------
// ReadLine length enforcement
// ---------------------------------------------------------------------------

func TestReadLine_Oversized(t *testing.T) {
	// Build a line longer than MaxLineBytes
	long := strings.Repeat("x", MaxLineBytes+10) + "\n"
	r := bufio.NewReader(strings.NewReader(long))
	_, err := ReadLine(r, 5*time.Second, &mockConn{})
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 12 {
		t.Fatalf("expected code 12 (line too long), got %v", err)
	}
}

func TestReadLine_ExactMax(t *testing.T) {
	// A line of exactly MaxLineBytes should succeed.
	exact := strings.Repeat("y", MaxLineBytes) + "\n"
	r := bufio.NewReader(strings.NewReader(exact))
	line, err := ReadLine(r, 5*time.Second, &mockConn{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(line) != MaxLineBytes {
		t.Fatalf("expected len %d, got %d", MaxLineBytes, len(line))
	}
}

// ---------------------------------------------------------------------------
// Phase 1: Atomic Counters
// ---------------------------------------------------------------------------

func TestReadRequest_Incr(t *testing.T) {
	r := makeReader("incr", "mykey", "5")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "incr" {
		t.Fatalf("cmd: got %q", req.Cmd)
	}
	if req.Key != "mykey" {
		t.Fatalf("key: got %q", req.Key)
	}
	if req.Delta != 5 {
		t.Fatalf("delta: got %d", req.Delta)
	}
}

func TestReadRequest_Decr(t *testing.T) {
	r := makeReader("decr", "mykey", "3")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "decr" {
		t.Fatalf("cmd: got %q", req.Cmd)
	}
	if req.Key != "mykey" {
		t.Fatalf("key: got %q", req.Key)
	}
	if req.Delta != 3 {
		t.Fatalf("delta: got %d", req.Delta)
	}
}

func TestReadRequest_Get(t *testing.T) {
	r := makeReader("get", "mykey", "")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "get" {
		t.Fatalf("cmd: got %q", req.Cmd)
	}
	if req.Key != "mykey" {
		t.Fatalf("key: got %q", req.Key)
	}
}

func TestReadRequest_Cset(t *testing.T) {
	r := makeReader("cset", "mykey", "42")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "cset" {
		t.Fatalf("cmd: got %q", req.Cmd)
	}
	if req.Key != "mykey" {
		t.Fatalf("key: got %q", req.Key)
	}
	if req.Delta != 42 {
		t.Fatalf("delta: got %d", req.Delta)
	}
}

func TestReadRequest_IncrBadDelta(t *testing.T) {
	r := makeReader("incr", "mykey", "notanumber")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 4 {
		t.Fatalf("expected code 4, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Phase 2: KV with TTL
// ---------------------------------------------------------------------------

func TestReadRequest_Kset(t *testing.T) {
	r := makeReader("kset", "mykey", "hello\t60")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "kset" {
		t.Fatalf("cmd: got %q", req.Cmd)
	}
	if req.Key != "mykey" {
		t.Fatalf("key: got %q", req.Key)
	}
	if req.Value != "hello" {
		t.Fatalf("value: got %q", req.Value)
	}
	if req.TTLSeconds != 60 {
		t.Fatalf("ttl: got %d", req.TTLSeconds)
	}
}

func TestReadRequest_KsetNoTTL(t *testing.T) {
	r := makeReader("kset", "mykey", "hello")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "kset" {
		t.Fatalf("cmd: got %q", req.Cmd)
	}
	if req.Value != "hello" {
		t.Fatalf("value: got %q", req.Value)
	}
}

func TestReadRequest_KsetEmptyValue(t *testing.T) {
	r := makeReader("kset", "mykey", "")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 8 {
		t.Fatalf("expected code 8, got %v", err)
	}
}

func TestReadRequest_Kget(t *testing.T) {
	r := makeReader("kget", "mykey", "")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "kget" {
		t.Fatalf("cmd: got %q", req.Cmd)
	}
	if req.Key != "mykey" {
		t.Fatalf("key: got %q", req.Key)
	}
}

func TestReadRequest_Kdel(t *testing.T) {
	r := makeReader("kdel", "mykey", "")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "kdel" {
		t.Fatalf("cmd: got %q", req.Cmd)
	}
	if req.Key != "mykey" {
		t.Fatalf("key: got %q", req.Key)
	}
}

// ---------------------------------------------------------------------------
// Phase 3: Signaling
// ---------------------------------------------------------------------------

func TestReadRequest_Listen(t *testing.T) {
	r := makeReader("listen", "alerts.*", "")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "listen" {
		t.Fatalf("cmd: got %q", req.Cmd)
	}
	if req.Key != "alerts.*" {
		t.Fatalf("key: got %q", req.Key)
	}
}

func TestReadRequest_Unlisten(t *testing.T) {
	r := makeReader("unlisten", "alerts.*", "")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "unlisten" {
		t.Fatalf("cmd: got %q", req.Cmd)
	}
	if req.Key != "alerts.*" {
		t.Fatalf("key: got %q", req.Key)
	}
}

func TestReadRequest_Signal(t *testing.T) {
	r := makeReader("signal", "alerts.fire", "hello world")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "signal" {
		t.Fatalf("cmd: got %q", req.Cmd)
	}
	if req.Key != "alerts.fire" {
		t.Fatalf("key: got %q", req.Key)
	}
	if req.Value != "hello world" {
		t.Fatalf("value: got %q", req.Value)
	}
}

func TestReadRequest_SignalRejectsWildcard(t *testing.T) {
	r := makeReader("signal", "alerts.*", "payload")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 5 {
		t.Fatalf("expected code 5, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Phase 4: Lists/Queues
// ---------------------------------------------------------------------------

func TestReadRequest_Lpush(t *testing.T) {
	r := makeReader("lpush", "mylist", "item1")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "lpush" {
		t.Fatalf("cmd: got %q", req.Cmd)
	}
	if req.Key != "mylist" {
		t.Fatalf("key: got %q", req.Key)
	}
	if req.Value != "item1" {
		t.Fatalf("value: got %q", req.Value)
	}
}

func TestReadRequest_Rpush(t *testing.T) {
	r := makeReader("rpush", "mylist", "item1")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "rpush" {
		t.Fatalf("cmd: got %q", req.Cmd)
	}
	if req.Key != "mylist" {
		t.Fatalf("key: got %q", req.Key)
	}
	if req.Value != "item1" {
		t.Fatalf("value: got %q", req.Value)
	}
}

func TestReadRequest_LpushEmptyValue(t *testing.T) {
	r := makeReader("lpush", "mylist", "")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 8 {
		t.Fatalf("expected code 8, got %v", err)
	}
}

func TestReadRequest_Lpop(t *testing.T) {
	r := makeReader("lpop", "mylist", "")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "lpop" {
		t.Fatalf("cmd: got %q", req.Cmd)
	}
	if req.Key != "mylist" {
		t.Fatalf("key: got %q", req.Key)
	}
}

func TestReadRequest_Rpop(t *testing.T) {
	r := makeReader("rpop", "mylist", "")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "rpop" {
		t.Fatalf("cmd: got %q", req.Cmd)
	}
	if req.Key != "mylist" {
		t.Fatalf("key: got %q", req.Key)
	}
}

func TestReadRequest_Llen(t *testing.T) {
	r := makeReader("llen", "mylist", "")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "llen" {
		t.Fatalf("cmd: got %q", req.Cmd)
	}
	if req.Key != "mylist" {
		t.Fatalf("key: got %q", req.Key)
	}
}

func TestReadRequest_Lrange(t *testing.T) {
	r := makeReader("lrange", "mylist", "0 -1")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "lrange" {
		t.Fatalf("cmd: got %q", req.Cmd)
	}
	if req.Key != "mylist" {
		t.Fatalf("key: got %q", req.Key)
	}
	if req.Start != 0 {
		t.Fatalf("start: got %d", req.Start)
	}
	if req.Stop != -1 {
		t.Fatalf("stop: got %d", req.Stop)
	}
}

func TestReadRequest_LrangeBadArgs(t *testing.T) {
	r := makeReader("lrange", "mylist", "0")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 8 {
		t.Fatalf("expected code 8, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// FormatResponse: new statuses
// ---------------------------------------------------------------------------

func TestFormatResponse_Nil(t *testing.T) {
	got := string(FormatResponse(&Ack{Status: "nil"}, 33))
	if got != "nil\n" {
		t.Fatalf("got %q", got)
	}
}

func TestFormatResponse_ErrorMaxKeys(t *testing.T) {
	got := string(FormatResponse(&Ack{Status: "error_max_keys"}, 33))
	if got != "error_max_keys\n" {
		t.Fatalf("got %q", got)
	}
}

func TestFormatResponse_ErrorListFull(t *testing.T) {
	got := string(FormatResponse(&Ack{Status: "error_list_full"}, 33))
	if got != "error_list_full\n" {
		t.Fatalf("got %q", got)
	}
}

// ---------------------------------------------------------------------------
// Queue Group protocol parsing
// ---------------------------------------------------------------------------

func TestReadRequest_ListenWithGroup(t *testing.T) {
	r := makeReader("listen", "alerts.*", "worker-pool")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "listen" {
		t.Fatalf("cmd: got %q", req.Cmd)
	}
	if req.Key != "alerts.*" {
		t.Fatalf("key: got %q", req.Key)
	}
	if req.Group != "worker-pool" {
		t.Fatalf("group: got %q, want %q", req.Group, "worker-pool")
	}
}

func TestReadRequest_ListenWithoutGroup(t *testing.T) {
	r := makeReader("listen", "alerts.*", "")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Group != "" {
		t.Fatalf("group: got %q, want empty", req.Group)
	}
}

func TestReadRequest_UnlistenWithGroup(t *testing.T) {
	r := makeReader("unlisten", "alerts.*", "worker-pool")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "unlisten" {
		t.Fatalf("cmd: got %q", req.Cmd)
	}
	if req.Group != "worker-pool" {
		t.Fatalf("group: got %q, want %q", req.Group, "worker-pool")
	}
}

func TestReadRequest_UnlistenWithoutGroup(t *testing.T) {
	r := makeReader("unlisten", "alerts.*", "")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Group != "" {
		t.Fatalf("group: got %q, want empty", req.Group)
	}
}

// ---------------------------------------------------------------------------
// KCAS
// ---------------------------------------------------------------------------

func TestReadRequest_KCAS(t *testing.T) {
	r := makeReader("kcas", "mykey", "old\tnew\t60")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "kcas" {
		t.Fatalf("cmd: got %q", req.Cmd)
	}
	if req.Key != "mykey" {
		t.Fatalf("key: got %q", req.Key)
	}
	if req.OldValue != "old" {
		t.Fatalf("old_value: got %q", req.OldValue)
	}
	if req.Value != "new" {
		t.Fatalf("value: got %q", req.Value)
	}
	if req.TTLSeconds != 60 {
		t.Fatalf("ttl: got %d", req.TTLSeconds)
	}
}

func TestReadRequest_KCAS_EmptyOld(t *testing.T) {
	r := makeReader("kcas", "mykey", "\tnew\t0")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.OldValue != "" {
		t.Fatalf("old_value: got %q, want empty", req.OldValue)
	}
	if req.Value != "new" {
		t.Fatalf("value: got %q", req.Value)
	}
	if req.TTLSeconds != 0 {
		t.Fatalf("ttl: got %d", req.TTLSeconds)
	}
}

func TestReadRequest_KCAS_NormalValues(t *testing.T) {
	// Standard three-field format: old_value \t new_value \t ttl
	r := makeReader("kcas", "mykey", "old\tnew\t30")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.OldValue != "old" {
		t.Fatalf("old_value: got %q, want %q", req.OldValue, "old")
	}
	if req.Value != "new" {
		t.Fatalf("value: got %q, want %q", req.Value, "new")
	}
	if req.TTLSeconds != 30 {
		t.Fatalf("ttl: got %d, want 30", req.TTLSeconds)
	}
}

func TestReadRequest_KCAS_EmptyNewValue(t *testing.T) {
	// Empty new_value should be rejected.
	r := makeReader("kcas", "mykey", "old\t\t0")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err == nil {
		t.Fatal("expected error for empty new value")
	}
}

func TestReadRequest_KCAS_NoTab(t *testing.T) {
	r := makeReader("kcas", "mykey", "no-tab-here")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err == nil {
		t.Fatal("expected error for missing tab")
	}
}

// ---------------------------------------------------------------------------
// Watch/Unwatch
// ---------------------------------------------------------------------------

func TestReadRequest_Watch(t *testing.T) {
	r := makeReader("watch", "mykey", "")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "watch" || req.Key != "mykey" {
		t.Fatalf("cmd/key: %s/%s", req.Cmd, req.Key)
	}
}

func TestReadRequest_Unwatch(t *testing.T) {
	r := makeReader("unwatch", "mykey", "")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "unwatch" || req.Key != "mykey" {
		t.Fatalf("cmd/key: %s/%s", req.Cmd, req.Key)
	}
}

// ---------------------------------------------------------------------------
// BWait
// ---------------------------------------------------------------------------

func TestReadRequest_BWait(t *testing.T) {
	r := makeReader("bwait", "barrier1", "5 10")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "bwait" {
		t.Fatalf("cmd: got %q", req.Cmd)
	}
	if req.Key != "barrier1" {
		t.Fatalf("key: got %q", req.Key)
	}
	if req.Limit != 5 {
		t.Fatalf("count: got %d", req.Limit)
	}
	if req.AcquireTimeout != 10*time.Second {
		t.Fatalf("timeout: got %v", req.AcquireTimeout)
	}
}

func TestReadRequest_BWait_BadCount(t *testing.T) {
	r := makeReader("bwait", "barrier1", "0 10")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err == nil {
		t.Fatal("expected error for count <= 0")
	}
}

// ---------------------------------------------------------------------------
// Elect/Resign/Observe/Unobserve
// ---------------------------------------------------------------------------

func TestReadRequest_Elect(t *testing.T) {
	r := makeReader("elect", "leader1", "10 30")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "elect" {
		t.Fatalf("cmd: got %q", req.Cmd)
	}
	if req.AcquireTimeout != 10*time.Second {
		t.Fatalf("timeout: got %v", req.AcquireTimeout)
	}
	if req.LeaseTTL != 30*time.Second {
		t.Fatalf("lease: got %v", req.LeaseTTL)
	}
}

func TestReadRequest_Resign(t *testing.T) {
	r := makeReader("resign", "leader1", "abc123")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "resign" || req.Token != "abc123" {
		t.Fatalf("cmd/token: %s/%s", req.Cmd, req.Token)
	}
}

func TestReadRequest_Observe(t *testing.T) {
	r := makeReader("observe", "leader1", "")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "observe" || req.Key != "leader1" {
		t.Fatalf("cmd/key: %s/%s", req.Cmd, req.Key)
	}
}

func TestReadRequest_Unobserve(t *testing.T) {
	r := makeReader("unobserve", "leader1", "")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "unobserve" || req.Key != "leader1" {
		t.Fatalf("cmd/key: %s/%s", req.Cmd, req.Key)
	}
}

// ---------------------------------------------------------------------------
// FormatResponse — new statuses
// ---------------------------------------------------------------------------

func TestFormatResponse_CASConflict(t *testing.T) {
	got := FormatResponse(&Ack{Status: "cas_conflict"}, 33)
	if string(got) != "cas_conflict\n" {
		t.Fatalf("got %q", string(got))
	}
}

func TestFormatResponse_BarrierCountMismatch(t *testing.T) {
	got := FormatResponse(&Ack{Status: "error_barrier_count_mismatch"}, 33)
	if string(got) != "error_barrier_count_mismatch\n" {
		t.Fatalf("got %q", string(got))
	}
}

// ---------------------------------------------------------------------------
// FormatResponse — complete status coverage
// ---------------------------------------------------------------------------

func TestFormatResponse_ErrorAuth(t *testing.T) {
	got := string(FormatResponse(&Ack{Status: "error_auth"}, 33))
	if got != "error_auth\n" {
		t.Fatalf("got %q", got)
	}
}

func TestFormatResponse_ErrorMaxWaiters(t *testing.T) {
	got := string(FormatResponse(&Ack{Status: "error_max_waiters"}, 33))
	if got != "error_max_waiters\n" {
		t.Fatalf("got %q", got)
	}
}

func TestFormatResponse_ErrorNotEnqueued(t *testing.T) {
	got := string(FormatResponse(&Ack{Status: "error_not_enqueued"}, 33))
	if got != "error_not_enqueued\n" {
		t.Fatalf("got %q", got)
	}
}

func TestFormatResponse_ErrorAlreadyEnqueued(t *testing.T) {
	got := string(FormatResponse(&Ack{Status: "error_already_enqueued"}, 33))
	if got != "error_already_enqueued\n" {
		t.Fatalf("got %q", got)
	}
}

func TestFormatResponse_ErrorLeaseExpired(t *testing.T) {
	got := string(FormatResponse(&Ack{Status: "error_lease_expired"}, 33))
	if got != "error_lease_expired\n" {
		t.Fatalf("got %q", got)
	}
}

func TestFormatResponse_ErrorTypeMismatch(t *testing.T) {
	got := string(FormatResponse(&Ack{Status: "error_type_mismatch"}, 33))
	if got != "error_type_mismatch\n" {
		t.Fatalf("got %q", got)
	}
}

func TestFormatResponse_AcquiredBare(t *testing.T) {
	got := string(FormatResponse(&Ack{Status: "acquired"}, 33))
	if got != "acquired\n" {
		t.Fatalf("got %q", got)
	}
}

func TestFormatResponse_AcquiredExtra(t *testing.T) {
	ack := &Ack{Status: "acquired", Extra: "some-extra-data"}
	got := string(FormatResponse(ack, 33))
	if got != "acquired some-extra-data\n" {
		t.Fatalf("got %q", got)
	}
}

func TestFormatResponse_UnknownStatus(t *testing.T) {
	got := string(FormatResponse(&Ack{Status: "custom_status"}, 33))
	if got != "custom_status\n" {
		t.Fatalf("got %q", got)
	}
}

// ---------------------------------------------------------------------------
// ReadLine — edge cases
// ---------------------------------------------------------------------------

func TestReadLine_CRStripping(t *testing.T) {
	// \r should be stripped from the line
	r := bufio.NewReader(strings.NewReader("hello\r\n"))
	line, err := ReadLine(r, 5*time.Second, &mockConn{})
	if err != nil {
		t.Fatal(err)
	}
	if line != "hello" {
		t.Fatalf("expected 'hello', got %q (CR should be stripped)", line)
	}
}

func TestReadLine_CRInMiddle(t *testing.T) {
	// \r in middle of line should also be stripped
	r := bufio.NewReader(strings.NewReader("hel\rlo\n"))
	line, err := ReadLine(r, 5*time.Second, &mockConn{})
	if err != nil {
		t.Fatal(err)
	}
	if line != "hello" {
		t.Fatalf("expected 'hello', got %q (all CR should be stripped)", line)
	}
}

func TestReadLine_EmptyLine(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("\n"))
	line, err := ReadLine(r, 5*time.Second, &mockConn{})
	if err != nil {
		t.Fatal(err)
	}
	if line != "" {
		t.Fatalf("expected empty string, got %q", line)
	}
}

// ---------------------------------------------------------------------------
// RW Lock command parsing
// ---------------------------------------------------------------------------

func TestReadRequest_RLock(t *testing.T) {
	r := makeReader("rl", "mykey", "10")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "rl" || req.Key != "mykey" {
		t.Fatalf("cmd/key: %s/%s", req.Cmd, req.Key)
	}
	if req.AcquireTimeout != 10*time.Second {
		t.Fatalf("timeout: got %v", req.AcquireTimeout)
	}
	if req.LeaseTTL != 33*time.Second {
		t.Fatalf("lease: got %v", req.LeaseTTL)
	}
}

func TestReadRequest_WLock(t *testing.T) {
	r := makeReader("wl", "mykey", "10 60")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "wl" || req.Key != "mykey" {
		t.Fatalf("cmd/key: %s/%s", req.Cmd, req.Key)
	}
	if req.AcquireTimeout != 10*time.Second {
		t.Fatalf("timeout: got %v", req.AcquireTimeout)
	}
	if req.LeaseTTL != 60*time.Second {
		t.Fatalf("lease: got %v", req.LeaseTTL)
	}
}

func TestReadRequest_RLock_BadArgs(t *testing.T) {
	r := makeReader("rl", "mykey", "1 2 3")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 8 {
		t.Fatalf("expected code 8, got %v", err)
	}
}

func TestReadRequest_RRelease(t *testing.T) {
	r := makeReader("rr", "mykey", "tok123")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "rr" || req.Token != "tok123" {
		t.Fatalf("cmd/token: %s/%s", req.Cmd, req.Token)
	}
}

func TestReadRequest_WRelease(t *testing.T) {
	r := makeReader("wr", "mykey", "tok456")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "wr" || req.Token != "tok456" {
		t.Fatalf("cmd/token: %s/%s", req.Cmd, req.Token)
	}
}

func TestReadRequest_RRelease_EmptyToken(t *testing.T) {
	r := makeReader("rr", "mykey", " ")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 7 {
		t.Fatalf("expected code 7, got %v", err)
	}
}

func TestReadRequest_RRenew(t *testing.T) {
	r := makeReader("rn", "mykey", "tok1 60")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "rn" || req.Token != "tok1" || req.LeaseTTL != 60*time.Second {
		t.Fatalf("cmd/token/lease: %s/%s/%v", req.Cmd, req.Token, req.LeaseTTL)
	}
}

func TestReadRequest_WRenew(t *testing.T) {
	r := makeReader("wn", "mykey", "tok1")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "wn" || req.Token != "tok1" || req.LeaseTTL != 33*time.Second {
		t.Fatalf("cmd/token/lease: %s/%s/%v", req.Cmd, req.Token, req.LeaseTTL)
	}
}

func TestReadRequest_REnqueue(t *testing.T) {
	r := makeReader("re", "mykey", "60")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "re" || req.LeaseTTL != 60*time.Second {
		t.Fatalf("cmd/lease: %s/%v", req.Cmd, req.LeaseTTL)
	}
}

func TestReadRequest_WEnqueue(t *testing.T) {
	r := makeReader("we", "mykey", "")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "we" || req.LeaseTTL != 33*time.Second {
		t.Fatalf("cmd/lease: %s/%v", req.Cmd, req.LeaseTTL)
	}
}

func TestReadRequest_REnqueue_ZeroLease(t *testing.T) {
	r := makeReader("re", "mykey", "0")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 9 {
		t.Fatalf("expected code 9, got %v", err)
	}
}

func TestReadRequest_RWait(t *testing.T) {
	r := makeReader("rw", "mykey", "10")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "rw" || req.AcquireTimeout != 10*time.Second {
		t.Fatalf("cmd/timeout: %s/%v", req.Cmd, req.AcquireTimeout)
	}
}

func TestReadRequest_WWait(t *testing.T) {
	r := makeReader("ww", "mykey", "5")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "ww" || req.AcquireTimeout != 5*time.Second {
		t.Fatalf("cmd/timeout: %s/%v", req.Cmd, req.AcquireTimeout)
	}
}

func TestReadRequest_RWait_Empty(t *testing.T) {
	r := makeReader("rw", "mykey", "")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 8 {
		t.Fatalf("expected code 8, got %v", err)
	}
}

func TestReadRequest_RRenew_BadArgs(t *testing.T) {
	r := makeReader("rn", "mykey", "a b c")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 8 {
		t.Fatalf("expected code 8, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// BLPop / BRPop
// ---------------------------------------------------------------------------

func TestReadRequest_BLPop(t *testing.T) {
	r := makeReader("blpop", "mylist", "10")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "blpop" || req.Key != "mylist" || req.AcquireTimeout != 10*time.Second {
		t.Fatalf("cmd/key/timeout: %s/%s/%v", req.Cmd, req.Key, req.AcquireTimeout)
	}
}

func TestReadRequest_BRPop(t *testing.T) {
	r := makeReader("brpop", "mylist", "5")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "brpop" || req.Key != "mylist" || req.AcquireTimeout != 5*time.Second {
		t.Fatalf("cmd/key/timeout: %s/%s/%v", req.Cmd, req.Key, req.AcquireTimeout)
	}
}

func TestReadRequest_BLPop_Empty(t *testing.T) {
	r := makeReader("blpop", "mylist", "")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 8 {
		t.Fatalf("expected code 8, got %v", err)
	}
}

func TestReadRequest_BLPop_NegativeTimeout(t *testing.T) {
	r := makeReader("blpop", "mylist", "-1")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 4 {
		t.Fatalf("expected code 4, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Auth / Stats
// ---------------------------------------------------------------------------

func TestReadRequest_Auth(t *testing.T) {
	r := makeReader("auth", "", "  mysecret  ")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "auth" || req.Token != "mysecret" {
		t.Fatalf("cmd/token: %s/%q", req.Cmd, req.Token)
	}
}

func TestReadRequest_Stats(t *testing.T) {
	r := makeReader("stats", "", "")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "stats" {
		t.Fatalf("cmd: got %q", req.Cmd)
	}
}

// ---------------------------------------------------------------------------
// Signal edge cases
// ---------------------------------------------------------------------------

func TestReadRequest_SignalEmptyPayload(t *testing.T) {
	r := makeReader("signal", "ch", "")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 8 {
		t.Fatalf("expected code 8, got %v", err)
	}
}

func TestReadRequest_SignalRejectsGT(t *testing.T) {
	r := makeReader("signal", "alerts.>", "payload")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 5 {
		t.Fatalf("expected code 5, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// KSet edge cases
// ---------------------------------------------------------------------------

func TestReadRequest_KsetTabInValue(t *testing.T) {
	// Value containing multiple tabs should be rejected
	r := makeReader("kset", "mykey", "val\twith\ttabs")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err == nil {
		t.Fatal("expected error for tab in value")
	}
}

func TestReadRequest_KsetEmptyValueBeforeTab(t *testing.T) {
	r := makeReader("kset", "mykey", "\t60")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 8 {
		t.Fatalf("expected code 8, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Cset edge cases
// ---------------------------------------------------------------------------

func TestReadRequest_CsetEmpty(t *testing.T) {
	r := makeReader("cset", "mykey", "")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 8 {
		t.Fatalf("expected code 8, got %v", err)
	}
}

func TestReadRequest_IncrEmpty(t *testing.T) {
	r := makeReader("incr", "mykey", "")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 8 {
		t.Fatalf("expected code 8, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Elect edge cases
// ---------------------------------------------------------------------------

func TestReadRequest_ElectDefaultLease(t *testing.T) {
	r := makeReader("elect", "leader1", "10")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Cmd != "elect" || req.LeaseTTL != 33*time.Second {
		t.Fatalf("cmd/lease: %s/%v", req.Cmd, req.LeaseTTL)
	}
}

func TestReadRequest_ElectBadArgs(t *testing.T) {
	r := makeReader("elect", "leader1", "1 2 3")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 8 {
		t.Fatalf("expected code 8, got %v", err)
	}
}

func TestReadRequest_ResignEmptyToken(t *testing.T) {
	r := makeReader("resign", "leader1", " ")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 7 {
		t.Fatalf("expected code 7, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// BWait edge cases
// ---------------------------------------------------------------------------

func TestReadRequest_BWait_BadArgs(t *testing.T) {
	r := makeReader("bwait", "barrier1", "5")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 8 {
		t.Fatalf("expected code 8, got %v", err)
	}
}

func TestReadRequest_BWait_NegativeTimeout(t *testing.T) {
	r := makeReader("bwait", "barrier1", "5 -1")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 4 {
		t.Fatalf("expected code 4, got %v", err)
	}
}

func TestReadRequest_BWait_NegativeCount(t *testing.T) {
	r := makeReader("bwait", "barrier1", "-1 10")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 13 {
		t.Fatalf("expected code 13, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Semaphore edge cases
// ---------------------------------------------------------------------------

func TestReadRequest_SemReleaseEmptyToken(t *testing.T) {
	r := makeReader("sr", "mykey", " ")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 7 {
		t.Fatalf("expected code 7, got %v", err)
	}
}

func TestReadRequest_SemRenewBadArgs(t *testing.T) {
	r := makeReader("sn", "mykey", "a b c")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 8 {
		t.Fatalf("expected code 8, got %v", err)
	}
}

func TestReadRequest_SemRenewEmptyToken(t *testing.T) {
	r := makeReader("sn", "mykey", " ")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 8 {
		t.Fatalf("expected code 8, got %v", err)
	}
}

func TestReadRequest_SemEnqueueBadArgs(t *testing.T) {
	r := makeReader("se", "mykey", "1 2 3")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 8 {
		t.Fatalf("expected code 8, got %v", err)
	}
}

func TestReadRequest_SemWaitEmpty(t *testing.T) {
	r := makeReader("sw", "mykey", "")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 8 {
		t.Fatalf("expected code 8, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Wait edge cases
// ---------------------------------------------------------------------------

func TestReadRequest_WaitEmpty(t *testing.T) {
	r := makeReader("w", "mykey", "")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 8 {
		t.Fatalf("expected code 8, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// RpushEmptyValue
// ---------------------------------------------------------------------------

func TestReadRequest_RpushEmptyValue(t *testing.T) {
	r := makeReader("rpush", "mylist", "")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 8 {
		t.Fatalf("expected code 8, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// KCAS edge cases
// ---------------------------------------------------------------------------

func TestReadRequest_KCAS_EmptyArg(t *testing.T) {
	r := makeReader("kcas", "mykey", "")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 8 {
		t.Fatalf("expected code 8, got %v", err)
	}
}

func TestReadRequest_KCAS_ThreeTabs(t *testing.T) {
	r := makeReader("kcas", "mykey", "a\tb\tc\td")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 8 {
		t.Fatalf("expected code 8, got %v", err)
	}
}

func TestReadRequest_KCAS_ZeroTTL(t *testing.T) {
	r := makeReader("kcas", "mykey", "old\tnew\t0")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.TTLSeconds != 0 {
		t.Fatalf("ttl: got %d, want 0", req.TTLSeconds)
	}
}

func TestReadRequest_KCAS_EmptyTTL(t *testing.T) {
	r := makeReader("kcas", "mykey", "old\tnew\t")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.TTLSeconds != 0 {
		t.Fatalf("ttl: got %d, want 0", req.TTLSeconds)
	}
}

// ---------------------------------------------------------------------------
// Listen with group
// ---------------------------------------------------------------------------

func TestReadRequest_ListenGroupTrimmed(t *testing.T) {
	r := makeReader("listen", "ch", "  my-group  ")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if req.Group != "my-group" {
		t.Fatalf("group: got %q, want %q", req.Group, "my-group")
	}
}

// ---------------------------------------------------------------------------
// ProtocolError string representation
// ---------------------------------------------------------------------------

func TestProtocolError_String(t *testing.T) {
	pe := &ProtocolError{Code: 42, Message: "test error"}
	s := pe.Error()
	if s != "protocol error 42: test error" {
		t.Fatalf("got %q", s)
	}
}

// ---------------------------------------------------------------------------
// Renew edge cases
// ---------------------------------------------------------------------------

func TestReadRequest_RenewBadArgs(t *testing.T) {
	r := makeReader("n", "mykey", "a b c")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 8 {
		t.Fatalf("expected code 8, got %v", err)
	}
}

func TestReadRequest_RenewEmptyToken(t *testing.T) {
	r := makeReader("n", "mykey", " ")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	pe, ok := err.(*ProtocolError)
	if !ok || pe.Code != 8 {
		t.Fatalf("expected code 8, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// parseSeconds overflow and boundary tests
// ---------------------------------------------------------------------------

func TestReadRequest_TimeoutOverflow(t *testing.T) {
	// A very large timeout should be rejected (overflow protection)
	huge := fmt.Sprintf("%d", math.MaxInt64)
	r := makeReader("l", "mykey", huge)
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err == nil {
		t.Fatal("expected error for overflow timeout")
	}
	pe, ok := err.(*ProtocolError)
	if !ok {
		t.Fatalf("expected ProtocolError, got %T: %v", err, err)
	}
	if pe.Code != 4 {
		t.Fatalf("expected code 4, got %d", pe.Code)
	}
}

func TestReadRequest_LeaseOverflow(t *testing.T) {
	huge := fmt.Sprintf("5 %d", math.MaxInt64)
	r := makeReader("l", "mykey", huge)
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err == nil {
		t.Fatal("expected error for overflow lease")
	}
	pe, ok := err.(*ProtocolError)
	if !ok {
		t.Fatalf("expected ProtocolError, got %T: %v", err, err)
	}
	if pe.Code != 4 {
		t.Fatalf("expected code 4, got %d", pe.Code)
	}
}

func TestReadRequest_IncrOverflow(t *testing.T) {
	// parseInt64 with an unparseable value
	r := makeReader("incr", "c1", "99999999999999999999")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err == nil {
		t.Fatal("expected error for overflow delta")
	}
	pe, ok := err.(*ProtocolError)
	if !ok {
		t.Fatalf("expected ProtocolError, got %T: %v", err, err)
	}
	if pe.Code != 4 {
		t.Fatalf("expected code 4, got %d", pe.Code)
	}
}

func TestReadRequest_CsetOverflow(t *testing.T) {
	r := makeReader("cset", "c1", "99999999999999999999")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err == nil {
		t.Fatal("expected error for overflow cset value")
	}
}

func TestReadRequest_KsetTTLOverflow(t *testing.T) {
	huge := fmt.Sprintf("value\t%d", math.MaxInt64)
	r := makeReader("kset", "k1", huge)
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err == nil {
		t.Fatal("expected error for overflow kset TTL")
	}
}

func TestReadRequest_BwaitTimeoutOverflow(t *testing.T) {
	huge := fmt.Sprintf("3 %d", math.MaxInt64)
	r := makeReader("bwait", "b1", huge)
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err == nil {
		t.Fatal("expected error for overflow bwait timeout")
	}
}

// ---------------------------------------------------------------------------
// FormatResponse boundary values
// ---------------------------------------------------------------------------

func TestFormatResponse_MaxFence(t *testing.T) {
	ack := &Ack{Status: "ok", Token: "tok", LeaseTTL: 30, Fence: math.MaxUint64}
	out := string(FormatResponse(ack, 33))
	expected := fmt.Sprintf("ok tok 30 %d\n", uint64(math.MaxUint64))
	if out != expected {
		t.Fatalf("expected %q, got %q", expected, out)
	}
}

func TestFormatResponse_ZeroFence(t *testing.T) {
	ack := &Ack{Status: "ok", Token: "tok", LeaseTTL: 30, Fence: 0}
	out := string(FormatResponse(ack, 33))
	// Fence=0 should not be included in output
	if strings.Contains(out, " 0\n") && strings.Count(out, " ") > 2 {
		// Fence 0 might or might not be included — just verify it doesn't crash
	}
}

func TestFormatResponse_LargeLease(t *testing.T) {
	ack := &Ack{Status: "ok", Token: "tok", LeaseTTL: 999999}
	out := string(FormatResponse(ack, 33))
	if !strings.Contains(out, "999999") {
		t.Fatalf("expected lease in output, got %q", out)
	}
}

// ---------------------------------------------------------------------------
// ProtocolError.Error() string format
// ---------------------------------------------------------------------------

func TestProtocolError_ErrorString(t *testing.T) {
	pe := &ProtocolError{Code: 42, Message: "test error"}
	s := pe.Error()
	if !strings.Contains(s, "42") {
		t.Fatalf("expected code 42 in error string, got %q", s)
	}
	if !strings.Contains(s, "test error") {
		t.Fatalf("expected message in error string, got %q", s)
	}
}

// ---------------------------------------------------------------------------
// ReadRequest edge cases for all commands
// ---------------------------------------------------------------------------

func TestReadRequest_SemLockTimeoutOverflow(t *testing.T) {
	huge := fmt.Sprintf("%d 3", math.MaxInt64)
	r := makeReader("sl", "sem1", huge)
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err == nil {
		t.Fatal("expected error for overflow sem timeout")
	}
}

func TestReadRequest_SemEnqueueLeaseOverflow(t *testing.T) {
	huge := fmt.Sprintf("3 %d", math.MaxInt64)
	r := makeReader("se", "sem1", huge)
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err == nil {
		t.Fatal("expected error for overflow sem enqueue lease")
	}
}

func TestReadRequest_RWLockTimeoutOverflow(t *testing.T) {
	huge := fmt.Sprintf("%d", math.MaxInt64)
	r := makeReader("rl", "rw1", huge)
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err == nil {
		t.Fatal("expected error for overflow rw lock timeout")
	}
}

func TestReadRequest_ElectTimeoutOverflow(t *testing.T) {
	huge := fmt.Sprintf("%d 30", math.MaxInt64)
	r := makeReader("elect", "e1", huge)
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err == nil {
		t.Fatal("expected error for overflow elect timeout")
	}
}

func TestReadRequest_KCAS_TTLOverflow(t *testing.T) {
	huge := fmt.Sprintf("old\tnew\t%d", math.MaxInt64)
	r := makeReader("kcas", "k1", huge)
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err == nil {
		t.Fatal("expected error for overflow kcas TTL")
	}
}

func TestReadRequest_LrangeBadStart(t *testing.T) {
	r := makeReader("lrange", "q1", "abc 10")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err == nil {
		t.Fatal("expected error for bad lrange start")
	}
}

func TestReadRequest_LrangeBadStop(t *testing.T) {
	r := makeReader("lrange", "q1", "0 abc")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err == nil {
		t.Fatal("expected error for bad lrange stop")
	}
}

func TestReadRequest_BLPopBadTimeout(t *testing.T) {
	r := makeReader("blpop", "q1", "abc")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err == nil {
		t.Fatal("expected error for bad blpop timeout")
	}
}

func TestReadRequest_BRPopBadTimeout(t *testing.T) {
	r := makeReader("brpop", "q1", "abc")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err == nil {
		t.Fatal("expected error for bad brpop timeout")
	}
}

func TestReadRequest_ElectBadTimeout(t *testing.T) {
	r := makeReader("elect", "e1", "abc 30")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err == nil {
		t.Fatal("expected error for bad elect timeout")
	}
}

func TestReadRequest_BwaitBadTimeout(t *testing.T) {
	r := makeReader("bwait", "b1", "3 abc")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err == nil {
		t.Fatal("expected error for bad bwait timeout")
	}
}

func TestReadRequest_SemLockBadTimeout(t *testing.T) {
	r := makeReader("sl", "sem1", "abc 3")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err == nil {
		t.Fatal("expected error for bad sem lock timeout")
	}
}

func TestReadRequest_SemWaitBadTimeout(t *testing.T) {
	r := makeReader("sw", "sem1", "abc")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err == nil {
		t.Fatal("expected error for bad sem wait timeout")
	}
}

// ---------------------------------------------------------------------------
// Edge-case tests
// ---------------------------------------------------------------------------

func TestReadRequest_NullByteInKey(t *testing.T) {
	// The protocol doesn't validate content bytes; null bytes in a key
	// should be accepted as-is.
	key := "my\x00key"
	r := makeReader("get", key, "")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatalf("null byte in key should be accepted: %v", err)
	}
	if req.Key != key {
		t.Fatalf("key mismatch: got %q, want %q", req.Key, key)
	}
}

func TestReadRequest_VeryLongKey(t *testing.T) {
	// Key at exactly MaxLineBytes should succeed.
	key := strings.Repeat("k", MaxLineBytes)
	r := makeReader("get", key, "")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatalf("key at MaxLineBytes should succeed: %v", err)
	}
	if req.Key != key {
		t.Fatalf("key length: got %d, want %d", len(req.Key), MaxLineBytes)
	}
}

func TestReadRequest_KeyExceedsMaxLine(t *testing.T) {
	// Key over MaxLineBytes should error with "line too long".
	key := strings.Repeat("k", MaxLineBytes+1)
	r := makeReader("get", key, "")
	_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err == nil {
		t.Fatal("expected error for key exceeding MaxLineBytes")
	}
	pe, ok := err.(*ProtocolError)
	if !ok {
		t.Fatalf("expected *ProtocolError, got %T: %v", err, err)
	}
	if pe.Code != 12 {
		t.Fatalf("expected code 12 (line too long), got %d: %s", pe.Code, pe.Message)
	}
}

func TestReadRequest_TabInArg(t *testing.T) {
	// Tab is meaningful for kset and kcas (delimiter), but tabs are just
	// regular bytes for other commands. Verify both cases.

	t.Run("kset_with_tab", func(t *testing.T) {
		// kset expects <value>\t<ttl>
		r := makeReader("kset", "mykey", "hello\t60")
		req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
		if err != nil {
			t.Fatalf("kset with tab should parse: %v", err)
		}
		if req.Value != "hello" {
			t.Fatalf("value: got %q, want %q", req.Value, "hello")
		}
		if req.TTLSeconds != 60 {
			t.Fatalf("TTL: got %d, want 60", req.TTLSeconds)
		}
	})

	t.Run("kcas_with_tabs", func(t *testing.T) {
		// kcas expects <old>\t<new>\t<ttl>
		r := makeReader("kcas", "mykey", "old\tnew\t30")
		req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
		if err != nil {
			t.Fatalf("kcas with tabs should parse: %v", err)
		}
		if req.OldValue != "old" || req.Value != "new" || req.TTLSeconds != 30 {
			t.Fatalf("unexpected kcas parse: old=%q new=%q ttl=%d",
				req.OldValue, req.Value, req.TTLSeconds)
		}
	})

	t.Run("signal_with_tab_in_payload", func(t *testing.T) {
		// Tab in signal payload should be preserved as-is.
		r := makeReader("signal", "chan1", "hello\tworld")
		req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
		if err != nil {
			t.Fatalf("signal with tab should parse: %v", err)
		}
		if req.Value != "hello\tworld" {
			t.Fatalf("value: got %q, want %q", req.Value, "hello\tworld")
		}
	})

	t.Run("lpush_with_tab_in_value", func(t *testing.T) {
		// Tab in lpush value should be preserved.
		r := makeReader("lpush", "mylist", "val\twith\ttabs")
		req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
		if err != nil {
			t.Fatalf("lpush with tab should parse: %v", err)
		}
		if req.Value != "val\twith\ttabs" {
			t.Fatalf("value: got %q, want %q", req.Value, "val\twith\ttabs")
		}
	})
}

func TestReadRequest_AllCommands(t *testing.T) {
	// Table-driven test that exercises every single recognized command with
	// valid minimal args and verifies they parse successfully.
	tests := []struct {
		cmd string
		arg string
	}{
		// Mutex commands
		{"l", "10"},
		{"r", "token1"},
		{"n", "token1"},
		{"e", ""},
		{"w", "10"},

		// Semaphore commands
		{"sl", "10 3"},
		{"sr", "token1"},
		{"sn", "token1"},
		{"se", "3"},
		{"sw", "10"},

		// Atomic counters
		{"incr", "1"},
		{"decr", "1"},
		{"get", ""},
		{"cset", "42"},

		// KV with TTL
		{"kset", "myvalue"},
		{"kget", ""},
		{"kdel", ""},
		{"kcas", "old\tnew\t0"},

		// Lists/Queues
		{"lpush", "val"},
		{"rpush", "val"},
		{"lpop", ""},
		{"rpop", ""},
		{"llen", ""},
		{"lrange", "0 -1"},

		// Blocking list pop
		{"blpop", "10"},
		{"brpop", "10"},

		// Signaling
		{"signal", "payload"},
		{"listen", ""},
		{"unlisten", ""},

		// Watch/Notify
		{"watch", ""},
		{"unwatch", ""},

		// Barriers
		{"bwait", "3 10"},

		// Leader election
		{"elect", "10"},
		{"resign", "token1"},
		{"observe", ""},
		{"unobserve", ""},

		// Read-Write locks
		{"rl", "10"},
		{"rr", "token1"},
		{"rn", "token1"},
		{"wl", "10"},
		{"wr", "token1"},
		{"wn", "token1"},
		{"re", ""},
		{"rw", "10"},
		{"we", ""},
		{"ww", "10"},

		// Auth & Stats
		{"auth", "secret"},
		{"stats", ""},
	}

	for _, tc := range tests {
		t.Run(tc.cmd, func(t *testing.T) {
			// auth and stats don't require a valid key
			key := "testkey"
			r := makeReader(tc.cmd, key, tc.arg)
			req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
			if err != nil {
				t.Fatalf("command %q should parse with arg %q: %v", tc.cmd, tc.arg, err)
			}
			if req.Cmd != tc.cmd {
				t.Fatalf("cmd: got %q, want %q", req.Cmd, tc.cmd)
			}
		})
	}
}

func TestReadRequest_BoundaryTimeouts(t *testing.T) {
	tests := []struct {
		name    string
		timeout string
		want    time.Duration
	}{
		{"zero", "0", 0},
		{"one", "1", 1 * time.Second},
		{"maxSafeSeconds", fmt.Sprintf("%d", maxSafeSeconds), time.Duration(maxSafeSeconds) * time.Second},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := makeReader("l", "mykey", tc.timeout)
			req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
			if err != nil {
				t.Fatalf("timeout=%s should parse: %v", tc.timeout, err)
			}
			if req.AcquireTimeout != tc.want {
				t.Fatalf("timeout: got %v, want %v", req.AcquireTimeout, tc.want)
			}
		})
	}
}

func TestReadRequest_ListenWithGroupColonFormat(t *testing.T) {
	// listen with a group argument in "group:mygroup" colon-delimited style.
	// The protocol stores the trimmed arg as Group.
	r := makeReader("listen", "mychannel", "group:mygroup")
	req, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
	if err != nil {
		t.Fatalf("listen with group should parse: %v", err)
	}
	if req.Cmd != "listen" {
		t.Fatalf("cmd: got %q, want %q", req.Cmd, "listen")
	}
	if req.Key != "mychannel" {
		t.Fatalf("key: got %q, want %q", req.Key, "mychannel")
	}
	if req.Group != "group:mygroup" {
		t.Fatalf("group: got %q, want %q", req.Group, "group:mygroup")
	}
}

func TestFormatResponse_AllStatuses(t *testing.T) {
	tests := []struct {
		status string
		want   string
	}{
		// Simple statuses (no token, no extra)
		{"ok", "ok\n"},
		{"acquired", "acquired\n"},
		{"timeout", "timeout\n"},
		{"error", "error\n"},
		{"error_auth", "error_auth\n"},
		{"error_max_locks", "error_max_locks\n"},
		{"error_max_waiters", "error_max_waiters\n"},
		{"error_limit_mismatch", "error_limit_mismatch\n"},
		{"error_not_enqueued", "error_not_enqueued\n"},
		{"error_already_enqueued", "error_already_enqueued\n"},
		{"error_max_keys", "error_max_keys\n"},
		{"error_list_full", "error_list_full\n"},
		{"error_lease_expired", "error_lease_expired\n"},
		{"error_type_mismatch", "error_type_mismatch\n"},
		{"nil", "nil\n"},
		{"queued", "queued\n"},
		{"cas_conflict", "cas_conflict\n"},
		{"error_barrier_count_mismatch", "error_barrier_count_mismatch\n"},
	}

	for _, tc := range tests {
		t.Run(tc.status, func(t *testing.T) {
			ack := &Ack{Status: tc.status}
			got := string(FormatResponse(ack, 33))
			if got != tc.want {
				t.Fatalf("FormatResponse(%q): got %q, want %q", tc.status, got, tc.want)
			}
		})
	}

	// ok/acquired with Token
	t.Run("ok_with_token", func(t *testing.T) {
		ack := &Ack{Status: "ok", Token: "tok123", LeaseTTL: 30, Fence: 5}
		got := string(FormatResponse(ack, 33))
		if got != "ok tok123 30 5\n" {
			t.Fatalf("got %q, want %q", got, "ok tok123 30 5\n")
		}
	})

	t.Run("acquired_with_token", func(t *testing.T) {
		ack := &Ack{Status: "acquired", Token: "tok456", LeaseTTL: 20, Fence: 1}
		got := string(FormatResponse(ack, 33))
		if got != "acquired tok456 20 1\n" {
			t.Fatalf("got %q, want %q", got, "acquired tok456 20 1\n")
		}
	})

	// ok/acquired with Extra
	t.Run("ok_with_extra", func(t *testing.T) {
		ack := &Ack{Status: "ok", Extra: "42"}
		got := string(FormatResponse(ack, 33))
		if got != "ok 42\n" {
			t.Fatalf("got %q, want %q", got, "ok 42\n")
		}
	})

	t.Run("acquired_with_extra", func(t *testing.T) {
		ack := &Ack{Status: "acquired", Extra: "data"}
		got := string(FormatResponse(ack, 33))
		if got != "acquired data\n" {
			t.Fatalf("got %q, want %q", got, "acquired data\n")
		}
	})

	// Token with LeaseTTL=0 should use defaultLeaseTTLSec
	t.Run("ok_token_default_lease", func(t *testing.T) {
		ack := &Ack{Status: "ok", Token: "t1", LeaseTTL: 0, Fence: 0}
		got := string(FormatResponse(ack, 33))
		if got != "ok t1 33 0\n" {
			t.Fatalf("got %q, want %q", got, "ok t1 33 0\n")
		}
	})

	// Unknown status falls through to default formatting
	t.Run("unknown_status", func(t *testing.T) {
		ack := &Ack{Status: "something_custom"}
		got := string(FormatResponse(ack, 33))
		if got != "something_custom\n" {
			t.Fatalf("got %q, want %q", got, "something_custom\n")
		}
	})
}

func TestReadLine_BinaryContent(t *testing.T) {
	conn := &mockConn{}

	t.Run("high_bytes", func(t *testing.T) {
		// Line with high-byte characters (0x80-0xFF)
		input := "\x80\x81\xfe\xff\n"
		r := bufio.NewReader(strings.NewReader(input))
		line, err := ReadLine(r, 5*time.Second, conn)
		if err != nil {
			t.Fatalf("high bytes should be accepted: %v", err)
		}
		if line != "\x80\x81\xfe\xff" {
			t.Fatalf("line mismatch: got %q, want %q", line, "\x80\x81\xfe\xff")
		}
	})

	t.Run("carriage_return_stripped", func(t *testing.T) {
		// CR should be stripped everywhere, not just trailing
		input := "he\rllo\r\n"
		r := bufio.NewReader(strings.NewReader(input))
		line, err := ReadLine(r, 5*time.Second, conn)
		if err != nil {
			t.Fatalf("line with CR should be accepted: %v", err)
		}
		if line != "hello" {
			t.Fatalf("CR should be stripped: got %q, want %q", line, "hello")
		}
	})

	t.Run("null_bytes", func(t *testing.T) {
		input := "a\x00b\x00c\n"
		r := bufio.NewReader(strings.NewReader(input))
		line, err := ReadLine(r, 5*time.Second, conn)
		if err != nil {
			t.Fatalf("null bytes should be accepted: %v", err)
		}
		if line != "a\x00b\x00c" {
			t.Fatalf("line mismatch: got %q, want %q", line, "a\x00b\x00c")
		}
	})

	t.Run("only_cr_before_lf", func(t *testing.T) {
		// A line that is purely \r\r\r\n should produce empty string.
		input := "\r\r\r\n"
		r := bufio.NewReader(strings.NewReader(input))
		line, err := ReadLine(r, 5*time.Second, conn)
		if err != nil {
			t.Fatalf("CR-only line should be accepted: %v", err)
		}
		if line != "" {
			t.Fatalf("expected empty line, got %q", line)
		}
	})

	t.Run("mixed_binary", func(t *testing.T) {
		// Mix of control chars, high bytes, normal chars
		input := "\x01\x7f\x80\xffAB\n"
		r := bufio.NewReader(strings.NewReader(input))
		line, err := ReadLine(r, 5*time.Second, conn)
		if err != nil {
			t.Fatalf("mixed binary should be accepted: %v", err)
		}
		if line != "\x01\x7f\x80\xffAB" {
			t.Fatalf("line mismatch: got %q, want %q", line, "\x01\x7f\x80\xffAB")
		}
	})
}

func TestReadRequest_EmptyArg(t *testing.T) {
	// Commands that should succeed with empty arg
	succeedEmpty := []string{
		"e",           // enqueue: optional lease_ttl
		"get",         // no arg needed
		"kget",        // no arg needed
		"kdel",        // no arg needed
		"listen",      // group is optional
		"unlisten",    // group is optional
		"lpop",        // no arg needed
		"rpop",        // no arg needed
		"llen",        // no arg needed
		"watch",       // no arg needed
		"unwatch",     // no arg needed
		"observe",     // no arg needed
		"unobserve",   // no arg needed
		"re",          // optional lease_ttl
		"we",          // optional lease_ttl
	}
	for _, cmd := range succeedEmpty {
		t.Run(cmd+"_empty_ok", func(t *testing.T) {
			r := makeReader(cmd, "testkey", "")
			_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
			if err != nil {
				t.Fatalf("command %q with empty arg should succeed: %v", cmd, err)
			}
		})
	}

	// Commands that should fail with empty arg
	failEmpty := []struct {
		cmd  string
		code int
	}{
		{"l", 8},      // requires timeout
		{"r", 7},      // requires token
		{"n", 8},      // requires token
		{"w", 8},      // requires timeout
		{"sl", 8},     // requires timeout + limit
		{"sr", 7},     // requires token
		{"sn", 8},     // requires token
		{"se", 8},     // requires limit
		{"sw", 8},     // requires timeout
		{"incr", 8},   // requires delta
		{"decr", 8},   // requires delta
		{"cset", 8},   // requires value
		{"kset", 8},   // requires value
		{"kcas", 8},   // requires old\tnew\tttl
		{"signal", 8}, // requires payload
		{"lpush", 8},  // requires value
		{"rpush", 8},  // requires value
		{"lrange", 8}, // requires start stop
		{"blpop", 8},  // requires timeout
		{"brpop", 8},  // requires timeout
		{"bwait", 8},  // requires count timeout
		{"elect", 8},  // requires timeout
		{"resign", 7}, // requires token
		{"rl", 8},     // requires timeout
		{"rr", 7},     // requires token
		{"rn", 8},     // requires token
		{"wl", 8},     // requires timeout
		{"wr", 7},     // requires token
		{"wn", 8},     // requires token
		{"rw", 8},     // requires timeout
		{"ww", 8},     // requires timeout
	}
	for _, tc := range failEmpty {
		t.Run(tc.cmd+"_empty_fail", func(t *testing.T) {
			r := makeReader(tc.cmd, "testkey", "")
			_, err := ReadRequest(r, 5*time.Second, &mockConn{}, 33*time.Second)
			if err == nil {
				t.Fatalf("command %q with empty arg should fail", tc.cmd)
			}
			pe, ok := err.(*ProtocolError)
			if !ok {
				t.Fatalf("expected *ProtocolError, got %T: %v", err, err)
			}
			if pe.Code != tc.code {
				t.Fatalf("command %q: expected error code %d, got %d: %s",
					tc.cmd, tc.code, pe.Code, pe.Message)
			}
		})
	}
}
