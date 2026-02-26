package protocol

import (
	"bufio"
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
	if !ok || pe.Code != 6 {
		t.Fatalf("expected code 6, got %v", err)
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
	if !ok || pe.Code != 6 {
		t.Fatalf("expected code 6, got %v", err)
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
	if !ok || pe.Code != 6 {
		t.Fatalf("expected code 6, got %v", err)
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
