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
	if got != "ok abc 30\n" {
		t.Fatalf("got %q", got)
	}
}

func TestFormatResponse_OkDefaultLease(t *testing.T) {
	ack := &Ack{Status: "ok", Token: "abc"}
	got := string(FormatResponse(ack, 33))
	if got != "ok abc 33\n" {
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
	if got != "acquired abc 30\n" {
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
