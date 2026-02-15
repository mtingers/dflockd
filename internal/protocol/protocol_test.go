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
