package protocol

import (
	"bufio"
	"bytes"
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

// fakeConn satisfies net.Conn enough for ReadRequest's deadline calls.
type fakeConn struct {
	net.Conn
	readDeadline time.Time
}

func (c *fakeConn) SetReadDeadline(t time.Time) error { c.readDeadline = t; return nil }
func (c *fakeConn) SetDeadline(t time.Time) error     { c.readDeadline = t; return nil }
func (c *fakeConn) Close() error                      { return nil }

func newReader(s string) (*bufio.Reader, *fakeConn) {
	return bufio.NewReader(strings.NewReader(s)), &fakeConn{}
}

func TestParseRequest_Ping(t *testing.T) {
	req, err := parseRequest("ping", "_", "", time.Second)
	if err != nil {
		t.Fatalf("ping: %v", err)
	}
	if req.Cmd != CmdPing {
		t.Errorf("got cmd %q, want ping", req.Cmd)
	}
}

func TestParseRequest_Stats(t *testing.T) {
	req, err := parseRequest("stats", "_", "", time.Second)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if req.Cmd != CmdStats {
		t.Errorf("got cmd %q, want stats", req.Cmd)
	}
}

func TestParseRequest_Auth(t *testing.T) {
	req, err := parseRequest("auth", "_", "  hunter2  ", time.Second)
	if err != nil {
		t.Fatalf("auth: %v", err)
	}
	if req.AuthToken != "hunter2" {
		t.Errorf("got token %q, want hunter2 (trimmed)", req.AuthToken)
	}
}

func TestParseRequest_Acquire(t *testing.T) {
	req, err := parseRequest("l", "key1", "10", 33*time.Second)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if req.Cmd != CmdAcquire || req.Key != "key1" {
		t.Errorf("got %+v", req)
	}
	if req.AcquireTimeout != 10*time.Second {
		t.Errorf("got timeout %v", req.AcquireTimeout)
	}
	if req.LeaseTTL != 33*time.Second {
		t.Errorf("got lease %v, want default", req.LeaseTTL)
	}
}

func TestParseRequest_Acquire_WithLease(t *testing.T) {
	req, err := parseRequest("l", "key1", "10 60", 33*time.Second)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if req.LeaseTTL != 60*time.Second {
		t.Errorf("got lease %v, want 60s", req.LeaseTTL)
	}
}

func TestParseRequest_SemAcquire(t *testing.T) {
	req, err := parseRequest("sl", "k", "5 3", 33*time.Second)
	if err != nil {
		t.Fatalf("sl: %v", err)
	}
	if req.Limit != 3 {
		t.Errorf("got limit %d, want 3", req.Limit)
	}
}

func TestParseRequest_SemAcquire_WithLease(t *testing.T) {
	req, err := parseRequest("sl", "k", "5 3 60", 33*time.Second)
	if err != nil {
		t.Fatalf("sl: %v", err)
	}
	if req.Limit != 3 || req.LeaseTTL != 60*time.Second {
		t.Errorf("got %+v", req)
	}
}

func TestParseRequest_BadKey(t *testing.T) {
	_, err := parseRequest("l", "", "1", time.Second)
	if err == nil {
		t.Fatal("expected error for empty key")
	}
	var pe *ProtocolError
	if !errors.As(err, &pe) || pe.Code != ErrCodeInvalidKey {
		t.Errorf("got %v, want code %d", err, ErrCodeInvalidKey)
	}
}

func TestParseRequest_KeyWithSpace(t *testing.T) {
	_, err := parseRequest("l", "bad key", "1", time.Second)
	if err == nil {
		t.Fatal("expected error for whitespace key")
	}
}

func TestParseRequest_Release(t *testing.T) {
	req, err := parseRequest("r", "k", "abc123", time.Second)
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if req.Token != "abc123" {
		t.Errorf("got %q", req.Token)
	}
}

func TestParseRequest_Release_EmptyToken(t *testing.T) {
	_, err := parseRequest("r", "k", "  ", time.Second)
	if err == nil {
		t.Fatal("expected error for empty token")
	}
}

func TestParseRequest_Renew(t *testing.T) {
	req, err := parseRequest("n", "k", "abc 60", 33*time.Second)
	if err != nil {
		t.Fatalf("renew: %v", err)
	}
	if req.Token != "abc" || req.LeaseTTL != 60*time.Second {
		t.Errorf("got %+v", req)
	}
}

func TestParseRequest_Enqueue(t *testing.T) {
	req, err := parseRequest("e", "k", "", 33*time.Second)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if req.LeaseTTL != 33*time.Second {
		t.Errorf("got lease %v", req.LeaseTTL)
	}
}

func TestParseRequest_Enqueue_WithLease(t *testing.T) {
	req, err := parseRequest("e", "k", "60", 33*time.Second)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if req.LeaseTTL != 60*time.Second {
		t.Errorf("got lease %v, want 60s", req.LeaseTTL)
	}
}

func TestParseRequest_SemEnqueue(t *testing.T) {
	req, err := parseRequest("se", "k", "3", 33*time.Second)
	if err != nil {
		t.Fatalf("se: %v", err)
	}
	if req.Limit != 3 {
		t.Errorf("got %+v", req)
	}
}

func TestParseRequest_Wait(t *testing.T) {
	req, err := parseRequest("w", "k", "5", 33*time.Second)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if req.AcquireTimeout != 5*time.Second {
		t.Errorf("got %v", req.AcquireTimeout)
	}
}

func TestParseRequest_SemRelease(t *testing.T) {
	req, err := parseRequest("sr", "k", "tok", time.Second)
	if err != nil {
		t.Fatalf("sr: %v", err)
	}
	if req.Token != "tok" {
		t.Errorf("got %q", req.Token)
	}
}

func TestParseRequest_SemRenew(t *testing.T) {
	req, err := parseRequest("sn", "k", "tok 60", time.Second)
	if err != nil {
		t.Fatalf("sn: %v", err)
	}
	if req.LeaseTTL != 60*time.Second {
		t.Errorf("got %v", req.LeaseTTL)
	}
}

func TestParseRequest_SemWait(t *testing.T) {
	req, err := parseRequest("sw", "k", "5", time.Second)
	if err != nil {
		t.Fatalf("sw: %v", err)
	}
	if req.AcquireTimeout != 5*time.Second {
		t.Errorf("got %v", req.AcquireTimeout)
	}
}

func TestParseRequest_BadCmd(t *testing.T) {
	_, err := parseRequest("nope", "k", "", time.Second)
	if err == nil {
		t.Fatal("expected error")
	}
	var pe *ProtocolError
	if !errors.As(err, &pe) || pe.Code != ErrCodeInvalidCmd {
		t.Errorf("got %v", err)
	}
}

func TestParseRequest_Acquire_NegativeTimeout(t *testing.T) {
	_, err := parseRequest("l", "k", "-1", time.Second)
	if err == nil {
		t.Fatal("expected error for negative timeout")
	}
}

func TestParseRequest_Acquire_NonNumeric(t *testing.T) {
	_, err := parseRequest("l", "k", "abc", time.Second)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestParseRequest_BadLimit(t *testing.T) {
	_, err := parseRequest("sl", "k", "5 0", time.Second)
	if err == nil {
		t.Fatal("expected error for limit=0")
	}
}

func TestReadRequest_FullFrame(t *testing.T) {
	r, conn := newReader("l\nkey1\n10\n")
	req, err := ReadRequest(r, time.Second, conn, 33*time.Second)
	if err != nil {
		t.Fatalf("ReadRequest: %v", err)
	}
	if req.Cmd != CmdAcquire || req.Key != "key1" {
		t.Errorf("got %+v", req)
	}
}

func TestReadRequest_StripsCR(t *testing.T) {
	r, conn := newReader("l\r\nkey1\r\n10\r\n")
	req, err := ReadRequest(r, time.Second, conn, 33*time.Second)
	if err != nil {
		t.Fatalf("ReadRequest: %v", err)
	}
	if req.Key != "key1" {
		t.Errorf("got %q", req.Key)
	}
}

func TestReadRequest_TruncatedFrame(t *testing.T) {
	r, conn := newReader("l\nkey1\n")
	_, err := ReadRequest(r, time.Second, conn, 33*time.Second)
	if err == nil {
		t.Fatal("expected error on truncated frame")
	}
	var pe *ProtocolError
	if !errors.As(err, &pe) || pe.Code != ErrCodeDisconnect {
		t.Errorf("got %v, want disconnect", err)
	}
}

func TestReadRequest_LineTooLong(t *testing.T) {
	long := strings.Repeat("x", MaxLineBytes+10)
	r, conn := newReader(long + "\nkey\n10\n")
	_, err := ReadRequest(r, time.Second, conn, 33*time.Second)
	if err == nil {
		t.Fatal("expected error")
	}
	var pe *ProtocolError
	if !errors.As(err, &pe) || pe.Code != ErrCodeLineTooLong {
		t.Errorf("got %v, want line too long", err)
	}
}

func TestReadRequest_AuthAcceptsLargePayload(t *testing.T) {
	long := strings.Repeat("x", MaxLineBytes+100)
	r, conn := newReader("auth\n_\n" + long + "\n")
	req, err := ReadRequest(r, time.Second, conn, 33*time.Second)
	if err != nil {
		t.Fatalf("auth: %v", err)
	}
	if req.AuthToken != long {
		t.Errorf("auth token mismatch")
	}
}

func TestFormatResponse_OK(t *testing.T) {
	got := FormatResponse(&Ack{Status: StatusOK}, 33)
	if !bytes.Equal(got, []byte("ok\n")) {
		t.Errorf("got %q", got)
	}
}

func TestFormatResponse_OKWithToken(t *testing.T) {
	got := FormatResponse(&Ack{Status: StatusOK, Token: "abc", LeaseTTL: 60}, 33)
	if !bytes.Equal(got, []byte("ok abc 60\n")) {
		t.Errorf("got %q", got)
	}
}

func TestFormatResponse_AcquiredWithToken(t *testing.T) {
	got := FormatResponse(&Ack{Status: StatusAcquired, Token: "abc", LeaseTTL: 60}, 33)
	if !bytes.Equal(got, []byte("acquired abc 60\n")) {
		t.Errorf("got %q", got)
	}
}

func TestFormatResponse_OKWithToken_DefaultsLease(t *testing.T) {
	got := FormatResponse(&Ack{Status: StatusOK, Token: "abc"}, 33)
	if !bytes.Equal(got, []byte("ok abc 33\n")) {
		t.Errorf("got %q", got)
	}
}

func TestFormatResponse_OKWithExtra(t *testing.T) {
	got := FormatResponse(&Ack{Status: StatusOK, Extra: "10"}, 33)
	if !bytes.Equal(got, []byte("ok 10\n")) {
		t.Errorf("got %q", got)
	}
}

func TestFormatResponse_AllErrors(t *testing.T) {
	cases := []struct {
		status string
		want   string
	}{
		{StatusTimeout, "timeout\n"},
		{StatusError, "error\n"},
		{StatusErrorAuth, "error_auth\n"},
		{StatusErrorMaxLocks, "error_max_locks\n"},
		{StatusErrorMaxWaiters, "error_max_waiters\n"},
		{StatusErrorLimitMismatch, "error_limit_mismatch\n"},
		{StatusErrorNotEnqueued, "error_not_enqueued\n"},
		{StatusErrorAlreadyEnqueued, "error_already_enqueued\n"},
		{StatusErrorLeaseExpired, "error_lease_expired\n"},
		{StatusErrorDraining, "error_draining\n"},
		{StatusQueued, "queued\n"},
	}
	for _, c := range cases {
		got := FormatResponse(&Ack{Status: c.status}, 33)
		if string(got) != c.want {
			t.Errorf("status %q: got %q, want %q", c.status, got, c.want)
		}
	}
}
