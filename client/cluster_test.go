package client

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeServer responds to one TCP frame with a configurable reply line,
// then closes. Used to drive client parsers without spinning up the
// full dflockd server.
type fakeServer struct {
	addr   string
	listen net.Listener
	reply  string
	wg     sync.WaitGroup
}

func startFakeServer(t *testing.T, reply string) *fakeServer {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	fs := &fakeServer{addr: l.Addr().String(), listen: l, reply: reply}
	fs.wg.Add(1)
	go fs.run()
	return fs
}

func (fs *fakeServer) run() {
	defer fs.wg.Done()
	conn, err := fs.listen.Accept()
	if err != nil {
		return
	}
	defer conn.Close()
	// Read the three lines of one request (ignore content).
	buf := make([]byte, 256)
	newlines := 0
	for newlines < 3 {
		n, err := conn.Read(buf)
		if err != nil {
			return
		}
		for _, b := range buf[:n] {
			if b == '\n' {
				newlines++
			}
		}
	}
	_, _ = conn.Write([]byte(fs.reply + "\n"))
}

func (fs *fakeServer) close() {
	fs.listen.Close()
	fs.wg.Wait()
}

func TestNotLeaderErrorMessage(t *testing.T) {
	e := &NotLeaderError{}
	if !strings.Contains(e.Error(), "no leader") {
		t.Fatalf("empty leader = %q", e.Error())
	}
	e2 := &NotLeaderError{Leader: "host:42"}
	if !strings.Contains(e2.Error(), "host:42") {
		t.Fatalf("with leader = %q", e2.Error())
	}
}

func TestNotLeaderFromRespParses(t *testing.T) {
	cases := map[string]string{
		"error_not_leader":               "",
		"error_not_leader host:42":       "host:42",
		"error_not_leader leader-3:6388": "leader-3:6388",
	}
	for resp, wantLeader := range cases {
		err := notLeaderFromResp(resp)
		nle, ok := err.(*NotLeaderError)
		if !ok {
			t.Fatalf("resp %q -> %T, want *NotLeaderError", resp, err)
		}
		if nle.Leader != wantLeader {
			t.Fatalf("resp %q -> Leader=%q, want %q", resp, nle.Leader, wantLeader)
		}
	}
	if err := notLeaderFromResp("ok abc 30"); err != nil {
		t.Fatalf("non-not_leader response should not yield NotLeaderError, got %v", err)
	}
}

func TestIsNotLeaderHelperUnwrapsWrapped(t *testing.T) {
	raw := &NotLeaderError{Leader: "h:1"}
	wrapped := fmt.Errorf("acquire: %w", raw)
	var nle *NotLeaderError
	if !IsNotLeader(wrapped, &nle) {
		t.Fatalf("IsNotLeader missed a wrapped NotLeaderError")
	}
	if nle.Leader != "h:1" {
		t.Fatalf("IsNotLeader gave Leader=%q", nle.Leader)
	}
	if IsNotLeader(errors.New("unrelated"), nil) {
		t.Fatalf("IsNotLeader matched an unrelated error")
	}
}

func TestSendRecvSurfacesNotLeaderTyped(t *testing.T) {
	fs := startFakeServer(t, "error_not_leader 10.0.0.1:6388")
	defer fs.close()
	conn, err := Dial(fs.addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()
	_, _, err = Acquire(conn, "k", 1*time.Second)
	if err == nil {
		t.Fatalf("Acquire against not_leader server: want error, got nil")
	}
	var nle *NotLeaderError
	if !IsNotLeader(err, &nle) {
		t.Fatalf("Acquire err = %v, want *NotLeaderError", err)
	}
	if nle.Leader != "10.0.0.1:6388" {
		t.Fatalf("Leader = %q, want 10.0.0.1:6388", nle.Leader)
	}
}

func TestSendRecvSurfacesNotLeaderForRelease(t *testing.T) {
	fs := startFakeServer(t, "error_not_leader")
	defer fs.close()
	conn, err := Dial(fs.addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()
	err = Release(conn, "k", strings.Repeat("0", 32))
	var nle *NotLeaderError
	if !IsNotLeader(err, &nle) || nle.Leader != "" {
		t.Fatalf("Release err = %v, want *NotLeaderError{Leader=''}", err)
	}
}
