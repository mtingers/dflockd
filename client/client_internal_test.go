package client

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mtingers/dflockd/internal/protocol"
)

type closeCountingConn struct {
	closes atomic.Int32
	once   sync.Once
}

func (c *closeCountingConn) Read([]byte) (int, error)         { return 0, net.ErrClosed }
func (c *closeCountingConn) Write([]byte) (int, error)        { return 0, net.ErrClosed }
func (c *closeCountingConn) LocalAddr() net.Addr              { return dummyAddr("local") }
func (c *closeCountingConn) RemoteAddr() net.Addr             { return dummyAddr("remote") }
func (c *closeCountingConn) SetDeadline(time.Time) error      { return nil }
func (c *closeCountingConn) SetReadDeadline(time.Time) error  { return nil }
func (c *closeCountingConn) SetWriteDeadline(time.Time) error { return nil }

func (c *closeCountingConn) Close() error {
	c.once.Do(func() {
		c.closes.Add(1)
	})
	return nil
}

type dummyAddr string

func (a dummyAddr) Network() string { return string(a) }
func (a dummyAddr) String() string  { return string(a) }

func TestCloseConnOnContextDoneStopPreventsLateClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	conn := &closeCountingConn{}

	stop := closeConnOnContextDone(ctx, conn)
	stop()
	cancel()
	time.Sleep(20 * time.Millisecond)

	if got := conn.closes.Load(); got != 0 {
		t.Fatalf("connection closed after watcher stopped: got %d closes, want 0", got)
	}
}

func TestCloseConnOnContextDoneCancelCloses(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	conn := &closeCountingConn{}

	stop := closeConnOnContextDone(ctx, conn)
	cancel()
	deadline := time.Now().Add(time.Second)
	for conn.closes.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	stop()

	if got := conn.closes.Load(); got != 1 {
		t.Fatalf("connection closes: got %d want 1", got)
	}
}

func TestParseOptionsRejectsInvalidLeaseTTL(t *testing.T) {
	if _, err := parseOptions([]Option{WithLeaseTTL(-1)}); err == nil {
		t.Fatal("expected negative lease TTL to fail")
	}

	tooLarge := maxProtocolSeconds + 1
	if int64(int(tooLarge)) != tooLarge {
		t.Skip("int cannot represent protocol overflow boundary on this platform")
	}
	if _, err := parseOptions([]Option{WithLeaseTTL(int(tooLarge))}); err == nil {
		t.Fatal("expected oversized lease TTL to fail")
	}
}

func TestValidateRenewConfigRejectsInvalidValues(t *testing.T) {
	cases := []struct {
		name       string
		leaseTTL   int
		renewRatio float64
	}{
		{name: "negative lease", leaseTTL: -1, renewRatio: 0.5},
		{name: "negative ratio", leaseTTL: 0, renewRatio: -0.1},
		{name: "one ratio", leaseTTL: 0, renewRatio: 1},
		{name: "nan ratio", leaseTTL: 0, renewRatio: math.NaN()},
	}

	tooLarge := maxProtocolSeconds + 1
	if int64(int(tooLarge)) == tooLarge {
		cases = append(cases, struct {
			name       string
			leaseTTL   int
			renewRatio float64
		}{name: "oversized lease", leaseTTL: int(tooLarge), renewRatio: 0.5})
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateRenewConfig(tc.leaseTTL, tc.renewRatio, 0); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestValidateRenewConfigRejectsBadJitter(t *testing.T) {
	for _, jitter := range []float64{-0.1, 1, math.NaN()} {
		if err := validateRenewConfig(0, 0.5, jitter); err == nil {
			t.Fatalf("expected validation error for jitter %v", jitter)
		}
	}
}

func TestJitteredRenewIntervalMovesEarlierOnly(t *testing.T) {
	base := 10 * time.Second
	for i := 0; i < 100; i++ {
		got := jitteredRenewInterval(base, 0.25)
		if got <= 0 || got > base {
			t.Fatalf("jittered interval %s outside (0, %s]", got, base)
		}
		if got < 7500*time.Millisecond {
			t.Fatalf("jittered interval %s earlier than 25%% bound", got)
		}
	}
}

func TestParseDrainingResponse(t *testing.T) {
	if _, _, err := parseAcquireResponse("error_draining"); !errors.Is(err, ErrDraining) {
		t.Fatalf("parseAcquireResponse error = %v, want ErrDraining", err)
	}
	if _, _, err := parseSemAcquireResponse("error_draining"); !errors.Is(err, ErrDraining) {
		t.Fatalf("parseSemAcquireResponse error = %v, want ErrDraining", err)
	}
}

func TestTimeoutArgRejectsOverflowAfterRounding(t *testing.T) {
	if _, err := timeoutArg(time.Duration(math.MaxInt64)); err == nil {
		t.Fatal("expected max duration rounded to whole seconds to exceed protocol max")
	}

	arg, err := timeoutArg(time.Duration(maxProtocolSeconds) * time.Second)
	if err != nil {
		t.Fatalf("max protocol timeout rejected: %v", err)
	}
	if arg != "9223372036" {
		t.Fatalf("arg: got %q", arg)
	}
}

func TestClientRejectsProtocolLineOverflow(t *testing.T) {
	long := strings.Repeat("x", protocol.MaxLineBytes+1)

	if err := validateKey(long); err == nil {
		t.Fatal("expected oversized key to fail")
	}
	if err := Release(&Conn{}, "k", long); err == nil {
		t.Fatal("expected oversized release token to fail")
	}
	if _, err := Renew(&Conn{}, "k", strings.Repeat("x", protocol.MaxLineBytes), WithLeaseTTL(1)); err == nil {
		t.Fatal("expected oversized renew argument to fail")
	}
	if _, err := Emit(&Conn{}, "events.large", strings.Repeat("x", protocol.MaxSignalPayloadBytes("events.large")+1)); err == nil {
		t.Fatal("expected oversized signal payload to fail")
	}
}

type errOnlyCanceledContext struct {
	context.Context
}

func (errOnlyCanceledContext) Done() <-chan struct{} { return nil }
func (errOnlyCanceledContext) Err() error            { return context.Canceled }

func startGrantCleanupServer(t *testing.T, acquireCmd, releaseCmd string) (string, <-chan string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	released := make(chan string, 1)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go serveGrantCleanupConn(conn, acquireCmd, releaseCmd, released)
		}
	}()
	t.Cleanup(func() {
		ln.Close()
	})
	return ln.Addr().String(), released
}

func serveGrantCleanupConn(conn net.Conn, acquireCmd, releaseCmd string, released chan<- string) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	for {
		cmd, err := readFakeLine(r)
		if err != nil {
			return
		}
		if _, err := readFakeLine(r); err != nil { // key
			return
		}
		arg, err := readFakeLine(r)
		if err != nil {
			return
		}
		switch cmd {
		case acquireCmd:
			fmt.Fprint(conn, "ok abandoned-token 33\n")
		case releaseCmd:
			released <- arg
			fmt.Fprint(conn, "ok\n")
			return
		default:
			fmt.Fprint(conn, "error\n")
		}
	}
}

func readFakeLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

func expectReleasedToken(t *testing.T, released <-chan string) {
	t.Helper()
	select {
	case got := <-released:
		if got != "abandoned-token" {
			t.Fatalf("released token: got %q want abandoned-token", got)
		}
	case <-time.After(time.Second):
		t.Fatal("abandoned grant was not released")
	}
}

func TestLockAcquireContextCanceledAfterGrantReleasesToken(t *testing.T) {
	addr, released := startGrantCleanupServer(t, "l", "r")
	l := &Lock{
		Key:            "abandoned",
		Servers:        []string{addr},
		AcquireTimeout: time.Second,
	}

	ok, err := l.Acquire(errOnlyCanceledContext{Context: context.Background()})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err: got %v want context.Canceled", err)
	}
	if ok {
		t.Fatal("acquire should report false after context cancellation")
	}
	if tok := l.Token(); tok != "" {
		t.Fatalf("lock token after cancellation: got %q want empty", tok)
	}
	expectReleasedToken(t, released)
}

func TestSemaphoreAcquireContextCanceledAfterGrantReleasesToken(t *testing.T) {
	addr, released := startGrantCleanupServer(t, "sl", "sr")
	s := &Semaphore{
		Key:            "abandoned",
		Limit:          2,
		Servers:        []string{addr},
		AcquireTimeout: time.Second,
	}

	ok, err := s.Acquire(errOnlyCanceledContext{Context: context.Background()})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err: got %v want context.Canceled", err)
	}
	if ok {
		t.Fatal("acquire should report false after context cancellation")
	}
	if tok := s.Token(); tok != "" {
		t.Fatalf("semaphore token after cancellation: got %q want empty", tok)
	}
	expectReleasedToken(t, released)
}

func TestCleanupAbandonedGrantFallsBackToFreshConnection(t *testing.T) {
	addr, released := startGrantCleanupServer(t, "unused", "r")
	clientSide, serverSide := net.Pipe()
	clientSide.Close()
	serverSide.Close()
	closedConn := &Conn{conn: clientSide, reader: bufio.NewReader(clientSide)}

	cleanupAbandonedGrant(closedConn, addr, nil, "", "abandoned", "abandoned-token", Release)

	expectReleasedToken(t, released)
}
