package client

import (
	"context"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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
			if err := validateRenewConfig(tc.leaseTTL, tc.renewRatio); err == nil {
				t.Fatal("expected validation error")
			}
		})
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
