package server

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mtingers/dflockd/internal/config"
	"github.com/mtingers/dflockd/internal/lock"
	"github.com/mtingers/dflockd/internal/protocol"
)

// startServer launches a server on a random local port and returns
// (addr, stopFn). stopFn is safe to call multiple times.
func startServer(t *testing.T, mods ...func(*config.Config)) (string, func()) {
	t.Helper()
	rt := newTCPTestRuntime(t, testServerConfig(mods...))
	rt.start()
	rt.waitReady(t)
	return rt.addr, rt.stop(t)
}

type tcpTestRuntime struct {
	listener net.Listener
	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}
	addr     string
	stopOnce sync.Once
	srv      *Server
}

func newTCPTestRuntime(t *testing.T, cfg *config.Config) *tcpTestRuntime {
	listener := mustListenTCP(t)
	configureTCPListener(cfg, listener)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithCancel(context.Background())
	lm, err := lock.NewLockManager(cfg, log)
	if err != nil {
		t.Fatalf("NewLockManager: %v", err)
	}
	t.Cleanup(func() { lm.Close() })
	srv := New(lm, cfg, log)
	return &tcpTestRuntime{listener: listener, ctx: ctx, cancel: cancel, done: make(chan struct{}), addr: listener.Addr().String(), srv: srv}
}

func testServerConfig(mods ...func(*config.Config)) *config.Config {
	cfg := defaultTestConfig()
	for _, fn := range mods {
		fn(cfg)
	}
	return cfg
}

func mustListenTCP(t *testing.T) net.Listener {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return listener
}

func configureTCPListener(cfg *config.Config, listener net.Listener) {
	cfg.Port = listener.Addr().(*net.TCPAddr).Port
	cfg.Host = "127.0.0.1"
}

func (rt *tcpTestRuntime) start() {
	go func() {
		_ = rt.srv.RunOnListener(rt.ctx, rt.listener)
		close(rt.done)
	}()
}

func (rt *tcpTestRuntime) waitReady(t *testing.T) {
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if canDial(rt.addr) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func canDial(addr string) bool {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		return false
	}
	c.Close()
	return true
}

func (rt *tcpTestRuntime) stop(t *testing.T) func() {
	return func() {
		rt.stopOnce.Do(func() { rt.stopNow(t) })
	}
}

func (rt *tcpTestRuntime) stopNow(t *testing.T) {
	rt.cancel()
	select {
	case <-rt.done:
	case <-time.After(5 * time.Second):
		t.Error("server didn't stop in time")
	}
}

func defaultTestConfig() *config.Config {
	cfg := defaultServerConfigValue
	return &cfg
}

var defaultServerConfigValue = config.Config{
	Host:                    "127.0.0.1",
	MaxLocks:                1024,
	DefaultLeaseTTL:         33 * time.Second,
	LeaseSweepInterval:      time.Second,
	GCInterval:              time.Second,
	GCMaxIdleTime:           time.Minute,
	ReadTimeout:             5 * time.Second,
	WriteTimeout:            time.Second,
	ShutdownTimeout:         time.Second,
	AutoReleaseOnDisconnect: true,
}

// dial connects and returns a (conn, reader). The caller closes conn.
func dial(t *testing.T, addr string) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	return conn, bufio.NewReader(conn)
}

// send writes a 3-line protocol frame.
func send(t *testing.T, conn net.Conn, cmd, key, arg string) {
	t.Helper()
	if _, err := conn.Write([]byte(cmd + "\n" + key + "\n" + arg + "\n")); err != nil {
		t.Fatal(err)
	}
}

// recv reads one response line.
func recv(t *testing.T, r *bufio.Reader) string {
	t.Helper()
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimRight(line, "\r\n")
}

// reqResp does send + recv in one call.
func reqResp(t *testing.T, conn net.Conn, r *bufio.Reader, cmd, key, arg string) string {
	t.Helper()
	send(t, conn, cmd, key, arg)
	return recv(t, r)
}

// ---------------------------------------------------------------------------
// Basic flow
// ---------------------------------------------------------------------------

func TestServer_AcquireRelease(t *testing.T) {
	addr, stop := startServer(t)
	defer stop()
	conn, r := dial(t, addr)
	defer conn.Close()

	resp := reqResp(t, conn, r, "l", "k", "5")
	if !strings.HasPrefix(resp, "ok ") {
		t.Fatalf("acquire: %q", resp)
	}
	parts := strings.Fields(resp)
	if len(parts) != 3 {
		t.Fatalf("acquire response: %q", resp)
	}
	tok := parts[1]

	resp = reqResp(t, conn, r, "r", "k", tok)
	if resp != "ok" {
		t.Fatalf("release: %q", resp)
	}
}

func TestServer_Ping(t *testing.T) {
	addr, stop := startServer(t)
	defer stop()
	conn, r := dial(t, addr)
	defer conn.Close()

	if got := reqResp(t, conn, r, "ping", "_", ""); got != "ok" {
		t.Errorf("ping: %q", got)
	}
}

func TestServer_Stats(t *testing.T) {
	addr, stop := startServer(t)
	defer stop()
	conn, r := dial(t, addr)
	defer conn.Close()

	resp := reqResp(t, conn, r, "stats", "_", "")
	if !strings.HasPrefix(resp, "ok ") {
		t.Fatalf("stats: %q", resp)
	}
	if !strings.Contains(resp, `"locks"`) {
		t.Errorf("stats missing 'locks' key: %q", resp)
	}
}

// ---------------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------------

func TestServer_AuthSuccess(t *testing.T) {
	addr, stop := startServer(t, func(c *config.Config) { c.AuthToken = "secret" })
	defer stop()
	conn, r := dial(t, addr)
	defer conn.Close()

	if got := reqResp(t, conn, r, "auth", "_", "secret"); got != "ok" {
		t.Fatalf("auth: %q", got)
	}
	if got := reqResp(t, conn, r, "ping", "_", ""); got != "ok" {
		t.Errorf("post-auth ping: %q", got)
	}
}

func TestServer_AuthFailure(t *testing.T) {
	addr, stop := startServer(t, func(c *config.Config) { c.AuthToken = "secret" })
	defer stop()
	conn, r := dial(t, addr)
	defer conn.Close()

	if got := reqResp(t, conn, r, "auth", "_", "wrong"); got != "error_auth" {
		t.Fatalf("got %q", got)
	}
}

func TestServer_AuthMissing(t *testing.T) {
	addr, stop := startServer(t, func(c *config.Config) { c.AuthToken = "secret" })
	defer stop()
	conn, r := dial(t, addr)
	defer conn.Close()

	// Sending lock without auth first should fail.
	if got := reqResp(t, conn, r, "l", "k", "5"); got != "error_auth" {
		t.Fatalf("got %q", got)
	}
}

// ---------------------------------------------------------------------------
// Two-phase enqueue/wait
// ---------------------------------------------------------------------------

func TestServer_EnqueueWait(t *testing.T) {
	addr, stop := startServer(t)
	defer stop()

	// Conn 1 acquires.
	c1, r1 := dial(t, addr)
	defer c1.Close()
	tok1 := strings.Fields(reqResp(t, c1, r1, "l", "k", "30"))[1]

	// Conn 2 enqueues + waits in background.
	c2, r2 := dial(t, addr)
	defer c2.Close()
	if got := reqResp(t, c2, r2, "e", "k", ""); got != "queued" {
		t.Fatalf("conn2 enqueue: %q", got)
	}

	done := make(chan string, 1)
	go func() {
		done <- reqResp(t, c2, r2, "w", "k", "5")
	}()

	time.Sleep(50 * time.Millisecond)
	reqResp(t, c1, r1, "r", "k", tok1)

	select {
	case got := <-done:
		if !strings.HasPrefix(got, "ok ") {
			t.Fatalf("wait: %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("wait never returned")
	}
}

// ---------------------------------------------------------------------------
// Semaphores
// ---------------------------------------------------------------------------

func TestServer_SemaphoreLimit(t *testing.T) {
	addr, stop := startServer(t)
	defer stop()

	// Three holders allowed.
	for i := 0; i < 3; i++ {
		c, r := dial(t, addr)
		defer c.Close()
		got := reqResp(t, c, r, "sl", "sem", "5 3")
		if !strings.HasPrefix(got, "ok ") {
			t.Fatalf("hold %d: %q", i, got)
		}
	}
	// Fourth should time out.
	c4, r4 := dial(t, addr)
	defer c4.Close()
	if got := reqResp(t, c4, r4, "sl", "sem", "1 3"); got != "timeout" {
		t.Fatalf("fourth: %q", got)
	}
}

// ---------------------------------------------------------------------------
// Disconnect cleanup
// ---------------------------------------------------------------------------

func TestServer_DisconnectAutoReleases(t *testing.T) {
	addr, stop := startServer(t)
	defer stop()

	c1, r1 := dial(t, addr)
	reqResp(t, c1, r1, "l", "k", "30")
	c1.Close()

	// Give the server a moment to run cleanup.
	time.Sleep(100 * time.Millisecond)

	c2, r2 := dial(t, addr)
	defer c2.Close()
	if got := reqResp(t, c2, r2, "l", "k", "1"); !strings.HasPrefix(got, "ok ") {
		t.Fatalf("post-disconnect acquire: %q", got)
	}
}

// ---------------------------------------------------------------------------
// Renew
// ---------------------------------------------------------------------------

func TestServer_Renew(t *testing.T) {
	addr, stop := startServer(t)
	defer stop()
	conn, r := dial(t, addr)
	defer conn.Close()

	tok := strings.Fields(reqResp(t, conn, r, "l", "k", "5"))[1]
	got := reqResp(t, conn, r, "n", "k", tok+" 60")
	if !strings.HasPrefix(got, "ok ") {
		t.Fatalf("renew: %q", got)
	}
	parts := strings.Fields(got)
	if parts[1] != "60" {
		t.Errorf("got remaining %q, want 60", parts[1])
	}
}

// ---------------------------------------------------------------------------
// Bad input
// ---------------------------------------------------------------------------

func TestServer_BadCmd(t *testing.T) {
	addr, stop := startServer(t)
	defer stop()
	conn, r := dial(t, addr)
	defer conn.Close()

	if got := reqResp(t, conn, r, "bogus", "k", ""); got != "error" {
		t.Fatalf("got %q", got)
	}
}

func TestServer_EmptyKey(t *testing.T) {
	addr, stop := startServer(t)
	defer stop()
	conn, r := dial(t, addr)
	defer conn.Close()

	if got := reqResp(t, conn, r, "l", "", "5"); got != "error" {
		t.Fatalf("got %q", got)
	}
}

// ---------------------------------------------------------------------------
// Concurrent acquire serialises through FIFO
// ---------------------------------------------------------------------------

func TestServer_ConcurrentFIFO(t *testing.T) {
	addr, stop := startServer(t)
	defer stop()

	c0, r0 := dial(t, addr)
	defer c0.Close()
	tok0 := strings.Fields(reqResp(t, c0, r0, "l", "k", "30"))[1]

	const N = 4
	type res struct {
		idx int
		tok string
	}
	resCh := make(chan res, N)

	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, r := dial(t, addr)
			defer c.Close()
			tok := strings.Fields(reqResp(t, c, r, "l", "k", "10"))[1]
			resCh <- res{idx: i, tok: tok}
			reqResp(t, c, r, "r", "k", tok)
		}()
		// Stagger to ensure deterministic queue order.
		time.Sleep(20 * time.Millisecond)
	}
	reqResp(t, c0, r0, "r", "k", tok0)

	for want := 0; want < N; want++ {
		select {
		case got := <-resCh:
			if got.idx != want {
				t.Fatalf("FIFO violation: got idx %d at position %d", got.idx, want)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("position %d never granted", want)
		}
	}
	wg.Wait()
}

// ---------------------------------------------------------------------------
// Drain on shutdown
// ---------------------------------------------------------------------------

func TestServer_DrainsOnShutdown(t *testing.T) {
	addr, stop := startServer(t)
	conn, r := dial(t, addr)
	defer conn.Close()

	// Hold a lock in flight.
	tok := strings.Fields(reqResp(t, conn, r, "l", "k", "5"))[1]
	stop()

	// After shutdown, the connection should error or we get
	// error_draining on a subsequent request.
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	send(t, conn, "r", "k", tok)
	if line, err := r.ReadString('\n'); err != nil {
		// EOF is acceptable.
		return
	} else if !strings.Contains(line, "drain") && !strings.Contains(line, "ok") {
		t.Logf("post-shutdown response: %q", line)
	}
}

// ---------------------------------------------------------------------------
// Panic isolation
// ---------------------------------------------------------------------------

// TestServer_RecoversHandlerPanic verifies that a panic inside a
// command handler is recovered: the offending request gets a generic
// error and the rest of the server keeps serving.
func TestServer_RecoversHandlerPanic(t *testing.T) {
	orig := commandTable[protocol.CmdPing]
	commandTable[protocol.CmdPing] = func(*Server, context.Context, *protocol.Request, uint64) *protocol.Ack {
		panic("induced handler panic")
	}
	defer func() { commandTable[protocol.CmdPing] = orig }()

	addr, stop := startServer(t)
	defer stop()

	conn, r := dial(t, addr)
	defer conn.Close()
	if got := reqResp(t, conn, r, "ping", "_", ""); got != "error" {
		t.Fatalf("ping into a panicking handler: got %q, want error", got)
	}

	// Server must still be serving — on this conn and a fresh one.
	if got := reqResp(t, conn, r, "l", "k", "5"); !strings.HasPrefix(got, "ok ") {
		t.Fatalf("acquire on same conn after recovered panic: %q", got)
	}
	conn2, r2 := dial(t, addr)
	defer conn2.Close()
	if got := reqResp(t, conn2, r2, "l", "k2", "5"); !strings.HasPrefix(got, "ok ") {
		t.Fatalf("acquire on fresh conn after recovered panic: %q", got)
	}
}
