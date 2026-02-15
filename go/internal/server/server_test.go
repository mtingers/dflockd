package server

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mtingers/dflockd/internal/config"
	"github.com/mtingers/dflockd/internal/lock"
)

func testConfig() *config.Config {
	return &config.Config{
		Host:                    "127.0.0.1",
		Port:                    0,
		DefaultLeaseTTL:         33 * time.Second,
		LeaseSweepInterval:      100 * time.Millisecond,
		GCInterval:              100 * time.Millisecond,
		GCMaxIdleTime:           60 * time.Second,
		MaxLocks:                1024,
		ReadTimeout:             5 * time.Second,
		AutoReleaseOnDisconnect: true,
	}
}

// startServer creates a server on a random port and returns a cancel func and the address.
func startServer(t *testing.T, cfg *config.Config) (context.CancelFunc, string, *lock.LockManager) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	lm := lock.NewLockManager(cfg, log)
	srv := New(lm, cfg, log)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.RunOnListener(ctx, ln)
	}()

	cleanup := func() {
		cancel()
		<-done
	}

	return cleanup, addr, lm
}

// sendCmd sends a 3-line protocol command and reads one response line.
func sendCmd(t *testing.T, conn net.Conn, cmd, key, arg string) string {
	t.Helper()
	msg := fmt.Sprintf("%s\n%s\n%s\n", cmd, key, arg)
	if _, err := conn.Write([]byte(msg)); err != nil {
		t.Fatalf("write: %v", err)
	}
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return strings.TrimRight(line, "\r\n")
}

// dialAndSendCmd dials, sends a command, returns the response.
// The connection is NOT closed; caller must close.
func dialAndSendCmd(t *testing.T, addr, cmd, key, arg string) (net.Conn, string) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	resp := sendCmd(t, conn, cmd, key, arg)
	return conn, resp
}

// connSendCmd is like sendCmd but uses a per-connection bufio.Reader for
// multi-command sessions on the same connection. The caller must create the
// reader once and reuse it across calls.
func connSendCmd(t *testing.T, conn net.Conn, reader *bufio.Reader, cmd, key, arg string) string {
	t.Helper()
	msg := fmt.Sprintf("%s\n%s\n%s\n", cmd, key, arg)
	if _, err := conn.Write([]byte(msg)); err != nil {
		t.Fatalf("write: %v", err)
	}
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return strings.TrimRight(line, "\r\n")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestIntegration_LockAndRelease(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Lock
	resp := connSendCmd(t, conn, reader, "l", "mykey", "10")
	parts := strings.Fields(resp)
	if len(parts) != 3 || parts[0] != "ok" {
		t.Fatalf("expected 'ok <token> <lease>', got %q", resp)
	}
	token := parts[1]

	// Release
	resp = connSendCmd(t, conn, reader, "r", "mykey", token)
	if resp != "ok" {
		t.Fatalf("expected 'ok', got %q", resp)
	}
}

func TestIntegration_LockTimeout(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	// conn1 acquires
	conn1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn1.Close()
	r1 := bufio.NewReader(conn1)
	resp := connSendCmd(t, conn1, r1, "l", "mykey", "10")
	if !strings.HasPrefix(resp, "ok ") {
		t.Fatalf("expected ok, got %q", resp)
	}

	// conn2 tries with 0 timeout → should timeout immediately
	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()
	r2 := bufio.NewReader(conn2)
	resp = connSendCmd(t, conn2, r2, "l", "mykey", "0")
	if resp != "timeout" {
		t.Fatalf("expected 'timeout', got %q", resp)
	}
}

func TestIntegration_Renew(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Lock
	resp := connSendCmd(t, conn, reader, "l", "mykey", "10")
	parts := strings.Fields(resp)
	token := parts[1]

	// Renew
	resp = connSendCmd(t, conn, reader, "n", "mykey", token)
	if !strings.HasPrefix(resp, "ok ") {
		t.Fatalf("expected 'ok <remaining>', got %q", resp)
	}

	// Renew with bad token
	resp = connSendCmd(t, conn, reader, "n", "mykey", "badtoken")
	if resp != "error" {
		t.Fatalf("expected 'error', got %q", resp)
	}
}

func TestIntegration_ReleaseWrongToken(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Lock
	connSendCmd(t, conn, reader, "l", "mykey", "10")

	// Release with wrong token
	resp := connSendCmd(t, conn, reader, "r", "mykey", "wrongtoken")
	if resp != "error" {
		t.Fatalf("expected 'error', got %q", resp)
	}
}

func TestIntegration_EnqueueImmediate(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Enqueue on free key → immediate acquire
	resp := connSendCmd(t, conn, reader, "e", "mykey", "")
	parts := strings.Fields(resp)
	if len(parts) != 3 || parts[0] != "acquired" {
		t.Fatalf("expected 'acquired <token> <lease>', got %q", resp)
	}
	token := parts[1]

	// Wait should return immediately with same token
	resp = connSendCmd(t, conn, reader, "w", "mykey", "10")
	parts = strings.Fields(resp)
	if len(parts) != 3 || parts[0] != "ok" || parts[1] != token {
		t.Fatalf("expected 'ok %s <lease>', got %q", token, resp)
	}

	// Release
	resp = connSendCmd(t, conn, reader, "r", "mykey", token)
	if resp != "ok" {
		t.Fatalf("expected 'ok', got %q", resp)
	}
}

func TestIntegration_TwoPhaseQueued(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	// conn1 acquires the lock
	conn1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn1.Close()
	r1 := bufio.NewReader(conn1)
	resp := connSendCmd(t, conn1, r1, "l", "mykey", "10")
	parts := strings.Fields(resp)
	token1 := parts[1]

	// conn2 enqueues → should get "queued"
	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()
	r2 := bufio.NewReader(conn2)
	resp = connSendCmd(t, conn2, r2, "e", "mykey", "")
	if resp != "queued" {
		t.Fatalf("expected 'queued', got %q", resp)
	}

	// conn2 waits in background
	type result struct {
		resp string
		err  error
	}
	waitDone := make(chan result, 1)
	go func() {
		msg := fmt.Sprintf("w\nmykey\n10\n")
		if _, err := conn2.Write([]byte(msg)); err != nil {
			waitDone <- result{err: err}
			return
		}
		conn2.SetReadDeadline(time.Now().Add(10 * time.Second))
		line, err := r2.ReadString('\n')
		waitDone <- result{resp: strings.TrimRight(line, "\r\n"), err: err}
	}()

	// Give wait time to block
	time.Sleep(50 * time.Millisecond)

	// conn1 releases → conn2 should get the lock
	resp = connSendCmd(t, conn1, r1, "r", "mykey", token1)
	if resp != "ok" {
		t.Fatalf("release: expected 'ok', got %q", resp)
	}

	// conn2 wait should complete
	r := <-waitDone
	if r.err != nil {
		t.Fatal(r.err)
	}
	parts = strings.Fields(r.resp)
	if len(parts) != 3 || parts[0] != "ok" {
		t.Fatalf("wait: expected 'ok <token> <lease>', got %q", r.resp)
	}
}

func TestIntegration_WaitTimeout(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	// conn1 holds lock
	conn1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn1.Close()
	r1 := bufio.NewReader(conn1)
	connSendCmd(t, conn1, r1, "l", "mykey", "10")

	// conn2 enqueue + wait with 0 timeout
	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()
	r2 := bufio.NewReader(conn2)
	resp := connSendCmd(t, conn2, r2, "e", "mykey", "")
	if resp != "queued" {
		t.Fatalf("expected 'queued', got %q", resp)
	}
	resp = connSendCmd(t, conn2, r2, "w", "mykey", "0")
	if resp != "timeout" {
		t.Fatalf("expected 'timeout', got %q", resp)
	}
}

func TestIntegration_WaitNotEnqueued(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Wait without enqueue → error (and server disconnects due to protocol error)
	resp := connSendCmd(t, conn, reader, "w", "mykey", "10")
	if resp != "error" {
		t.Fatalf("expected 'error', got %q", resp)
	}
}

func TestIntegration_EnqueueCustomLease(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Enqueue with custom lease
	resp := connSendCmd(t, conn, reader, "e", "mykey", "60")
	parts := strings.Fields(resp)
	if len(parts) != 3 || parts[0] != "acquired" || parts[2] != "60" {
		t.Fatalf("expected 'acquired <token> 60', got %q", resp)
	}
}

func TestIntegration_LockCustomLease(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Lock with custom lease
	resp := connSendCmd(t, conn, reader, "l", "mykey", "10 60")
	parts := strings.Fields(resp)
	if len(parts) != 3 || parts[0] != "ok" || parts[2] != "60" {
		t.Fatalf("expected 'ok <token> 60', got %q", resp)
	}
}

func TestIntegration_DisconnectReleasesLock(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	// conn1 acquires then disconnects
	conn1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	r1 := bufio.NewReader(conn1)
	connSendCmd(t, conn1, r1, "l", "mykey", "10")
	conn1.Close()

	// Give server time to cleanup
	time.Sleep(100 * time.Millisecond)

	// conn2 should be able to acquire immediately
	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()
	r2 := bufio.NewReader(conn2)
	resp := connSendCmd(t, conn2, r2, "l", "mykey", "0")
	parts := strings.Fields(resp)
	if len(parts) != 3 || parts[0] != "ok" {
		t.Fatalf("expected 'ok <token> <lease>', got %q", resp)
	}
}

func TestIntegration_FIFOOrdering(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	// conn1 acquires
	conn1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn1.Close()
	r1 := bufio.NewReader(conn1)
	resp := connSendCmd(t, conn1, r1, "l", "mykey", "10")
	token1 := strings.Fields(resp)[1]

	type result struct {
		order int
		resp  string
	}
	results := make(chan result, 2)

	// conn2 and conn3 both wait
	for i, timeout := range []string{"10", "10"} {
		i := i
		timeout := timeout
		go func() {
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				return
			}
			defer conn.Close()
			reader := bufio.NewReader(conn)
			resp := connSendCmd(t, conn, reader, "l", "mykey", timeout)
			results <- result{order: i, resp: resp}
		}()
		// Stagger to ensure FIFO ordering
		time.Sleep(50 * time.Millisecond)
	}

	// Release conn1 → conn2 should get it first
	time.Sleep(50 * time.Millisecond)
	connSendCmd(t, conn1, r1, "r", "mykey", token1)

	r := <-results
	parts := strings.Fields(r.resp)
	if parts[0] != "ok" {
		t.Fatalf("first waiter: expected 'ok', got %q", r.resp)
	}

	// Release the second lock so third can get it
	conn2tok := parts[1]
	release, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer release.Close()
	rr := bufio.NewReader(release)
	connSendCmd(t, release, rr, "r", "mykey", conn2tok)

	r = <-results
	parts = strings.Fields(r.resp)
	if parts[0] != "ok" {
		t.Fatalf("second waiter: expected 'ok', got %q", r.resp)
	}
}

func TestIntegration_MaxLocks(t *testing.T) {
	cfg := testConfig()
	cfg.MaxLocks = 2
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Acquire 2 locks
	connSendCmd(t, conn, reader, "l", "key1", "0")
	connSendCmd(t, conn, reader, "l", "key2", "0")

	// Third should hit max locks
	resp := connSendCmd(t, conn, reader, "l", "key3", "0")
	if resp != "error_max_locks" {
		t.Fatalf("expected 'error_max_locks', got %q", resp)
	}
}

func TestIntegration_EnqueueMaxLocks(t *testing.T) {
	cfg := testConfig()
	cfg.MaxLocks = 1
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	connSendCmd(t, conn, reader, "e", "key1", "")

	// Second key should hit max locks
	resp := connSendCmd(t, conn, reader, "e", "key2", "")
	if resp != "error_max_locks" {
		t.Fatalf("expected 'error_max_locks', got %q", resp)
	}
}

func TestIntegration_MultipleCommandsSameConn(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Lock, renew, release on same connection
	resp := connSendCmd(t, conn, reader, "l", "k1", "10")
	parts := strings.Fields(resp)
	if parts[0] != "ok" {
		t.Fatalf("lock: expected ok, got %q", resp)
	}
	token := parts[1]

	resp = connSendCmd(t, conn, reader, "n", "k1", token)
	if !strings.HasPrefix(resp, "ok ") {
		t.Fatalf("renew: expected ok, got %q", resp)
	}

	resp = connSendCmd(t, conn, reader, "r", "k1", token)
	if resp != "ok" {
		t.Fatalf("release: expected ok, got %q", resp)
	}

	// Lock again on same conn
	resp = connSendCmd(t, conn, reader, "l", "k1", "10")
	if !strings.HasPrefix(resp, "ok ") {
		t.Fatalf("second lock: expected ok, got %q", resp)
	}
}

func TestIntegration_DefaultLease(t *testing.T) {
	cfg := testConfig()
	cfg.DefaultLeaseTTL = 42 * time.Second
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	resp := connSendCmd(t, conn, reader, "l", "mykey", "10")
	parts := strings.Fields(resp)
	if len(parts) != 3 || parts[2] != "42" {
		t.Fatalf("expected lease 42, got %q", resp)
	}
}

func TestIntegration_EnqueueDefaultLease(t *testing.T) {
	cfg := testConfig()
	cfg.DefaultLeaseTTL = 42 * time.Second
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	resp := connSendCmd(t, conn, reader, "e", "mykey", "")
	parts := strings.Fields(resp)
	if len(parts) != 3 || parts[2] != "42" {
		t.Fatalf("expected lease 42, got %q", resp)
	}
}

func TestIntegration_TwoPhaseFullCycle(t *testing.T) {
	// Full e→w→r cycle with contention
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	// conn1 does two-phase: e → w → r
	conn1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn1.Close()
	r1 := bufio.NewReader(conn1)

	// Enqueue → immediate acquire
	resp := connSendCmd(t, conn1, r1, "e", "mykey", "")
	parts := strings.Fields(resp)
	if parts[0] != "acquired" {
		t.Fatalf("e: expected acquired, got %q", resp)
	}
	token1 := parts[1]

	// Wait → returns same token
	resp = connSendCmd(t, conn1, r1, "w", "mykey", "10")
	parts = strings.Fields(resp)
	if parts[0] != "ok" || parts[1] != token1 {
		t.Fatalf("w: expected ok %s, got %q", token1, resp)
	}

	// conn2 enqueues while conn1 holds
	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()
	r2 := bufio.NewReader(conn2)

	resp = connSendCmd(t, conn2, r2, "e", "mykey", "")
	if resp != "queued" {
		t.Fatalf("e conn2: expected queued, got %q", resp)
	}

	// conn2 waits in background
	waitDone := make(chan string, 1)
	go func() {
		msg := "w\nmykey\n10\n"
		conn2.Write([]byte(msg))
		conn2.SetReadDeadline(time.Now().Add(10 * time.Second))
		line, _ := r2.ReadString('\n')
		waitDone <- strings.TrimRight(line, "\r\n")
	}()

	time.Sleep(50 * time.Millisecond)

	// conn1 releases
	resp = connSendCmd(t, conn1, r1, "r", "mykey", token1)
	if resp != "ok" {
		t.Fatalf("r: expected ok, got %q", resp)
	}

	// conn2 should now have the lock
	resp = <-waitDone
	parts = strings.Fields(resp)
	if len(parts) != 3 || parts[0] != "ok" {
		t.Fatalf("w conn2: expected ok <token> <lease>, got %q", resp)
	}
	token2 := parts[1]

	// conn2 releases
	resp = connSendCmd(t, conn2, r2, "r", "mykey", token2)
	if resp != "ok" {
		t.Fatalf("r conn2: expected ok, got %q", resp)
	}
}
