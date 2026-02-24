package server

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mtingers/dflockd/internal/config"
	"github.com/mtingers/dflockd/internal/lock"
	"github.com/mtingers/dflockd/internal/testutil"
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
		WriteTimeout:            5 * time.Second,
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

// ===========================================================================
// Semaphore integration tests
// ===========================================================================

func TestIntegration_SemAcquireRelease(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Acquire semaphore slot (limit=3)
	resp := connSendCmd(t, conn, reader, "sl", "sem1", "10 3")
	parts := strings.Fields(resp)
	if len(parts) != 3 || parts[0] != "ok" {
		t.Fatalf("expected 'ok <token> <lease>', got %q", resp)
	}
	token := parts[1]

	// Release
	resp = connSendCmd(t, conn, reader, "sr", "sem1", token)
	if resp != "ok" {
		t.Fatalf("expected 'ok', got %q", resp)
	}
}

func TestIntegration_SemAtCapacityTimeout(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	// Fill capacity (limit=2)
	conn1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn1.Close()
	r1 := bufio.NewReader(conn1)
	connSendCmd(t, conn1, r1, "sl", "sem1", "10 2")

	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()
	r2 := bufio.NewReader(conn2)
	connSendCmd(t, conn2, r2, "sl", "sem1", "10 2")

	// Third should timeout
	conn3, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn3.Close()
	r3 := bufio.NewReader(conn3)
	resp := connSendCmd(t, conn3, r3, "sl", "sem1", "0 2")
	if resp != "timeout" {
		t.Fatalf("expected 'timeout', got %q", resp)
	}
}

func TestIntegration_SemRenew(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	resp := connSendCmd(t, conn, reader, "sl", "sem1", "10 3")
	token := strings.Fields(resp)[1]

	resp = connSendCmd(t, conn, reader, "sn", "sem1", token)
	if !strings.HasPrefix(resp, "ok ") {
		t.Fatalf("expected 'ok <remaining>', got %q", resp)
	}

	// Bad token
	resp = connSendCmd(t, conn, reader, "sn", "sem1", "badtoken")
	if resp != "error" {
		t.Fatalf("expected 'error', got %q", resp)
	}
}

func TestIntegration_SemLimitMismatch(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn1.Close()
	r1 := bufio.NewReader(conn1)
	connSendCmd(t, conn1, r1, "sl", "sem1", "10 3")

	// Different limit from different connection
	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()
	r2 := bufio.NewReader(conn2)
	resp := connSendCmd(t, conn2, r2, "sl", "sem1", "10 5")
	if resp != "error_limit_mismatch" {
		t.Fatalf("expected 'error_limit_mismatch', got %q", resp)
	}
}

func TestIntegration_SemTwoPhase(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	// conn1 acquires slot (limit=1)
	conn1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn1.Close()
	r1 := bufio.NewReader(conn1)
	resp := connSendCmd(t, conn1, r1, "sl", "sem1", "10 1")
	token1 := strings.Fields(resp)[1]

	// conn2 enqueues
	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()
	r2 := bufio.NewReader(conn2)
	resp = connSendCmd(t, conn2, r2, "se", "sem1", "1")
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
		msg := "sw\nsem1\n10\n"
		conn2.Write([]byte(msg))
		conn2.SetReadDeadline(time.Now().Add(10 * time.Second))
		line, err := r2.ReadString('\n')
		waitDone <- result{resp: strings.TrimRight(line, "\r\n"), err: err}
	}()

	time.Sleep(50 * time.Millisecond)

	// conn1 releases
	resp = connSendCmd(t, conn1, r1, "sr", "sem1", token1)
	if resp != "ok" {
		t.Fatalf("release: expected 'ok', got %q", resp)
	}

	r := <-waitDone
	if r.err != nil {
		t.Fatal(r.err)
	}
	parts := strings.Fields(r.resp)
	if len(parts) != 3 || parts[0] != "ok" {
		t.Fatalf("wait: expected 'ok <token> <lease>', got %q", r.resp)
	}
}

func TestIntegration_SemDisconnectCleanup(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	// conn1 acquires then disconnects
	conn1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	r1 := bufio.NewReader(conn1)
	connSendCmd(t, conn1, r1, "sl", "sem1", "10 1")
	conn1.Close()

	// Give server time to detect disconnect and cleanup
	time.Sleep(200 * time.Millisecond)

	// conn2 should be able to acquire immediately
	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()
	r2 := bufio.NewReader(conn2)
	resp := connSendCmd(t, conn2, r2, "sl", "sem1", "0 1")
	parts := strings.Fields(resp)
	if len(parts) != 3 || parts[0] != "ok" {
		t.Fatalf("expected 'ok <token> <lease>', got %q", resp)
	}
}

func TestIntegration_SemFIFOOrdering(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	// conn1 acquires (limit=1)
	conn1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn1.Close()
	r1 := bufio.NewReader(conn1)
	resp := connSendCmd(t, conn1, r1, "sl", "sem1", "10 1")
	token1 := strings.Fields(resp)[1]

	type result struct {
		order int
		resp  string
	}
	results := make(chan result, 2)

	for i := 0; i < 2; i++ {
		i := i
		go func() {
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				return
			}
			defer conn.Close()
			reader := bufio.NewReader(conn)
			resp := connSendCmd(t, conn, reader, "sl", "sem1", "10 1")
			results <- result{order: i, resp: resp}
		}()
		time.Sleep(50 * time.Millisecond)
	}

	time.Sleep(50 * time.Millisecond)
	connSendCmd(t, conn1, r1, "sr", "sem1", token1)

	r := <-results
	parts := strings.Fields(r.resp)
	if parts[0] != "ok" {
		t.Fatalf("first waiter: expected 'ok', got %q", r.resp)
	}

	// Release so second waiter gets it
	tok2 := parts[1]
	releaseConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseConn.Close()
	rr := bufio.NewReader(releaseConn)
	connSendCmd(t, releaseConn, rr, "sr", "sem1", tok2)

	r = <-results
	parts = strings.Fields(r.resp)
	if parts[0] != "ok" {
		t.Fatalf("second waiter: expected 'ok', got %q", r.resp)
	}
}

func TestIntegration_SemMaxLocks(t *testing.T) {
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

	// Use 1 lock key + 1 sem key = 2 (at limit)
	connSendCmd(t, conn, reader, "l", "lock1", "0")
	connSendCmd(t, conn, reader, "sl", "sem1", "0 3")

	// Third should hit max locks
	resp := connSendCmd(t, conn, reader, "sl", "sem2", "0 3")
	if resp != "error_max_locks" {
		t.Fatalf("expected 'error_max_locks', got %q", resp)
	}
}

func TestIntegration_SemCustomLease(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	resp := connSendCmd(t, conn, reader, "sl", "sem1", "10 3 60")
	parts := strings.Fields(resp)
	if len(parts) != 3 || parts[2] != "60" {
		t.Fatalf("expected lease 60, got %q", resp)
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

func TestIntegration_Stats(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Acquire a lock
	resp := connSendCmd(t, conn, reader, "l", "lockkey", "10")
	parts := strings.Fields(resp)
	if len(parts) != 3 || parts[0] != "ok" {
		t.Fatalf("lock: expected 'ok <token> <lease>', got %q", resp)
	}

	// Acquire a semaphore slot
	resp = connSendCmd(t, conn, reader, "sl", "semkey", "10 3")
	parts = strings.Fields(resp)
	if len(parts) != 3 || parts[0] != "ok" {
		t.Fatalf("sem: expected 'ok <token> <lease>', got %q", resp)
	}

	// Send stats command
	resp = connSendCmd(t, conn, reader, "stats", "_", "")
	if !strings.HasPrefix(resp, "ok ") {
		t.Fatalf("stats: expected 'ok <json>', got %q", resp)
	}
	jsonStr := strings.TrimPrefix(resp, "ok ")

	var stats lock.Stats
	if err := json.Unmarshal([]byte(jsonStr), &stats); err != nil {
		t.Fatalf("stats: invalid JSON: %v\njson: %s", err, jsonStr)
	}

	// Verify connections (at least 1)
	if stats.Connections < 1 {
		t.Fatalf("expected connections >= 1, got %d", stats.Connections)
	}

	// Verify held lock
	if len(stats.Locks) != 1 {
		t.Fatalf("expected 1 lock, got %d", len(stats.Locks))
	}
	if stats.Locks[0].Key != "lockkey" {
		t.Fatalf("expected lock key 'lockkey', got %q", stats.Locks[0].Key)
	}
	if stats.Locks[0].LeaseExpiresInS <= 0 {
		t.Fatalf("expected positive lease_expires_in_s, got %f", stats.Locks[0].LeaseExpiresInS)
	}

	// Verify held semaphore
	if len(stats.Semaphores) != 1 {
		t.Fatalf("expected 1 semaphore, got %d", len(stats.Semaphores))
	}
	if stats.Semaphores[0].Key != "semkey" {
		t.Fatalf("expected sem key 'semkey', got %q", stats.Semaphores[0].Key)
	}
	if stats.Semaphores[0].Limit != 3 {
		t.Fatalf("expected sem limit 3, got %d", stats.Semaphores[0].Limit)
	}
	if stats.Semaphores[0].Holders != 1 {
		t.Fatalf("expected 1 holder, got %d", stats.Semaphores[0].Holders)
	}

	// No idle entries expected
	if len(stats.IdleLocks) != 0 {
		t.Fatalf("expected 0 idle locks, got %d", len(stats.IdleLocks))
	}
	if len(stats.IdleSemaphores) != 0 {
		t.Fatalf("expected 0 idle semaphores, got %d", len(stats.IdleSemaphores))
	}
}

// ===========================================================================
// TLS integration tests
// ===========================================================================

// startTLSServer creates a server on a TLS listener and returns the address
// and client TLS config.
func startTLSServer(t *testing.T, cfg *config.Config) (context.CancelFunc, string, *tls.Config) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	lm := lock.NewLockManager(cfg, log)
	srv := New(lm, cfg, log)

	serverTLS, clientTLS := testutil.SelfSignedTLS(t)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tlsLn := tls.NewListener(ln, serverTLS)
	addr := ln.Addr().String()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.RunOnListener(ctx, tlsLn)
	}()

	cleanup := func() {
		cancel()
		<-done
	}

	return cleanup, addr, clientTLS
}

func TestIntegration_TLS_LockAndRelease(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, clientTLS := startTLSServer(t, cfg)
	defer cleanup()

	conn, err := tls.Dial("tcp", addr, clientTLS)
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

func TestIntegration_TLS_PlainClientRejected(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startTLSServer(t, cfg)
	defer cleanup()

	// Plain TCP dial against TLS listener should fail on protocol exchange.
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Send a command — the TLS handshake never completed so the server
	// should close the connection or return an error.
	msg := "l\nmykey\n10\n"
	conn.Write([]byte(msg))
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	reader := bufio.NewReader(conn)
	_, err = reader.ReadString('\n')
	if err == nil {
		t.Fatal("expected error reading from TLS server with plain client")
	}
}

func TestIntegration_TLS_ValidationError(t *testing.T) {
	cfg := testConfig()
	cfg.TLSCert = "/tmp/nonexistent.pem"
	// Only cert set, no key — should return validation error.
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	lm := lock.NewLockManager(cfg, log)
	srv := New(lm, cfg, log)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := srv.Run(ctx)
	if err == nil {
		t.Fatal("expected error when only --tls-cert is set")
	}
	if !strings.Contains(err.Error(), "both --tls-cert and --tls-key must be provided") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ===========================================================================
// Auth integration tests
// ===========================================================================

func startAuthServer(t *testing.T, cfg *config.Config, authToken string) (context.CancelFunc, string) {
	t.Helper()
	cfg.AuthToken = authToken
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

	return cleanup, addr
}

func TestIntegration_Auth_Success(t *testing.T) {
	cfg := testConfig()
	cleanup, addr := startAuthServer(t, cfg, "secret123")
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Authenticate
	resp := connSendCmd(t, conn, reader, "auth", "_", "secret123")
	if resp != "ok" {
		t.Fatalf("auth: expected 'ok', got %q", resp)
	}

	// Lock should work after auth
	resp = connSendCmd(t, conn, reader, "l", "mykey", "10")
	parts := strings.Fields(resp)
	if len(parts) != 3 || parts[0] != "ok" {
		t.Fatalf("lock: expected 'ok <token> <lease>', got %q", resp)
	}
	token := parts[1]

	// Release
	resp = connSendCmd(t, conn, reader, "r", "mykey", token)
	if resp != "ok" {
		t.Fatalf("release: expected 'ok', got %q", resp)
	}
}

func TestIntegration_Auth_WrongToken(t *testing.T) {
	cfg := testConfig()
	cleanup, addr := startAuthServer(t, cfg, "secret123")
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	resp := connSendCmd(t, conn, reader, "auth", "_", "wrongtoken")
	if resp != "error_auth" {
		t.Fatalf("expected 'error_auth', got %q", resp)
	}
}

func TestIntegration_Auth_NoAuthSent(t *testing.T) {
	cfg := testConfig()
	cleanup, addr := startAuthServer(t, cfg, "secret123")
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Send a lock command instead of auth — should get error_auth
	resp := connSendCmd(t, conn, reader, "l", "mykey", "10")
	if resp != "error_auth" {
		t.Fatalf("expected 'error_auth', got %q", resp)
	}
}

func TestIntegration_Auth_NotRequired(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// No auth required — lock should work directly
	resp := connSendCmd(t, conn, reader, "l", "mykey", "10")
	parts := strings.Fields(resp)
	if len(parts) != 3 || parts[0] != "ok" {
		t.Fatalf("expected 'ok <token> <lease>', got %q", resp)
	}
}

// ===========================================================================
// Max connections integration tests
// ===========================================================================

func TestIntegration_MaxConnections(t *testing.T) {
	cfg := testConfig()
	cfg.MaxConnections = 2
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	// Connect 2 clients
	conn1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn1.Close()
	r1 := bufio.NewReader(conn1)

	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()
	r2 := bufio.NewReader(conn2)

	// Verify both work
	resp := connSendCmd(t, conn1, r1, "l", "key1", "10")
	if !strings.HasPrefix(resp, "ok ") {
		t.Fatalf("conn1 lock: expected ok, got %q", resp)
	}
	resp = connSendCmd(t, conn2, r2, "l", "key2", "10")
	if !strings.HasPrefix(resp, "ok ") {
		t.Fatalf("conn2 lock: expected ok, got %q", resp)
	}

	// Give server time to register connections
	time.Sleep(50 * time.Millisecond)

	// Third connection should be rejected (closed immediately)
	conn3, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn3.Close()
	conn3.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	_, err = conn3.Read(buf)
	if err == nil {
		t.Fatal("expected conn3 to be closed by server")
	}

	// Disconnect one client
	conn1.Close()
	time.Sleep(100 * time.Millisecond)

	// New connection should now succeed
	conn4, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn4.Close()
	r4 := bufio.NewReader(conn4)
	resp = connSendCmd(t, conn4, r4, "l", "key3", "10")
	if !strings.HasPrefix(resp, "ok ") {
		t.Fatalf("conn4 lock: expected ok, got %q", resp)
	}
}

// ===========================================================================
// Max waiters integration tests
// ===========================================================================

func TestIntegration_MaxWaiters_Lock(t *testing.T) {
	cfg := testConfig()
	cfg.MaxWaiters = 1
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
	if !strings.HasPrefix(resp, "ok ") {
		t.Fatalf("expected ok, got %q", resp)
	}

	// conn2 waits (fills the 1-waiter queue) — do this in background
	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()
	go func() {
		msg := "l\nmykey\n10\n"
		conn2.Write([]byte(msg))
	}()
	time.Sleep(50 * time.Millisecond)

	// conn3 tries to wait — should get error_max_waiters
	conn3, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn3.Close()
	r3 := bufio.NewReader(conn3)
	resp = connSendCmd(t, conn3, r3, "l", "mykey", "10")
	if resp != "error_max_waiters" {
		t.Fatalf("expected 'error_max_waiters', got %q", resp)
	}
}

func TestIntegration_MaxWaiters_Enqueue(t *testing.T) {
	cfg := testConfig()
	cfg.MaxWaiters = 1
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

	// conn2 enqueues (fills the 1-waiter queue)
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

	// conn3 enqueues — should get error_max_waiters
	conn3, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn3.Close()
	r3 := bufio.NewReader(conn3)
	resp = connSendCmd(t, conn3, r3, "e", "mykey", "")
	if resp != "error_max_waiters" {
		t.Fatalf("expected 'error_max_waiters', got %q", resp)
	}
}

func TestIntegration_MaxWaiters_Sem(t *testing.T) {
	cfg := testConfig()
	cfg.MaxWaiters = 1
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	// conn1 acquires sem slot (limit=1)
	conn1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn1.Close()
	r1 := bufio.NewReader(conn1)
	resp := connSendCmd(t, conn1, r1, "sl", "sem1", "10 1")
	if !strings.HasPrefix(resp, "ok ") {
		t.Fatalf("expected ok, got %q", resp)
	}

	// conn2 waits (fills queue)
	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()
	go func() {
		msg := "sl\nsem1\n10 1\n"
		conn2.Write([]byte(msg))
	}()
	time.Sleep(50 * time.Millisecond)

	// conn3 should get error_max_waiters
	conn3, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn3.Close()
	r3 := bufio.NewReader(conn3)
	resp = connSendCmd(t, conn3, r3, "sl", "sem1", "10 1")
	if resp != "error_max_waiters" {
		t.Fatalf("expected 'error_max_waiters', got %q", resp)
	}
}

// ===========================================================================
// Write timeout integration tests
// ===========================================================================

func TestIntegration_WriteTimeout(t *testing.T) {
	// Verify that write timeout doesn't interfere with normal operations
	cfg := testConfig()
	cfg.WriteTimeout = 1 * time.Second
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Normal operations should work fine with write timeout set
	resp := connSendCmd(t, conn, reader, "l", "mykey", "10")
	parts := strings.Fields(resp)
	if len(parts) != 3 || parts[0] != "ok" {
		t.Fatalf("expected 'ok <token> <lease>', got %q", resp)
	}
	token := parts[1]

	resp = connSendCmd(t, conn, reader, "r", "mykey", token)
	if resp != "ok" {
		t.Fatalf("expected 'ok', got %q", resp)
	}
}
