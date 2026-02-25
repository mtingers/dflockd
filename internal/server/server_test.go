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

	// Wait without enqueue → error_not_enqueued
	resp := connSendCmd(t, conn, reader, "w", "mykey", "10")
	if resp != "error_not_enqueued" {
		t.Fatalf("expected 'error_not_enqueued', got %q", resp)
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
// Graceful shutdown integration tests
// ===========================================================================

func TestIntegration_GracefulShutdown_DrainCompletes(t *testing.T) {
	cfg := testConfig()
	cfg.ShutdownTimeout = 5 * time.Second
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

	// Connect a client and acquire a lock
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	resp := connSendCmd(t, conn, reader, "l", "mykey", "10")
	parts := strings.Fields(resp)
	if len(parts) != 3 || parts[0] != "ok" {
		t.Fatalf("expected 'ok <token> <lease>', got %q", resp)
	}
	token := parts[1]

	// Release the lock and close the connection so drain completes
	connSendCmd(t, conn, reader, "r", "mykey", token)
	conn.Close()

	// Cancel context to trigger shutdown
	cancel()

	// Server should drain and exit cleanly within the timeout
	select {
	case <-done:
		// success
	case <-time.After(3 * time.Second):
		t.Fatal("server did not shut down within expected time")
	}
}

func TestIntegration_GracefulShutdown_ForceClose(t *testing.T) {
	cfg := testConfig()
	cfg.ShutdownTimeout = 100 * time.Millisecond
	cfg.ReadTimeout = 30 * time.Second // long read timeout so client blocks
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

	// conn2 sends a blocking lock request (will wait for conn1's lock)
	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()
	// Write the lock request — it will block waiting for the lock
	fmt.Fprintf(conn2, "l\nmykey\n30\n")

	// Give the server time to accept both connections
	time.Sleep(50 * time.Millisecond)

	// Cancel context to trigger shutdown
	cancel()

	// Server should force-close connections and exit within ~shutdown timeout
	select {
	case <-done:
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("server did not shut down within expected time after force-close")
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

// ===========================================================================
// Regression tests for unpushed fixes
// ===========================================================================

// ---------------------------------------------------------------------------
// ErrAlreadyEnqueued / error_already_enqueued protocol response
// ---------------------------------------------------------------------------

func TestIntegration_EnqueueAlreadyEnqueued(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// First enqueue — should get "acquired" (no contention)
	resp := connSendCmd(t, conn, reader, "e", "k1", "")
	parts := strings.Fields(resp)
	if parts[0] != "acquired" {
		t.Fatalf("expected acquired, got %q", resp)
	}

	// Second enqueue on same key from same connection — should get error
	resp = connSendCmd(t, conn, reader, "e", "k1", "")
	if resp != "error_already_enqueued" {
		t.Fatalf("expected error_already_enqueued, got %q", resp)
	}
}

func TestIntegration_SemEnqueueAlreadyEnqueued(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	resp := connSendCmd(t, conn, reader, "se", "s1", "3")
	parts := strings.Fields(resp)
	if parts[0] != "acquired" {
		t.Fatalf("expected acquired, got %q", resp)
	}

	resp = connSendCmd(t, conn, reader, "se", "s1", "3")
	if resp != "error_already_enqueued" {
		t.Fatalf("expected error_already_enqueued, got %q", resp)
	}
}

func TestIntegration_WaitLeaseExpired(t *testing.T) {
	cfg := testConfig()
	cfg.LeaseSweepInterval = 50 * time.Millisecond
	cleanup, addr, lm := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Enqueue with short lease
	resp := connSendCmd(t, conn, reader, "e", "k1", "1")
	parts := strings.Fields(resp)
	if parts[0] != "acquired" {
		t.Fatalf("expected acquired, got %q", resp)
	}

	// Force expire the lease (but don't wait for the sweep to run —
	// FIFOWait's fast path checks LeaseExpires directly).
	lm.ResetLeaseForTest("k1")

	// Wait — should get error_lease_expired from the fast-path check.
	resp = connSendCmd(t, conn, reader, "w", "k1", "1")
	if resp != "error_lease_expired" {
		t.Fatalf("expected error_lease_expired, got %q", resp)
	}
}

func TestIntegration_DisconnectReleasesLockRegression(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	// conn1 acquires lock then disconnects
	conn1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	reader1 := bufio.NewReader(conn1)
	resp := connSendCmd(t, conn1, reader1, "l", "k1", "10")
	parts := strings.Fields(resp)
	if parts[0] != "ok" {
		t.Fatalf("expected ok, got %q", resp)
	}
	conn1.Close()
	time.Sleep(200 * time.Millisecond)

	// conn2 should acquire immediately (timeout=0)
	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()
	reader2 := bufio.NewReader(conn2)
	resp = connSendCmd(t, conn2, reader2, "l", "k1", "0")
	parts = strings.Fields(resp)
	if parts[0] != "ok" {
		t.Fatalf("expected ok (lock auto-released), got %q", resp)
	}
}

func TestIntegration_DisconnectReleasesSemSlot(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	reader1 := bufio.NewReader(conn1)
	resp := connSendCmd(t, conn1, reader1, "sl", "s1", "10 1")
	parts := strings.Fields(resp)
	if parts[0] != "ok" {
		t.Fatalf("expected ok, got %q", resp)
	}
	conn1.Close()
	time.Sleep(200 * time.Millisecond)

	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()
	reader2 := bufio.NewReader(conn2)
	resp = connSendCmd(t, conn2, reader2, "sl", "s1", "0 1")
	parts = strings.Fields(resp)
	if parts[0] != "ok" {
		t.Fatalf("expected ok (slot auto-released), got %q", resp)
	}
}

func TestIntegration_WaitWithoutEnqueue(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	resp := connSendCmd(t, conn, reader, "w", "k1", "1")
	if resp != "error_not_enqueued" {
		t.Fatalf("expected error_not_enqueued, got %q", resp)
	}
}

func TestIntegration_SemWaitWithoutEnqueue(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	resp := connSendCmd(t, conn, reader, "sw", "s1", "1")
	if resp != "error_not_enqueued" {
		t.Fatalf("expected error_not_enqueued, got %q", resp)
	}
}

func TestIntegration_SemLimitMismatchRegression(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn1, resp1 := dialAndSendCmd(t, addr, "sl", "s1", "10 3")
	defer conn1.Close()
	if !strings.HasPrefix(resp1, "ok") {
		t.Fatalf("expected ok, got %q", resp1)
	}

	conn2, resp2 := dialAndSendCmd(t, addr, "sl", "s1", "10 5")
	defer conn2.Close()
	if resp2 != "error_limit_mismatch" {
		t.Fatalf("expected error_limit_mismatch, got %q", resp2)
	}
}

func TestIntegration_MaxWaitersRegression(t *testing.T) {
	cfg := testConfig()
	cfg.MaxWaiters = 1
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn1, resp1 := dialAndSendCmd(t, addr, "l", "k1", "10")
	defer conn1.Close()
	if !strings.HasPrefix(resp1, "ok") {
		t.Fatalf("c1: expected ok, got %q", resp1)
	}

	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()
	reader2 := bufio.NewReader(conn2)
	resp := connSendCmd(t, conn2, reader2, "e", "k1", "")
	if resp != "queued" {
		t.Fatalf("c2: expected queued, got %q", resp)
	}

	conn3, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn3.Close()
	reader3 := bufio.NewReader(conn3)
	resp = connSendCmd(t, conn3, reader3, "e", "k1", "")
	if resp != "error_max_waiters" {
		t.Fatalf("c3: expected error_max_waiters, got %q", resp)
	}
}

func TestIntegration_StatsResponse(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	resp := connSendCmd(t, conn, reader, "l", "stats-key", "10")
	parts := strings.Fields(resp)
	if parts[0] != "ok" {
		t.Fatalf("expected ok, got %q", resp)
	}

	statsResp := connSendCmd(t, conn, reader, "stats", "_", "_")
	if !strings.HasPrefix(statsResp, "ok ") {
		t.Fatalf("expected 'ok {...}', got %q", statsResp)
	}
	jsonPart := strings.TrimPrefix(statsResp, "ok ")
	var result map[string]interface{}
	if err := json.Unmarshal([]byte(jsonPart), &result); err != nil {
		t.Fatalf("stats JSON unmarshal failed: %v", err)
	}
	if _, ok := result["connections"]; !ok {
		t.Fatal("stats should include 'connections' field")
	}
	if _, ok := result["locks"]; !ok {
		t.Fatal("stats should include 'locks' field")
	}
}

// ===========================================================================
// Phase 1: Counter Integration Tests
// ===========================================================================

func TestIntegration_IncrDecr(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	resp := connSendCmd(t, conn, reader, "incr", "myctr", "5")
	if resp != "ok 5" {
		t.Fatalf("expected 'ok 5', got %q", resp)
	}

	resp = connSendCmd(t, conn, reader, "incr", "myctr", "3")
	if resp != "ok 8" {
		t.Fatalf("expected 'ok 8', got %q", resp)
	}

	resp = connSendCmd(t, conn, reader, "decr", "myctr", "2")
	if resp != "ok 6" {
		t.Fatalf("expected 'ok 6', got %q", resp)
	}

	resp = connSendCmd(t, conn, reader, "get", "myctr", "")
	if resp != "ok 6" {
		t.Fatalf("expected 'ok 6', got %q", resp)
	}
}

func TestIntegration_CounterNamespaceIsolation(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Acquire a lock on "foo"
	resp := connSendCmd(t, conn, reader, "l", "foo", "5")
	if !strings.HasPrefix(resp, "ok ") {
		t.Fatalf("lock failed: %s", resp)
	}

	// Incr counter "foo" — different namespace
	resp = connSendCmd(t, conn, reader, "incr", "foo", "10")
	if resp != "ok 10" {
		t.Fatalf("expected 'ok 10', got %q", resp)
	}
}

func TestIntegration_Cset(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	resp := connSendCmd(t, conn, reader, "cset", "myctr", "42")
	if resp != "ok" {
		t.Fatalf("expected 'ok', got %q", resp)
	}

	resp = connSendCmd(t, conn, reader, "get", "myctr", "")
	if resp != "ok 42" {
		t.Fatalf("expected 'ok 42', got %q", resp)
	}
}

func TestIntegration_MaxKeys(t *testing.T) {
	cfg := testConfig()
	cfg.MaxKeys = 2
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	connSendCmd(t, conn, reader, "incr", "c1", "1")
	connSendCmd(t, conn, reader, "incr", "c2", "1")
	resp := connSendCmd(t, conn, reader, "incr", "c3", "1")
	if resp != "error_max_keys" {
		t.Fatalf("expected error_max_keys, got %q", resp)
	}
}

// ===========================================================================
// Phase 2: KV Integration Tests
// ===========================================================================

func TestIntegration_KVSetGetDel(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	resp := connSendCmd(t, conn, reader, "kset", "mykey", "hello")
	if resp != "ok" {
		t.Fatalf("expected 'ok', got %q", resp)
	}

	resp = connSendCmd(t, conn, reader, "kget", "mykey", "")
	if resp != "ok hello" {
		t.Fatalf("expected 'ok hello', got %q", resp)
	}

	resp = connSendCmd(t, conn, reader, "kdel", "mykey", "")
	if resp != "ok" {
		t.Fatalf("expected 'ok', got %q", resp)
	}

	resp = connSendCmd(t, conn, reader, "kget", "mykey", "")
	if resp != "nil" {
		t.Fatalf("expected 'nil', got %q", resp)
	}
}

func TestIntegration_KVSetWithTTL(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	resp := connSendCmd(t, conn, reader, "kset", "mykey", "hello 1")
	if resp != "ok" {
		t.Fatalf("expected 'ok', got %q", resp)
	}

	// Immediately should be available
	resp = connSendCmd(t, conn, reader, "kget", "mykey", "")
	if resp != "ok hello" {
		t.Fatalf("expected 'ok hello', got %q", resp)
	}

	// Wait for TTL to expire
	time.Sleep(1200 * time.Millisecond)

	resp = connSendCmd(t, conn, reader, "kget", "mykey", "")
	if resp != "nil" {
		t.Fatalf("expected 'nil' after TTL expiry, got %q", resp)
	}
}

// ===========================================================================
// Phase 3: Signal Integration Tests
// ===========================================================================

func TestIntegration_Signal_Basic(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	// Listener connection
	listenerConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer listenerConn.Close()
	listenerReader := bufio.NewReader(listenerConn)

	resp := connSendCmd(t, listenerConn, listenerReader, "listen", "alerts.*", "")
	if resp != "ok" {
		t.Fatalf("listen failed: %s", resp)
	}

	// Emitter connection
	emitterConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer emitterConn.Close()
	emitterReader := bufio.NewReader(emitterConn)

	resp = connSendCmd(t, emitterConn, emitterReader, "signal", "alerts.fire", "building-7!")
	if resp != "ok 1" {
		t.Fatalf("signal failed: %s", resp)
	}

	// Read pushed signal on listener
	listenerConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := listenerReader.ReadString('\n')
	if err != nil {
		t.Fatalf("read signal: %v", err)
	}
	line = strings.TrimRight(line, "\r\n")
	if line != "sig alerts.fire building-7!" {
		t.Fatalf("expected 'sig alerts.fire building-7!', got %q", line)
	}
}

func TestIntegration_Signal_CatchAll(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	listenerConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer listenerConn.Close()
	listenerReader := bufio.NewReader(listenerConn)

	resp := connSendCmd(t, listenerConn, listenerReader, "listen", ">", "")
	if resp != "ok" {
		t.Fatalf("listen failed: %s", resp)
	}

	emitterConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer emitterConn.Close()
	emitterReader := bufio.NewReader(emitterConn)

	resp = connSendCmd(t, emitterConn, emitterReader, "signal", "any.channel.name", "payload")
	if resp != "ok 1" {
		t.Fatalf("expected 'ok 1', got %q", resp)
	}

	listenerConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := listenerReader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	line = strings.TrimRight(line, "\r\n")
	if line != "sig any.channel.name payload" {
		t.Fatalf("expected signal push, got %q", line)
	}
}

func TestIntegration_Signal_Unlisten(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	listenerConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer listenerConn.Close()
	listenerReader := bufio.NewReader(listenerConn)

	connSendCmd(t, listenerConn, listenerReader, "listen", "ch", "")

	// Unlisten
	resp := connSendCmd(t, listenerConn, listenerReader, "unlisten", "ch", "")
	if resp != "ok" {
		t.Fatalf("unlisten failed: %s", resp)
	}

	// Signal should now have 0 receivers
	emitterConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer emitterConn.Close()
	emitterReader := bufio.NewReader(emitterConn)

	resp = connSendCmd(t, emitterConn, emitterReader, "signal", "ch", "hello")
	if resp != "ok 0" {
		t.Fatalf("expected 'ok 0', got %q", resp)
	}
}

func TestIntegration_Signal_DisconnectCleanup(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	// Connect, listen, then disconnect
	listenerConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	listenerReader := bufio.NewReader(listenerConn)
	connSendCmd(t, listenerConn, listenerReader, "listen", "ch", "")
	listenerConn.Close()

	// Give the server a moment to clean up
	time.Sleep(100 * time.Millisecond)

	// Signal should have 0 receivers
	emitterConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer emitterConn.Close()
	emitterReader := bufio.NewReader(emitterConn)

	resp := connSendCmd(t, emitterConn, emitterReader, "signal", "ch", "hello")
	if resp != "ok 0" {
		t.Fatalf("expected 'ok 0' after disconnect cleanup, got %q", resp)
	}
}

func TestIntegration_Signal_MixedCommands(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Use same connection for lock + signal + counter
	resp := connSendCmd(t, conn, reader, "incr", "ctr", "5")
	if resp != "ok 5" {
		t.Fatalf("incr: %s", resp)
	}

	resp = connSendCmd(t, conn, reader, "signal", "ch", "hello")
	if resp != "ok 0" {
		t.Fatalf("signal: %s", resp)
	}

	resp = connSendCmd(t, conn, reader, "get", "ctr", "")
	if resp != "ok 5" {
		t.Fatalf("get: %s", resp)
	}
}

// ===========================================================================
// Phase 4: List Integration Tests
// ===========================================================================

func TestIntegration_FIFO_Queue(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	connSendCmd(t, conn, reader, "rpush", "q1", "a")
	connSendCmd(t, conn, reader, "rpush", "q1", "b")
	resp := connSendCmd(t, conn, reader, "rpush", "q1", "c")
	if resp != "ok 3" {
		t.Fatalf("expected 'ok 3', got %q", resp)
	}

	resp = connSendCmd(t, conn, reader, "lpop", "q1", "")
	if resp != "ok a" {
		t.Fatalf("expected 'ok a', got %q", resp)
	}
	resp = connSendCmd(t, conn, reader, "lpop", "q1", "")
	if resp != "ok b" {
		t.Fatalf("expected 'ok b', got %q", resp)
	}
	resp = connSendCmd(t, conn, reader, "lpop", "q1", "")
	if resp != "ok c" {
		t.Fatalf("expected 'ok c', got %q", resp)
	}
	resp = connSendCmd(t, conn, reader, "lpop", "q1", "")
	if resp != "nil" {
		t.Fatalf("expected 'nil', got %q", resp)
	}
}

func TestIntegration_LRange_JSON(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	connSendCmd(t, conn, reader, "rpush", "q1", "a")
	connSendCmd(t, conn, reader, "rpush", "q1", "b")
	connSendCmd(t, conn, reader, "rpush", "q1", "c")

	resp := connSendCmd(t, conn, reader, "lrange", "q1", "0 -1")
	if !strings.HasPrefix(resp, "ok ") {
		t.Fatalf("expected ok, got %q", resp)
	}
	jsonPart := resp[3:]
	var items []string
	if err := json.Unmarshal([]byte(jsonPart), &items); err != nil {
		t.Fatalf("JSON parse: %v", err)
	}
	if len(items) != 3 || items[0] != "a" || items[1] != "b" || items[2] != "c" {
		t.Fatalf("expected [a b c], got %v", items)
	}
}

func TestIntegration_LIFO_Stack(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	connSendCmd(t, conn, reader, "lpush", "stack", "a")
	connSendCmd(t, conn, reader, "lpush", "stack", "b")
	connSendCmd(t, conn, reader, "lpush", "stack", "c")

	resp := connSendCmd(t, conn, reader, "lpop", "stack", "")
	if resp != "ok c" {
		t.Fatalf("expected 'ok c', got %q", resp)
	}
	resp = connSendCmd(t, conn, reader, "lpop", "stack", "")
	if resp != "ok b" {
		t.Fatalf("expected 'ok b', got %q", resp)
	}
}

func TestIntegration_LLen(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	resp := connSendCmd(t, conn, reader, "llen", "q1", "")
	if resp != "ok 0" {
		t.Fatalf("expected 'ok 0', got %q", resp)
	}

	connSendCmd(t, conn, reader, "rpush", "q1", "a")
	connSendCmd(t, conn, reader, "rpush", "q1", "b")

	resp = connSendCmd(t, conn, reader, "llen", "q1", "")
	if resp != "ok 2" {
		t.Fatalf("expected 'ok 2', got %q", resp)
	}
}

func TestIntegration_ListFull(t *testing.T) {
	cfg := testConfig()
	cfg.MaxListLength = 2
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	connSendCmd(t, conn, reader, "rpush", "q1", "a")
	connSendCmd(t, conn, reader, "rpush", "q1", "b")
	resp := connSendCmd(t, conn, reader, "rpush", "q1", "c")
	if resp != "error_list_full" {
		t.Fatalf("expected 'error_list_full', got %q", resp)
	}
}
