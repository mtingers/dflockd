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
	"strconv"
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
	if len(parts) != 4 || parts[0] != "ok" {
		t.Fatalf("expected 'ok <token> <lease> <fence>', got %q", resp)
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
		t.Fatalf("expected 'ok <remaining> <fence>', got %q", resp)
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
	if len(parts) != 4 || parts[0] != "acquired" {
		t.Fatalf("expected 'acquired <token> <lease> <fence>', got %q", resp)
	}
	token := parts[1]

	// Wait should return immediately with same token
	resp = connSendCmd(t, conn, reader, "w", "mykey", "10")
	parts = strings.Fields(resp)
	if len(parts) != 4 || parts[0] != "ok" || parts[1] != token {
		t.Fatalf("expected 'ok %s <lease> <fence>', got %q", token, resp)
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
	if len(parts) != 4 || parts[0] != "ok" {
		t.Fatalf("wait: expected 'ok <token> <lease> <fence>', got %q", r.resp)
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
	if len(parts) != 4 || parts[0] != "acquired" || parts[2] != "60" {
		t.Fatalf("expected 'acquired <token> 60 <fence>', got %q", resp)
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
	if len(parts) != 4 || parts[0] != "ok" || parts[2] != "60" {
		t.Fatalf("expected 'ok <token> 60 <fence>', got %q", resp)
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
	if len(parts) != 4 || parts[0] != "ok" {
		t.Fatalf("expected 'ok <token> <lease> <fence>', got %q", resp)
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
	if len(parts) != 4 || parts[2] != "42" {
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
	if len(parts) != 4 || parts[2] != "42" {
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
	if len(parts) != 4 || parts[0] != "ok" {
		t.Fatalf("expected 'ok <token> <lease> <fence>', got %q", resp)
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
		t.Fatalf("expected 'ok <remaining> <fence>', got %q", resp)
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
	if len(parts) != 4 || parts[0] != "ok" {
		t.Fatalf("wait: expected 'ok <token> <lease> <fence>', got %q", r.resp)
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
	if len(parts) != 4 || parts[0] != "ok" {
		t.Fatalf("expected 'ok <token> <lease> <fence>', got %q", resp)
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
	if len(parts) != 4 || parts[2] != "60" {
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
	if len(parts) != 4 || parts[0] != "ok" {
		t.Fatalf("w conn2: expected ok <token> <lease> <fence>, got %q", resp)
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
	if len(parts) != 4 || parts[0] != "ok" {
		t.Fatalf("lock: expected 'ok <token> <lease> <fence>', got %q", resp)
	}

	// Acquire a semaphore slot
	resp = connSendCmd(t, conn, reader, "sl", "semkey", "10 3")
	parts = strings.Fields(resp)
	if len(parts) != 4 || parts[0] != "ok" {
		t.Fatalf("sem: expected 'ok <token> <lease> <fence>', got %q", resp)
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
	if len(parts) != 4 || parts[0] != "ok" {
		t.Fatalf("expected 'ok <token> <lease> <fence>', got %q", resp)
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
	if len(parts) != 4 || parts[0] != "ok" {
		t.Fatalf("lock: expected 'ok <token> <lease> <fence>', got %q", resp)
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
	if len(parts) != 4 || parts[0] != "ok" {
		t.Fatalf("expected 'ok <token> <lease> <fence>', got %q", resp)
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
	if len(parts) != 4 || parts[0] != "ok" {
		t.Fatalf("expected 'ok <token> <lease> <fence>', got %q", resp)
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
	if len(parts) != 4 || parts[0] != "ok" {
		t.Fatalf("expected 'ok <token> <lease> <fence>', got %q", resp)
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

func TestIntegration_KVSetNumericValue(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// A purely-numeric value should be stored as-is, not parsed as TTL
	resp := connSendCmd(t, conn, reader, "kset", "mykey", "42")
	if resp != "ok" {
		t.Fatalf("expected 'ok', got %q", resp)
	}

	resp = connSendCmd(t, conn, reader, "kget", "mykey", "")
	if resp != "ok 42" {
		t.Fatalf("expected 'ok 42', got %q", resp)
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

	resp := connSendCmd(t, conn, reader, "kset", "mykey", "hello\t1")
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

// ---------------------------------------------------------------------------
// Queue Group Integration Tests
// ---------------------------------------------------------------------------

func TestIntegration_QueueGroup_Basic(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	// Two listeners in the same group
	lc1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer lc1.Close()
	lr1 := bufio.NewReader(lc1)

	lc2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer lc2.Close()
	lr2 := bufio.NewReader(lc2)

	// Subscribe both to "ch" in group "workers"
	resp := connSendCmd(t, lc1, lr1, "listen", "ch", "workers")
	if resp != "ok" {
		t.Fatalf("listen1: %s", resp)
	}
	resp = connSendCmd(t, lc2, lr2, "listen", "ch", "workers")
	if resp != "ok" {
		t.Fatalf("listen2: %s", resp)
	}

	// Emitter
	ec, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer ec.Close()
	er := bufio.NewReader(ec)

	// Signal should deliver to exactly 1
	resp = connSendCmd(t, ec, er, "signal", "ch", "job1")
	if resp != "ok 1" {
		t.Fatalf("expected 'ok 1', got %q", resp)
	}
}

func TestIntegration_QueueGroup_WithIndividual(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	// Grouped listener
	lc1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer lc1.Close()
	lr1 := bufio.NewReader(lc1)

	resp := connSendCmd(t, lc1, lr1, "listen", "ch", "workers")
	if resp != "ok" {
		t.Fatalf("listen grouped: %s", resp)
	}

	// Non-grouped listener
	lc2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer lc2.Close()
	lr2 := bufio.NewReader(lc2)

	resp = connSendCmd(t, lc2, lr2, "listen", "ch", "")
	if resp != "ok" {
		t.Fatalf("listen individual: %s", resp)
	}

	// Emitter
	ec, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer ec.Close()
	er := bufio.NewReader(ec)

	// individual + 1 from group = 2
	resp = connSendCmd(t, ec, er, "signal", "ch", "payload")
	if resp != "ok 2" {
		t.Fatalf("expected 'ok 2', got %q", resp)
	}
}

func TestIntegration_QueueGroup_DisconnectCleanup(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	// Connect, listen in group, then disconnect
	lc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	lr := bufio.NewReader(lc)
	connSendCmd(t, lc, lr, "listen", "ch", "workers")
	lc.Close()

	time.Sleep(100 * time.Millisecond)

	ec, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer ec.Close()
	er := bufio.NewReader(ec)

	resp := connSendCmd(t, ec, er, "signal", "ch", "hello")
	if resp != "ok 0" {
		t.Fatalf("expected 'ok 0' after disconnect cleanup, got %q", resp)
	}
}

func TestIntegration_QueueGroup_Unlisten(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	lc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer lc.Close()
	lr := bufio.NewReader(lc)

	connSendCmd(t, lc, lr, "listen", "ch", "workers")

	// Unlisten with matching group
	resp := connSendCmd(t, lc, lr, "unlisten", "ch", "workers")
	if resp != "ok" {
		t.Fatalf("unlisten: %s", resp)
	}

	ec, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer ec.Close()
	er := bufio.NewReader(ec)

	resp = connSendCmd(t, ec, er, "signal", "ch", "hello")
	if resp != "ok 0" {
		t.Fatalf("expected 'ok 0' after unlisten, got %q", resp)
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

// ===========================================================================
// Fencing Token Integration Tests
// ===========================================================================

func TestIntegration_FencingToken(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Acquire a lock, check response has 4 fields and fence > 0
	resp := connSendCmd(t, conn, reader, "l", "fencekey1", "10")
	parts := strings.Fields(resp)
	if len(parts) != 4 || parts[0] != "ok" {
		t.Fatalf("expected 'ok <token> <lease> <fence>', got %q", resp)
	}
	token1 := parts[1]
	fence1 := parts[3]
	if fence1 == "0" {
		t.Fatalf("expected fence > 0, got %q", fence1)
	}

	// Release and acquire on another key, check fence is higher
	connSendCmd(t, conn, reader, "r", "fencekey1", token1)

	resp = connSendCmd(t, conn, reader, "l", "fencekey2", "10")
	parts = strings.Fields(resp)
	if len(parts) != 4 || parts[0] != "ok" {
		t.Fatalf("expected 'ok <token> <lease> <fence>', got %q", resp)
	}
	token2 := parts[1]
	fence2 := parts[3]

	// fence2 should be greater than fence1 (monotonically increasing)
	f1, err1 := strconv.ParseUint(fence1, 10, 64)
	f2, err2 := strconv.ParseUint(fence2, 10, 64)
	if err1 != nil || err2 != nil {
		t.Fatalf("fence parse error: %v / %v", err1, err2)
	}
	if f2 <= f1 {
		t.Fatalf("expected fence2 (%d) > fence1 (%d)", f2, f1)
	}

	// Renew and verify fence is returned
	resp = connSendCmd(t, conn, reader, "n", "fencekey2", token2)
	renewParts := strings.Fields(resp)
	if len(renewParts) != 3 || renewParts[0] != "ok" {
		t.Fatalf("expected 'ok <remaining> <fence>', got %q", resp)
	}
	renewFence, err := strconv.ParseUint(renewParts[2], 10, 64)
	if err != nil {
		t.Fatalf("fence parse error on renew: %v", err)
	}
	if renewFence == 0 {
		t.Fatalf("expected fence > 0 on renew, got 0")
	}
}

// ===========================================================================
// Blocking Pop Integration Tests
// ===========================================================================

func TestIntegration_BLPop_Immediate(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Push a value first
	resp := connSendCmd(t, conn, reader, "rpush", "blkey1", "world")
	if resp != "ok 1" {
		t.Fatalf("rpush: expected 'ok 1', got %q", resp)
	}

	// BLPop should return immediately
	resp = connSendCmd(t, conn, reader, "blpop", "blkey1", "5")
	if resp != "ok world" {
		t.Fatalf("expected 'ok world', got %q", resp)
	}
}

func TestIntegration_BLPop_Blocks(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	// conn1 will block on blpop
	conn1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn1.Close()
	r1 := bufio.NewReader(conn1)

	type result struct {
		resp string
		err  error
	}
	done := make(chan result, 1)
	go func() {
		msg := "blpop\nblkey2\n5\n"
		if _, err := conn1.Write([]byte(msg)); err != nil {
			done <- result{err: err}
			return
		}
		conn1.SetReadDeadline(time.Now().Add(10 * time.Second))
		line, err := r1.ReadString('\n')
		done <- result{resp: strings.TrimRight(line, "\r\n"), err: err}
	}()

	// Give blpop time to block
	time.Sleep(100 * time.Millisecond)

	// conn2 pushes a value
	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()
	r2 := bufio.NewReader(conn2)

	resp := connSendCmd(t, conn2, r2, "rpush", "blkey2", "hello")
	// When a pop waiter is waiting, the push hands off directly — the item
	// never enters the list, so the list length remains 0.
	if resp != "ok 0" {
		t.Fatalf("rpush: expected 'ok 0', got %q", resp)
	}

	// conn1 should receive the value
	r := <-done
	if r.err != nil {
		t.Fatalf("blpop read error: %v", r.err)
	}
	if r.resp != "ok hello" {
		t.Fatalf("expected 'ok hello', got %q", r.resp)
	}
}

func TestIntegration_BLPop_Timeout(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// BLPop with timeout 0 on empty list should return nil immediately
	resp := connSendCmd(t, conn, reader, "blpop", "blkey_empty", "0")
	if resp != "nil" {
		t.Fatalf("expected 'nil', got %q", resp)
	}
}

func TestIntegration_BRPop(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Push values (a, b, c) using rpush
	connSendCmd(t, conn, reader, "rpush", "brkey1", "a")
	connSendCmd(t, conn, reader, "rpush", "brkey1", "b")
	connSendCmd(t, conn, reader, "rpush", "brkey1", "c")

	// BRPop should return the last element (right pop)
	resp := connSendCmd(t, conn, reader, "brpop", "brkey1", "5")
	if resp != "ok c" {
		t.Fatalf("expected 'ok c', got %q", resp)
	}
}

// ===========================================================================
// Read-Write Lock Integration Tests
// ===========================================================================

func TestIntegration_RWLock_ReadRead(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	// Two connections both acquire read locks on the same key
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

	resp1 := connSendCmd(t, conn1, r1, "rl", "rwkey1", "5")
	parts1 := strings.Fields(resp1)
	if len(parts1) != 4 || parts1[0] != "ok" {
		t.Fatalf("conn1 rl: expected 'ok <token> <lease> <fence>', got %q", resp1)
	}

	resp2 := connSendCmd(t, conn2, r2, "rl", "rwkey1", "5")
	parts2 := strings.Fields(resp2)
	if len(parts2) != 4 || parts2[0] != "ok" {
		t.Fatalf("conn2 rl: expected 'ok <token> <lease> <fence>', got %q", resp2)
	}

	// Both should have gotten different tokens
	if parts1[1] == parts2[1] {
		t.Fatalf("expected different tokens, both got %q", parts1[1])
	}
}

func TestIntegration_RWLock_WriteExclusive(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	// conn1 acquires write lock
	conn1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn1.Close()
	r1 := bufio.NewReader(conn1)

	resp := connSendCmd(t, conn1, r1, "wl", "rwkey2", "5")
	parts := strings.Fields(resp)
	if len(parts) != 4 || parts[0] != "ok" {
		t.Fatalf("conn1 wl: expected 'ok <token> <lease> <fence>', got %q", resp)
	}

	// conn2 tries write lock with timeout 0 — should timeout
	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()
	r2 := bufio.NewReader(conn2)

	resp = connSendCmd(t, conn2, r2, "wl", "rwkey2", "0")
	if resp != "timeout" {
		t.Fatalf("conn2 wl: expected 'timeout', got %q", resp)
	}
}

func TestIntegration_RWLock_ReleaseAndGrant(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	// conn1 acquires write lock
	conn1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn1.Close()
	r1 := bufio.NewReader(conn1)

	resp := connSendCmd(t, conn1, r1, "wl", "rwkey3", "5")
	parts := strings.Fields(resp)
	if len(parts) != 4 || parts[0] != "ok" {
		t.Fatalf("conn1 wl: expected 'ok <token> <lease> <fence>', got %q", resp)
	}
	token1 := parts[1]

	// conn2 requests write lock in a goroutine (will block)
	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()
	r2 := bufio.NewReader(conn2)

	type result struct {
		resp string
		err  error
	}
	done := make(chan result, 1)
	go func() {
		msg := "wl\nrwkey3\n5\n"
		if _, err := conn2.Write([]byte(msg)); err != nil {
			done <- result{err: err}
			return
		}
		conn2.SetReadDeadline(time.Now().Add(10 * time.Second))
		line, err := r2.ReadString('\n')
		done <- result{resp: strings.TrimRight(line, "\r\n"), err: err}
	}()

	// Give conn2 time to block
	time.Sleep(100 * time.Millisecond)

	// conn1 releases the write lock
	resp = connSendCmd(t, conn1, r1, "wr", "rwkey3", token1)
	if resp != "ok" {
		t.Fatalf("conn1 wr: expected 'ok', got %q", resp)
	}

	// conn2 should now get the lock
	r := <-done
	if r.err != nil {
		t.Fatalf("conn2 wl read error: %v", r.err)
	}
	parts = strings.Fields(r.resp)
	if len(parts) != 4 || parts[0] != "ok" {
		t.Fatalf("conn2 wl: expected 'ok <token> <lease> <fence>', got %q", r.resp)
	}
}

func TestIntegration_RWLock_Renew(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Acquire read lock
	resp := connSendCmd(t, conn, reader, "rl", "rwkey4", "5")
	parts := strings.Fields(resp)
	if len(parts) != 4 || parts[0] != "ok" {
		t.Fatalf("rl: expected 'ok <token> <lease> <fence>', got %q", resp)
	}
	token := parts[1]

	// Renew with "rn"
	resp = connSendCmd(t, conn, reader, "rn", "rwkey4", token)
	renewParts := strings.Fields(resp)
	if len(renewParts) != 3 || renewParts[0] != "ok" {
		t.Fatalf("rn: expected 'ok <remaining> <fence>', got %q", resp)
	}
	remaining, err1 := strconv.Atoi(renewParts[1])
	fence, err2 := strconv.ParseUint(renewParts[2], 10, 64)
	if err1 != nil || err2 != nil {
		t.Fatalf("rn parse error: remaining=%v fence=%v", err1, err2)
	}
	if remaining <= 0 {
		t.Fatalf("expected remaining > 0, got %d", remaining)
	}
	if fence == 0 {
		t.Fatalf("expected fence > 0 on rn, got 0")
	}
}

func TestIntegration_RWLock_TypeMismatch(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Acquire a regular lock on a key
	resp := connSendCmd(t, conn, reader, "l", "rwkey5", "5")
	parts := strings.Fields(resp)
	if len(parts) != 4 || parts[0] != "ok" {
		t.Fatalf("l: expected 'ok <token> <lease> <fence>', got %q", resp)
	}

	// Try to acquire a read lock on the same key — should get type mismatch
	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()
	r2 := bufio.NewReader(conn2)

	resp = connSendCmd(t, conn2, r2, "rl", "rwkey5", "0")
	if resp != "error_type_mismatch" {
		t.Fatalf("expected 'error_type_mismatch', got %q", resp)
	}
}

// ---------------------------------------------------------------------------
// CAS Integration Tests
// ---------------------------------------------------------------------------

func TestIntegration_KVCAS_Basic(t *testing.T) {
	cleanup, addr, _ := startServer(t, testConfig())
	defer cleanup()

	conn := dialTest(t, addr)
	defer conn.Close()

	// Create via CAS (empty old value, tab separator)
	resp := sendCmd(t, conn, "kcas", "mykey", "\thello\t0")
	if resp != "ok" {
		t.Fatalf("expected ok, got %q", resp)
	}

	// Verify value
	resp = sendCmd(t, conn, "kget", "mykey", "")
	if resp != "ok hello" {
		t.Fatalf("expected 'ok hello', got %q", resp)
	}

	// Swap value
	resp = sendCmd(t, conn, "kcas", "mykey", "hello\tworld\t0")
	if resp != "ok" {
		t.Fatalf("expected ok, got %q", resp)
	}

	// Verify swapped value
	resp = sendCmd(t, conn, "kget", "mykey", "")
	if resp != "ok world" {
		t.Fatalf("expected 'ok world', got %q", resp)
	}
}

func TestIntegration_KVCAS_Conflict(t *testing.T) {
	cleanup, addr, _ := startServer(t, testConfig())
	defer cleanup()

	conn := dialTest(t, addr)
	defer conn.Close()

	// Set initial value
	resp := sendCmd(t, conn, "kset", "mykey", "actual\t0")
	if resp != "ok" {
		t.Fatalf("expected ok, got %q", resp)
	}

	// CAS with wrong old value
	resp = sendCmd(t, conn, "kcas", "mykey", "wrong\tnew\t0")
	if resp != "cas_conflict" {
		t.Fatalf("expected cas_conflict, got %q", resp)
	}
}

// ---------------------------------------------------------------------------
// Watch Integration Tests
// ---------------------------------------------------------------------------

func TestIntegration_Watch_KVSet(t *testing.T) {
	cleanup, addr, _ := startServer(t, testConfig())
	defer cleanup()

	// Watcher connection
	wConn := dialTest(t, addr)
	defer wConn.Close()
	wReader := bufio.NewReader(wConn)

	resp := sendCmd(t, wConn, "watch", "mykey", "")
	if resp != "ok" {
		t.Fatalf("expected ok, got %q", resp)
	}

	// Writer connection
	wrConn := dialTest(t, addr)
	defer wrConn.Close()

	resp = sendCmd(t, wrConn, "kset", "mykey", "hello\t0")
	if resp != "ok" {
		t.Fatalf("expected ok, got %q", resp)
	}

	// Read push notification
	wConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := wReader.ReadString('\n')
	if err != nil {
		t.Fatalf("read watch event: %v", err)
	}
	line = strings.TrimRight(line, "\n\r")
	if line != "watch kset mykey" {
		t.Fatalf("expected 'watch kset mykey', got %q", line)
	}
}

func TestIntegration_Watch_Disconnect(t *testing.T) {
	cleanup, addr, _ := startServer(t, testConfig())
	defer cleanup()

	// Watcher subscribes, then disconnects
	wConn := dialTest(t, addr)
	sendCmd(t, wConn, "watch", "mykey", "")
	wConn.Close()

	time.Sleep(100 * time.Millisecond)

	// Writer should not block or error
	wrConn := dialTest(t, addr)
	defer wrConn.Close()
	resp := sendCmd(t, wrConn, "kset", "mykey", "hello\t0")
	if resp != "ok" {
		t.Fatalf("expected ok, got %q", resp)
	}
}

// ---------------------------------------------------------------------------
// Barrier Integration Tests
// ---------------------------------------------------------------------------

func TestIntegration_Barrier_TwoParticipants(t *testing.T) {
	cleanup, addr, _ := startServer(t, testConfig())
	defer cleanup()

	type result struct {
		resp string
		err  error
	}
	ch := make(chan result, 2)

	for i := range 2 {
		go func(id int) {
			conn := dialTest(t, addr)
			defer conn.Close()
			r := sendCmd(t, conn, "bwait", "barrier1", "2 10")
			ch <- result{resp: r}
		}(i)
	}

	for range 2 {
		r := <-ch
		if r.resp != "ok" {
			t.Fatalf("expected ok, got %q", r.resp)
		}
	}
}

func TestIntegration_Barrier_Timeout(t *testing.T) {
	cleanup, addr, _ := startServer(t, testConfig())
	defer cleanup()

	conn := dialTest(t, addr)
	defer conn.Close()

	// Only 1 of 3 arrives
	resp := sendCmd(t, conn, "bwait", "barrier1", "3 1")
	if resp != "timeout" {
		t.Fatalf("expected timeout, got %q", resp)
	}
}

// ---------------------------------------------------------------------------
// Leader Election Integration Tests
// ---------------------------------------------------------------------------

func TestIntegration_Election_Campaign(t *testing.T) {
	cleanup, addr, _ := startServer(t, testConfig())
	defer cleanup()

	conn := dialTest(t, addr)
	defer conn.Close()

	// Elect
	resp := sendCmd(t, conn, "elect", "leader1", "5 10")
	if !strings.HasPrefix(resp, "ok ") {
		t.Fatalf("expected ok <token> ..., got %q", resp)
	}
	parts := strings.Fields(resp)
	if len(parts) < 4 {
		t.Fatalf("expected ok <token> <lease> <fence>, got %q", resp)
	}
	token := parts[1]

	// Resign
	resp = sendCmd(t, conn, "resign", "leader1", token)
	if resp != "ok" {
		t.Fatalf("expected ok, got %q", resp)
	}
}

func TestIntegration_Election_Observe(t *testing.T) {
	cleanup, addr, _ := startServer(t, testConfig())
	defer cleanup()

	// Observer connection
	obsConn := dialTest(t, addr)
	defer obsConn.Close()
	obsReader := bufio.NewReader(obsConn)

	resp := sendCmd(t, obsConn, "observe", "leader1", "")
	if resp != "ok" {
		t.Fatalf("expected ok, got %q", resp)
	}

	// Leader connection
	leaderConn := dialTest(t, addr)
	defer leaderConn.Close()

	resp = sendCmd(t, leaderConn, "elect", "leader1", "5 10")
	if !strings.HasPrefix(resp, "ok ") {
		t.Fatalf("expected ok, got %q", resp)
	}

	// Observer should receive "leader elected leader1"
	obsConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := obsReader.ReadString('\n')
	if err != nil {
		t.Fatalf("read leader event: %v", err)
	}
	line = strings.TrimRight(line, "\n\r")
	if line != "leader elected leader1" {
		t.Fatalf("expected 'leader elected leader1', got %q", line)
	}
}

// ---------------------------------------------------------------------------
// Additional integration tests for full coverage
// ---------------------------------------------------------------------------

func TestIntegration_Election_Unobserve(t *testing.T) {
	cleanup, addr, _ := startServer(t, testConfig())
	defer cleanup()

	obsConn := dialTest(t, addr)
	defer obsConn.Close()
	obsReader := bufio.NewReader(obsConn)

	// Observe
	resp := connSendCmd(t, obsConn, obsReader, "observe", "leader1", "")
	if resp != "ok" {
		t.Fatalf("expected ok, got %q", resp)
	}

	// Unobserve
	resp = connSendCmd(t, obsConn, obsReader, "unobserve", "leader1", "")
	if resp != "ok" {
		t.Fatalf("unobserve expected ok, got %q", resp)
	}

	// Leader acquires — observer should NOT receive notification
	leaderConn := dialTest(t, addr)
	defer leaderConn.Close()
	resp = sendCmd(t, leaderConn, "elect", "leader1", "5 10")
	if !strings.HasPrefix(resp, "ok ") {
		t.Fatalf("expected ok, got %q", resp)
	}

	// Verify observer does NOT receive event
	obsConn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	_, err := obsReader.ReadString('\n')
	if err == nil {
		t.Fatal("should NOT receive leader event after unobserve")
	}
}

func TestIntegration_RWLock_TwoPhase(t *testing.T) {
	cleanup, addr, _ := startServer(t, testConfig())
	defer cleanup()

	// Writer holds the lock
	conn1 := dialTest(t, addr)
	defer conn1.Close()
	reader1 := bufio.NewReader(conn1)

	resp := connSendCmd(t, conn1, reader1, "wl", "rw1", "5 10")
	if !strings.HasPrefix(resp, "ok ") {
		t.Fatalf("expected ok <token> ..., got %q", resp)
	}
	wToken := strings.Fields(resp)[1]

	// Reader enqueues (two-phase)
	conn2 := dialTest(t, addr)
	defer conn2.Close()
	reader2 := bufio.NewReader(conn2)

	resp = connSendCmd(t, conn2, reader2, "re", "rw1", "10")
	if !strings.HasPrefix(resp, "queued") {
		t.Fatalf("expected queued, got %q", resp)
	}

	// Writer releases
	resp = connSendCmd(t, conn1, reader1, "wr", "rw1", wToken)
	if resp != "ok" {
		t.Fatalf("expected ok, got %q", resp)
	}

	// Reader waits — should now acquire
	resp = connSendCmd(t, conn2, reader2, "rw", "rw1", "5")
	if !strings.HasPrefix(resp, "ok ") {
		t.Fatalf("expected ok <token> ..., got %q", resp)
	}
}

func TestIntegration_BarrierCountMismatch(t *testing.T) {
	cleanup, addr, _ := startServer(t, testConfig())
	defer cleanup()

	// First participant with count=2
	conn1 := dialTest(t, addr)
	defer conn1.Close()

	done := make(chan string, 1)
	go func() {
		resp := sendCmd(t, conn1, "bwait", "b1", "2 5")
		done <- resp
	}()

	time.Sleep(50 * time.Millisecond)

	// Second participant with count=3 — should get error
	conn2 := dialTest(t, addr)
	defer conn2.Close()

	resp := sendCmd(t, conn2, "bwait", "b1", "3 5")
	if !strings.HasPrefix(resp, "error") {
		t.Fatalf("expected error for count mismatch, got %q", resp)
	}

	// Let conn3 join correctly to trip the barrier
	conn3 := dialTest(t, addr)
	defer conn3.Close()
	resp = sendCmd(t, conn3, "bwait", "b1", "2 5")
	if resp != "ok" {
		t.Fatalf("expected ok, got %q", resp)
	}

	// conn1 should also return ok
	select {
	case r := <-done:
		if r != "ok" {
			t.Fatalf("expected ok from conn1, got %q", r)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("conn1 did not return after barrier trip")
	}
}

func TestIntegration_KVSetGet_NamespaceIsolation(t *testing.T) {
	cleanup, addr, _ := startServer(t, testConfig())
	defer cleanup()

	conn := dialTest(t, addr)
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Set a KV key and a counter with same name
	resp := connSendCmd(t, conn, reader, "kset", "ns1", "hello 0")
	if resp != "ok" {
		t.Fatalf("expected ok, got %q", resp)
	}

	resp = connSendCmd(t, conn, reader, "incr", "ns1", "42")
	if resp != "ok 42" {
		t.Fatalf("expected ok 42, got %q", resp)
	}

	// KV should still have its value (response: "ok <value> <ttl>")
	resp = connSendCmd(t, conn, reader, "kget", "ns1", "")
	if !strings.HasPrefix(resp, "ok hello") {
		t.Fatalf("expected ok hello ..., got %q", resp)
	}

	// Counter should have its value
	resp = connSendCmd(t, conn, reader, "get", "ns1", "")
	if resp != "ok 42" {
		t.Fatalf("expected ok 42, got %q", resp)
	}
}

func TestIntegration_Watch_LockUnlock(t *testing.T) {
	cleanup, addr, _ := startServer(t, testConfig())
	defer cleanup()

	// Watcher connection
	watchConn := dialTest(t, addr)
	defer watchConn.Close()
	watchReader := bufio.NewReader(watchConn)

	resp := connSendCmd(t, watchConn, watchReader, "watch", "lock.*", "")
	if resp != "ok" {
		t.Fatalf("expected ok, got %q", resp)
	}

	// Acquire a lock (triggers watch notification)
	lockConn := dialTest(t, addr)
	defer lockConn.Close()
	lockReader := bufio.NewReader(lockConn)

	resp = connSendCmd(t, lockConn, lockReader, "l", "lock.mykey", "5 10")
	if !strings.HasPrefix(resp, "ok ") {
		t.Fatalf("expected ok, got %q", resp)
	}
	token := strings.Fields(resp)[1]

	// Watcher should receive notification
	watchConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := watchReader.ReadString('\n')
	if err != nil {
		t.Fatalf("read watch event: %v", err)
	}
	line = strings.TrimRight(line, "\n\r")
	if !strings.Contains(line, "lock.mykey") {
		t.Fatalf("expected watch event for lock.mykey, got %q", line)
	}

	// Release the lock
	resp = connSendCmd(t, lockConn, lockReader, "r", "lock.mykey", token)
	if resp != "ok" {
		t.Fatalf("expected ok, got %q", resp)
	}

	// Should get another notification
	line, err = watchReader.ReadString('\n')
	if err != nil {
		t.Fatalf("read second watch event: %v", err)
	}
	line = strings.TrimRight(line, "\n\r")
	if !strings.Contains(line, "lock.mykey") {
		t.Fatalf("expected watch event for release, got %q", line)
	}
}

func TestIntegration_Counter_CsetEdgeCases(t *testing.T) {
	cleanup, addr, _ := startServer(t, testConfig())
	defer cleanup()

	conn := dialTest(t, addr)
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Set counter
	resp := connSendCmd(t, conn, reader, "cset", "c1", "100")
	if resp != "ok" {
		t.Fatalf("expected ok, got %q", resp)
	}

	// Get should return 100
	resp = connSendCmd(t, conn, reader, "get", "c1", "")
	if resp != "ok 100" {
		t.Fatalf("expected ok 100, got %q", resp)
	}

	// Decrement below zero
	resp = connSendCmd(t, conn, reader, "decr", "c1", "200")
	if resp != "ok -100" {
		t.Fatalf("expected ok -100, got %q", resp)
	}

	// Set to negative
	resp = connSendCmd(t, conn, reader, "cset", "c1", "-50")
	if resp != "ok" {
		t.Fatalf("expected ok, got %q", resp)
	}

	resp = connSendCmd(t, conn, reader, "get", "c1", "")
	if resp != "ok -50" {
		t.Fatalf("expected ok -50, got %q", resp)
	}
}

func TestIntegration_List_BLPop_Disconnect(t *testing.T) {
	cleanup, addr, _ := startServer(t, testConfig())
	defer cleanup()

	// conn1 sends blpop and then disconnects
	conn1 := dialTest(t, addr)
	msg := fmt.Sprintf("blpop\nlist1\n5\n")
	conn1.Write([]byte(msg))

	time.Sleep(100 * time.Millisecond)

	// Disconnect conn1 while blocked
	conn1.Close()

	time.Sleep(50 * time.Millisecond)

	// Push something to the list — should not panic the server
	conn2 := dialTest(t, addr)
	defer conn2.Close()

	resp := sendCmd(t, conn2, "rpush", "list1", "hello")
	if !strings.HasPrefix(resp, "ok") {
		t.Fatalf("expected ok, got %q", resp)
	}
}

func TestIntegration_FencingToken_TwoPhase(t *testing.T) {
	cleanup, addr, _ := startServer(t, testConfig())
	defer cleanup()

	conn := dialTest(t, addr)
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Enqueue (should acquire immediately since no contention)
	resp := connSendCmd(t, conn, reader, "e", "fkey", "10")
	// Response: "acquired <token> <lease> <fence>"
	parts := strings.Fields(resp)
	if parts[0] != "acquired" {
		t.Fatalf("expected acquired, got %q", resp)
	}
	if len(parts) < 4 {
		t.Fatalf("expected acquired <token> <lease> <fence>, got %q", resp)
	}
	token := parts[1]
	fence1 := parts[3]

	// Release
	resp = connSendCmd(t, conn, reader, "r", "fkey", token)
	if resp != "ok" {
		t.Fatalf("expected ok, got %q", resp)
	}

	// Acquire again — fence should be higher
	resp = connSendCmd(t, conn, reader, "e", "fkey", "10")
	parts = strings.Fields(resp)
	if parts[0] != "acquired" {
		t.Fatalf("expected acquired, got %q", resp)
	}
	fence2 := parts[3]

	f1, _ := strconv.ParseUint(fence1, 10, 64)
	f2, _ := strconv.ParseUint(fence2, 10, 64)
	if f2 <= f1 {
		t.Fatalf("fence should increase: %d -> %d", f1, f2)
	}
}

func TestIntegration_RPop(t *testing.T) {
	cleanup, addr, _ := startServer(t, testConfig())
	defer cleanup()

	conn := dialTest(t, addr)
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Push two items
	resp := connSendCmd(t, conn, reader, "rpush", "rlist", "first")
	if !strings.HasPrefix(resp, "ok") {
		t.Fatalf("rpush first: expected ok, got %q", resp)
	}
	resp = connSendCmd(t, conn, reader, "rpush", "rlist", "second")
	if !strings.HasPrefix(resp, "ok") {
		t.Fatalf("rpush second: expected ok, got %q", resp)
	}

	// RPop should return the last pushed item
	resp = connSendCmd(t, conn, reader, "rpop", "rlist", "")
	if resp != "ok second" {
		t.Fatalf("rpop: expected 'ok second', got %q", resp)
	}

	// RPop again should return the first
	resp = connSendCmd(t, conn, reader, "rpop", "rlist", "")
	if resp != "ok first" {
		t.Fatalf("rpop: expected 'ok first', got %q", resp)
	}

	// RPop on empty list returns nil
	resp = connSendCmd(t, conn, reader, "rpop", "rlist", "")
	if resp != "nil" {
		t.Fatalf("rpop empty: expected 'nil', got %q", resp)
	}
}

func TestIntegration_RWLock_WriteEnqueueWait(t *testing.T) {
	cleanup, addr, _ := startServer(t, testConfig())
	defer cleanup()

	// conn1 takes a read lock
	conn1 := dialTest(t, addr)
	defer conn1.Close()
	reader1 := bufio.NewReader(conn1)

	resp := connSendCmd(t, conn1, reader1, "rl", "rwkey", "10")
	if !strings.HasPrefix(resp, "ok ") {
		t.Fatalf("rl: expected ok, got %q", resp)
	}
	readToken := strings.Fields(resp)[1]

	// conn2 write-enqueues (should be queued since read lock is held)
	conn2 := dialTest(t, addr)
	defer conn2.Close()
	reader2 := bufio.NewReader(conn2)

	resp = connSendCmd(t, conn2, reader2, "we", "rwkey", "10")
	if resp != "queued" {
		t.Fatalf("we: expected queued, got %q", resp)
	}

	// Release the read lock so the writer can proceed
	resp = connSendCmd(t, conn1, reader1, "rr", "rwkey", readToken)
	if resp != "ok" {
		t.Fatalf("rr: expected ok, got %q", resp)
	}

	// conn2 write-waits to get the write lock
	resp = connSendCmd(t, conn2, reader2, "ww", "rwkey", "5")
	parts := strings.Fields(resp)
	if parts[0] != "ok" {
		t.Fatalf("ww: expected ok, got %q", resp)
	}
	writeToken := parts[1]

	// Renew the write lock
	resp = connSendCmd(t, conn2, reader2, "wn", "rwkey", writeToken)
	if !strings.HasPrefix(resp, "ok ") {
		t.Fatalf("wn: expected ok, got %q", resp)
	}

	// Release write lock
	resp = connSendCmd(t, conn2, reader2, "wr", "rwkey", writeToken)
	if resp != "ok" {
		t.Fatalf("wr: expected ok, got %q", resp)
	}
}

func TestIntegration_Unwatch(t *testing.T) {
	cleanup, addr, _ := startServer(t, testConfig())
	defer cleanup()

	conn := dialTest(t, addr)
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Subscribe to a watch pattern
	resp := connSendCmd(t, conn, reader, "watch", "mypattern", "")
	if resp != "ok" {
		t.Fatalf("watch: expected ok, got %q", resp)
	}

	// Unwatch it
	resp = connSendCmd(t, conn, reader, "unwatch", "mypattern", "")
	if resp != "ok" {
		t.Fatalf("unwatch: expected ok, got %q", resp)
	}

	// Trigger the pattern — conn should NOT receive watch notification
	conn2 := dialTest(t, addr)
	defer conn2.Close()
	reader2 := bufio.NewReader(conn2)
	connSendCmd(t, conn2, reader2, "kset", "mypattern", "val 0")

	// Try to read from conn — set short deadline
	conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	_, err := reader.ReadString('\n')
	if err == nil {
		t.Fatal("expected no data after unwatch, but got a message")
	}
}

func TestIntegration_BarrierCountMismatch_Explicit(t *testing.T) {
	cleanup, addr, _ := startServer(t, testConfig())
	defer cleanup()

	// First connection sets barrier count = 3
	conn1 := dialTest(t, addr)
	defer conn1.Close()

	msg := fmt.Sprintf("bwait\nbarr1\n3 5\n")
	conn1.Write([]byte(msg))
	time.Sleep(50 * time.Millisecond)

	// Second connection tries to join with count = 5 — should get error_barrier_count_mismatch
	conn2 := dialTest(t, addr)
	defer conn2.Close()
	reader2 := bufio.NewReader(conn2)

	resp := connSendCmd(t, conn2, reader2, "bwait", "barr1", "5 5")
	if resp != "error_barrier_count_mismatch" {
		t.Fatalf("expected 'error_barrier_count_mismatch', got %q", resp)
	}
}

func TestIntegration_RWLock_DisconnectReleases(t *testing.T) {
	cleanup, addr, _ := startServer(t, testConfig())
	defer cleanup()

	// conn1 acquires a write lock
	conn1 := dialTest(t, addr)
	reader1 := bufio.NewReader(conn1)

	resp := connSendCmd(t, conn1, reader1, "wl", "rwdisco", "10")
	if !strings.HasPrefix(resp, "ok ") {
		t.Fatalf("wl: expected ok, got %q", resp)
	}

	// Disconnect conn1
	conn1.Close()
	time.Sleep(200 * time.Millisecond)

	// conn2 should be able to acquire the same write lock now
	conn2 := dialTest(t, addr)
	defer conn2.Close()
	reader2 := bufio.NewReader(conn2)

	resp = connSendCmd(t, conn2, reader2, "wl", "rwdisco", "10")
	if !strings.HasPrefix(resp, "ok ") {
		t.Fatalf("wl after disconnect: expected ok, got %q", resp)
	}
}

func TestIntegration_RWLock_ReadDisconnectReleases(t *testing.T) {
	cleanup, addr, _ := startServer(t, testConfig())
	defer cleanup()

	// conn1 acquires a read lock
	conn1 := dialTest(t, addr)
	reader1 := bufio.NewReader(conn1)

	resp := connSendCmd(t, conn1, reader1, "rl", "rwdisco2", "10")
	if !strings.HasPrefix(resp, "ok ") {
		t.Fatalf("rl: expected ok, got %q", resp)
	}

	// Disconnect conn1
	conn1.Close()
	time.Sleep(200 * time.Millisecond)

	// conn2 should be able to acquire a write lock (write lock blocked by readers)
	conn2 := dialTest(t, addr)
	defer conn2.Close()
	reader2 := bufio.NewReader(conn2)

	resp = connSendCmd(t, conn2, reader2, "wl", "rwdisco2", "10")
	if !strings.HasPrefix(resp, "ok ") {
		t.Fatalf("wl after read disconnect: expected ok, got %q", resp)
	}
}

func dialTest(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return conn
}

// ---------------------------------------------------------------------------
// Regression: Watch notifications use semantic event names, not command names
// ---------------------------------------------------------------------------

// TestIntegration_Watch_SemanticEventNames verifies that watch notifications
// use semantic event names (e.g. "acquire", "release") instead of raw protocol
// command names (e.g. "l", "r", "e").
func TestIntegration_Watch_SemanticEventNames(t *testing.T) {
	cleanup, addr, _ := startServer(t, testConfig())
	defer cleanup()

	// Watcher connection
	wConn := dialTest(t, addr)
	defer wConn.Close()
	wReader := bufio.NewReader(wConn)

	resp := connSendCmd(t, wConn, wReader, "watch", "semkey", "")
	if resp != "ok" {
		t.Fatalf("watch: expected ok, got %q", resp)
	}

	// Another connection acquires a lock (command "l")
	conn2 := dialTest(t, addr)
	defer conn2.Close()
	reader2 := bufio.NewReader(conn2)
	resp = connSendCmd(t, conn2, reader2, "l", "semkey", "10")
	if !strings.HasPrefix(resp, "ok ") {
		t.Fatalf("lock: expected ok, got %q", resp)
	}
	token := strings.Fields(resp)[1]

	// Watch event should say "acquire", NOT "l"
	wConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := wReader.ReadString('\n')
	if err != nil {
		t.Fatalf("read watch event: %v", err)
	}
	line = strings.TrimRight(line, "\n\r")
	if line != "watch acquire semkey" {
		t.Fatalf("expected 'watch acquire semkey', got %q", line)
	}

	// Release (command "r") should produce "release" event
	resp = connSendCmd(t, conn2, reader2, "r", "semkey", token)
	if resp != "ok" {
		t.Fatalf("release: expected ok, got %q", resp)
	}

	line, err = wReader.ReadString('\n')
	if err != nil {
		t.Fatalf("read release event: %v", err)
	}
	line = strings.TrimRight(line, "\n\r")
	if line != "watch release semkey" {
		t.Fatalf("expected 'watch release semkey', got %q", line)
	}
}

// TestIntegration_Watch_RWLockSemanticEvents verifies that RW lock enqueue
// and release produce "acquire" and "release" watch events.
func TestIntegration_Watch_RWLockSemanticEvents(t *testing.T) {
	cleanup, addr, _ := startServer(t, testConfig())
	defer cleanup()

	wConn := dialTest(t, addr)
	defer wConn.Close()
	wReader := bufio.NewReader(wConn)

	resp := connSendCmd(t, wConn, wReader, "watch", "rwkey", "")
	if resp != "ok" {
		t.Fatalf("watch: expected ok, got %q", resp)
	}

	// Acquire a read lock (command "rl")
	conn2 := dialTest(t, addr)
	defer conn2.Close()
	reader2 := bufio.NewReader(conn2)
	resp = connSendCmd(t, conn2, reader2, "rl", "rwkey", "10")
	if !strings.HasPrefix(resp, "ok ") {
		t.Fatalf("rl: expected ok, got %q", resp)
	}
	token := strings.Fields(resp)[1]

	wConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := wReader.ReadString('\n')
	if err != nil {
		t.Fatalf("read rl event: %v", err)
	}
	line = strings.TrimRight(line, "\n\r")
	if line != "watch acquire rwkey" {
		t.Fatalf("expected 'watch acquire rwkey', got %q", line)
	}

	// Release (command "rr")
	resp = connSendCmd(t, conn2, reader2, "rr", "rwkey", token)
	if resp != "ok" {
		t.Fatalf("rr: expected ok, got %q", resp)
	}

	line, err = wReader.ReadString('\n')
	if err != nil {
		t.Fatalf("read rr event: %v", err)
	}
	line = strings.TrimRight(line, "\n\r")
	if line != "watch release rwkey" {
		t.Fatalf("expected 'watch release rwkey', got %q", line)
	}
}

// ---------------------------------------------------------------------------
// Regression: MaxSubscriptions limit for watch/listen registrations
// ---------------------------------------------------------------------------

func TestIntegration_MaxSubscriptions_Watch(t *testing.T) {
	cfg := testConfig()
	cfg.MaxSubscriptions = 2
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn := dialTest(t, addr)
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// First two watches should succeed.
	resp := connSendCmd(t, conn, reader, "watch", "key1", "")
	if resp != "ok" {
		t.Fatalf("watch 1: expected ok, got %q", resp)
	}
	resp = connSendCmd(t, conn, reader, "watch", "key2", "")
	if resp != "ok" {
		t.Fatalf("watch 2: expected ok, got %q", resp)
	}

	// Third watch should be rejected.
	resp = connSendCmd(t, conn, reader, "watch", "key3", "")
	if !strings.HasPrefix(resp, "error") {
		t.Fatalf("watch 3: expected error, got %q", resp)
	}
}

func TestIntegration_MaxSubscriptions_Listen(t *testing.T) {
	cfg := testConfig()
	cfg.MaxSubscriptions = 2
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn := dialTest(t, addr)
	defer conn.Close()
	reader := bufio.NewReader(conn)

	resp := connSendCmd(t, conn, reader, "listen", "ch1", "")
	if resp != "ok" {
		t.Fatalf("listen 1: expected ok, got %q", resp)
	}
	resp = connSendCmd(t, conn, reader, "listen", "ch2", "")
	if resp != "ok" {
		t.Fatalf("listen 2: expected ok, got %q", resp)
	}

	// Third should be rejected.
	resp = connSendCmd(t, conn, reader, "listen", "ch3", "")
	if !strings.HasPrefix(resp, "error") {
		t.Fatalf("listen 3: expected error, got %q", resp)
	}
}

func TestIntegration_MaxSubscriptions_MixedWatchListen(t *testing.T) {
	cfg := testConfig()
	cfg.MaxSubscriptions = 2
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn := dialTest(t, addr)
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// One watch + one listen = 2 subscriptions.
	resp := connSendCmd(t, conn, reader, "watch", "key1", "")
	if resp != "ok" {
		t.Fatalf("watch: expected ok, got %q", resp)
	}
	resp = connSendCmd(t, conn, reader, "listen", "ch1", "")
	if resp != "ok" {
		t.Fatalf("listen: expected ok, got %q", resp)
	}

	// Third (either type) should be rejected.
	resp = connSendCmd(t, conn, reader, "watch", "key2", "")
	if !strings.HasPrefix(resp, "error") {
		t.Fatalf("expected error for exceeding subscription limit, got %q", resp)
	}
}

func TestIntegration_MaxSubscriptions_UnwatchFreesSlot(t *testing.T) {
	cfg := testConfig()
	cfg.MaxSubscriptions = 2
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn := dialTest(t, addr)
	defer conn.Close()
	reader := bufio.NewReader(conn)

	resp := connSendCmd(t, conn, reader, "watch", "key1", "")
	if resp != "ok" {
		t.Fatalf("watch 1: expected ok, got %q", resp)
	}
	resp = connSendCmd(t, conn, reader, "watch", "key2", "")
	if resp != "ok" {
		t.Fatalf("watch 2: expected ok, got %q", resp)
	}

	// At limit — unwatch one to free a slot.
	resp = connSendCmd(t, conn, reader, "unwatch", "key1", "")
	if resp != "ok" {
		t.Fatalf("unwatch: expected ok, got %q", resp)
	}

	// Now key3 should succeed.
	resp = connSendCmd(t, conn, reader, "watch", "key3", "")
	if resp != "ok" {
		t.Fatalf("watch 3 after unwatch: expected ok, got %q", resp)
	}
}

// ===========================================================================
// Additional comprehensive integration tests
// ===========================================================================

func TestIntegration_MalformedCommand(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Send garbage bytes that do not form a valid 3-line command.
	garbage := []byte{0xFF, 0xFE, 0x00, 0x01, 0x02, '\n', 'x', '\n', 'y', '\n'}
	if _, err := conn.Write(garbage); err != nil {
		t.Fatalf("write garbage: %v", err)
	}

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		// Connection was closed by server, which is acceptable.
		return
	}
	line = strings.TrimRight(line, "\r\n")
	// Server should respond with "error" for an invalid command.
	if !strings.HasPrefix(line, "error") {
		t.Fatalf("expected error response for garbage input, got %q", line)
	}

	// After the error response, the server should still be alive.
	// Try to send a valid command on a new connection to verify no crash.
	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("server crashed after garbage input: %v", err)
	}
	defer conn2.Close()
	r2 := bufio.NewReader(conn2)
	resp := connSendCmd(t, conn2, r2, "incr", "alive_check", "1")
	if resp != "ok 1" {
		t.Fatalf("expected 'ok 1' after garbage test, got %q", resp)
	}
}

func TestIntegration_EmptyKeyErrors(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	// Each command that requires a key should fail with "error" when the key
	// is empty. We test several representative commands.
	commands := []struct {
		cmd string
		arg string
	}{
		{"l", "10"},
		{"sl", "10 3"},
		{"kset", "hello"},
		{"incr", "1"},
		{"rpush", "value"},
		{"watch", ""},
		{"listen", ""},
		{"bwait", "2 5"},
		{"elect", "5 10"},
	}

	for _, tc := range commands {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("dial for cmd %q: %v", tc.cmd, err)
		}
		reader := bufio.NewReader(conn)

		// Send the command with an empty key (second line is empty).
		msg := fmt.Sprintf("%s\n\n%s\n", tc.cmd, tc.arg)
		if _, err := conn.Write([]byte(msg)); err != nil {
			conn.Close()
			t.Fatalf("write cmd %q: %v", tc.cmd, err)
		}
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		line, err := reader.ReadString('\n')
		conn.Close()
		if err != nil {
			// Connection closed is acceptable for protocol errors.
			continue
		}
		line = strings.TrimRight(line, "\r\n")
		if !strings.HasPrefix(line, "error") {
			t.Fatalf("cmd %q with empty key: expected error, got %q", tc.cmd, line)
		}
	}
}

func TestIntegration_ReleaseWrongKey(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Acquire a lock to get a valid token.
	resp := connSendCmd(t, conn, reader, "l", "realkey", "10")
	parts := strings.Fields(resp)
	if len(parts) != 4 || parts[0] != "ok" {
		t.Fatalf("lock: expected ok, got %q", resp)
	}
	token := parts[1]

	// Try to release a completely different (non-existent) key with the same token.
	resp = connSendCmd(t, conn, reader, "r", "nonexistent_key", token)
	if resp != "error" {
		t.Fatalf("expected 'error' for release on wrong key, got %q", resp)
	}
}

func TestIntegration_RenewAfterRelease(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Acquire
	resp := connSendCmd(t, conn, reader, "l", "renewkey", "10")
	parts := strings.Fields(resp)
	if len(parts) != 4 || parts[0] != "ok" {
		t.Fatalf("lock: expected ok, got %q", resp)
	}
	token := parts[1]

	// Release
	resp = connSendCmd(t, conn, reader, "r", "renewkey", token)
	if resp != "ok" {
		t.Fatalf("release: expected ok, got %q", resp)
	}

	// Try to renew with the same token after release.
	resp = connSendCmd(t, conn, reader, "n", "renewkey", token)
	if resp != "error" {
		t.Fatalf("renew after release: expected 'error', got %q", resp)
	}
}

func TestIntegration_Election_ResignAndReelect(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn := dialTest(t, addr)
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// First election
	resp := connSendCmd(t, conn, reader, "elect", "election1", "5 10")
	parts := strings.Fields(resp)
	if len(parts) < 4 || parts[0] != "ok" {
		t.Fatalf("elect1: expected ok <token> <lease> <fence>, got %q", resp)
	}
	token1 := parts[1]
	fence1, err := strconv.ParseUint(parts[3], 10, 64)
	if err != nil {
		t.Fatalf("parse fence1: %v", err)
	}

	// Resign
	resp = connSendCmd(t, conn, reader, "resign", "election1", token1)
	if resp != "ok" {
		t.Fatalf("resign: expected ok, got %q", resp)
	}

	// Re-elect on the same key
	resp = connSendCmd(t, conn, reader, "elect", "election1", "5 10")
	parts = strings.Fields(resp)
	if len(parts) < 4 || parts[0] != "ok" {
		t.Fatalf("elect2: expected ok <token> <lease> <fence>, got %q", resp)
	}
	fence2, err := strconv.ParseUint(parts[3], 10, 64)
	if err != nil {
		t.Fatalf("parse fence2: %v", err)
	}

	// Fencing token must monotonically increase.
	if fence2 <= fence1 {
		t.Fatalf("expected fence2 (%d) > fence1 (%d)", fence2, fence1)
	}
}

func TestIntegration_Election_ObserveElectResign(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	// Observer connection
	obsConn := dialTest(t, addr)
	defer obsConn.Close()
	obsReader := bufio.NewReader(obsConn)

	resp := connSendCmd(t, obsConn, obsReader, "observe", "obs_election", "")
	if resp != "ok" {
		t.Fatalf("observe: expected ok, got %q", resp)
	}

	// Leader connection
	leaderConn := dialTest(t, addr)
	defer leaderConn.Close()
	leaderReader := bufio.NewReader(leaderConn)

	// First elect
	resp = connSendCmd(t, leaderConn, leaderReader, "elect", "obs_election", "5 10")
	parts := strings.Fields(resp)
	if len(parts) < 4 || parts[0] != "ok" {
		t.Fatalf("elect1: expected ok, got %q", resp)
	}
	token1 := parts[1]

	// Observer should receive "leader elected obs_election"
	obsConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := obsReader.ReadString('\n')
	if err != nil {
		t.Fatalf("read elected1 event: %v", err)
	}
	line = strings.TrimRight(line, "\n\r")
	if line != "leader elected obs_election" {
		t.Fatalf("expected 'leader elected obs_election', got %q", line)
	}

	// Resign
	resp = connSendCmd(t, leaderConn, leaderReader, "resign", "obs_election", token1)
	if resp != "ok" {
		t.Fatalf("resign: expected ok, got %q", resp)
	}

	// Observer should receive "leader resigned obs_election"
	line, err = obsReader.ReadString('\n')
	if err != nil {
		t.Fatalf("read resigned event: %v", err)
	}
	line = strings.TrimRight(line, "\n\r")
	if line != "leader resigned obs_election" {
		t.Fatalf("expected 'leader resigned obs_election', got %q", line)
	}

	// Re-elect
	resp = connSendCmd(t, leaderConn, leaderReader, "elect", "obs_election", "5 10")
	parts = strings.Fields(resp)
	if len(parts) < 4 || parts[0] != "ok" {
		t.Fatalf("elect2: expected ok, got %q", resp)
	}

	// Observer should receive "leader elected obs_election" again
	line, err = obsReader.ReadString('\n')
	if err != nil {
		t.Fatalf("read elected2 event: %v", err)
	}
	line = strings.TrimRight(line, "\n\r")
	if line != "leader elected obs_election" {
		t.Fatalf("expected 'leader elected obs_election' (2nd), got %q", line)
	}
}

func TestIntegration_Watch_CounterOps(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	// Watcher connection
	wConn := dialTest(t, addr)
	defer wConn.Close()
	wReader := bufio.NewReader(wConn)

	resp := connSendCmd(t, wConn, wReader, "watch", "ctr_watch", "")
	if resp != "ok" {
		t.Fatalf("watch: expected ok, got %q", resp)
	}

	// Mutator connection
	mConn := dialTest(t, addr)
	defer mConn.Close()
	mReader := bufio.NewReader(mConn)

	// incr
	resp = connSendCmd(t, mConn, mReader, "incr", "ctr_watch", "5")
	if resp != "ok 5" {
		t.Fatalf("incr: expected 'ok 5', got %q", resp)
	}

	wConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := wReader.ReadString('\n')
	if err != nil {
		t.Fatalf("read incr event: %v", err)
	}
	line = strings.TrimRight(line, "\n\r")
	if line != "watch incr ctr_watch" {
		t.Fatalf("expected 'watch incr ctr_watch', got %q", line)
	}

	// decr
	resp = connSendCmd(t, mConn, mReader, "decr", "ctr_watch", "2")
	if resp != "ok 3" {
		t.Fatalf("decr: expected 'ok 3', got %q", resp)
	}

	line, err = wReader.ReadString('\n')
	if err != nil {
		t.Fatalf("read decr event: %v", err)
	}
	line = strings.TrimRight(line, "\n\r")
	if line != "watch decr ctr_watch" {
		t.Fatalf("expected 'watch decr ctr_watch', got %q", line)
	}

	// cset
	resp = connSendCmd(t, mConn, mReader, "cset", "ctr_watch", "100")
	if resp != "ok" {
		t.Fatalf("cset: expected 'ok', got %q", resp)
	}

	line, err = wReader.ReadString('\n')
	if err != nil {
		t.Fatalf("read cset event: %v", err)
	}
	line = strings.TrimRight(line, "\n\r")
	if line != "watch cset ctr_watch" {
		t.Fatalf("expected 'watch cset ctr_watch', got %q", line)
	}
}

func TestIntegration_Watch_ListOps(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	// Watcher connection
	wConn := dialTest(t, addr)
	defer wConn.Close()
	wReader := bufio.NewReader(wConn)

	resp := connSendCmd(t, wConn, wReader, "watch", "list_watch", "")
	if resp != "ok" {
		t.Fatalf("watch: expected ok, got %q", resp)
	}

	// Mutator connection
	mConn := dialTest(t, addr)
	defer mConn.Close()
	mReader := bufio.NewReader(mConn)

	// lpush
	resp = connSendCmd(t, mConn, mReader, "lpush", "list_watch", "a")
	if !strings.HasPrefix(resp, "ok") {
		t.Fatalf("lpush: expected ok, got %q", resp)
	}

	wConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := wReader.ReadString('\n')
	if err != nil {
		t.Fatalf("read lpush event: %v", err)
	}
	line = strings.TrimRight(line, "\n\r")
	if line != "watch lpush list_watch" {
		t.Fatalf("expected 'watch lpush list_watch', got %q", line)
	}

	// rpush
	resp = connSendCmd(t, mConn, mReader, "rpush", "list_watch", "b")
	if !strings.HasPrefix(resp, "ok") {
		t.Fatalf("rpush: expected ok, got %q", resp)
	}

	line, err = wReader.ReadString('\n')
	if err != nil {
		t.Fatalf("read rpush event: %v", err)
	}
	line = strings.TrimRight(line, "\n\r")
	if line != "watch rpush list_watch" {
		t.Fatalf("expected 'watch rpush list_watch', got %q", line)
	}

	// lpop
	resp = connSendCmd(t, mConn, mReader, "lpop", "list_watch", "")
	if !strings.HasPrefix(resp, "ok") {
		t.Fatalf("lpop: expected ok, got %q", resp)
	}

	line, err = wReader.ReadString('\n')
	if err != nil {
		t.Fatalf("read lpop event: %v", err)
	}
	line = strings.TrimRight(line, "\n\r")
	if line != "watch lpop list_watch" {
		t.Fatalf("expected 'watch lpop list_watch', got %q", line)
	}

	// rpop
	resp = connSendCmd(t, mConn, mReader, "rpop", "list_watch", "")
	if !strings.HasPrefix(resp, "ok") {
		t.Fatalf("rpop: expected ok, got %q", resp)
	}

	line, err = wReader.ReadString('\n')
	if err != nil {
		t.Fatalf("read rpop event: %v", err)
	}
	line = strings.TrimRight(line, "\n\r")
	if line != "watch rpop list_watch" {
		t.Fatalf("expected 'watch rpop list_watch', got %q", line)
	}
}

func TestIntegration_Watch_WildcardNotMatch(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	// Watcher subscribes to "foo.*"
	wConn := dialTest(t, addr)
	defer wConn.Close()
	wReader := bufio.NewReader(wConn)

	resp := connSendCmd(t, wConn, wReader, "watch", "foo.*", "")
	if resp != "ok" {
		t.Fatalf("watch: expected ok, got %q", resp)
	}

	// Signal "bar.baz" via a kset (which triggers watch notify) on a non-matching key.
	mConn := dialTest(t, addr)
	defer mConn.Close()
	mReader := bufio.NewReader(mConn)

	resp = connSendCmd(t, mConn, mReader, "kset", "bar.baz", "value")
	if resp != "ok" {
		t.Fatalf("kset: expected ok, got %q", resp)
	}

	// Watcher should NOT receive any event.
	wConn.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	_, err := wReader.ReadString('\n')
	if err == nil {
		t.Fatal("expected no watch event for non-matching key, but got one")
	}
}

func TestIntegration_Signal_NoListeners(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn := dialTest(t, addr)
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Signal with no listeners at all.
	resp := connSendCmd(t, conn, reader, "signal", "no.listeners.channel", "payload")
	if resp != "ok 0" {
		t.Fatalf("expected 'ok 0', got %q", resp)
	}
}

func TestIntegration_Barrier_ZeroTimeout(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn := dialTest(t, addr)
	defer conn.Close()

	// Barrier with count=3 but only 1 participant and timeout=0.
	resp := sendCmd(t, conn, "bwait", "barrier_zero", "3 0")
	if resp != "timeout" {
		t.Fatalf("expected 'timeout', got %q", resp)
	}
}

func TestIntegration_KVCAS_MultiStep(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn := dialTest(t, addr)
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Step 1: Create key via CAS (old value empty, new value "v1").
	resp := connSendCmd(t, conn, reader, "kcas", "cas_multi", "\tv1\t0")
	if resp != "ok" {
		t.Fatalf("cas create: expected ok, got %q", resp)
	}

	// Verify
	resp = connSendCmd(t, conn, reader, "kget", "cas_multi", "")
	if resp != "ok v1" {
		t.Fatalf("get after create: expected 'ok v1', got %q", resp)
	}

	// Step 2: Swap v1 -> v2
	resp = connSendCmd(t, conn, reader, "kcas", "cas_multi", "v1\tv2\t0")
	if resp != "ok" {
		t.Fatalf("cas swap1: expected ok, got %q", resp)
	}

	resp = connSendCmd(t, conn, reader, "kget", "cas_multi", "")
	if resp != "ok v2" {
		t.Fatalf("get after swap1: expected 'ok v2', got %q", resp)
	}

	// Step 3: Swap v2 -> v3
	resp = connSendCmd(t, conn, reader, "kcas", "cas_multi", "v2\tv3\t0")
	if resp != "ok" {
		t.Fatalf("cas swap2: expected ok, got %q", resp)
	}

	resp = connSendCmd(t, conn, reader, "kget", "cas_multi", "")
	if resp != "ok v3" {
		t.Fatalf("get after swap2: expected 'ok v3', got %q", resp)
	}

	// Step 4: Attempt with wrong old value should conflict.
	resp = connSendCmd(t, conn, reader, "kcas", "cas_multi", "v1\tv4\t0")
	if resp != "cas_conflict" {
		t.Fatalf("cas conflict: expected cas_conflict, got %q", resp)
	}

	// Value should still be v3.
	resp = connSendCmd(t, conn, reader, "kget", "cas_multi", "")
	if resp != "ok v3" {
		t.Fatalf("get after conflict: expected 'ok v3', got %q", resp)
	}
}

func TestIntegration_MaxSubscriptions_UnlistenFreesSlot(t *testing.T) {
	cfg := testConfig()
	cfg.MaxSubscriptions = 2
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn := dialTest(t, addr)
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Fill both subscription slots with listen.
	resp := connSendCmd(t, conn, reader, "listen", "ch1", "")
	if resp != "ok" {
		t.Fatalf("listen 1: expected ok, got %q", resp)
	}
	resp = connSendCmd(t, conn, reader, "listen", "ch2", "")
	if resp != "ok" {
		t.Fatalf("listen 2: expected ok, got %q", resp)
	}

	// Third should be rejected.
	resp = connSendCmd(t, conn, reader, "listen", "ch3", "")
	if !strings.HasPrefix(resp, "error") {
		t.Fatalf("listen 3: expected error, got %q", resp)
	}

	// Unlisten one to free a slot.
	resp = connSendCmd(t, conn, reader, "unlisten", "ch1", "")
	if resp != "ok" {
		t.Fatalf("unlisten: expected ok, got %q", resp)
	}

	// Now a new listen should succeed.
	resp = connSendCmd(t, conn, reader, "listen", "ch3", "")
	if resp != "ok" {
		t.Fatalf("listen 3 after unlisten: expected ok, got %q", resp)
	}
}

func TestIntegration_RWLock_MultipleReadersOneWriter(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	// Three readers acquire the same read lock.
	readers := make([]net.Conn, 3)
	rReaders := make([]*bufio.Reader, 3)
	tokens := make([]string, 3)

	for i := 0; i < 3; i++ {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("dial reader %d: %v", i, err)
		}
		defer conn.Close()
		readers[i] = conn
		rReaders[i] = bufio.NewReader(conn)

		resp := connSendCmd(t, readers[i], rReaders[i], "rl", "rw_multi", "10")
		parts := strings.Fields(resp)
		if len(parts) != 4 || parts[0] != "ok" {
			t.Fatalf("reader %d rl: expected ok, got %q", i, resp)
		}
		tokens[i] = parts[1]
	}

	// Writer tries to acquire with timeout=0 -- should fail because readers hold the lock.
	wConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer wConn.Close()
	wReader := bufio.NewReader(wConn)

	resp := connSendCmd(t, wConn, wReader, "wl", "rw_multi", "0")
	if resp != "timeout" {
		t.Fatalf("writer wl with readers held: expected 'timeout', got %q", resp)
	}

	// Writer requests with longer timeout in background.
	type result struct {
		resp string
		err  error
	}
	writerDone := make(chan result, 1)

	wConn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer wConn2.Close()
	wReader2 := bufio.NewReader(wConn2)

	go func() {
		msg := "wl\nrw_multi\n10\n"
		if _, err := wConn2.Write([]byte(msg)); err != nil {
			writerDone <- result{err: err}
			return
		}
		wConn2.SetReadDeadline(time.Now().Add(15 * time.Second))
		line, err := wReader2.ReadString('\n')
		writerDone <- result{resp: strings.TrimRight(line, "\r\n"), err: err}
	}()

	// Give writer time to block.
	time.Sleep(100 * time.Millisecond)

	// Release readers one by one.
	for i := 0; i < 3; i++ {
		r := connSendCmd(t, readers[i], rReaders[i], "rr", "rw_multi", tokens[i])
		if r != "ok" {
			t.Fatalf("rr reader %d: expected ok, got %q", i, r)
		}

		if i < 2 {
			// Writer should still be blocked after partial release.
			select {
			case <-writerDone:
				t.Fatalf("writer acquired too early (after releasing only %d of 3 readers)", i+1)
			case <-time.After(100 * time.Millisecond):
				// Expected: writer still blocked.
			}
		}
	}

	// After all readers released, writer should acquire.
	wr := <-writerDone
	if wr.err != nil {
		t.Fatalf("writer read error: %v", wr.err)
	}
	parts := strings.Fields(wr.resp)
	if len(parts) != 4 || parts[0] != "ok" {
		t.Fatalf("writer wl: expected ok <token> <lease> <fence>, got %q", wr.resp)
	}
}

func TestIntegration_SemTwoPhase_Contention(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	// conn1 acquires the semaphore (limit=1)
	conn1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn1.Close()
	r1 := bufio.NewReader(conn1)

	resp := connSendCmd(t, conn1, r1, "sl", "sem_fifo", "10 1")
	parts := strings.Fields(resp)
	if len(parts) != 4 || parts[0] != "ok" {
		t.Fatalf("conn1 sl: expected ok, got %q", resp)
	}
	token1 := parts[1]

	// conn2 and conn3 enqueue (two-phase) in order.
	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()
	r2 := bufio.NewReader(conn2)

	resp = connSendCmd(t, conn2, r2, "se", "sem_fifo", "1")
	if resp != "queued" {
		t.Fatalf("conn2 se: expected queued, got %q", resp)
	}

	time.Sleep(50 * time.Millisecond)

	conn3, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn3.Close()
	r3 := bufio.NewReader(conn3)

	resp = connSendCmd(t, conn3, r3, "se", "sem_fifo", "1")
	if resp != "queued" {
		t.Fatalf("conn3 se: expected queued, got %q", resp)
	}

	// conn2 and conn3 wait in background.
	type result struct {
		id   int
		resp string
		err  error
	}
	ch := make(chan result, 2)

	for i, pair := range []struct {
		conn   net.Conn
		reader *bufio.Reader
	}{{conn2, r2}, {conn3, r3}} {
		i := i
		c := pair.conn
		rd := pair.reader
		go func() {
			msg := "sw\nsem_fifo\n10\n"
			if _, err := c.Write([]byte(msg)); err != nil {
				ch <- result{id: i, err: err}
				return
			}
			c.SetReadDeadline(time.Now().Add(10 * time.Second))
			line, err := rd.ReadString('\n')
			ch <- result{id: i, resp: strings.TrimRight(line, "\r\n"), err: err}
		}()
	}

	time.Sleep(100 * time.Millisecond)

	// Release conn1 -- conn2 (first enqueued) should get it.
	resp = connSendCmd(t, conn1, r1, "sr", "sem_fifo", token1)
	if resp != "ok" {
		t.Fatalf("conn1 sr: expected ok, got %q", resp)
	}

	// First result should be conn2 (id=0)
	first := <-ch
	if first.err != nil {
		t.Fatalf("first wait error: %v", first.err)
	}
	parts = strings.Fields(first.resp)
	if len(parts) != 4 || parts[0] != "ok" {
		t.Fatalf("first wait: expected ok, got %q", first.resp)
	}
	if first.id != 0 {
		t.Fatalf("expected conn2 (id=0) to acquire first, but got id=%d", first.id)
	}
	token2 := parts[1]

	// Release conn2's semaphore so conn3 can acquire.
	releaseConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseConn.Close()
	releaseReader := bufio.NewReader(releaseConn)
	connSendCmd(t, releaseConn, releaseReader, "sr", "sem_fifo", token2)

	// conn3 should acquire now.
	second := <-ch
	if second.err != nil {
		t.Fatalf("second wait error: %v", second.err)
	}
	parts = strings.Fields(second.resp)
	if len(parts) != 4 || parts[0] != "ok" {
		t.Fatalf("second wait: expected ok, got %q", second.resp)
	}
}

func TestIntegration_MultipleListOps(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// rpush 5 items: a, b, c, d, e
	for i, val := range []string{"a", "b", "c", "d", "e"} {
		resp := connSendCmd(t, conn, reader, "rpush", "biglist", val)
		expected := fmt.Sprintf("ok %d", i+1)
		if resp != expected {
			t.Fatalf("rpush %s: expected %q, got %q", val, expected, resp)
		}
	}

	// lpop 2: should get a, then b
	resp := connSendCmd(t, conn, reader, "lpop", "biglist", "")
	if resp != "ok a" {
		t.Fatalf("lpop 1: expected 'ok a', got %q", resp)
	}
	resp = connSendCmd(t, conn, reader, "lpop", "biglist", "")
	if resp != "ok b" {
		t.Fatalf("lpop 2: expected 'ok b', got %q", resp)
	}

	// lrange: remaining should be [c, d, e]
	resp = connSendCmd(t, conn, reader, "lrange", "biglist", "0 -1")
	if !strings.HasPrefix(resp, "ok ") {
		t.Fatalf("lrange: expected ok, got %q", resp)
	}
	jsonPart := resp[3:]
	var items []string
	if err := json.Unmarshal([]byte(jsonPart), &items); err != nil {
		t.Fatalf("lrange JSON parse: %v", err)
	}
	if len(items) != 3 || items[0] != "c" || items[1] != "d" || items[2] != "e" {
		t.Fatalf("lrange: expected [c d e], got %v", items)
	}

	// rpop 1: should get e
	resp = connSendCmd(t, conn, reader, "rpop", "biglist", "")
	if resp != "ok e" {
		t.Fatalf("rpop: expected 'ok e', got %q", resp)
	}

	// llen: should be 2
	resp = connSendCmd(t, conn, reader, "llen", "biglist", "")
	if resp != "ok 2" {
		t.Fatalf("llen: expected 'ok 2', got %q", resp)
	}
}

func TestIntegration_BLPop_MultipleWaiters(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	type result struct {
		id   int
		resp string
		err  error
	}
	ch := make(chan result, 2)

	// Two connections block on blpop.
	conns := make([]net.Conn, 2)
	for i := 0; i < 2; i++ {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("dial waiter %d: %v", i, err)
		}
		defer conn.Close()
		conns[i] = conn

		i := i
		c := conn
		go func() {
			rd := bufio.NewReader(c)
			msg := "blpop\nblq_multi\n10\n"
			if _, err := c.Write([]byte(msg)); err != nil {
				ch <- result{id: i, err: err}
				return
			}
			c.SetReadDeadline(time.Now().Add(15 * time.Second))
			line, err := rd.ReadString('\n')
			ch <- result{id: i, resp: strings.TrimRight(line, "\r\n"), err: err}
		}()
		// Stagger to ensure FIFO ordering.
		time.Sleep(100 * time.Millisecond)
	}

	// Pusher connection
	pusher, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer pusher.Close()
	pReader := bufio.NewReader(pusher)

	// First push: first waiter should get it.
	connSendCmd(t, pusher, pReader, "rpush", "blq_multi", "first_val")

	r1 := <-ch
	if r1.err != nil {
		t.Fatalf("waiter 1 error: %v", r1.err)
	}
	if r1.resp != "ok first_val" {
		t.Fatalf("waiter 1: expected 'ok first_val', got %q", r1.resp)
	}
	if r1.id != 0 {
		t.Fatalf("expected first waiter (id=0) to get first push, got id=%d", r1.id)
	}

	// Second push: second waiter should get it.
	connSendCmd(t, pusher, pReader, "rpush", "blq_multi", "second_val")

	r2 := <-ch
	if r2.err != nil {
		t.Fatalf("waiter 2 error: %v", r2.err)
	}
	if r2.resp != "ok second_val" {
		t.Fatalf("waiter 2: expected 'ok second_val', got %q", r2.resp)
	}
}

func TestIntegration_Auth_CommandBeforeAuth(t *testing.T) {
	cfg := testConfig()
	cleanup, addr := startAuthServer(t, cfg, "secret_token")
	defer cleanup()

	// When auth is required, the server reads the first command. If it is not
	// a valid "auth" command with the correct token, it responds with
	// "error_auth" and closes the connection.

	// Test 1: Sending a lock command instead of auth on a fresh connection.
	conn1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn1.Close()
	r1 := bufio.NewReader(conn1)
	resp := connSendCmd(t, conn1, r1, "l", "mykey", "10")
	if resp != "error_auth" {
		t.Fatalf("lock before auth: expected 'error_auth', got %q", resp)
	}

	// Test 2: Sending an incr command instead of auth on a fresh connection.
	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()
	r2 := bufio.NewReader(conn2)
	resp = connSendCmd(t, conn2, r2, "incr", "ctr", "1")
	if resp != "error_auth" {
		t.Fatalf("incr before auth: expected 'error_auth', got %q", resp)
	}

	// Test 3: Sending a kset command instead of auth on a fresh connection.
	conn3, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn3.Close()
	r3 := bufio.NewReader(conn3)
	resp = connSendCmd(t, conn3, r3, "kset", "key", "val")
	if resp != "error_auth" {
		t.Fatalf("kset before auth: expected 'error_auth', got %q", resp)
	}

	// Test 4: After authenticating correctly on a new connection, commands work.
	// Allow time for the auth failure delay to pass.
	time.Sleep(150 * time.Millisecond)

	conn4, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn4.Close()
	r4 := bufio.NewReader(conn4)

	resp = connSendCmd(t, conn4, r4, "auth", "_", "secret_token")
	if resp != "ok" {
		t.Fatalf("auth: expected 'ok', got %q", resp)
	}

	resp = connSendCmd(t, conn4, r4, "incr", "ctr", "1")
	if resp != "ok 1" {
		t.Fatalf("incr after auth: expected 'ok 1', got %q", resp)
	}
}

// ---------------------------------------------------------------------------
// Gap-filling: observe after elect (initial status notification)
// ---------------------------------------------------------------------------

func TestIntegration_Election_ObserveAfterElect(t *testing.T) {
	cleanup, addr, _ := startServer(t, testConfig())
	defer cleanup()

	// Elect a leader FIRST.
	leaderConn := dialTest(t, addr)
	defer leaderConn.Close()
	resp := sendCmd(t, leaderConn, "elect", "obs-late", "5 10")
	if !strings.HasPrefix(resp, "ok ") {
		t.Fatalf("elect: expected ok, got %q", resp)
	}

	// THEN observe — should receive immediate "leader elected" notification.
	obsConn := dialTest(t, addr)
	defer obsConn.Close()
	obsReader := bufio.NewReader(obsConn)

	resp = connSendCmd(t, obsConn, obsReader, "observe", "obs-late", "")
	if resp != "ok" {
		t.Fatalf("observe: expected ok, got %q", resp)
	}

	// Read the initial notification.
	obsConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := obsReader.ReadString('\n')
	if err != nil {
		t.Fatalf("read initial notification: %v", err)
	}
	line = strings.TrimRight(line, "\n\r")
	if line != "leader elected obs-late" {
		t.Fatalf("expected 'leader elected obs-late', got %q", line)
	}
}

// TestIntegration_Election_ResignFailoverObserve tests the full resign-failover
// cycle as seen by an observer: elected → resigned → elected (failover).
func TestIntegration_Election_ResignFailoverObserve(t *testing.T) {
	cleanup, addr, _ := startServer(t, testConfig())
	defer cleanup()

	// Observer
	obsConn := dialTest(t, addr)
	defer obsConn.Close()
	obsReader := bufio.NewReader(obsConn)
	resp := connSendCmd(t, obsConn, obsReader, "observe", "failover1", "")
	if resp != "ok" {
		t.Fatalf("observe: expected ok, got %q", resp)
	}

	// Leader 1 elects
	l1Conn := dialTest(t, addr)
	defer l1Conn.Close()
	l1Reader := bufio.NewReader(l1Conn)
	resp = connSendCmd(t, l1Conn, l1Reader, "elect", "failover1", "5 10")
	if !strings.HasPrefix(resp, "ok ") {
		t.Fatalf("elect 1: expected ok, got %q", resp)
	}
	token1 := strings.Fields(resp)[1]

	// Observer sees "elected"
	obsConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := obsReader.ReadString('\n')
	if err != nil {
		t.Fatalf("read elected 1: %v", err)
	}
	if strings.TrimRight(line, "\n\r") != "leader elected failover1" {
		t.Fatalf("expected elected event, got %q", line)
	}

	// Leader 2 blocks on elect (waits for leader 1 to resign)
	l2Conn := dialTest(t, addr)
	defer l2Conn.Close()
	l2Reader := bufio.NewReader(l2Conn)
	go func() {
		msg := "elect\nfailover1\n5 10\n"
		l2Conn.Write([]byte(msg))
	}()

	time.Sleep(100 * time.Millisecond)

	// Leader 1 resigns
	resp = connSendCmd(t, l1Conn, l1Reader, "resign", "failover1", token1)
	if resp != "ok" {
		t.Fatalf("resign: expected ok, got %q", resp)
	}

	// Observer sees "resigned"
	line, err = obsReader.ReadString('\n')
	if err != nil {
		t.Fatalf("read resigned: %v", err)
	}
	if strings.TrimRight(line, "\n\r") != "leader resigned failover1" {
		t.Fatalf("expected resigned event, got %q", line)
	}

	// Observer sees "elected" (failover to leader 2)
	line, err = obsReader.ReadString('\n')
	if err != nil {
		t.Fatalf("read failover elected: %v", err)
	}
	trimmed := strings.TrimRight(line, "\n\r")
	if trimmed != "leader elected failover1" && trimmed != "leader failover failover1" {
		t.Fatalf("expected failover elected event, got %q", line)
	}

	// Leader 2 should have received its elect response.
	l2Conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	l2line, err := l2Reader.ReadString('\n')
	if err != nil {
		t.Fatalf("leader 2 response: %v", err)
	}
	if !strings.HasPrefix(strings.TrimRight(l2line, "\n\r"), "ok ") {
		t.Fatalf("leader 2: expected ok, got %q", l2line)
	}
}

// ---------------------------------------------------------------------------
// Gap-filling: Server.Run() happy-path and error-path tests
// ---------------------------------------------------------------------------

func TestServer_Run_HappyPath(t *testing.T) {
	cfg := testConfig()
	// Use port 0 to let OS assign a free port
	cfg.Port = 0
	cfg.Host = "127.0.0.1"
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	lm := lock.NewLockManager(cfg, log)
	srv := New(lm, cfg, log)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Run(ctx)
	}()

	// Give it time to start listening
	time.Sleep(100 * time.Millisecond)

	// Shut it down
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after context cancel")
	}
}

func TestServer_Run_PortAlreadyBound(t *testing.T) {
	// Bind a port first
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	// Extract port from the listener
	port := ln.Addr().(*net.TCPAddr).Port

	cfg := testConfig()
	cfg.Port = port
	cfg.Host = "127.0.0.1"
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	lm := lock.NewLockManager(cfg, log)
	srv := New(lm, cfg, log)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = srv.Run(ctx)
	if err == nil {
		t.Fatal("expected error when port is already bound")
	}
	if !strings.Contains(err.Error(), "listen") {
		t.Fatalf("expected listen error, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Regression: duplicate listen/watch should NOT inflate subscription counter,
// and unlisten/unwatch on nonexistent pattern should NOT deflate it.
// ---------------------------------------------------------------------------

func TestIntegration_DuplicateListenDoesNotInflateCounter(t *testing.T) {
	cfg := testConfig()
	cfg.MaxSubscriptions = 2
	_, addr, _ := startServer(t, cfg)

	conn := dialTest(t, addr)
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Listen to a pattern
	resp := connSendCmd(t, conn, reader, "listen", "ch1", "")
	if resp != "ok" {
		t.Fatalf("listen: expected ok, got %q", resp)
	}

	// Duplicate listen should succeed but NOT use a subscription slot
	resp = connSendCmd(t, conn, reader, "listen", "ch1", "")
	if resp != "ok" {
		t.Fatalf("duplicate listen: expected ok, got %q", resp)
	}

	// Second distinct subscription should still work (not blocked by duplicate)
	resp = connSendCmd(t, conn, reader, "listen", "ch2", "")
	if resp != "ok" {
		t.Fatalf("listen ch2: expected ok, got %q", resp)
	}
}

func TestIntegration_DuplicateWatchDoesNotInflateCounter(t *testing.T) {
	cfg := testConfig()
	cfg.MaxSubscriptions = 2
	_, addr, _ := startServer(t, cfg)

	conn := dialTest(t, addr)
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Watch a key
	resp := connSendCmd(t, conn, reader, "watch", "k1", "")
	if resp != "ok" {
		t.Fatalf("watch: expected ok, got %q", resp)
	}

	// Duplicate watch should NOT use a subscription slot
	resp = connSendCmd(t, conn, reader, "watch", "k1", "")
	if resp != "ok" {
		t.Fatalf("duplicate watch: expected ok, got %q", resp)
	}

	// Second distinct subscription should still work
	resp = connSendCmd(t, conn, reader, "watch", "k2", "")
	if resp != "ok" {
		t.Fatalf("watch k2: expected ok, got %q", resp)
	}
}

func TestIntegration_SpuriousUnlistenDoesNotDeflateCounter(t *testing.T) {
	cfg := testConfig()
	cfg.MaxSubscriptions = 1
	_, addr, _ := startServer(t, cfg)

	conn := dialTest(t, addr)
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Use the one subscription slot
	resp := connSendCmd(t, conn, reader, "listen", "ch1", "")
	if resp != "ok" {
		t.Fatalf("listen: expected ok, got %q", resp)
	}

	// Spurious unlisten on a nonexistent pattern should NOT free a slot
	connSendCmd(t, conn, reader, "unlisten", "no-such-ch", "")

	// This should fail — the slot should still be occupied by ch1
	resp = connSendCmd(t, conn, reader, "listen", "ch2", "")
	if resp == "ok" {
		// If we get here, the counter was incorrectly deflated by the spurious unlisten
		t.Fatal("spurious unlisten should NOT free a subscription slot")
	}
}

func TestIntegration_SpuriousUnwatchDoesNotDeflateCounter(t *testing.T) {
	cfg := testConfig()
	cfg.MaxSubscriptions = 1
	_, addr, _ := startServer(t, cfg)

	conn := dialTest(t, addr)
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Use the one subscription slot
	resp := connSendCmd(t, conn, reader, "watch", "k1", "")
	if resp != "ok" {
		t.Fatalf("watch: expected ok, got %q", resp)
	}

	// Spurious unwatch should NOT free a slot
	connSendCmd(t, conn, reader, "unwatch", "no-such-key", "")

	// This should fail — the slot should still be occupied
	resp = connSendCmd(t, conn, reader, "watch", "k2", "")
	if resp == "ok" {
		t.Fatal("spurious unwatch should NOT free a subscription slot")
	}
}

// ---------------------------------------------------------------------------
// Regression: observe must respect MaxSubscriptions, unobserve must free slot
// ---------------------------------------------------------------------------

func TestIntegration_MaxSubscriptions_Observe(t *testing.T) {
	cfg := testConfig()
	cfg.MaxSubscriptions = 2
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn := dialTest(t, addr)
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// First two observes should succeed.
	resp := connSendCmd(t, conn, reader, "observe", "election1", "")
	if resp != "ok" {
		t.Fatalf("observe 1: expected ok, got %q", resp)
	}
	resp = connSendCmd(t, conn, reader, "observe", "election2", "")
	if resp != "ok" {
		t.Fatalf("observe 2: expected ok, got %q", resp)
	}

	// Third should be rejected.
	resp = connSendCmd(t, conn, reader, "observe", "election3", "")
	if !strings.HasPrefix(resp, "error") {
		t.Fatalf("observe 3: expected error, got %q", resp)
	}
}

func TestIntegration_MaxSubscriptions_UnobserveFreesSlot(t *testing.T) {
	cfg := testConfig()
	cfg.MaxSubscriptions = 1
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn := dialTest(t, addr)
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Use the one subscription slot
	resp := connSendCmd(t, conn, reader, "observe", "election1", "")
	if resp != "ok" {
		t.Fatalf("observe: expected ok, got %q", resp)
	}

	// Second observe should be rejected
	resp = connSendCmd(t, conn, reader, "observe", "election2", "")
	if !strings.HasPrefix(resp, "error") {
		t.Fatalf("observe 2: expected error, got %q", resp)
	}

	// Unobserve should free the slot
	connSendCmd(t, conn, reader, "unobserve", "election1", "")

	// Now another observe should succeed
	resp = connSendCmd(t, conn, reader, "observe", "election3", "")
	if resp != "ok" {
		t.Fatalf("observe after unobserve: expected ok, got %q", resp)
	}
}

func TestIntegration_MaxSubscriptions_ObserveMixed(t *testing.T) {
	cfg := testConfig()
	cfg.MaxSubscriptions = 2
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn := dialTest(t, addr)
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Mix watch and observe — both should count toward the same limit.
	resp := connSendCmd(t, conn, reader, "watch", "key1", "")
	if resp != "ok" {
		t.Fatalf("watch: expected ok, got %q", resp)
	}
	resp = connSendCmd(t, conn, reader, "observe", "election1", "")
	if resp != "ok" {
		t.Fatalf("observe: expected ok, got %q", resp)
	}

	// Third should be rejected (either type).
	resp = connSendCmd(t, conn, reader, "observe", "election2", "")
	if !strings.HasPrefix(resp, "error") {
		t.Fatalf("observe 3: expected error, got %q", resp)
	}
	resp = connSendCmd(t, conn, reader, "watch", "key2", "")
	if !strings.HasPrefix(resp, "error") {
		t.Fatalf("watch 3: expected error, got %q", resp)
	}
}

func TestIntegration_MaxSubscriptions_DuplicateObserveNoop(t *testing.T) {
	cfg := testConfig()
	cfg.MaxSubscriptions = 1
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn := dialTest(t, addr)
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// First observe should succeed.
	resp := connSendCmd(t, conn, reader, "observe", "election1", "")
	if resp != "ok" {
		t.Fatalf("observe 1: expected ok, got %q", resp)
	}

	// Duplicate observe for same key should succeed (idempotent, no new slot).
	resp = connSendCmd(t, conn, reader, "observe", "election1", "")
	if resp != "ok" {
		t.Fatalf("duplicate observe: expected ok, got %q", resp)
	}
}

func TestIntegration_MaxSubscriptions_SpuriousUnobserve(t *testing.T) {
	cfg := testConfig()
	cfg.MaxSubscriptions = 1
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn := dialTest(t, addr)
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Use the one subscription slot
	resp := connSendCmd(t, conn, reader, "observe", "election1", "")
	if resp != "ok" {
		t.Fatalf("observe: expected ok, got %q", resp)
	}

	// Spurious unobserve for a different key should NOT free a slot
	connSendCmd(t, conn, reader, "unobserve", "no-such-election", "")

	// This should fail — the slot should still be occupied
	resp = connSendCmd(t, conn, reader, "observe", "election2", "")
	if !strings.HasPrefix(resp, "error") {
		t.Fatal("spurious unobserve should NOT free a subscription slot")
	}
}

// ===========================================================================
// Coverage gap-filling tests
// ===========================================================================

func TestIntegration_UnknownCommand(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	resp := connSendCmd(t, conn, reader, "boguscmd", "somekey", "somearg")
	if resp != "error" {
		t.Fatalf("expected 'error' for unknown command, got %q", resp)
	}
}

func TestIntegration_ResignBadToken(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn := dialTest(t, addr)
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Elect a leader
	resp := connSendCmd(t, conn, reader, "elect", "resign_bad", "5 10")
	parts := strings.Fields(resp)
	if len(parts) < 4 || parts[0] != "ok" {
		t.Fatalf("elect: expected ok, got %q", resp)
	}

	// Resign with wrong token
	resp = connSendCmd(t, conn, reader, "resign", "resign_bad", "wrong-token")
	if resp != "error" {
		t.Fatalf("expected 'error' for resign with wrong token, got %q", resp)
	}
}

func TestIntegration_ElectTimeout(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	// conn1 holds the election key with a long timeout
	conn1 := dialTest(t, addr)
	defer conn1.Close()
	reader1 := bufio.NewReader(conn1)

	resp := connSendCmd(t, conn1, reader1, "elect", "elect_to", "10 30")
	parts := strings.Fields(resp)
	if len(parts) < 4 || parts[0] != "ok" {
		t.Fatalf("elect conn1: expected ok, got %q", resp)
	}

	// conn2 tries to elect with timeout=0 -- should timeout immediately
	conn2 := dialTest(t, addr)
	defer conn2.Close()
	reader2 := bufio.NewReader(conn2)

	resp = connSendCmd(t, conn2, reader2, "elect", "elect_to", "0 10")
	if resp != "timeout" {
		t.Fatalf("expected 'timeout' for elect with timeout=0, got %q", resp)
	}
}

func TestIntegration_RWRelease_WrongToken(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn := dialTest(t, addr)
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Acquire a read lock
	resp := connSendCmd(t, conn, reader, "rl", "rwrel_bad", "10")
	parts := strings.Fields(resp)
	if len(parts) != 4 || parts[0] != "ok" {
		t.Fatalf("rl: expected ok, got %q", resp)
	}

	// Try to release with wrong token via rr
	resp = connSendCmd(t, conn, reader, "rr", "rwrel_bad", "wrong-token")
	if resp != "error" {
		t.Fatalf("expected 'error' for rr with wrong token, got %q", resp)
	}
}

func TestIntegration_RWRenew_BadToken(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn := dialTest(t, addr)
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Try rn with a bogus token (no lock held)
	resp := connSendCmd(t, conn, reader, "rn", "rwren_bad", "bogus-token-123")
	if resp != "error" {
		t.Fatalf("expected 'error' for rn with bogus token, got %q", resp)
	}
}

func TestIntegration_RWWait_NotEnqueued(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn := dialTest(t, addr)
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Send rw without prior re
	resp := connSendCmd(t, conn, reader, "rw", "rwwait_noenq", "5")
	if resp != "error_not_enqueued" {
		t.Fatalf("expected 'error_not_enqueued', got %q", resp)
	}
}

func TestIntegration_RWEnqueue_AlreadyEnqueued(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn := dialTest(t, addr)
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// First re should succeed (immediate acquire since no contention)
	resp := connSendCmd(t, conn, reader, "re", "rw_dup_enq", "10")
	parts := strings.Fields(resp)
	if parts[0] != "acquired" {
		t.Fatalf("first re: expected acquired, got %q", resp)
	}

	// Second re on same key from same connection should get error_already_enqueued
	resp = connSendCmd(t, conn, reader, "re", "rw_dup_enq", "10")
	if resp != "error_already_enqueued" {
		t.Fatalf("expected 'error_already_enqueued', got %q", resp)
	}
}

func TestIntegration_BRPop_Timeout(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn := dialTest(t, addr)
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// brpop on empty list with timeout=0 should return nil immediately
	resp := connSendCmd(t, conn, reader, "brpop", "brpop_empty", "0")
	if resp != "nil" {
		t.Fatalf("expected 'nil' for brpop timeout, got %q", resp)
	}
}

func TestIntegration_Decr_MaxKeys(t *testing.T) {
	cfg := testConfig()
	cfg.MaxKeys = 1
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn := dialTest(t, addr)
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Create one counter via incr
	resp := connSendCmd(t, conn, reader, "incr", "ctr_existing", "1")
	if resp != "ok 1" {
		t.Fatalf("incr: expected 'ok 1', got %q", resp)
	}

	// Try decr on a different key -- should hit max keys
	resp = connSendCmd(t, conn, reader, "decr", "ctr_new", "1")
	if resp != "error_max_keys" {
		t.Fatalf("expected 'error_max_keys', got %q", resp)
	}
}

func TestIntegration_Cset_MaxKeys(t *testing.T) {
	cfg := testConfig()
	cfg.MaxKeys = 1
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn := dialTest(t, addr)
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Create one counter
	resp := connSendCmd(t, conn, reader, "incr", "cset_existing", "1")
	if resp != "ok 1" {
		t.Fatalf("incr: expected 'ok 1', got %q", resp)
	}

	// Try cset on a different key -- should hit max keys
	resp = connSendCmd(t, conn, reader, "cset", "cset_new", "42")
	if resp != "error_max_keys" {
		t.Fatalf("expected 'error_max_keys', got %q", resp)
	}
}

func TestIntegration_KSet_MaxKeys(t *testing.T) {
	cfg := testConfig()
	cfg.MaxKeys = 1
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn := dialTest(t, addr)
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Create one KV entry
	resp := connSendCmd(t, conn, reader, "kset", "kv_existing", "hello")
	if resp != "ok" {
		t.Fatalf("kset: expected 'ok', got %q", resp)
	}

	// Try kset on a different key -- should hit max keys
	resp = connSendCmd(t, conn, reader, "kset", "kv_new", "world")
	if resp != "error_max_keys" {
		t.Fatalf("expected 'error_max_keys', got %q", resp)
	}
}

func TestIntegration_LPush_ListFull(t *testing.T) {
	cfg := testConfig()
	cfg.MaxListLength = 1
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn := dialTest(t, addr)
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Push one item
	resp := connSendCmd(t, conn, reader, "lpush", "lpush_full", "a")
	if resp != "ok 1" {
		t.Fatalf("lpush: expected 'ok 1', got %q", resp)
	}

	// Try lpush to same list -- should be full
	resp = connSendCmd(t, conn, reader, "lpush", "lpush_full", "b")
	if resp != "error_list_full" {
		t.Fatalf("expected 'error_list_full', got %q", resp)
	}
}

func TestIntegration_LPush_MaxKeys(t *testing.T) {
	cfg := testConfig()
	cfg.MaxKeys = 1
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn := dialTest(t, addr)
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Create one list
	resp := connSendCmd(t, conn, reader, "lpush", "list_existing", "a")
	if resp != "ok 1" {
		t.Fatalf("lpush: expected 'ok 1', got %q", resp)
	}

	// Try lpush on a different key -- should hit max keys
	resp = connSendCmd(t, conn, reader, "lpush", "list_new", "b")
	if resp != "error_max_keys" {
		t.Fatalf("expected 'error_max_keys', got %q", resp)
	}
}

func TestIntegration_RPush_MaxKeys(t *testing.T) {
	cfg := testConfig()
	cfg.MaxKeys = 1
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn := dialTest(t, addr)
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Create one list
	resp := connSendCmd(t, conn, reader, "rpush", "rlist_existing", "a")
	if resp != "ok 1" {
		t.Fatalf("rpush: expected 'ok 1', got %q", resp)
	}

	// Try rpush on a different key -- should hit max keys
	resp = connSendCmd(t, conn, reader, "rpush", "rlist_new", "b")
	if resp != "error_max_keys" {
		t.Fatalf("expected 'error_max_keys', got %q", resp)
	}
}

func TestIntegration_Stats_SignalChannels(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn := dialTest(t, addr)
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Set up a signal listener
	resp := connSendCmd(t, conn, reader, "listen", "stats.sig.*", "")
	if resp != "ok" {
		t.Fatalf("listen: expected ok, got %q", resp)
	}

	// Call stats
	resp = connSendCmd(t, conn, reader, "stats", "_", "")
	if !strings.HasPrefix(resp, "ok ") {
		t.Fatalf("stats: expected 'ok <json>', got %q", resp)
	}
	jsonStr := strings.TrimPrefix(resp, "ok ")

	var stats lock.Stats
	if err := json.Unmarshal([]byte(jsonStr), &stats); err != nil {
		t.Fatalf("stats JSON: %v\njson: %s", err, jsonStr)
	}

	// Verify signal_channels data appears
	if len(stats.SignalChannels) == 0 {
		t.Fatal("expected signal_channels to be populated, got empty")
	}
	found := false
	for _, sc := range stats.SignalChannels {
		if sc.Pattern == "stats.sig.*" && sc.Listeners > 0 {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected signal_channels to contain pattern 'stats.sig.*', got %+v", stats.SignalChannels)
	}
}

func TestIntegration_Stats_WatchChannels(t *testing.T) {
	cfg := testConfig()
	cleanup, addr, _ := startServer(t, cfg)
	defer cleanup()

	conn := dialTest(t, addr)
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Set up a watcher
	resp := connSendCmd(t, conn, reader, "watch", "stats.watch.*", "")
	if resp != "ok" {
		t.Fatalf("watch: expected ok, got %q", resp)
	}

	// Call stats
	resp = connSendCmd(t, conn, reader, "stats", "_", "")
	if !strings.HasPrefix(resp, "ok ") {
		t.Fatalf("stats: expected 'ok <json>', got %q", resp)
	}
	jsonStr := strings.TrimPrefix(resp, "ok ")

	var stats lock.Stats
	if err := json.Unmarshal([]byte(jsonStr), &stats); err != nil {
		t.Fatalf("stats JSON: %v\njson: %s", err, jsonStr)
	}

	// Verify watch_channels data appears
	if len(stats.WatchChannels) == 0 {
		t.Fatal("expected watch_channels to be populated, got empty")
	}
	found := false
	for _, wc := range stats.WatchChannels {
		if wc.Pattern == "stats.watch.*" && wc.Watchers > 0 {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected watch_channels to contain pattern 'stats.watch.*', got %+v", stats.WatchChannels)
	}
}

func TestIntegration_Drain_ZeroTimeout(t *testing.T) {
	cfg := testConfig()
	cfg.ShutdownTimeout = 0
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

	resp := connSendCmd(t, conn, reader, "l", "drain_key", "10")
	parts := strings.Fields(resp)
	if len(parts) != 4 || parts[0] != "ok" {
		t.Fatalf("lock: expected ok, got %q", resp)
	}

	// Close the client connection so the server can drain cleanly
	conn.Close()

	// Give server time to detect disconnect
	time.Sleep(100 * time.Millisecond)

	// Cancel context to trigger shutdown with ShutdownTimeout=0
	// (drain path: wg.Wait without force-close)
	cancel()

	// Server should drain and exit cleanly
	select {
	case <-done:
		// success -- clean drain completed
	case <-time.After(3 * time.Second):
		t.Fatal("server did not shut down within expected time with ShutdownTimeout=0")
	}
}
