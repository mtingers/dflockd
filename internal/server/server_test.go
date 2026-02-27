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
