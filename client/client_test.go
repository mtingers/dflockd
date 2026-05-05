package client_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mtingers/dflockd/client"
	"github.com/mtingers/dflockd/internal/config"
	"github.com/mtingers/dflockd/internal/lock"
	"github.com/mtingers/dflockd/internal/server"
)

// startServer mirrors the server-package helper but lives here to
// avoid a cross-package test import. Returns (addr, stopFn).
func startServer(t *testing.T, mods ...func(*config.Config)) (string, func()) {
	t.Helper()
	cfg := &config.Config{
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
	for _, fn := range mods {
		fn(cfg)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Port = listener.Addr().(*net.TCPAddr).Port

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	lm := lock.NewLockManager(cfg, log)
	srv := server.New(lm, cfg, log)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = srv.RunOnListener(ctx, listener)
		close(done)
	}()
	addr := listener.Addr().String()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		c, err := net.Dial("tcp", addr)
		if err == nil {
			c.Close()
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	stopOnce := sync.Once{}
	return addr, func() {
		stopOnce.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("server didn't stop")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Conn-level
// ---------------------------------------------------------------------------

func TestDial_AcquireRelease(t *testing.T) {
	addr, stop := startServer(t)
	defer stop()
	conn, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	tok, lease, err := client.Acquire(conn, "k", time.Second)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if tok == "" || lease == 0 {
		t.Errorf("got %q %d", tok, lease)
	}
	if err := client.Release(conn, "k", tok); err != nil {
		t.Errorf("Release: %v", err)
	}
}

func TestDial_AcquireTimeout(t *testing.T) {
	addr, stop := startServer(t)
	defer stop()
	conn1, _ := client.Dial(addr)
	defer conn1.Close()
	tok, _, err := client.Acquire(conn1, "k", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Release(conn1, "k", tok)

	conn2, _ := client.Dial(addr)
	defer conn2.Close()
	_, _, err = client.Acquire(conn2, "k", 100*time.Millisecond)
	if !errors.Is(err, client.ErrTimeout) {
		t.Errorf("got %v, want ErrTimeout", err)
	}
}

func TestDial_Renew(t *testing.T) {
	addr, stop := startServer(t)
	defer stop()
	conn, _ := client.Dial(addr)
	defer conn.Close()
	tok, _, _ := client.Acquire(conn, "k", time.Second)
	remaining, err := client.Renew(conn, "k", tok, client.WithLeaseTTL(60))
	if err != nil {
		t.Fatal(err)
	}
	if remaining != 60 {
		t.Errorf("got %d, want 60", remaining)
	}
}

func TestDial_EnqueueWait(t *testing.T) {
	addr, stop := startServer(t)
	defer stop()

	holder, _ := client.Dial(addr)
	defer holder.Close()
	hTok, _, _ := client.Acquire(holder, "k", time.Second, client.WithLeaseTTL(30))

	queuer, _ := client.Dial(addr)
	defer queuer.Close()
	status, _, _, err := client.Enqueue(queuer, "k", client.WithLeaseTTL(30))
	if err != nil {
		t.Fatal(err)
	}
	if status != "queued" {
		t.Fatalf("got %q", status)
	}

	done := make(chan string, 1)
	go func() {
		tok, _, err := client.Wait(queuer, "k", 5*time.Second)
		if err != nil {
			done <- "ERR"
			return
		}
		done <- tok
	}()
	time.Sleep(50 * time.Millisecond)
	client.Release(holder, "k", hTok)

	select {
	case got := <-done:
		if got == "" || got == "ERR" {
			t.Fatalf("wait got %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("wait never returned")
	}
}

func TestDial_Semaphore(t *testing.T) {
	addr, stop := startServer(t)
	defer stop()

	for i := 0; i < 3; i++ {
		c, _ := client.Dial(addr)
		defer c.Close()
		tok, _, err := client.SemAcquire(c, "sem", time.Second, 3, client.WithLeaseTTL(30))
		if err != nil {
			t.Fatalf("hold %d: %v", i, err)
		}
		if tok == "" {
			t.Fatalf("hold %d: empty token", i)
		}
	}
	c, _ := client.Dial(addr)
	defer c.Close()
	_, _, err := client.SemAcquire(c, "sem", 100*time.Millisecond, 3)
	if !errors.Is(err, client.ErrTimeout) {
		t.Errorf("got %v, want ErrTimeout", err)
	}
}

// ---------------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------------

func TestAuthenticate(t *testing.T) {
	addr, stop := startServer(t, func(c *config.Config) { c.AuthToken = "secret" })
	defer stop()
	conn, _ := client.Dial(addr)
	defer conn.Close()

	if err := client.Authenticate(conn, "secret"); err != nil {
		t.Fatalf("auth: %v", err)
	}
	if _, _, err := client.Acquire(conn, "k", time.Second); err != nil {
		t.Errorf("post-auth acquire: %v", err)
	}
}

func TestAuthenticate_WrongToken(t *testing.T) {
	addr, stop := startServer(t, func(c *config.Config) { c.AuthToken = "secret" })
	defer stop()
	conn, _ := client.Dial(addr)
	defer conn.Close()
	err := client.Authenticate(conn, "wrong")
	if !errors.Is(err, client.ErrAuth) {
		t.Errorf("got %v, want ErrAuth", err)
	}
}

// ---------------------------------------------------------------------------
// High-level Lock
// ---------------------------------------------------------------------------

func TestLock_AcquireRelease(t *testing.T) {
	addr, stop := startServer(t)
	defer stop()

	l := &client.Lock{
		Key:            "lk",
		AcquireTimeout: time.Second,
		Servers:        []string{addr},
	}
	got, err := l.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if !got {
		t.Fatal("Acquire returned false")
	}
	if l.Token() == "" {
		t.Fatal("empty token")
	}
	if err := l.Release(context.Background()); err != nil {
		t.Errorf("Release: %v", err)
	}
}

func TestLock_TwoCallers(t *testing.T) {
	addr, stop := startServer(t)
	defer stop()

	l1 := &client.Lock{Key: "lk", Servers: []string{addr}, LeaseTTL: 30}
	l2 := &client.Lock{Key: "lk", Servers: []string{addr}, AcquireTimeout: 100 * time.Millisecond}

	got, err := l1.Acquire(context.Background())
	if err != nil || !got {
		t.Fatalf("l1: %v %v", got, err)
	}
	defer l1.Release(context.Background())

	got, err = l2.Acquire(context.Background())
	if err != nil {
		t.Fatalf("l2: %v", err)
	}
	if got {
		t.Fatal("l2 should have timed out")
	}
}

func TestLock_BackgroundRenew(t *testing.T) {
	addr, stop := startServer(t)
	defer stop()

	l := &client.Lock{
		Key:            "lk",
		AcquireTimeout: time.Second,
		LeaseTTL:       2,
		RenewRatio:     0.25,
		Servers:        []string{addr},
	}
	got, err := l.Acquire(context.Background())
	if err != nil || !got {
		t.Fatalf("Acquire: %v %v", got, err)
	}
	defer l.Release(context.Background())

	// Sleep through several would-be expiries; renewal should keep
	// the lock alive.
	time.Sleep(3 * time.Second)
	if l.Token() == "" {
		t.Fatal("token gone — renewal failed")
	}
}

func TestLock_TwoPhase(t *testing.T) {
	addr, stop := startServer(t)
	defer stop()

	holder := &client.Lock{Key: "lk", Servers: []string{addr}, LeaseTTL: 30}
	got, _ := holder.Acquire(context.Background())
	if !got {
		t.Fatal("holder didn't acquire")
	}
	defer holder.Release(context.Background())

	queuer := &client.Lock{Key: "lk", Servers: []string{addr}, LeaseTTL: 30}
	status, err := queuer.Enqueue(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status != "queued" {
		t.Fatalf("got %q", status)
	}
	done := make(chan bool, 1)
	go func() {
		ok, _ := queuer.Wait(context.Background(), 5*time.Second)
		done <- ok
	}()
	time.Sleep(50 * time.Millisecond)
	holder.Release(context.Background())

	select {
	case ok := <-done:
		if !ok {
			t.Fatal("Wait returned false")
		}
		queuer.Release(context.Background())
	case <-time.After(2 * time.Second):
		t.Fatal("Wait never returned")
	}
}

// ---------------------------------------------------------------------------
// High-level Semaphore
// ---------------------------------------------------------------------------

func TestSemaphore_AcquireRelease(t *testing.T) {
	addr, stop := startServer(t)
	defer stop()
	s := &client.Semaphore{Key: "sem", Limit: 3, Servers: []string{addr}}
	ok, err := s.Acquire(context.Background())
	if err != nil || !ok {
		t.Fatalf("Acquire: %v %v", ok, err)
	}
	if err := s.Release(context.Background()); err != nil {
		t.Errorf("Release: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

func TestValidation_EmptyKey(t *testing.T) {
	addr, stop := startServer(t)
	defer stop()
	conn, _ := client.Dial(addr)
	defer conn.Close()
	_, _, err := client.Acquire(conn, "", time.Second)
	if err == nil || !strings.Contains(err.Error(), "empty key") {
		t.Errorf("got %v, want empty-key error", err)
	}
}

func TestValidation_BadLeaseTTL(t *testing.T) {
	addr, stop := startServer(t)
	defer stop()
	conn, _ := client.Dial(addr)
	defer conn.Close()
	_, _, err := client.Acquire(conn, "k", time.Second, client.WithLeaseTTL(-1))
	if err == nil {
		t.Errorf("expected error for negative lease TTL")
	}
}

func TestValidation_BadSemLimit(t *testing.T) {
	addr, stop := startServer(t)
	defer stop()
	conn, _ := client.Dial(addr)
	defer conn.Close()
	_, _, err := client.SemAcquire(conn, "k", time.Second, 0)
	if err == nil {
		t.Error("expected error for limit=0")
	}
}

// ---------------------------------------------------------------------------
// Sharding
// ---------------------------------------------------------------------------

func TestCRC32Shard_Stable(t *testing.T) {
	if client.CRC32Shard("foo", 4) != client.CRC32Shard("foo", 4) {
		t.Error("CRC32Shard not stable")
	}
	if client.CRC32Shard("foo", 0) != 0 {
		t.Error("zero servers should return 0")
	}
}

// ---------------------------------------------------------------------------
// Disconnect cleanup
// ---------------------------------------------------------------------------

func TestDisconnect_AutoRelease(t *testing.T) {
	addr, stop := startServer(t)
	defer stop()

	c1, _ := client.Dial(addr)
	tok, _, _ := client.Acquire(c1, "k", time.Second)
	_ = tok
	c1.Close()

	time.Sleep(100 * time.Millisecond)

	c2, _ := client.Dial(addr)
	defer c2.Close()
	_, _, err := client.Acquire(c2, "k", time.Second)
	if err != nil {
		t.Fatalf("post-disconnect acquire: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Context cancellation on Lock.Acquire
// ---------------------------------------------------------------------------

func TestLock_ContextCancel(t *testing.T) {
	addr, stop := startServer(t)
	defer stop()

	holder := &client.Lock{Key: "lk", Servers: []string{addr}, LeaseTTL: 30}
	holder.Acquire(context.Background())
	defer holder.Release(context.Background())

	l := &client.Lock{Key: "lk", AcquireTimeout: 30 * time.Second, Servers: []string{addr}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := l.Acquire(ctx)
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error on cancel")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Acquire never returned")
	}
}
