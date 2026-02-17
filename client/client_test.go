package client_test

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/mtingers/dflockd/client"
	"github.com/mtingers/dflockd/internal/config"
	"github.com/mtingers/dflockd/internal/lock"
	"github.com/mtingers/dflockd/internal/server"
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

func startServer(t *testing.T, cfg *config.Config) (cancel context.CancelFunc, addr string) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	lm := lock.NewLockManager(cfg, log)
	srv := server.New(lm, cfg, log)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr = ln.Addr().String()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.RunOnListener(ctx, ln)
	}()

	t.Cleanup(func() {
		cancel()
		<-done
	})

	return cancel, addr
}

// ---------------------------------------------------------------------------
// Low-level tests
// ---------------------------------------------------------------------------

func TestAcquireRelease(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	token, lease, err := client.Acquire(c, "mykey", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if token == "" {
		t.Fatal("expected non-empty token")
	}
	if lease <= 0 {
		t.Fatalf("expected positive lease, got %d", lease)
	}

	if err := client.Release(c, "mykey", token); err != nil {
		t.Fatal(err)
	}
}

func TestAcquireTimeout(t *testing.T) {
	_, addr := startServer(t, testConfig())

	// First client holds the lock.
	c1, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()

	_, _, err = client.Acquire(c1, "mykey", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// Second client tries with 0 timeout — should get ErrTimeout.
	c2, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	_, _, err = client.Acquire(c2, "mykey", 0)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !isTimeout(err) {
		t.Fatalf("expected ErrTimeout, got %v", err)
	}
}

func TestRenew(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	token, _, err := client.Acquire(c, "mykey", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	remaining, err := client.Renew(c, "mykey", token)
	if err != nil {
		t.Fatal(err)
	}
	if remaining <= 0 {
		t.Fatalf("expected positive remaining, got %d", remaining)
	}
}

func TestEnqueueWait(t *testing.T) {
	_, addr := startServer(t, testConfig())

	// c1 holds the lock.
	c1, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()

	token1, _, err := client.Acquire(c1, "mykey", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// c2 enqueues — should get "queued".
	c2, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	status, _, _, err := client.Enqueue(c2, "mykey")
	if err != nil {
		t.Fatal(err)
	}
	if status != "queued" {
		t.Fatalf("expected 'queued', got %q", status)
	}

	// Release in background after a short delay.
	go func() {
		time.Sleep(50 * time.Millisecond)
		client.Release(c1, "mykey", token1)
	}()

	// c2 waits — should succeed after c1 releases.
	token2, lease2, err := client.Wait(c2, "mykey", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if token2 == "" {
		t.Fatal("expected non-empty token from wait")
	}
	if lease2 <= 0 {
		t.Fatalf("expected positive lease, got %d", lease2)
	}
}

func TestEnqueueImmediate(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	status, token, lease, err := client.Enqueue(c, "mykey")
	if err != nil {
		t.Fatal(err)
	}
	if status != "acquired" {
		t.Fatalf("expected 'acquired', got %q", status)
	}
	if token == "" {
		t.Fatal("expected non-empty token")
	}
	if lease <= 0 {
		t.Fatalf("expected positive lease, got %d", lease)
	}
}

func TestCustomLeaseTTL(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	_, lease, err := client.Acquire(c, "mykey", 10*time.Second, client.WithLeaseTTL(60))
	if err != nil {
		t.Fatal(err)
	}
	if lease != 60 {
		t.Fatalf("expected lease 60, got %d", lease)
	}
}

// ---------------------------------------------------------------------------
// High-level Lock tests
// ---------------------------------------------------------------------------

func TestLockAcquireRelease(t *testing.T) {
	_, addr := startServer(t, testConfig())

	l := &client.Lock{
		Key:            "hl-key",
		AcquireTimeout: 10 * time.Second,
		Servers:        []string{addr},
	}

	ok, err := l.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected acquire to succeed")
	}
	if l.Token() == "" {
		t.Fatal("expected non-empty token")
	}

	if err := l.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestLockEnqueueWait(t *testing.T) {
	_, addr := startServer(t, testConfig())

	// l1 holds the lock.
	l1 := &client.Lock{
		Key:            "hl-key",
		AcquireTimeout: 10 * time.Second,
		Servers:        []string{addr},
	}
	ok, err := l1.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("l1 acquire failed")
	}

	// l2 enqueues.
	l2 := &client.Lock{
		Key:     "hl-key",
		Servers: []string{addr},
	}
	status, err := l2.Enqueue(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status != "queued" {
		t.Fatalf("expected 'queued', got %q", status)
	}

	// Release l1 in background.
	go func() {
		time.Sleep(50 * time.Millisecond)
		l1.Release(context.Background())
	}()

	// l2 waits.
	ok, err = l2.Wait(context.Background(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("l2 wait timed out")
	}
	if l2.Token() == "" {
		t.Fatal("expected non-empty token after wait")
	}

	if err := l2.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestAutoRenewal(t *testing.T) {
	cfg := testConfig()
	cfg.DefaultLeaseTTL = 4 * time.Second
	_, addr := startServer(t, cfg)

	l := &client.Lock{
		Key:            "renew-key",
		AcquireTimeout: 10 * time.Second,
		Servers:        []string{addr},
		RenewRatio:     0.25, // renew every 1s on a 4s lease
	}

	ok, err := l.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("acquire failed")
	}

	// Wait longer than the original lease to ensure renewal is working.
	time.Sleep(5 * time.Second)

	// Lock should still be held — releasing should succeed.
	if err := l.Release(context.Background()); err != nil {
		t.Fatalf("release after auto-renew failed: %v", err)
	}
}

func TestContextCancellation(t *testing.T) {
	_, addr := startServer(t, testConfig())

	// l1 holds the lock.
	l1 := &client.Lock{
		Key:            "ctx-key",
		AcquireTimeout: 10 * time.Second,
		Servers:        []string{addr},
	}
	ok, err := l1.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("l1 acquire failed")
	}
	defer l1.Release(context.Background())

	// l2 tries to acquire with a context that we cancel quickly.
	ctx, cancel := context.WithCancel(context.Background())
	l2 := &client.Lock{
		Key:            "ctx-key",
		AcquireTimeout: 30 * time.Second, // long timeout
		Servers:        []string{addr},
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err := l2.Acquire(ctx)
		if err == nil {
			// Should have gotten an error from cancellation.
			t.Error("expected error from cancelled context")
		}
	}()

	// Give the acquire time to connect and start waiting on the server.
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Success — Acquire returned after cancellation.
	case <-time.After(3 * time.Second):
		t.Fatal("Acquire did not return after context cancellation")
	}
}

// ---------------------------------------------------------------------------
// Semaphore low-level tests
// ---------------------------------------------------------------------------

func TestSemAcquireRelease(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	token, lease, err := client.SemAcquire(c, "sem1", 10*time.Second, 3)
	if err != nil {
		t.Fatal(err)
	}
	if token == "" {
		t.Fatal("expected non-empty token")
	}
	if lease <= 0 {
		t.Fatalf("expected positive lease, got %d", lease)
	}

	if err := client.SemRelease(c, "sem1", token); err != nil {
		t.Fatal(err)
	}
}

func TestSemAcquireTimeout(t *testing.T) {
	_, addr := startServer(t, testConfig())

	// Fill capacity (limit=1)
	c1, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	_, _, err = client.SemAcquire(c1, "sem1", 10*time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}

	// Second client tries with 0 timeout
	c2, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	_, _, err = client.SemAcquire(c2, "sem1", 0, 1)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !isTimeout(err) {
		t.Fatalf("expected ErrTimeout, got %v", err)
	}
}

func TestSemRenew(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	token, _, err := client.SemAcquire(c, "sem1", 10*time.Second, 3)
	if err != nil {
		t.Fatal(err)
	}

	remaining, err := client.SemRenew(c, "sem1", token)
	if err != nil {
		t.Fatal(err)
	}
	if remaining <= 0 {
		t.Fatalf("expected positive remaining, got %d", remaining)
	}
}

func TestSemEnqueueWait(t *testing.T) {
	_, addr := startServer(t, testConfig())

	// c1 fills capacity
	c1, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	token1, _, err := client.SemAcquire(c1, "sem1", 10*time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}

	// c2 enqueues
	c2, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	status, _, _, err := client.SemEnqueue(c2, "sem1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if status != "queued" {
		t.Fatalf("expected 'queued', got %q", status)
	}

	// Release in background
	go func() {
		time.Sleep(50 * time.Millisecond)
		client.SemRelease(c1, "sem1", token1)
	}()

	token2, lease2, err := client.SemWait(c2, "sem1", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if token2 == "" {
		t.Fatal("expected non-empty token from wait")
	}
	if lease2 <= 0 {
		t.Fatalf("expected positive lease, got %d", lease2)
	}
}

func TestSemLimitMismatch(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c1, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	_, _, err = client.SemAcquire(c1, "sem1", 10*time.Second, 3)
	if err != nil {
		t.Fatal(err)
	}

	c2, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	_, _, err = client.SemAcquire(c2, "sem1", 10*time.Second, 5)
	if err == nil {
		t.Fatal("expected error")
	}
	if err != client.ErrLimitMismatch {
		t.Fatalf("expected ErrLimitMismatch, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Semaphore high-level tests
// ---------------------------------------------------------------------------

func TestSemaphoreAcquireRelease(t *testing.T) {
	_, addr := startServer(t, testConfig())

	s := &client.Semaphore{
		Key:            "hl-sem",
		Limit:          3,
		AcquireTimeout: 10 * time.Second,
		Servers:        []string{addr},
	}

	ok, err := s.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected acquire to succeed")
	}
	if s.Token() == "" {
		t.Fatal("expected non-empty token")
	}

	if err := s.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSemaphoreTwoPhase(t *testing.T) {
	_, addr := startServer(t, testConfig())

	// s1 fills capacity
	s1 := &client.Semaphore{
		Key:            "hl-sem",
		Limit:          1,
		AcquireTimeout: 10 * time.Second,
		Servers:        []string{addr},
	}
	ok, err := s1.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("s1 acquire failed")
	}

	// s2 enqueues
	s2 := &client.Semaphore{
		Key:     "hl-sem",
		Limit:   1,
		Servers: []string{addr},
	}
	status, err := s2.Enqueue(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status != "queued" {
		t.Fatalf("expected 'queued', got %q", status)
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		s1.Release(context.Background())
	}()

	ok, err = s2.Wait(context.Background(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("s2 wait timed out")
	}
	if s2.Token() == "" {
		t.Fatal("expected non-empty token after wait")
	}

	if err := s2.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSemaphoreAutoRenewal(t *testing.T) {
	cfg := testConfig()
	cfg.DefaultLeaseTTL = 4 * time.Second
	_, addr := startServer(t, cfg)

	s := &client.Semaphore{
		Key:            "renew-sem",
		Limit:          3,
		AcquireTimeout: 10 * time.Second,
		Servers:        []string{addr},
		RenewRatio:     0.25,
	}

	ok, err := s.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("acquire failed")
	}

	// Wait longer than original lease
	time.Sleep(5 * time.Second)

	if err := s.Release(context.Background()); err != nil {
		t.Fatalf("release after auto-renew failed: %v", err)
	}
}

func TestSemaphoreConcurrent(t *testing.T) {
	_, addr := startServer(t, testConfig())

	// Acquire 3 slots concurrently on limit=3
	var wg sync.WaitGroup
	errs := make(chan error, 3)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s := &client.Semaphore{
				Key:            "conc-sem",
				Limit:          3,
				AcquireTimeout: 10 * time.Second,
				Servers:        []string{addr},
			}
			ok, err := s.Acquire(context.Background())
			if err != nil {
				errs <- err
				return
			}
			if !ok {
				errs <- fmt.Errorf("acquire returned false")
				return
			}
			time.Sleep(50 * time.Millisecond)
			if err := s.Release(context.Background()); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

// ---------------------------------------------------------------------------
// Sharding tests
// ---------------------------------------------------------------------------

func TestCRC32ShardConsistency(t *testing.T) {
	// Same key, same server count → same index, every time.
	for i := 0; i < 100; i++ {
		idx := client.CRC32Shard("test-key", 3)
		if idx < 0 || idx >= 3 {
			t.Fatalf("shard index out of range: %d", idx)
		}
	}

	// Different keys should (statistically) produce different indices.
	seen := make(map[int]bool)
	for i := 0; i < 100; i++ {
		key := "key-" + string(rune('a'+i%26))
		seen[client.CRC32Shard(key, 10)] = true
	}
	if len(seen) < 2 {
		t.Fatal("sharding produced suspiciously uniform results")
	}
}

func TestCRC32ShardSingleServer(t *testing.T) {
	// With 1 server, all keys map to index 0.
	for i := 0; i < 50; i++ {
		if idx := client.CRC32Shard("any-key", 1); idx != 0 {
			t.Fatalf("expected 0 with 1 server, got %d", idx)
		}
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func isTimeout(err error) bool {
	return err != nil && (err == client.ErrTimeout || err.Error() == "dflockd: timeout")
}
