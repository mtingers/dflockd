package client_test

import (
	"context"
	"crypto/tls"
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
// Client key validation
// ---------------------------------------------------------------------------

func TestValidateKey_Empty(t *testing.T) {
	_, addr := startServer(t, testConfig())
	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	_, _, err = client.Acquire(c, "", 1*time.Second)
	if err == nil {
		t.Fatal("expected error for empty key")
	}
}

func TestValidateKey_Whitespace(t *testing.T) {
	_, addr := startServer(t, testConfig())
	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	_, _, err = client.Acquire(c, "bad key", 1*time.Second)
	if err == nil {
		t.Fatal("expected error for key with space")
	}
}

// ---------------------------------------------------------------------------
// Context cancellation for Lock.Enqueue + Lock.Wait
// ---------------------------------------------------------------------------

func TestLockEnqueue_ContextCancel(t *testing.T) {
	_, addr := startServer(t, testConfig())

	// l1 holds the lock so l2's Enqueue will need to connect (non-blocking)
	l1 := &client.Lock{
		Key:            "eq-cancel",
		AcquireTimeout: 10 * time.Second,
		Servers:        []string{addr},
	}
	ok, err := l1.Acquire(context.Background())
	if err != nil || !ok {
		t.Fatal("l1 acquire failed")
	}
	defer l1.Release(context.Background())

	// Enqueue with a normal context — should succeed with "queued"
	l2 := &client.Lock{
		Key:     "eq-cancel",
		Servers: []string{addr},
	}
	status, err := l2.Enqueue(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status != "queued" {
		t.Fatalf("expected queued, got %s", status)
	}

	// Wait with cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err := l2.Wait(ctx, 30*time.Second)
		if err == nil {
			t.Error("expected error from cancelled context")
		}
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Wait did not return after context cancellation")
	}
}

// ---------------------------------------------------------------------------
// Semaphore context cancellation
// ---------------------------------------------------------------------------

func TestSemaphoreAcquire_ContextCancel(t *testing.T) {
	_, addr := startServer(t, testConfig())

	s1 := &client.Semaphore{
		Key:            "sem-cancel",
		Limit:          1,
		AcquireTimeout: 10 * time.Second,
		Servers:        []string{addr},
	}
	ok, err := s1.Acquire(context.Background())
	if err != nil || !ok {
		t.Fatal("s1 acquire failed")
	}
	defer s1.Release(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	s2 := &client.Semaphore{
		Key:            "sem-cancel",
		Limit:          1,
		AcquireTimeout: 30 * time.Second,
		Servers:        []string{addr},
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err := s2.Acquire(ctx)
		if err == nil {
			t.Error("expected error from cancelled context")
		}
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Semaphore.Acquire did not return after context cancellation")
	}
}

// ---------------------------------------------------------------------------
// Lock timeout returns false, nil (not an error)
// ---------------------------------------------------------------------------

func TestLockAcquire_Timeout(t *testing.T) {
	_, addr := startServer(t, testConfig())

	l1 := &client.Lock{
		Key:            "timeout-key",
		AcquireTimeout: 10 * time.Second,
		Servers:        []string{addr},
	}
	ok, err := l1.Acquire(context.Background())
	if err != nil || !ok {
		t.Fatal("l1 acquire failed")
	}
	defer l1.Release(context.Background())

	l2 := &client.Lock{
		Key:            "timeout-key",
		AcquireTimeout: 1 * time.Millisecond, // very short timeout
		Servers:        []string{addr},
	}
	ok, err = l2.Acquire(context.Background())
	if err != nil {
		t.Fatalf("expected nil error on timeout, got %v", err)
	}
	if ok {
		t.Fatal("expected false on timeout")
	}
}

func TestSemaphoreAcquire_Timeout(t *testing.T) {
	_, addr := startServer(t, testConfig())

	s1 := &client.Semaphore{
		Key:            "sem-timeout",
		Limit:          1,
		AcquireTimeout: 10 * time.Second,
		Servers:        []string{addr},
	}
	ok, err := s1.Acquire(context.Background())
	if err != nil || !ok {
		t.Fatal("s1 acquire failed")
	}
	defer s1.Release(context.Background())

	s2 := &client.Semaphore{
		Key:            "sem-timeout",
		Limit:          1,
		AcquireTimeout: 1 * time.Millisecond, // very short timeout
		Servers:        []string{addr},
	}
	ok, err = s2.Acquire(context.Background())
	if err != nil {
		t.Fatalf("expected nil error on timeout, got %v", err)
	}
	if ok {
		t.Fatal("expected false on timeout")
	}
}

// ---------------------------------------------------------------------------
// Lock.Close without explicit Release
// ---------------------------------------------------------------------------

func TestLockClose_NoRelease(t *testing.T) {
	_, addr := startServer(t, testConfig())

	l := &client.Lock{
		Key:            "close-key",
		AcquireTimeout: 10 * time.Second,
		Servers:        []string{addr},
	}
	ok, err := l.Acquire(context.Background())
	if err != nil || !ok {
		t.Fatal("acquire failed")
	}
	if l.Token() == "" {
		t.Fatal("expected non-empty token")
	}

	// Close without release — should not panic
	if err := l.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if l.Token() != "" {
		t.Fatal("token should be cleared after Close")
	}

	// Double close should be safe
	if err := l.Close(); err != nil {
		t.Fatalf("double Close failed: %v", err)
	}
}

func TestSemaphoreClose_NoRelease(t *testing.T) {
	_, addr := startServer(t, testConfig())

	s := &client.Semaphore{
		Key:            "close-sem",
		Limit:          3,
		AcquireTimeout: 10 * time.Second,
		Servers:        []string{addr},
	}
	ok, err := s.Acquire(context.Background())
	if err != nil || !ok {
		t.Fatal("acquire failed")
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if s.Token() != "" {
		t.Fatal("token should be cleared after Close")
	}
}

// ---------------------------------------------------------------------------
// Disconnect auto-releases locks (end-to-end)
// ---------------------------------------------------------------------------

func TestDisconnectAutoRelease(t *testing.T) {
	_, addr := startServer(t, testConfig())

	// c1 acquires a lock and then disconnects
	c1, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = client.Acquire(c1, "auto-key", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	c1.Close() // disconnect without release

	// Give server time to detect disconnect and clean up
	time.Sleep(200 * time.Millisecond)

	// c2 should be able to acquire immediately
	c2, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	token, _, err := client.Acquire(c2, "auto-key", 0)
	if err != nil {
		t.Fatalf("expected acquire after disconnect, got %v", err)
	}
	if token == "" {
		t.Fatal("expected non-empty token — auto-release should have freed the lock")
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func isTimeout(err error) bool {
	return err != nil && (err == client.ErrTimeout || err.Error() == "dflockd: timeout")
}

// ---------------------------------------------------------------------------
// TLS helpers and tests
// ---------------------------------------------------------------------------

func startTLSServer(t *testing.T, cfg *config.Config) (addr string, clientTLS *tls.Config) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	lm := lock.NewLockManager(cfg, log)
	srv := server.New(lm, cfg, log)

	serverTLS, clientTLS := testutil.SelfSignedTLS(t)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tlsLn := tls.NewListener(ln, serverTLS)
	addr = ln.Addr().String()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.RunOnListener(ctx, tlsLn)
	}()

	t.Cleanup(func() {
		cancel()
		<-done
	})

	return addr, clientTLS
}

func TestDialTLS(t *testing.T) {
	addr, clientTLS := startTLSServer(t, testConfig())

	c, err := client.DialTLS(addr, clientTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	token, lease, err := client.Acquire(c, "tls-key", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if token == "" {
		t.Fatal("expected non-empty token")
	}
	if lease <= 0 {
		t.Fatalf("expected positive lease, got %d", lease)
	}

	if err := client.Release(c, "tls-key", token); err != nil {
		t.Fatal(err)
	}
}

func TestLockAcquireReleaseTLS(t *testing.T) {
	addr, clientTLS := startTLSServer(t, testConfig())

	l := &client.Lock{
		Key:            "tls-lock",
		AcquireTimeout: 10 * time.Second,
		Servers:        []string{addr},
		TLSConfig:      clientTLS,
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

func TestSemaphoreAcquireReleaseTLS(t *testing.T) {
	addr, clientTLS := startTLSServer(t, testConfig())

	s := &client.Semaphore{
		Key:            "tls-sem",
		Limit:          3,
		AcquireTimeout: 10 * time.Second,
		Servers:        []string{addr},
		TLSConfig:      clientTLS,
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

func TestDialTLS_BadCA(t *testing.T) {
	addr, _ := startTLSServer(t, testConfig())

	// Use an empty TLS config (no CA pool) — should fail verification.
	_, err := client.DialTLS(addr, &tls.Config{})
	if err == nil {
		t.Fatal("expected error with untrusted CA")
	}
}

// ---------------------------------------------------------------------------
// Auth helpers and tests
// ---------------------------------------------------------------------------

func startAuthServer(t *testing.T, cfg *config.Config, authToken string) (addr string) {
	t.Helper()
	cfg.AuthToken = authToken
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

	return addr
}

func TestAuthenticate(t *testing.T) {
	addr := startAuthServer(t, testConfig(), "secret123")

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := client.Authenticate(c, "secret123"); err != nil {
		t.Fatal(err)
	}

	// Acquire/release should work after auth
	token, lease, err := client.Acquire(c, "auth-key", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if token == "" || lease <= 0 {
		t.Fatalf("unexpected token=%q lease=%d", token, lease)
	}
	if err := client.Release(c, "auth-key", token); err != nil {
		t.Fatal(err)
	}
}

func TestAuthenticate_WrongToken(t *testing.T) {
	addr := startAuthServer(t, testConfig(), "secret123")

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	err = client.Authenticate(c, "wrongtoken")
	if err == nil {
		t.Fatal("expected error")
	}
	if err != client.ErrAuth {
		t.Fatalf("expected ErrAuth, got %v", err)
	}
}

func TestLockAcquireReleaseAuth(t *testing.T) {
	addr := startAuthServer(t, testConfig(), "secret123")

	l := &client.Lock{
		Key:            "auth-lock",
		AcquireTimeout: 10 * time.Second,
		Servers:        []string{addr},
		AuthToken:      "secret123",
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

func TestSemaphoreAcquireReleaseAuth(t *testing.T) {
	addr := startAuthServer(t, testConfig(), "secret123")

	s := &client.Semaphore{
		Key:            "auth-sem",
		Limit:          3,
		AcquireTimeout: 10 * time.Second,
		Servers:        []string{addr},
		AuthToken:      "secret123",
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
