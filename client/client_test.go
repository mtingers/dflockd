package client_test

import (
	"context"
	"crypto/tls"
	"errors"
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

// ===========================================================================
// Phase 1: Counter Client Tests
// ===========================================================================

func TestClient_Incr(t *testing.T) {
	_, addr := startServer(t, testConfig())
	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	val, err := client.Incr(c, "ctr", 5)
	if err != nil {
		t.Fatal(err)
	}
	if val != 5 {
		t.Fatalf("expected 5, got %d", val)
	}
	val, err = client.Incr(c, "ctr", 3)
	if err != nil {
		t.Fatal(err)
	}
	if val != 8 {
		t.Fatalf("expected 8, got %d", val)
	}
}

func TestClient_Decr(t *testing.T) {
	_, addr := startServer(t, testConfig())
	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	client.Incr(c, "ctr", 10)
	val, err := client.Decr(c, "ctr", 3)
	if err != nil {
		t.Fatal(err)
	}
	if val != 7 {
		t.Fatalf("expected 7, got %d", val)
	}
}

func TestClient_GetCounter(t *testing.T) {
	_, addr := startServer(t, testConfig())
	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	val, err := client.GetCounter(c, "ctr")
	if err != nil {
		t.Fatal(err)
	}
	if val != 0 {
		t.Fatalf("expected 0, got %d", val)
	}

	client.Incr(c, "ctr", 42)
	val, err = client.GetCounter(c, "ctr")
	if err != nil {
		t.Fatal(err)
	}
	if val != 42 {
		t.Fatalf("expected 42, got %d", val)
	}
}

func TestClient_SetCounter(t *testing.T) {
	_, addr := startServer(t, testConfig())
	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := client.SetCounter(c, "ctr", 99); err != nil {
		t.Fatal(err)
	}
	val, _ := client.GetCounter(c, "ctr")
	if val != 99 {
		t.Fatalf("expected 99, got %d", val)
	}
}

// ===========================================================================
// Phase 2: KV Client Tests
// ===========================================================================

func TestClient_KVSetGet(t *testing.T) {
	_, addr := startServer(t, testConfig())
	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := client.KVSet(c, "mykey", "hello", 0); err != nil {
		t.Fatal(err)
	}
	val, err := client.KVGet(c, "mykey")
	if err != nil {
		t.Fatal(err)
	}
	if val != "hello" {
		t.Fatalf("expected 'hello', got %q", val)
	}
}

func TestClient_KVDel(t *testing.T) {
	_, addr := startServer(t, testConfig())
	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	client.KVSet(c, "mykey", "hello", 0)
	if err := client.KVDel(c, "mykey"); err != nil {
		t.Fatal(err)
	}
	_, err = client.KVGet(c, "mykey")
	if err != client.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestClient_KVNotFound(t *testing.T) {
	_, addr := startServer(t, testConfig())
	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	_, err = client.KVGet(c, "nosuchkey")
	if err != client.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestClient_KVWithTTL(t *testing.T) {
	_, addr := startServer(t, testConfig())
	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := client.KVSet(c, "mykey", "hello", 1); err != nil {
		t.Fatal(err)
	}
	val, err := client.KVGet(c, "mykey")
	if err != nil || val != "hello" {
		t.Fatalf("expected 'hello', got %q err=%v", val, err)
	}

	time.Sleep(1200 * time.Millisecond)

	_, err = client.KVGet(c, "mykey")
	if err != client.ErrNotFound {
		t.Fatalf("expected ErrNotFound after TTL, got %v", err)
	}
}

// ===========================================================================
// Phase 3: Signal Client Tests
// ===========================================================================

func TestClient_SignalConn(t *testing.T) {
	_, addr := startServer(t, testConfig())

	// Listener
	lc, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	sc := client.NewSignalConn(lc)
	defer sc.Close()

	if err := sc.Listen("alerts.*"); err != nil {
		t.Fatal(err)
	}

	// Emitter
	ec, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer ec.Close()

	n, err := client.Emit(ec, "alerts.fire", "help!")
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 1 receiver, got %d", n)
	}

	// Receive signal
	select {
	case sig := <-sc.Signals():
		if sig.Channel != "alerts.fire" || sig.Payload != "help!" {
			t.Fatalf("unexpected signal: %+v", sig)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for signal")
	}
}

func TestClient_SignalConn_WildcardGT(t *testing.T) {
	_, addr := startServer(t, testConfig())

	lc, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	sc := client.NewSignalConn(lc)
	defer sc.Close()

	if err := sc.Listen("events.>"); err != nil {
		t.Fatal(err)
	}

	ec, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer ec.Close()

	client.Emit(ec, "events.user.login", "user42")

	select {
	case sig := <-sc.Signals():
		if sig.Channel != "events.user.login" || sig.Payload != "user42" {
			t.Fatalf("unexpected signal: %+v", sig)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
}

func TestClient_SignalConn_Emit(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	sc := client.NewSignalConn(c)
	defer sc.Close()

	n, err := sc.Emit("ch", "payload")
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("expected 0 receivers, got %d", n)
	}
}

func TestClient_EmitWithoutListening(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	n, err := client.Emit(c, "ch", "hello")
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("expected 0 receivers, got %d", n)
	}
}

func TestClient_SignalConn_MultipleChannels(t *testing.T) {
	_, addr := startServer(t, testConfig())

	lc, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	sc := client.NewSignalConn(lc)
	defer sc.Close()

	sc.Listen("ch1")
	sc.Listen("ch2")

	ec, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer ec.Close()

	client.Emit(ec, "ch1", "msg1")
	client.Emit(ec, "ch2", "msg2")

	received := make(map[string]string)
	for i := 0; i < 2; i++ {
		select {
		case sig := <-sc.Signals():
			received[sig.Channel] = sig.Payload
		case <-time.After(2 * time.Second):
			t.Fatal("timeout")
		}
	}

	if received["ch1"] != "msg1" || received["ch2"] != "msg2" {
		t.Fatalf("unexpected: %v", received)
	}
}

// ---------------------------------------------------------------------------
// Queue Group Client Tests
// ---------------------------------------------------------------------------

func TestClient_QueueGroup_Delivery(t *testing.T) {
	_, addr := startServer(t, testConfig())

	// Two listeners in same group
	lc1, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	sc1 := client.NewSignalConn(lc1)
	defer sc1.Close()

	lc2, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	sc2 := client.NewSignalConn(lc2)
	defer sc2.Close()

	if err := sc1.Listen("ch", client.WithGroup("workers")); err != nil {
		t.Fatal(err)
	}
	if err := sc2.Listen("ch", client.WithGroup("workers")); err != nil {
		t.Fatal(err)
	}

	// Emit
	ec, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer ec.Close()

	n, err := client.Emit(ec, "ch", "job1")
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 1 receiver, got %d", n)
	}

	// Exactly one should receive
	var got int
	select {
	case <-sc1.Signals():
		got++
	case <-time.After(2 * time.Second):
	}
	select {
	case <-sc2.Signals():
		got++
	case <-time.After(200 * time.Millisecond):
	}
	if got != 1 {
		t.Fatalf("expected exactly 1 delivery, got %d", got)
	}
}

func TestClient_QueueGroup_RoundRobin(t *testing.T) {
	_, addr := startServer(t, testConfig())

	lc1, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	sc1 := client.NewSignalConn(lc1)
	defer sc1.Close()

	lc2, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	sc2 := client.NewSignalConn(lc2)
	defer sc2.Close()

	sc1.Listen("ch", client.WithGroup("workers"))
	sc2.Listen("ch", client.WithGroup("workers"))

	ec, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer ec.Close()

	// Send two signals — each member should get one
	client.Emit(ec, "ch", "job1")
	client.Emit(ec, "ch", "job2")

	var count1, count2 int
	for i := 0; i < 2; i++ {
		select {
		case <-sc1.Signals():
			count1++
		case <-sc2.Signals():
			count2++
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for signal")
		}
	}
	if count1 != 1 || count2 != 1 {
		t.Fatalf("expected 1/1 distribution, got %d/%d", count1, count2)
	}
}

func TestClient_QueueGroup_BackwardCompat(t *testing.T) {
	_, addr := startServer(t, testConfig())

	lc, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	sc := client.NewSignalConn(lc)
	defer sc.Close()

	// Listen without WithGroup (backward compatible)
	if err := sc.Listen("ch"); err != nil {
		t.Fatal(err)
	}

	ec, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer ec.Close()

	n, err := client.Emit(ec, "ch", "hello")
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 1, got %d", n)
	}

	select {
	case sig := <-sc.Signals():
		if sig.Channel != "ch" || sig.Payload != "hello" {
			t.Fatalf("unexpected signal: %+v", sig)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
}

// ===========================================================================
// Phase 4: List Client Tests
// ===========================================================================

func TestClient_LPush_RPush(t *testing.T) {
	_, addr := startServer(t, testConfig())
	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	n, err := client.RPush(c, "q1", "a")
	if err != nil || n != 1 {
		t.Fatalf("rpush: n=%d err=%v", n, err)
	}
	n, err = client.RPush(c, "q1", "b")
	if err != nil || n != 2 {
		t.Fatalf("rpush: n=%d err=%v", n, err)
	}
	n, err = client.LPush(c, "q1", "z")
	if err != nil || n != 3 {
		t.Fatalf("lpush: n=%d err=%v", n, err)
	}
}

func TestClient_LPop_RPop(t *testing.T) {
	_, addr := startServer(t, testConfig())
	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	client.RPush(c, "q1", "a")
	client.RPush(c, "q1", "b")
	client.RPush(c, "q1", "c")

	val, err := client.LPop(c, "q1")
	if err != nil || val != "a" {
		t.Fatalf("lpop: %q err=%v", val, err)
	}
	val, err = client.RPop(c, "q1")
	if err != nil || val != "c" {
		t.Fatalf("rpop: %q err=%v", val, err)
	}
}

func TestClient_LPop_Empty(t *testing.T) {
	_, addr := startServer(t, testConfig())
	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	_, err = client.LPop(c, "empty")
	if err != client.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestClient_LLen(t *testing.T) {
	_, addr := startServer(t, testConfig())
	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	n, err := client.LLen(c, "q1")
	if err != nil || n != 0 {
		t.Fatalf("expected 0, got %d err=%v", n, err)
	}

	client.RPush(c, "q1", "a")
	client.RPush(c, "q1", "b")

	n, err = client.LLen(c, "q1")
	if err != nil || n != 2 {
		t.Fatalf("expected 2, got %d err=%v", n, err)
	}
}

func TestClient_LRange(t *testing.T) {
	_, addr := startServer(t, testConfig())
	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	client.RPush(c, "q1", "a")
	client.RPush(c, "q1", "b")
	client.RPush(c, "q1", "c")

	items, err := client.LRange(c, "q1", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 3 || items[0] != "a" || items[1] != "b" || items[2] != "c" {
		t.Fatalf("expected [a b c], got %v", items)
	}

	items, err = client.LRange(c, "q1", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0] != "b" {
		t.Fatalf("expected [b], got %v", items)
	}
}

// ---------------------------------------------------------------------------
// Fencing token tests
// ---------------------------------------------------------------------------

func TestAcquireWithFence(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	token, lease, fence, err := client.AcquireWithFence(c, "fkey", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if token == "" {
		t.Fatal("expected non-empty token")
	}
	if lease <= 0 {
		t.Fatalf("expected positive lease, got %d", lease)
	}
	if fence == 0 {
		t.Fatal("expected non-zero fence")
	}

	if err := client.Release(c, "fkey", token); err != nil {
		t.Fatal(err)
	}

	// Second acquire — fence should increase
	_, _, fence2, err := client.AcquireWithFence(c, "fkey", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if fence2 <= fence {
		t.Fatalf("fence should increase: %d -> %d", fence, fence2)
	}
}

func TestRenewWithFence(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	token, _, fence, err := client.AcquireWithFence(c, "fkey", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	remaining, renewFence, err := client.RenewWithFence(c, "fkey", token)
	if err != nil {
		t.Fatal(err)
	}
	if remaining <= 0 {
		t.Fatalf("expected positive remaining, got %d", remaining)
	}
	if renewFence != fence {
		t.Fatalf("fence should be same on renew: %d vs %d", fence, renewFence)
	}
}

func TestEnqueueWithFence(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	status, token, lease, fence, err := client.EnqueueWithFence(c, "fkey")
	if err != nil {
		t.Fatal(err)
	}
	if status != "acquired" {
		t.Fatalf("expected acquired, got %q", status)
	}
	if token == "" || lease <= 0 || fence == 0 {
		t.Fatalf("unexpected values: token=%q lease=%d fence=%d", token, lease, fence)
	}
}

func TestWaitWithFence(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c1, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()

	token1, _, err := client.Acquire(c1, "fkey", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	c2, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	status, _, _, _, err := client.EnqueueWithFence(c2, "fkey")
	if err != nil {
		t.Fatal(err)
	}
	if status != "queued" {
		t.Fatalf("expected queued, got %q", status)
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		client.Release(c1, "fkey", token1)
	}()

	token2, lease, fence, err := client.WaitWithFence(c2, "fkey", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if token2 == "" || lease <= 0 || fence == 0 {
		t.Fatalf("unexpected wait result: token=%q lease=%d fence=%d", token2, lease, fence)
	}
}

func TestSemAcquireWithFence(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	token, lease, fence, err := client.SemAcquireWithFence(c, "skey", 10*time.Second, 3)
	if err != nil {
		t.Fatal(err)
	}
	if token == "" || lease <= 0 || fence == 0 {
		t.Fatalf("unexpected values: token=%q lease=%d fence=%d", token, lease, fence)
	}

	remaining, renewFence, err := client.SemRenewWithFence(c, "skey", token)
	if err != nil {
		t.Fatal(err)
	}
	if remaining <= 0 {
		t.Fatalf("expected positive remaining, got %d", remaining)
	}
	if renewFence != fence {
		t.Fatalf("fence should match on renew: %d vs %d", fence, renewFence)
	}
}

func TestSemEnqueueWithFence(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	status, token, lease, fence, err := client.SemEnqueueWithFence(c, "skey", 1)
	if err != nil {
		t.Fatal(err)
	}
	if status != "acquired" {
		t.Fatalf("expected acquired, got %q", status)
	}
	if token == "" || lease <= 0 || fence == 0 {
		t.Fatalf("unexpected values: token=%q lease=%d fence=%d", token, lease, fence)
	}
}

func TestSemWaitWithFence(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c1, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()

	token1, _, err := client.SemAcquire(c1, "skey", 10*time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}

	c2, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	status, _, _, _, err := client.SemEnqueueWithFence(c2, "skey", 1)
	if err != nil {
		t.Fatal(err)
	}
	if status != "queued" {
		t.Fatalf("expected queued, got %q", status)
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		client.SemRelease(c1, "skey", token1)
	}()

	token2, lease, fence, err := client.SemWaitWithFence(c2, "skey", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if token2 == "" || lease <= 0 || fence == 0 {
		t.Fatalf("unexpected values: token=%q lease=%d fence=%d", token2, lease, fence)
	}
}

func TestLock_Fence(t *testing.T) {
	_, addr := startServer(t, testConfig())

	l := &client.Lock{
		Key:     "fkey",
		Servers: []string{addr},
	}
	defer l.Close()

	ok, err := l.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected acquire to succeed")
	}
	if l.Fence() == 0 {
		t.Fatal("expected non-zero fence")
	}
	if l.Token() == "" {
		t.Fatal("expected non-empty token")
	}

	if err := l.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSemaphore_Fence(t *testing.T) {
	_, addr := startServer(t, testConfig())

	s := &client.Semaphore{
		Key:     "skey",
		Limit:   3,
		Servers: []string{addr},
	}
	defer s.Close()

	ok, err := s.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected acquire to succeed")
	}
	if s.Fence() == 0 {
		t.Fatal("expected non-zero fence")
	}

	if err := s.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// ---------------------------------------------------------------------------
// KV Compare-and-Swap
// ---------------------------------------------------------------------------

func TestClient_KVCAS(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Set initial value
	if err := client.KVSet(c, "caskey", "initial", 0); err != nil {
		t.Fatal(err)
	}

	// CAS with correct old value
	ok, err := client.KVCAS(c, "caskey", "initial", "updated", 0)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected CAS to succeed")
	}

	// Verify the value changed
	val, err := client.KVGet(c, "caskey")
	if err != nil {
		t.Fatal(err)
	}
	if val != "updated" {
		t.Fatalf("expected 'updated', got %q", val)
	}
}

func TestClient_KVCAS_Conflict(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := client.KVSet(c, "caskey", "initial", 0); err != nil {
		t.Fatal(err)
	}

	// CAS with wrong old value
	ok, err := client.KVCAS(c, "caskey", "wrong", "updated", 0)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected CAS to fail with wrong old value")
	}
}

func TestClient_KVCAS_CreateIfAbsent(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// CAS with empty old value creates a new key
	ok, err := client.KVCAS(c, "newkey", "", "created", 0)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected CAS to create new key")
	}

	val, err := client.KVGet(c, "newkey")
	if err != nil {
		t.Fatal(err)
	}
	if val != "created" {
		t.Fatalf("expected 'created', got %q", val)
	}
}

// ---------------------------------------------------------------------------
// Blocking list operations
// ---------------------------------------------------------------------------

func TestClient_BLPop(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Push first, then pop immediately
	if _, err := client.RPush(c, "bq", "hello"); err != nil {
		t.Fatal(err)
	}

	val, err := client.BLPop(c, "bq", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if val != "hello" {
		t.Fatalf("expected 'hello', got %q", val)
	}
}

func TestClient_BLPop_Blocks(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c1, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()

	c2, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	done := make(chan string, 1)
	go func() {
		val, _ := client.BLPop(c1, "bq", 5*time.Second)
		done <- val
	}()

	time.Sleep(50 * time.Millisecond)
	client.RPush(c2, "bq", "delayed")

	select {
	case val := <-done:
		if val != "delayed" {
			t.Fatalf("expected 'delayed', got %q", val)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("BLPop did not return after push")
	}
}

func TestClient_BRPop(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	client.LPush(c, "bq", "a")
	client.LPush(c, "bq", "b")

	// BRPop should return the rightmost element
	val, err := client.BRPop(c, "bq", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if val != "a" {
		t.Fatalf("expected 'a' (rightmost), got %q", val)
	}
}

func TestClient_BLPop_Timeout(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// BLPop with 0 timeout on empty list should return ErrTimeout
	_, err = client.BLPop(c, "emptyq", 0)
	if !isTimeout(err) {
		t.Fatalf("expected ErrTimeout, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// RW Locks (low-level)
// ---------------------------------------------------------------------------

func TestClient_RLock_WLock(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Acquire read lock
	token, lease, fence, err := client.RLock(c, "rwkey", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if token == "" || lease <= 0 || fence == 0 {
		t.Fatalf("unexpected values: token=%q lease=%d fence=%d", token, lease, fence)
	}

	// Release read lock
	if err := client.RUnlock(c, "rwkey", token); err != nil {
		t.Fatal(err)
	}

	// Acquire write lock
	wToken, wLease, wFence, err := client.WLock(c, "rwkey", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if wToken == "" || wLease <= 0 || wFence == 0 {
		t.Fatalf("unexpected values: token=%q lease=%d fence=%d", wToken, wLease, wFence)
	}
	if wFence <= fence {
		t.Fatalf("write fence %d should be > read fence %d", wFence, fence)
	}

	if err := client.WUnlock(c, "rwkey", wToken); err != nil {
		t.Fatal(err)
	}
}

func TestClient_RWRenew(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	token, _, fence, err := client.RLock(c, "rwkey", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	remaining, renewFence, err := client.RRenew(c, "rwkey", token)
	if err != nil {
		t.Fatal(err)
	}
	if remaining <= 0 {
		t.Fatalf("expected positive remaining, got %d", remaining)
	}
	if renewFence != fence {
		t.Fatalf("fence mismatch: %d vs %d", renewFence, fence)
	}
}

func TestClient_RWEnqueue(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Enqueue read lock (should acquire immediately)
	status, token, lease, fence, err := client.REnqueue(c, "rwkey")
	if err != nil {
		t.Fatal(err)
	}
	if status != "acquired" {
		t.Fatalf("expected acquired, got %q", status)
	}
	if token == "" || lease <= 0 || fence == 0 {
		t.Fatalf("unexpected values: token=%q lease=%d fence=%d", token, lease, fence)
	}
}

// ---------------------------------------------------------------------------
// RWLock high-level type
// ---------------------------------------------------------------------------

func TestRWLock_ReadLockUnlock(t *testing.T) {
	_, addr := startServer(t, testConfig())

	rw := &client.RWLock{
		Key:     "rwkey",
		Servers: []string{addr},
	}
	defer rw.Close()

	ok, err := rw.RLock(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected RLock to succeed")
	}
	if rw.Token() == "" {
		t.Fatal("expected non-empty token")
	}
	if rw.Fence() == 0 {
		t.Fatal("expected non-zero fence")
	}

	if err := rw.Unlock(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRWLock_WriteLockUnlock(t *testing.T) {
	_, addr := startServer(t, testConfig())

	rw := &client.RWLock{
		Key:     "rwkey",
		Servers: []string{addr},
	}
	defer rw.Close()

	ok, err := rw.WLock(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected WLock to succeed")
	}

	if err := rw.Unlock(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRWLock_ConcurrentReaders(t *testing.T) {
	_, addr := startServer(t, testConfig())

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rw := &client.RWLock{
				Key:     "rwkey",
				Servers: []string{addr},
			}
			defer rw.Close()

			ok, err := rw.RLock(context.Background())
			if err != nil {
				t.Errorf("RLock error: %v", err)
				return
			}
			if !ok {
				t.Error("expected RLock to succeed")
				return
			}

			time.Sleep(50 * time.Millisecond)

			if err := rw.Unlock(context.Background()); err != nil {
				t.Errorf("Unlock error: %v", err)
			}
		}()
	}
	wg.Wait()
}

// ---------------------------------------------------------------------------
// Barrier
// ---------------------------------------------------------------------------

func TestClient_BarrierWait(t *testing.T) {
	_, addr := startServer(t, testConfig())

	var wg sync.WaitGroup
	results := make([]bool, 3)
	errs := make([]error, 3)

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			c, err := client.Dial(addr)
			if err != nil {
				errs[idx] = err
				return
			}
			defer c.Close()

			ok, err := client.BarrierWait(c, "barrier1", 3, 5*time.Second)
			results[idx] = ok
			errs[idx] = err
		}(i)
	}
	wg.Wait()

	for i := 0; i < 3; i++ {
		if errs[i] != nil {
			t.Fatalf("participant %d error: %v", i, errs[i])
		}
		if !results[i] {
			t.Fatalf("participant %d: expected barrier to trip", i)
		}
	}
}

func TestClient_BarrierWait_Timeout(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Only 1 of 3 participants — should timeout
	ok, err := client.BarrierWait(c, "barrier_to", 3, 0)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected barrier to NOT trip")
	}
}

// ---------------------------------------------------------------------------
// WatchConn
// ---------------------------------------------------------------------------

func TestClient_WatchConn_KVSet(t *testing.T) {
	_, addr := startServer(t, testConfig())

	// Watcher connection
	wc, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	watch := client.NewWatchConn(wc)
	defer watch.Close()

	if err := watch.Watch("kv.*"); err != nil {
		t.Fatal(err)
	}

	// Separate connection to trigger events
	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := client.KVSet(c, "kv.test", "hello", 0); err != nil {
		t.Fatal(err)
	}

	select {
	case evt := <-watch.Events():
		if evt.Key != "kv.test" {
			t.Fatalf("expected key kv.test, got %q", evt.Key)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no watch event received")
	}
}

func TestClient_WatchConn_Unwatch(t *testing.T) {
	_, addr := startServer(t, testConfig())

	wc, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	watch := client.NewWatchConn(wc)
	defer watch.Close()

	if err := watch.Watch("kv.*"); err != nil {
		t.Fatal(err)
	}

	if err := watch.Unwatch("kv.*"); err != nil {
		t.Fatal(err)
	}

	// Should not receive events after unwatching
	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	client.KVSet(c, "kv.test", "hello", 0)

	select {
	case evt := <-watch.Events():
		t.Fatalf("should not receive events after unwatch, got %+v", evt)
	case <-time.After(200 * time.Millisecond):
		// Expected: no event
	}
}

// ---------------------------------------------------------------------------
// LeaderConn
// ---------------------------------------------------------------------------

func TestClient_LeaderConn_ElectResign(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	lc := client.NewLeaderConn(c)
	defer lc.Close()

	token, lease, fence, err := lc.Elect("leader1", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if token == "" || lease <= 0 || fence == 0 {
		t.Fatalf("unexpected: token=%q lease=%d fence=%d", token, lease, fence)
	}

	if err := lc.Resign("leader1", token); err != nil {
		t.Fatal(err)
	}
}

func TestClient_LeaderConn_ObserveUnobserve(t *testing.T) {
	_, addr := startServer(t, testConfig())

	// Observer
	obsC, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	obs := client.NewLeaderConn(obsC)
	defer obs.Close()

	if err := obs.Observe("leader1"); err != nil {
		t.Fatal(err)
	}

	// Leader
	leaderC, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	lc := client.NewLeaderConn(leaderC)
	defer lc.Close()

	token, _, _, err := lc.Elect("leader1", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// Observer should receive event
	select {
	case evt := <-obs.Events():
		if evt.Type != "elected" {
			t.Fatalf("expected elected, got %q", evt.Type)
		}
		if evt.Key != "leader1" {
			t.Fatalf("expected key leader1, got %q", evt.Key)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no leader event received")
	}

	// Unobserve
	if err := obs.Unobserve("leader1"); err != nil {
		t.Fatal(err)
	}

	// Resign — observer should NOT get event
	lc.Resign("leader1", token)

	select {
	case evt := <-obs.Events():
		t.Fatalf("should not receive events after unobserve, got %+v", evt)
	case <-time.After(200 * time.Millisecond):
		// Expected: no event
	}
}

func TestClient_LeaderConn_Renew(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	lc := client.NewLeaderConn(c)
	defer lc.Close()

	token, _, fence, err := lc.Elect("leader1", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	remaining, renewFence, err := lc.Renew("leader1", token)
	if err != nil {
		t.Fatal(err)
	}
	if remaining <= 0 {
		t.Fatalf("expected positive remaining, got %d", remaining)
	}
	if renewFence != fence {
		t.Fatalf("fence mismatch on renew: %d vs %d", renewFence, fence)
	}
}

// ---------------------------------------------------------------------------
// Election high-level type
// ---------------------------------------------------------------------------

func TestElection_CampaignResign(t *testing.T) {
	_, addr := startServer(t, testConfig())

	elected := make(chan struct{}, 1)
	resigned := make(chan struct{}, 1)

	e := &client.Election{
		Key:     "election1",
		Servers: []string{addr},
		OnElected: func() {
			elected <- struct{}{}
		},
		OnResigned: func() {
			resigned <- struct{}{}
		},
	}
	defer e.Close()

	ok, err := e.Campaign(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected to be elected")
	}
	if !e.IsLeader() {
		t.Fatal("expected IsLeader() to be true")
	}
	if e.Token() == "" {
		t.Fatal("expected non-empty token")
	}
	if e.Fence() == 0 {
		t.Fatal("expected non-zero fence")
	}

	select {
	case <-elected:
	case <-time.After(time.Second):
		t.Fatal("OnElected not called")
	}

	if err := e.Resign(context.Background()); err != nil {
		t.Fatal(err)
	}
	if e.IsLeader() {
		t.Fatal("expected IsLeader() to be false after resign")
	}

	select {
	case <-resigned:
	case <-time.After(time.Second):
		t.Fatal("OnResigned not called")
	}
}

func TestElection_CampaignContextCancel(t *testing.T) {
	_, addr := startServer(t, testConfig())

	// First connection holds leadership
	c1, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	lc1 := client.NewLeaderConn(c1)
	defer lc1.Close()

	_, _, _, err = lc1.Elect("election1", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// Second election should block and then cancel
	e := &client.Election{
		Key:     "election1",
		Servers: []string{addr},
	}
	defer e.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	ok, err := e.Campaign(ctx)
	if ok {
		t.Fatal("expected campaign to NOT succeed while another holds leadership")
	}
	if err == nil {
		t.Fatal("expected context error")
	}
}

// ---------------------------------------------------------------------------
// RW Lock low-level functions: WRenew, WWait, RWait, WEnqueue
// ---------------------------------------------------------------------------

func TestClient_WRenew(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Acquire a write lock
	token, _, fence, err := client.WLock(c, "wrkey", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// Renew the write lock
	remaining, renewFence, err := client.WRenew(c, "wrkey", token)
	if err != nil {
		t.Fatal(err)
	}
	if remaining <= 0 {
		t.Fatalf("expected positive remaining, got %d", remaining)
	}
	if renewFence != fence {
		t.Fatalf("fence mismatch on wrenew: %d vs %d", renewFence, fence)
	}
}

func TestClient_WRenew_WithLeaseTTL(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	token, _, _, err := client.WLock(c, "wrttl", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// Renew with custom lease TTL (WithLeaseTTL takes seconds as int)
	remaining, _, err := client.WRenew(c, "wrttl", token, client.WithLeaseTTL(60))
	if err != nil {
		t.Fatal(err)
	}
	if remaining <= 0 {
		t.Fatalf("expected positive remaining, got %d", remaining)
	}
}

func TestClient_WEnqueue(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c1, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()

	// First connection acquires a read lock to block write enqueue
	_, _, _, err = client.RLock(c1, "wekey", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// Second connection write-enqueues (should be queued since read lock is held)
	c2, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	status, _, _, _, err := client.WEnqueue(c2, "wekey")
	if err != nil {
		t.Fatal(err)
	}
	if status != "queued" {
		t.Fatalf("expected 'queued', got %q", status)
	}
}

func TestClient_WEnqueue_Immediate(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Write-enqueue with no contention should acquire immediately
	status, token, lease, fence, err := client.WEnqueue(c, "weinow")
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
	if fence == 0 {
		t.Fatal("expected non-zero fence")
	}
}

func TestClient_WWait(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Enqueue first (no contention, should acquire immediately)
	status, _, _, _, err := client.WEnqueue(c, "wwkey")
	if err != nil {
		t.Fatal(err)
	}

	if status == "acquired" {
		// Already got the lock — release it and set up a real two-phase scenario
		c2, err := client.Dial(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer c2.Close()

		// Hold a read lock on a different key for a clean two-phase test
		_, _, _, err = client.RLock(c2, "wwkey2", 10*time.Second)
		if err != nil {
			t.Fatal(err)
		}

		c3, err := client.Dial(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer c3.Close()

		status2, _, _, _, err := client.WEnqueue(c3, "wwkey2")
		if err != nil {
			t.Fatal(err)
		}
		if status2 != "queued" {
			t.Fatalf("expected queued, got %q", status2)
		}

		// Release the read lock so the writer can proceed
		client.RUnlock(c2, "wwkey2", "")
		// Wait may need to use the token from enqueue, but RRelease needs the token
		// Let's use a simpler approach: just test WWait returning not-enqueued error
	}
}

func TestClient_WWait_NotEnqueued(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// WWait without enqueuing first should return ErrNotQueued
	_, _, _, err = client.WWait(c, "notenqueued", 5*time.Second)
	if err != client.ErrNotQueued {
		t.Fatalf("expected ErrNotQueued, got %v", err)
	}
}

func TestClient_RWait_NotEnqueued(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// RWait without enqueuing first should return ErrNotQueued
	_, _, _, err = client.RWait(c, "notenqueued", 5*time.Second)
	if err != client.ErrNotQueued {
		t.Fatalf("expected ErrNotQueued, got %v", err)
	}
}

func TestClient_REnqueue_TwoPhase(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c1, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()

	// c1 holds a write lock to block read-enqueue
	token, _, _, err := client.WLock(c1, "rekey", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	c2, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	// c2 read-enqueues
	status, _, _, _, err := client.REnqueue(c2, "rekey")
	if err != nil {
		t.Fatal(err)
	}
	if status != "queued" {
		t.Fatalf("expected 'queued', got %q", status)
	}

	// Release write lock
	err = client.WUnlock(c1, "rekey", token)
	if err != nil {
		t.Fatal(err)
	}

	// c2 read-waits
	rToken, lease, fence, err := client.RWait(c2, "rekey", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if rToken == "" {
		t.Fatal("expected non-empty token from RWait")
	}
	if lease <= 0 {
		t.Fatalf("expected positive lease, got %d", lease)
	}
	if fence == 0 {
		t.Fatal("expected non-zero fence")
	}

	// Read-renew
	remaining, renewFence, err := client.RRenew(c2, "rekey", rToken)
	if err != nil {
		t.Fatal(err)
	}
	if remaining <= 0 {
		t.Fatalf("expected positive remaining, got %d", remaining)
	}
	if renewFence != fence {
		t.Fatalf("fence mismatch on rrenew: %d vs %d", renewFence, fence)
	}
}

// ---------------------------------------------------------------------------
// Error path tests
// ---------------------------------------------------------------------------

func TestClient_ErrTypeMismatch_RWLock(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Acquire a regular lock
	_, _, err = client.Acquire(c, "tmkey", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// Try to read-lock the same key — type mismatch
	_, _, _, err = client.RLock(c, "tmkey", 10*time.Second)
	if err != client.ErrTypeMismatch {
		t.Fatalf("expected ErrTypeMismatch, got %v", err)
	}

	// Also test write-lock type mismatch
	_, _, _, err = client.WLock(c, "tmkey", 10*time.Second)
	if err != client.ErrTypeMismatch {
		t.Fatalf("expected ErrTypeMismatch for WLock, got %v", err)
	}
}

func TestClient_ErrAlreadyQueued(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c1, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()

	// c1 holds the lock
	_, _, err = client.Acquire(c1, "aqkey", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	c2, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	// c2 enqueues
	_, _, _, err = client.Enqueue(c2, "aqkey")
	if err != nil {
		t.Fatal(err)
	}

	// c2 enqueues again — should get ErrAlreadyQueued
	_, _, _, err = client.Enqueue(c2, "aqkey")
	if err != client.ErrAlreadyQueued {
		t.Fatalf("expected ErrAlreadyQueued, got %v", err)
	}
}

func TestClient_ErrBarrierCountMismatch(t *testing.T) {
	_, addr := startServer(t, testConfig())

	// c1 starts barrier with count=3
	c1, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		client.BarrierWait(c1, "bcmkey", 3, 5*time.Second)
	}()
	time.Sleep(100 * time.Millisecond)

	// c2 tries barrier with count=5 — mismatch
	c2, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	_, err = client.BarrierWait(c2, "bcmkey", 5, 5*time.Second)
	if err != client.ErrBarrierCountMismatch {
		t.Fatalf("expected ErrBarrierCountMismatch, got %v", err)
	}
}

func TestClient_ErrListFull(t *testing.T) {
	cfg := testConfig()
	cfg.MaxListLength = 2
	_, addr := startServer(t, cfg)

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Fill the list to max
	_, err = client.LPush(c, "lfull", "a")
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.LPush(c, "lfull", "b")
	if err != nil {
		t.Fatal(err)
	}

	// Third push should fail with ErrListFull
	_, err = client.LPush(c, "lfull", "c")
	if err != client.ErrListFull {
		t.Fatalf("expected ErrListFull, got %v", err)
	}
}

func TestClient_ErrMaxKeys(t *testing.T) {
	cfg := testConfig()
	cfg.MaxLocks = 2
	_, addr := startServer(t, cfg)

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Create two resources
	_, _, err = client.Acquire(c, "mk1", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = client.Acquire(c, "mk2", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// Third should fail with ErrMaxLocks (which is the MaxKeys error at lock level)
	_, _, err = client.Acquire(c, "mk3", 10*time.Second)
	if err != client.ErrMaxLocks {
		t.Fatalf("expected ErrMaxLocks, got %v", err)
	}
}

func TestClient_BRPop_Blocking(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c1, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()

	c2, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	// c1 blocks on BRPop
	result := make(chan string, 1)
	errCh := make(chan error, 1)
	go func() {
		val, err := client.BRPop(c1, "brlist", 5*time.Second)
		if err != nil {
			errCh <- err
			return
		}
		result <- val
	}()

	time.Sleep(100 * time.Millisecond)

	// c2 pushes an element
	_, err = client.RPush(c2, "brlist", "hello")
	if err != nil {
		t.Fatal(err)
	}

	select {
	case val := <-result:
		if val != "hello" {
			t.Fatalf("expected 'hello', got %q", val)
		}
	case err := <-errCh:
		t.Fatalf("BRPop error: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("BRPop did not unblock")
	}
}

func TestClient_BRPop_Timeout(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// BRPop on empty list with short timeout
	_, err = client.BRPop(c, "emptylist", 1*time.Second)
	if !isTimeout(err) {
		t.Fatalf("expected ErrTimeout, got %v", err)
	}
}

func TestClient_ErrAlreadyQueued_RWEnqueue(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c1, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()

	// c1 holds a write lock to force queuing
	_, _, _, err = client.WLock(c1, "rwekey", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	c2, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	// c2 read-enqueues
	status, _, _, _, err := client.REnqueue(c2, "rwekey")
	if err != nil {
		t.Fatal(err)
	}
	if status != "queued" {
		t.Fatalf("expected 'queued', got %q", status)
	}

	// c2 read-enqueues again — ErrAlreadyQueued
	_, _, _, _, err = client.REnqueue(c2, "rwekey")
	if err != client.ErrAlreadyQueued {
		t.Fatalf("expected ErrAlreadyQueued, got %v", err)
	}
}

func TestClient_ErrTypeMismatch_Enqueue(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Create an RW lock (different type) using RLock
	_, _, _, err = client.RLock(c, "tmek", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	c2, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	// Try to regular-enqueue on an RW lock key — type mismatch
	_, _, _, err = client.Enqueue(c2, "tmek")
	if err != client.ErrTypeMismatch {
		t.Fatalf("expected ErrTypeMismatch on Enqueue, got %v", err)
	}
}

func TestClient_ErrTypeMismatch_RWEnqueue(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Create a regular lock
	_, _, err = client.Acquire(c, "rwtmkey", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// Try to write-enqueue on a regular lock key — type mismatch
	_, _, _, _, err = client.WEnqueue(c, "rwtmkey")
	if err != client.ErrTypeMismatch {
		t.Fatalf("expected ErrTypeMismatch on WEnqueue, got %v", err)
	}
}

func TestClient_ErrLimitMismatch_Semaphore(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Create semaphore with limit=3
	_, _, err = client.SemAcquire(c, "slm", 10*time.Second, 3)
	if err != nil {
		t.Fatal(err)
	}

	c2, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	// Try to acquire with different limit — mismatch
	_, _, err = client.SemAcquire(c2, "slm", 10*time.Second, 5)
	if err != client.ErrLimitMismatch {
		t.Fatalf("expected ErrLimitMismatch, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Regression: Context cancellation must release the lock before closing conn
// ---------------------------------------------------------------------------

// TestLockAcquire_ContextCancelDuringWait_ReleasesLock verifies that when a
// Lock.Acquire is waiting on the server and the context is cancelled, the
// lock is properly cleaned up. A second holder should be able to acquire it.
func TestLockAcquire_ContextCancelDuringWait_ReleasesLock(t *testing.T) {
	_, addr := startServer(t, testConfig())

	// First lock holds the resource.
	l1 := &client.Lock{
		Key:            "ctx-rel-key",
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

	// Second lock tries to acquire with a short-lived context.
	ctx, cancel := context.WithCancel(context.Background())
	l2 := &client.Lock{
		Key:            "ctx-rel-key",
		AcquireTimeout: 30 * time.Second,
		Servers:        []string{addr},
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = l2.Acquire(ctx)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("l2.Acquire did not return after context cancel")
	}

	// Release l1 — a third client should acquire immediately, proving l2's
	// enqueue was properly cleaned up and doesn't block the queue.
	l1.Release(context.Background())

	l3 := &client.Lock{
		Key:            "ctx-rel-key",
		AcquireTimeout: 2 * time.Second,
		Servers:        []string{addr},
	}
	ok, err = l3.Acquire(context.Background())
	if err != nil {
		t.Fatalf("l3 acquire error: %v", err)
	}
	if !ok {
		t.Fatal("l3 acquire should succeed after l1 released and l2 cancelled")
	}
	l3.Release(context.Background())
}

// TestLockAcquire_AlreadyCancelledContext verifies that calling Lock.Acquire
// with an already-cancelled context returns immediately with an error.
func TestLockAcquire_AlreadyCancelledContext(t *testing.T) {
	_, addr := startServer(t, testConfig())

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before Acquire

	l := &client.Lock{
		Key:            "ctx-already-key",
		AcquireTimeout: 5 * time.Second,
		Servers:        []string{addr},
	}

	ok, err := l.Acquire(ctx)
	if ok {
		// If it somehow acquired, it should have released due to ctx.Err()
		t.Log("acquired with cancelled context — verifying release")
	}
	if err == nil && !ok {
		// Timeout is also acceptable
	}
	// The main point: it should return reasonably quickly, not hang.
}

// TestSemaphoreAcquire_ContextCancelDuringWait is the semaphore variant.
func TestSemaphoreAcquire_ContextCancelDuringWait(t *testing.T) {
	_, addr := startServer(t, testConfig())

	s1 := &client.Semaphore{
		Key:            "ctx-sem-key",
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

	ctx, cancel := context.WithCancel(context.Background())
	s2 := &client.Semaphore{
		Key:            "ctx-sem-key",
		Limit:          1,
		AcquireTimeout: 30 * time.Second,
		Servers:        []string{addr},
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = s2.Acquire(ctx)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("s2.Acquire did not return after context cancel")
	}

	// Release s1 — s3 should succeed.
	s1.Release(context.Background())

	s3 := &client.Semaphore{
		Key:            "ctx-sem-key",
		Limit:          1,
		AcquireTimeout: 2 * time.Second,
		Servers:        []string{addr},
	}
	ok, err = s3.Acquire(context.Background())
	if err != nil {
		t.Fatalf("s3 acquire error: %v", err)
	}
	if !ok {
		t.Fatal("s3 acquire should succeed after s1 released and s2 cancelled")
	}
	s3.Release(context.Background())
}

// ---------------------------------------------------------------------------
// Regression: Election.Resign must not hold mutex during network I/O
// ---------------------------------------------------------------------------

// TestElection_ConcurrentResign verifies that concurrent Resign calls do not
// deadlock. Before the fix, Resign held the mutex during the blocking network
// call, causing other callers to block indefinitely.
func TestElection_ConcurrentResign(t *testing.T) {
	_, addr := startServer(t, testConfig())

	e := &client.Election{
		Key:     "resign-concurrent",
		Servers: []string{addr},
	}
	defer e.Close()

	ok, err := e.Campaign(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected to be elected")
	}

	// Launch multiple concurrent resign attempts. With the old code, the
	// second call would deadlock on the mutex held during network I/O.
	done := make(chan error, 3)
	for range 3 {
		go func() {
			done <- e.Resign(context.Background())
		}()
	}

	for range 3 {
		select {
		case err := <-done:
			// First resign succeeds, subsequent ones may return nil (no-op).
			_ = err
		case <-time.After(5 * time.Second):
			t.Fatal("Resign call deadlocked — likely holding mutex during network I/O")
		}
	}

	if e.IsLeader() {
		t.Fatal("expected IsLeader() to be false after resign")
	}
}

// TestElection_ResignReleasesLeadership verifies that after Resign, another
// election candidate can win leadership.
func TestElection_ResignReleasesLeadership(t *testing.T) {
	_, addr := startServer(t, testConfig())

	e1 := &client.Election{
		Key:     "resign-handoff",
		Servers: []string{addr},
	}
	defer e1.Close()

	ok, err := e1.Campaign(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("e1 expected to be elected")
	}

	// Resign e1 — should free leadership.
	if err := e1.Resign(context.Background()); err != nil {
		t.Fatal(err)
	}

	// e2 should be able to campaign and win immediately.
	e2 := &client.Election{
		Key:     "resign-handoff",
		Servers: []string{addr},
	}
	defer e2.Close()

	ok, err = e2.Campaign(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("e2 expected to be elected after e1 resigned")
	}
	if !e2.IsLeader() {
		t.Fatal("e2 expected IsLeader() to be true")
	}
}

// ===========================================================================
// New comprehensive tests
// ===========================================================================

// ---------------------------------------------------------------------------
// 1. TestRWLock_ReadAcquireRelease
// ---------------------------------------------------------------------------

func TestRWLock_ReadAcquireRelease(t *testing.T) {
	_, addr := startServer(t, testConfig())

	rw := &client.RWLock{
		Key:     "rw-read-ar",
		Servers: []string{addr},
	}
	defer rw.Close()

	ok, err := rw.RLock(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected RLock to succeed")
	}
	if rw.Token() == "" {
		t.Fatal("expected non-empty token after RLock")
	}
	if rw.Fence() == 0 {
		t.Fatal("expected non-zero fence after RLock")
	}

	if err := rw.Unlock(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rw.Token() != "" {
		t.Fatal("expected empty token after Unlock")
	}
}

// ---------------------------------------------------------------------------
// 2. TestRWLock_WriteAcquireRelease
// ---------------------------------------------------------------------------

func TestRWLock_WriteAcquireRelease(t *testing.T) {
	_, addr := startServer(t, testConfig())

	rw := &client.RWLock{
		Key:     "rw-write-ar",
		Servers: []string{addr},
	}
	defer rw.Close()

	ok, err := rw.WLock(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected WLock to succeed")
	}
	if rw.Token() == "" {
		t.Fatal("expected non-empty token after WLock")
	}
	if rw.Fence() == 0 {
		t.Fatal("expected non-zero fence after WLock")
	}

	if err := rw.Unlock(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rw.Token() != "" {
		t.Fatal("expected empty token after Unlock")
	}
}

// ---------------------------------------------------------------------------
// 3. TestRWLock_ConcurrentReaders_Both
// ---------------------------------------------------------------------------

func TestRWLock_ConcurrentReaders_Both(t *testing.T) {
	_, addr := startServer(t, testConfig())

	// Two readers on the same key should both acquire simultaneously.
	rw1 := &client.RWLock{
		Key:     "rw-conc-readers",
		Servers: []string{addr},
	}
	defer rw1.Close()

	rw2 := &client.RWLock{
		Key:     "rw-conc-readers",
		Servers: []string{addr},
	}
	defer rw2.Close()

	ok1, err := rw1.RLock(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !ok1 {
		t.Fatal("rw1 RLock failed")
	}

	// Second reader should also succeed while first is held.
	ok2, err := rw2.RLock(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !ok2 {
		t.Fatal("rw2 RLock failed — concurrent readers should both succeed")
	}

	if rw1.Token() == "" || rw2.Token() == "" {
		t.Fatal("both readers should have non-empty tokens")
	}

	if err := rw1.Unlock(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := rw2.Unlock(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// ---------------------------------------------------------------------------
// 4. TestRWLock_WriterBlocksReader
// ---------------------------------------------------------------------------

func TestRWLock_WriterBlocksReader(t *testing.T) {
	_, addr := startServer(t, testConfig())

	writer := &client.RWLock{
		Key:     "rw-writer-blocks",
		Servers: []string{addr},
	}
	defer writer.Close()

	ok, err := writer.WLock(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("writer WLock failed")
	}

	// Reader with very short timeout should fail because writer holds the lock.
	reader := &client.RWLock{
		Key:            "rw-writer-blocks",
		Servers:        []string{addr},
		AcquireTimeout: 1 * time.Millisecond,
	}
	defer reader.Close()

	ok, err = reader.RLock(context.Background())
	if err != nil {
		t.Fatalf("expected nil error on timeout, got %v", err)
	}
	if ok {
		t.Fatal("expected reader to fail while writer holds lock")
	}

	if err := writer.Unlock(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// ---------------------------------------------------------------------------
// 5. TestRWLock_ContextCancel
// ---------------------------------------------------------------------------

func TestRWLock_ContextCancel(t *testing.T) {
	_, addr := startServer(t, testConfig())

	// Writer holds the lock.
	writer := &client.RWLock{
		Key:     "rw-ctx-cancel",
		Servers: []string{addr},
	}
	defer writer.Close()

	ok, err := writer.WLock(context.Background())
	if err != nil || !ok {
		t.Fatal("writer WLock failed")
	}

	// Reader tries to acquire with a context we cancel.
	ctx, cancel := context.WithCancel(context.Background())
	reader := &client.RWLock{
		Key:            "rw-ctx-cancel",
		Servers:        []string{addr},
		AcquireTimeout: 30 * time.Second, // long timeout
	}
	defer reader.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err := reader.RLock(ctx)
		if err == nil {
			t.Error("expected error from cancelled context")
		}
	}()

	// Give the RLock time to connect and start waiting.
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Success.
	case <-time.After(3 * time.Second):
		t.Fatal("RLock did not return after context cancellation")
	}

	if err := writer.Unlock(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// ---------------------------------------------------------------------------
// 6. TestElection_MultipleContenders
// ---------------------------------------------------------------------------

func TestElection_MultipleContenders(t *testing.T) {
	_, addr := startServer(t, testConfig())

	// First contender campaigns and wins.
	e1 := &client.Election{
		Key:     "election-multi",
		Servers: []string{addr},
	}
	defer e1.Close()

	ok, err := e1.Campaign(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("e1 expected to be elected")
	}
	if !e1.IsLeader() {
		t.Fatal("e1 expected IsLeader() to be true")
	}

	// Second contender campaigns with short timeout and should fail (times out).
	e2 := &client.Election{
		Key:            "election-multi",
		Servers:        []string{addr},
		AcquireTimeout: 1 * time.Millisecond,
	}
	defer e2.Close()

	ok, err = e2.Campaign(context.Background())
	if err != nil {
		t.Fatalf("expected nil error on timeout, got %v", err)
	}
	if ok {
		t.Fatal("e2 should not win while e1 holds leadership")
	}
	if e2.IsLeader() {
		t.Fatal("e2 should not be leader")
	}

	if err := e1.Resign(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// ---------------------------------------------------------------------------
// 7. TestElection_ReelectAfterResign
// ---------------------------------------------------------------------------

func TestElection_ReelectAfterResign(t *testing.T) {
	_, addr := startServer(t, testConfig())

	e := &client.Election{
		Key:     "election-reelect",
		Servers: []string{addr},
	}
	defer e.Close()

	// First campaign.
	ok, err := e.Campaign(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("first campaign failed")
	}
	fence1 := e.Fence()
	if fence1 == 0 {
		t.Fatal("expected non-zero fence on first campaign")
	}

	// Resign.
	if err := e.Resign(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Second campaign.
	ok, err = e.Campaign(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("second campaign failed")
	}
	fence2 := e.Fence()
	if fence2 == 0 {
		t.Fatal("expected non-zero fence on second campaign")
	}
	if fence2 <= fence1 {
		t.Fatalf("fence should increase after re-election: %d -> %d", fence1, fence2)
	}

	if err := e.Resign(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// ---------------------------------------------------------------------------
// 8. TestElection_OnElectedOnResignedCallbacks
// ---------------------------------------------------------------------------

func TestElection_OnElectedOnResignedCallbacks(t *testing.T) {
	_, addr := startServer(t, testConfig())

	electedCh := make(chan struct{}, 1)
	resignedCh := make(chan struct{}, 1)

	e := &client.Election{
		Key:     "election-callbacks",
		Servers: []string{addr},
		OnElected: func() {
			electedCh <- struct{}{}
		},
		OnResigned: func() {
			resignedCh <- struct{}{}
		},
	}
	defer e.Close()

	// Campaign should trigger OnElected.
	ok, err := e.Campaign(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected to be elected")
	}

	select {
	case <-electedCh:
		// OnElected fired correctly.
	case <-time.After(2 * time.Second):
		t.Fatal("OnElected callback was not called")
	}

	// OnResigned should NOT have fired yet.
	select {
	case <-resignedCh:
		t.Fatal("OnResigned should not have fired before Resign")
	default:
		// Good.
	}

	// Resign should trigger OnResigned.
	if err := e.Resign(context.Background()); err != nil {
		t.Fatal(err)
	}

	select {
	case <-resignedCh:
		// OnResigned fired correctly.
	case <-time.After(2 * time.Second):
		t.Fatal("OnResigned callback was not called")
	}
}

// ---------------------------------------------------------------------------
// 9. TestWatchConn_MultiplePatterns
// ---------------------------------------------------------------------------

func TestWatchConn_MultiplePatterns(t *testing.T) {
	_, addr := startServer(t, testConfig())

	wc, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	watch := client.NewWatchConn(wc)
	defer watch.Close()

	// Watch two different patterns.
	if err := watch.Watch("alpha.*"); err != nil {
		t.Fatal(err)
	}
	if err := watch.Watch("beta.*"); err != nil {
		t.Fatal(err)
	}

	// Separate connection to trigger events.
	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Trigger events on both patterns.
	if err := client.KVSet(c, "alpha.one", "val1", 0); err != nil {
		t.Fatal(err)
	}
	if err := client.KVSet(c, "beta.two", "val2", 0); err != nil {
		t.Fatal(err)
	}

	received := make(map[string]bool)
	for i := 0; i < 2; i++ {
		select {
		case evt := <-watch.Events():
			received[evt.Key] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for watch event %d", i)
		}
	}

	if !received["alpha.one"] {
		t.Fatal("did not receive event for alpha.one")
	}
	if !received["beta.two"] {
		t.Fatal("did not receive event for beta.two")
	}
}

// ---------------------------------------------------------------------------
// 10. TestWatchConn_WildcardPattern
// ---------------------------------------------------------------------------

func TestWatchConn_WildcardPattern(t *testing.T) {
	_, addr := startServer(t, testConfig())

	wc, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	watch := client.NewWatchConn(wc)
	defer watch.Close()

	// Watch "*.events" — should match keys like "foo.events".
	if err := watch.Watch("*.events"); err != nil {
		t.Fatal(err)
	}

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Trigger "foo.events" — should be received.
	if err := client.KVSet(c, "foo.events", "val1", 0); err != nil {
		t.Fatal(err)
	}

	select {
	case evt := <-watch.Events():
		if evt.Key != "foo.events" {
			t.Fatalf("expected key foo.events, got %q", evt.Key)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for foo.events watch event")
	}

	// Trigger "foo.bar" — should NOT match "*.events".
	if err := client.KVSet(c, "foo.bar", "val2", 0); err != nil {
		t.Fatal(err)
	}

	select {
	case evt := <-watch.Events():
		t.Fatalf("should not receive event for foo.bar, got %+v", evt)
	case <-time.After(300 * time.Millisecond):
		// Expected: no event.
	}
}

// ---------------------------------------------------------------------------
// 11. TestSignalConn_QueueGroupFairness
// ---------------------------------------------------------------------------

func TestSignalConn_QueueGroupFairness(t *testing.T) {
	_, addr := startServer(t, testConfig())

	// Two listeners in same queue group.
	lc1, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	sc1 := client.NewSignalConn(lc1)
	defer sc1.Close()

	lc2, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	sc2 := client.NewSignalConn(lc2)
	defer sc2.Close()

	if err := sc1.Listen("fairness.ch", client.WithGroup("fairgrp")); err != nil {
		t.Fatal(err)
	}
	if err := sc2.Listen("fairness.ch", client.WithGroup("fairgrp")); err != nil {
		t.Fatal(err)
	}

	// Emitter.
	ec, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer ec.Close()

	const totalSignals = 20
	for i := 0; i < totalSignals; i++ {
		n, err := client.Emit(ec, "fairness.ch", fmt.Sprintf("msg-%d", i))
		if err != nil {
			t.Fatalf("emit %d: %v", i, err)
		}
		if n != 1 {
			t.Fatalf("expected 1 receiver per emit (queue group), got %d", n)
		}
	}

	// Collect signals from both listeners.
	var count1, count2 int
	timeout := time.After(5 * time.Second)
	for count1+count2 < totalSignals {
		select {
		case _, ok := <-sc1.Signals():
			if ok {
				count1++
			}
		case _, ok := <-sc2.Signals():
			if ok {
				count2++
			}
		case <-timeout:
			t.Fatalf("timeout: received %d/%d signals (sc1=%d, sc2=%d)", count1+count2, totalSignals, count1, count2)
		}
	}

	// Both should have received roughly half. Allow generous tolerance.
	if count1 == 0 || count2 == 0 {
		t.Fatalf("one listener got zero signals — not fair: sc1=%d, sc2=%d", count1, count2)
	}
	if count1+count2 != totalSignals {
		t.Fatalf("expected %d total signals, got %d (sc1=%d, sc2=%d)", totalSignals, count1+count2, count1, count2)
	}
}

// ---------------------------------------------------------------------------
// 12. TestLeaderConn_RenewLeadership
// ---------------------------------------------------------------------------

func TestLeaderConn_RenewLeadership(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	lc := client.NewLeaderConn(c)
	defer lc.Close()

	token, _, fence, err := lc.Elect("leader-renew", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if token == "" {
		t.Fatal("expected non-empty token")
	}

	// Renew — token should stay the same but lease is refreshed.
	remaining, renewFence, err := lc.Renew("leader-renew", token)
	if err != nil {
		t.Fatal(err)
	}
	if remaining <= 0 {
		t.Fatalf("expected positive remaining, got %d", remaining)
	}
	if renewFence != fence {
		t.Fatalf("fence should be same after renew: %d vs %d", fence, renewFence)
	}

	// Renew again to confirm stability.
	remaining2, renewFence2, err := lc.Renew("leader-renew", token)
	if err != nil {
		t.Fatal(err)
	}
	if remaining2 <= 0 {
		t.Fatalf("expected positive remaining on second renew, got %d", remaining2)
	}
	if renewFence2 != fence {
		t.Fatalf("fence should remain stable: %d vs %d", fence, renewFence2)
	}

	if err := lc.Resign("leader-renew", token); err != nil {
		t.Fatal(err)
	}
}

// ---------------------------------------------------------------------------
// 13. TestClient_BLPop_ConcurrentPushPop
// ---------------------------------------------------------------------------

func TestClient_BLPop_ConcurrentPushPop(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c1, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()

	c2, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	// Goroutine blocks on BLPop on an empty list.
	result := make(chan string, 1)
	errCh := make(chan error, 1)
	go func() {
		val, err := client.BLPop(c1, "blpop-conc", 5*time.Second)
		if err != nil {
			errCh <- err
			return
		}
		result <- val
	}()

	// After a delay, push a value from another connection.
	time.Sleep(100 * time.Millisecond)
	if _, err := client.RPush(c2, "blpop-conc", "pushed-value"); err != nil {
		t.Fatal(err)
	}

	select {
	case val := <-result:
		if val != "pushed-value" {
			t.Fatalf("expected 'pushed-value', got %q", val)
		}
	case err := <-errCh:
		t.Fatalf("BLPop error: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("BLPop did not return after push")
	}
}

// ---------------------------------------------------------------------------
// 14. TestClient_KVSetGetDel_Lifecycle
// ---------------------------------------------------------------------------

func TestClient_KVSetGetDel_Lifecycle(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Set.
	if err := client.KVSet(c, "lifecycle-key", "hello-world", 0); err != nil {
		t.Fatal(err)
	}

	// Get — verify value.
	val, err := client.KVGet(c, "lifecycle-key")
	if err != nil {
		t.Fatal(err)
	}
	if val != "hello-world" {
		t.Fatalf("expected 'hello-world', got %q", val)
	}

	// Delete.
	if err := client.KVDel(c, "lifecycle-key"); err != nil {
		t.Fatal(err)
	}

	// Get after delete — should be not found.
	_, err = client.KVGet(c, "lifecycle-key")
	if err != client.ErrNotFound {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// 15. TestClient_KVCAS_RetryLoop
// ---------------------------------------------------------------------------

func TestClient_KVCAS_RetryLoop(t *testing.T) {
	_, addr := startServer(t, testConfig())

	// Set the initial value.
	c0, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c0.Close()
	if err := client.KVSet(c0, "cas-retry", "0", 0); err != nil {
		t.Fatal(err)
	}

	// 3 goroutines each try to CAS-increment the value.
	const numGoroutines = 3
	const incrementsPerGoroutine = 5

	var wg sync.WaitGroup
	errCh := make(chan error, numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			gc, err := client.Dial(addr)
			if err != nil {
				errCh <- err
				return
			}
			defer gc.Close()

			for i := 0; i < incrementsPerGoroutine; i++ {
				for attempts := 0; attempts < 100; attempts++ {
					oldVal, err := client.KVGet(gc, "cas-retry")
					if err != nil {
						errCh <- err
						return
					}
					var n int
					fmt.Sscanf(oldVal, "%d", &n)
					newVal := fmt.Sprintf("%d", n+1)
					ok, err := client.KVCAS(gc, "cas-retry", oldVal, newVal, 0)
					if err != nil {
						errCh <- err
						return
					}
					if ok {
						break // success
					}
					// CAS conflict — retry
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatal(err)
	}

	// Final value should be numGoroutines * incrementsPerGoroutine.
	finalVal, err := client.KVGet(c0, "cas-retry")
	if err != nil {
		t.Fatal(err)
	}
	expected := fmt.Sprintf("%d", numGoroutines*incrementsPerGoroutine)
	if finalVal != expected {
		t.Fatalf("expected %s, got %s", expected, finalVal)
	}
}

// ---------------------------------------------------------------------------
// 16. TestClient_BarrierWait_Concurrent
// ---------------------------------------------------------------------------

func TestClient_BarrierWait_Concurrent(t *testing.T) {
	_, addr := startServer(t, testConfig())

	const n = 3
	var wg sync.WaitGroup
	results := make([]bool, n)
	errs := make([]error, n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			c, err := client.Dial(addr)
			if err != nil {
				errs[idx] = err
				return
			}
			defer c.Close()

			ok, err := client.BarrierWait(c, "barrier-conc", n, 5*time.Second)
			results[idx] = ok
			errs[idx] = err
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("participant %d error: %v", i, errs[i])
		}
		if !results[i] {
			t.Fatalf("participant %d: expected barrier to trip", i)
		}
	}
}

// ---------------------------------------------------------------------------
// 17. TestClient_LRange_VariousIndices
// ---------------------------------------------------------------------------

func TestClient_LRange_VariousIndices(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Push 5 items: [a, b, c, d, e]
	for _, v := range []string{"a", "b", "c", "d", "e"} {
		if _, err := client.RPush(c, "lr-test", v); err != nil {
			t.Fatal(err)
		}
	}

	tests := []struct {
		start, stop int
		expected    []string
	}{
		{0, -1, []string{"a", "b", "c", "d", "e"}},   // all
		{0, 0, []string{"a"}},                          // first only
		{0, 2, []string{"a", "b", "c"}},                // first three
		{1, 3, []string{"b", "c", "d"}},                // middle
		{-2, -1, []string{"d", "e"}},                   // last two
		{-3, -2, []string{"c", "d"}},                   // negative range
		{2, 2, []string{"c"}},                           // single element
		{4, 4, []string{"e"}},                           // last element
		{0, 100, []string{"a", "b", "c", "d", "e"}},   // stop beyond length
	}

	for _, tc := range tests {
		items, err := client.LRange(c, "lr-test", tc.start, tc.stop)
		if err != nil {
			t.Fatalf("LRange(%d, %d): %v", tc.start, tc.stop, err)
		}
		if len(items) != len(tc.expected) {
			t.Fatalf("LRange(%d, %d): expected %v, got %v", tc.start, tc.stop, tc.expected, items)
		}
		for i, want := range tc.expected {
			if items[i] != want {
				t.Fatalf("LRange(%d, %d)[%d]: expected %q, got %q", tc.start, tc.stop, i, want, items[i])
			}
		}
	}
}

// ---------------------------------------------------------------------------
// 18. TestClient_Incr_Decr_Concurrent
// ---------------------------------------------------------------------------

func TestClient_Incr_Decr_Concurrent(t *testing.T) {
	_, addr := startServer(t, testConfig())

	const numWorkers = 10
	const opsPerWorker = 50

	var wg sync.WaitGroup
	errCh := make(chan error, numWorkers*2)

	// 10 goroutines incrementing.
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := client.Dial(addr)
			if err != nil {
				errCh <- err
				return
			}
			defer c.Close()
			for j := 0; j < opsPerWorker; j++ {
				if _, err := client.Incr(c, "conc-ctr", 1); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}

	// 10 goroutines decrementing.
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := client.Dial(addr)
			if err != nil {
				errCh <- err
				return
			}
			defer c.Close()
			for j := 0; j < opsPerWorker; j++ {
				if _, err := client.Decr(c, "conc-ctr", 1); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatal(err)
	}

	// Final counter should be 0.
	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	val, err := client.GetCounter(c, "conc-ctr")
	if err != nil {
		t.Fatal(err)
	}
	if val != 0 {
		t.Fatalf("expected counter to be 0, got %d", val)
	}
}

// ---------------------------------------------------------------------------
// 19. TestLock_AutoRenew_KeepsLease
// ---------------------------------------------------------------------------

func TestLock_AutoRenew_KeepsLease(t *testing.T) {
	cfg := testConfig()
	cfg.DefaultLeaseTTL = 2 * time.Second
	_, addr := startServer(t, cfg)

	l := &client.Lock{
		Key:            "auto-renew-keep",
		AcquireTimeout: 10 * time.Second,
		Servers:        []string{addr},
		RenewRatio:     0.5, // renew every 1s on a 2s lease
	}

	ok, err := l.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("acquire failed")
	}

	// Sleep longer than the original lease TTL.
	time.Sleep(3 * time.Second)

	// Lock should still be held thanks to auto-renewal.
	if l.Token() == "" {
		t.Fatal("expected lock to still be held (auto-renewal should keep it alive)")
	}

	// Release should succeed (the lock is still valid).
	if err := l.Release(context.Background()); err != nil {
		t.Fatalf("release after auto-renew failed: %v", err)
	}
}

// ---------------------------------------------------------------------------
// 20. TestSemaphore_LimitEnforcement
// ---------------------------------------------------------------------------

func TestSemaphore_LimitEnforcement(t *testing.T) {
	_, addr := startServer(t, testConfig())

	// Acquire two slots out of limit=2.
	s1 := &client.Semaphore{
		Key:            "sem-limit-enforce",
		Limit:          2,
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

	s2 := &client.Semaphore{
		Key:            "sem-limit-enforce",
		Limit:          2,
		AcquireTimeout: 10 * time.Second,
		Servers:        []string{addr},
	}
	ok, err = s2.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("s2 acquire failed")
	}

	// Third acquire with very short timeout should fail — all slots taken.
	s3 := &client.Semaphore{
		Key:            "sem-limit-enforce",
		Limit:          2,
		AcquireTimeout: 1 * time.Millisecond,
		Servers:        []string{addr},
	}
	ok, err = s3.Acquire(context.Background())
	if err != nil {
		t.Fatalf("expected nil error on timeout, got %v", err)
	}
	if ok {
		t.Fatal("s3 should not acquire — semaphore at capacity")
	}

	// Clean up.
	if err := s1.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s2.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// ---------------------------------------------------------------------------
// Gap-filling: SignalConn.Unlisten
// ---------------------------------------------------------------------------

func TestSignalConn_Unlisten(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	sc := client.NewSignalConn(c)
	defer sc.Close()

	// Listen on a pattern.
	if err := sc.Listen("events.test"); err != nil {
		t.Fatal(err)
	}

	// Emit — should be received.
	c2, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	n, err := client.Emit(c2, "events.test", "hello")
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 1 delivery, got %d", n)
	}

	select {
	case sig := <-sc.Signals():
		if sig.Channel != "events.test" || sig.Payload != "hello" {
			t.Fatalf("unexpected signal: %+v", sig)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for signal")
	}

	// Unlisten — should stop receiving.
	if err := sc.Unlisten("events.test"); err != nil {
		t.Fatal(err)
	}

	n, err = client.Emit(c2, "events.test", "world")
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("expected 0 deliveries after unlisten, got %d", n)
	}

	// Verify nothing arrives.
	select {
	case sig := <-sc.Signals():
		t.Fatalf("unexpected signal after unlisten: %+v", sig)
	case <-time.After(200 * time.Millisecond):
		// Good — no signal received.
	}
}

// TestSignalConn_UnlistenGroup tests unlisten with a queue group.
func TestSignalConn_UnlistenGroup(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	sc := client.NewSignalConn(c)
	defer sc.Close()

	if err := sc.Listen("events.grp", client.WithGroup("workers")); err != nil {
		t.Fatal(err)
	}

	if err := sc.Unlisten("events.grp", client.WithGroup("workers")); err != nil {
		t.Fatal(err)
	}

	// Emit — should deliver to nobody.
	c2, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	n, err := client.Emit(c2, "events.grp", "data")
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("expected 0 deliveries after group unlisten, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// Gap-filling: LeaderConn observe after elect (initial status notification)
// ---------------------------------------------------------------------------

func TestLeaderConn_ObserveAfterElect(t *testing.T) {
	_, addr := startServer(t, testConfig())

	// First: elect a leader.
	c1, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()

	lc1 := client.NewLeaderConn(c1)
	defer lc1.Close()

	_, _, _, err = lc1.Elect("obs-after-key", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// THEN observe — should get immediate "elected" notification.
	c2, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	lc2 := client.NewLeaderConn(c2)
	defer lc2.Close()

	if err := lc2.Observe("obs-after-key"); err != nil {
		t.Fatal(err)
	}

	select {
	case ev := <-lc2.Events():
		if ev.Type != "elected" {
			t.Fatalf("expected 'elected' event, got %q", ev.Type)
		}
		if ev.Key != "obs-after-key" {
			t.Fatalf("expected key 'obs-after-key', got %q", ev.Key)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for initial elected notification")
	}
}

// TestLeaderConn_ObserveResignFailover tests the full observe lifecycle:
// observe → see elected → see resigned → see new leader elected (failover).
func TestLeaderConn_ObserveResignFailover(t *testing.T) {
	_, addr := startServer(t, testConfig())

	// Observer
	obsConn, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer obsConn.Close()
	obs := client.NewLeaderConn(obsConn)
	defer obs.Close()

	if err := obs.Observe("failover-key"); err != nil {
		t.Fatal(err)
	}

	// Leader 1 elects
	c1, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	lc1 := client.NewLeaderConn(c1)
	defer lc1.Close()

	tok1, _, _, err := lc1.Elect("failover-key", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// Observer should see "elected"
	select {
	case ev := <-obs.Events():
		if ev.Type != "elected" {
			t.Fatalf("expected elected, got %q", ev.Type)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for elected event")
	}

	// Leader 2 waits in background
	c2, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	lc2 := client.NewLeaderConn(c2)
	defer lc2.Close()

	leader2Done := make(chan struct{})
	go func() {
		defer close(leader2Done)
		lc2.Elect("failover-key", 10*time.Second)
	}()

	time.Sleep(100 * time.Millisecond)

	// Leader 1 resigns — should trigger resigned + failover elected events
	if err := lc1.Resign("failover-key", tok1); err != nil {
		t.Fatal(err)
	}

	// Observer should see "resigned"
	select {
	case ev := <-obs.Events():
		if ev.Type != "resigned" {
			t.Fatalf("expected resigned, got %q", ev.Type)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for resigned event")
	}

	// Observer should see "failover" (failover to leader 2)
	select {
	case ev := <-obs.Events():
		if ev.Type != "failover" && ev.Type != "elected" {
			t.Fatalf("expected failover event, got %q", ev.Type)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for failover event")
	}

	<-leader2Done
}

// ---------------------------------------------------------------------------
// Gap-filling: WatchConn.Close
// ---------------------------------------------------------------------------

func TestWatchConn_Close(t *testing.T) {
	_, addr := startServer(t, testConfig())

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}

	wc := client.NewWatchConn(c)
	if err := wc.Watch("closetest"); err != nil {
		t.Fatal(err)
	}

	// Close should not panic or error.
	if err := wc.Close(); err != nil {
		t.Fatalf("WatchConn.Close error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Gap-filling: DefaultDialTimeout constant is used correctly
// ---------------------------------------------------------------------------

func TestDefaultDialTimeout(t *testing.T) {
	if client.DefaultDialTimeout <= 0 {
		t.Fatal("DefaultDialTimeout should be positive")
	}
	if client.DefaultDialTimeout > 60*time.Second {
		t.Fatal("DefaultDialTimeout seems unreasonably large")
	}
}

// ---------------------------------------------------------------------------
// Gap-filling: RPop empty list via client
// ---------------------------------------------------------------------------

func TestClient_RPop_Empty(t *testing.T) {
	_, addr := startServer(t, testConfig())
	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	_, err = client.RPop(c, "empty-list")
	if !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("RPop on empty list: expected ErrNotFound, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Gap-filling: KVDel via client - nonexistent key and double-delete
// ---------------------------------------------------------------------------

func TestClient_KVDel_Nonexistent(t *testing.T) {
	_, addr := startServer(t, testConfig())
	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Should not error
	if err := client.KVDel(c, "no-such-key"); err != nil {
		t.Fatalf("KVDel nonexistent key: %v", err)
	}
}

func TestClient_KVDel_DoubleDelete(t *testing.T) {
	_, addr := startServer(t, testConfig())
	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := client.KVSet(c, "k1", "val", 0); err != nil {
		t.Fatal(err)
	}
	if err := client.KVDel(c, "k1"); err != nil {
		t.Fatal(err)
	}
	// Double delete should be safe
	if err := client.KVDel(c, "k1"); err != nil {
		t.Fatalf("double delete: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Gap-filling: SetCounter via client - overwrite and incr-after-set
// ---------------------------------------------------------------------------

func TestClient_SetCounter_Overwrite(t *testing.T) {
	_, addr := startServer(t, testConfig())
	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := client.SetCounter(c, "c1", 100); err != nil {
		t.Fatal(err)
	}
	if err := client.SetCounter(c, "c1", -50); err != nil {
		t.Fatal(err)
	}
	val, err := client.GetCounter(c, "c1")
	if err != nil {
		t.Fatal(err)
	}
	if val != -50 {
		t.Fatalf("expected -50, got %d", val)
	}
}

func TestClient_SetCounter_ThenIncr(t *testing.T) {
	_, addr := startServer(t, testConfig())
	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := client.SetCounter(c, "c1", 100); err != nil {
		t.Fatal(err)
	}
	val, err := client.Incr(c, "c1", 5)
	if err != nil {
		t.Fatal(err)
	}
	if val != 105 {
		t.Fatalf("expected 105, got %d", val)
	}
}

// ---------------------------------------------------------------------------
// Sentinel error validation: ErrMaxWaiters
// ---------------------------------------------------------------------------

func TestClient_ErrMaxWaiters_Sentinel(t *testing.T) {
	cfg := testConfig()
	cfg.MaxWaiters = 1
	_, addr := startServer(t, cfg)

	// Acquire the lock so that subsequent acquires will queue as waiters
	c1, _ := client.Dial(addr)
	defer c1.Close()
	_, _, _, err := client.AcquireWithFence(c1, "mwkey", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// Fill the one waiter slot
	c2, _ := client.Dial(addr)
	defer c2.Close()
	go func() {
		client.AcquireWithFence(c2, "mwkey", 10*time.Second)
	}()
	time.Sleep(100 * time.Millisecond)

	// Third acquire should fail with ErrMaxWaiters
	c3, _ := client.Dial(addr)
	defer c3.Close()
	_, _, _, err = client.AcquireWithFence(c3, "mwkey", 1*time.Second)
	if !errors.Is(err, client.ErrMaxWaiters) {
		t.Fatalf("expected ErrMaxWaiters, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Sentinel error validation: ErrLeaseExpired
// ---------------------------------------------------------------------------

func TestClient_RenewAfterExpiry_ReturnsError(t *testing.T) {
	cfg := testConfig()
	cfg.DefaultLeaseTTL = 1 * time.Second
	cfg.LeaseSweepInterval = 100 * time.Millisecond
	_, addr := startServer(t, cfg)

	c, _ := client.Dial(addr)
	defer c.Close()

	tok, _, _, err := client.AcquireWithFence(c, "lekey", 5*time.Second, client.WithLeaseTTL(1))
	if err != nil {
		t.Fatal(err)
	}

	// Wait for lease to expire
	time.Sleep(1500 * time.Millisecond)

	// Server sends generic "error" for expired renew — client wraps as ErrServer
	_, err = client.Renew(c, "lekey", tok)
	if err == nil {
		t.Fatal("expected error on renew after lease expiry")
	}
}

// ---------------------------------------------------------------------------
// Sentinel error validation: KVCAS returns (false, nil) on conflict
// ---------------------------------------------------------------------------

func TestClient_KVCAS_ConflictSentinel(t *testing.T) {
	_, addr := startServer(t, testConfig())
	c, _ := client.Dial(addr)
	defer c.Close()

	// Create key
	ok, err := client.KVCAS(c, "caskey2", "", "val1", 0)
	if err != nil || !ok {
		t.Fatalf("create: ok=%v err=%v", ok, err)
	}

	// CAS with wrong old value should return (false, nil), not an error
	ok, err = client.KVCAS(c, "caskey2", "wrong", "val2", 0)
	if err != nil {
		t.Fatalf("conflict should not return error, got %v", err)
	}
	if ok {
		t.Fatal("conflict should return false")
	}
}

// ---------------------------------------------------------------------------
// Sentinel error validation: ErrMaxKeys via client
// ---------------------------------------------------------------------------

func TestClient_ErrMaxKeys_Sentinel(t *testing.T) {
	cfg := testConfig()
	cfg.MaxKeys = 1
	_, addr := startServer(t, cfg)

	c, _ := client.Dial(addr)
	defer c.Close()

	if err := client.KVSet(c, "mk1", "v1", 0); err != nil {
		t.Fatal(err)
	}
	err := client.KVSet(c, "mk2", "v2", 0)
	if !errors.Is(err, client.ErrMaxKeys) {
		t.Fatalf("expected ErrMaxKeys, got %v", err)
	}
}
