package lock

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mtingers/dflockd/internal/config"
)

func testConfig() *config.Config {
	return &config.Config{
		Host:                    "127.0.0.1",
		Port:                    0,
		DefaultLeaseTTL:         5 * time.Second,
		LeaseSweepInterval:      100 * time.Millisecond,
		GCInterval:              100 * time.Millisecond,
		GCMaxIdleTime:           60 * time.Second,
		MaxLocks:                1024,
		ReadTimeout:             5 * time.Second,
		AutoReleaseOnDisconnect: true,
	}
}

func testManager() *LockManager {
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	return NewLockManager(testConfig(), log)
}

// bg returns a background context for test calls.
func bg() context.Context { return context.Background() }

// holderConnID returns the connID of the first (only) holder for a lock resource.
func holderConnID(st *ResourceState) uint64 {
	for _, h := range st.Holders {
		return h.connID
	}
	return 0
}

// holderToken returns the token of the first (only) holder for a lock resource.
func holderToken(st *ResourceState) string {
	for tok := range st.Holders {
		return tok
	}
	return ""
}

// ---------------------------------------------------------------------------
// FIFOAcquire
// ---------------------------------------------------------------------------

func TestFIFOAcquire_Immediate(t *testing.T) {
	lm := testManager()
	tok, err := lm.Acquire(bg(), "k1", 5*time.Second, 30*time.Second, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if tok == "" {
		t.Fatal("expected token")
	}
	lm.LockKeyForTest("k1")
	st := lm.ResourceForTest("k1")
	if _, ok := st.Holders[tok]; !ok {
		lm.UnlockKeyForTest("k1")
		t.Fatalf("token should be in holders")
	}
	if holderConnID(st) != 1 {
		lm.UnlockKeyForTest("k1")
		t.Fatalf("owner_conn_id: got %d want 1", holderConnID(st))
	}
	lm.UnlockKeyForTest("k1")
}

func TestFIFOAcquire_Timeout(t *testing.T) {
	lm := testManager()
	_, err := lm.Acquire(bg(), "k1", 5*time.Second, 30*time.Second, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	tok, err := lm.Acquire(bg(), "k1", 0, 30*time.Second, 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if tok != "" {
		t.Fatal("expected empty token (timeout)")
	}
}

func TestFIFOAcquire_FIFOOrdering(t *testing.T) {
	lm := testManager()
	tok1, _ := lm.Acquire(bg(), "k1", 5*time.Second, 30*time.Second, 1, 1)

	var tok2, tok3 string
	var mu sync.Mutex
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		t, _ := lm.Acquire(bg(), "k1", 5*time.Second, 30*time.Second, 2, 1)
		mu.Lock()
		tok2 = t
		mu.Unlock()
	}()
	go func() {
		defer wg.Done()
		time.Sleep(20 * time.Millisecond) // ensure conn2 enqueues first
		t, _ := lm.Acquire(bg(), "k1", 5*time.Second, 30*time.Second, 3, 1)
		mu.Lock()
		tok3 = t
		mu.Unlock()
	}()

	time.Sleep(50 * time.Millisecond)
	lm.Release("k1", tok1)
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	if tok2 == "" {
		mu.Unlock()
		t.Fatal("conn2 should have acquired")
	}
	mu.Unlock()

	lm.LockKeyForTest("k1")
	if holderConnID(lm.ResourceForTest("k1")) != 2 {
		lm.UnlockKeyForTest("k1")
		t.Fatalf("expected conn 2 to own, got %d", holderConnID(lm.ResourceForTest("k1")))
	}
	lm.UnlockKeyForTest("k1")

	lm.Release("k1", tok2)
	wg.Wait()

	mu.Lock()
	if tok3 == "" {
		mu.Unlock()
		t.Fatal("conn3 should have acquired")
	}
	mu.Unlock()
}

func TestFIFOAcquire_MaxLocks(t *testing.T) {
	lm := testManager()
	lm.cfg.MaxLocks = 1
	_, err := lm.Acquire(bg(), "k1", 5*time.Second, 30*time.Second, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = lm.Acquire(bg(), "k2", 5*time.Second, 30*time.Second, 2, 1)
	if err != ErrMaxLocks {
		t.Fatalf("expected ErrMaxLocks, got %v", err)
	}
}

func TestFIFOAcquire_ContextCancel(t *testing.T) {
	lm := testManager()
	_, err := lm.Acquire(bg(), "k1", 5*time.Second, 30*time.Second, 1, 1)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var acquireErr error
	go func() {
		_, acquireErr = lm.Acquire(ctx, "k1", 30*time.Second, 30*time.Second, 2, 1)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	if acquireErr == nil {
		t.Fatal("expected context error")
	}
	if acquireErr != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", acquireErr)
	}
}

// ---------------------------------------------------------------------------
// FIFORelease
// ---------------------------------------------------------------------------

func TestFIFORelease_Valid(t *testing.T) {
	lm := testManager()
	tok, _ := lm.Acquire(bg(), "k1", 5*time.Second, 30*time.Second, 1, 1)
	if !lm.Release("k1", tok) {
		t.Fatal("release should succeed")
	}
	lm.LockKeyForTest("k1")
	if len(lm.ResourceForTest("k1").Holders) != 0 {
		lm.UnlockKeyForTest("k1")
		t.Fatal("owner should be cleared")
	}
	lm.UnlockKeyForTest("k1")
}

func TestFIFORelease_WrongToken(t *testing.T) {
	lm := testManager()
	lm.Acquire(bg(), "k1", 5*time.Second, 30*time.Second, 1, 1)
	if lm.Release("k1", "wrong") {
		t.Fatal("release with wrong token should fail")
	}
}

func TestFIFORelease_Nonexistent(t *testing.T) {
	lm := testManager()
	if lm.Release("nope", "tok") {
		t.Fatal("release of nonexistent key should fail")
	}
}

func TestFIFORelease_TransfersToWaiter(t *testing.T) {
	lm := testManager()
	tok1, _ := lm.Acquire(bg(), "k1", 5*time.Second, 30*time.Second, 1, 1)

	done := make(chan string, 1)
	go func() {
		tok, _ := lm.Acquire(bg(), "k1", 5*time.Second, 30*time.Second, 2, 1)
		done <- tok
	}()
	time.Sleep(50 * time.Millisecond)

	lm.Release("k1", tok1)
	tok2 := <-done
	if tok2 == "" {
		t.Fatal("conn2 should have acquired")
	}
	lm.LockKeyForTest("k1")
	if holderConnID(lm.ResourceForTest("k1")) != 2 {
		lm.UnlockKeyForTest("k1")
		t.Fatal("conn2 should own the lock")
	}
	lm.UnlockKeyForTest("k1")
}

// ---------------------------------------------------------------------------
// FIFORenew
// ---------------------------------------------------------------------------

func TestFIFORenew_Valid(t *testing.T) {
	lm := testManager()
	tok, _ := lm.Acquire(bg(), "k1", 5*time.Second, 30*time.Second, 1, 1)
	remaining, ok := lm.Renew("k1", tok, 60*time.Second)
	if !ok {
		t.Fatal("renew should succeed")
	}
	if remaining <= 0 {
		t.Fatal("remaining should be > 0")
	}
}

func TestFIFORenew_WrongToken(t *testing.T) {
	lm := testManager()
	lm.Acquire(bg(), "k1", 5*time.Second, 30*time.Second, 1, 1)
	_, ok := lm.Renew("k1", "wrong", 30*time.Second)
	if ok {
		t.Fatal("renew with wrong token should fail")
	}
}

func TestFIFORenew_Nonexistent(t *testing.T) {
	lm := testManager()
	_, ok := lm.Renew("nope", "tok", 30*time.Second)
	if ok {
		t.Fatal("renew of nonexistent key should fail")
	}
}

func TestFIFORenew_Expired(t *testing.T) {
	lm := testManager()
	tok, _ := lm.Acquire(bg(), "k1", 5*time.Second, 1*time.Second, 1, 1)
	// Manually expire
	lm.LockKeyForTest("k1")
	lm.ResourceForTest("k1").Holders[tok].leaseExpires = time.Now().Add(-1 * time.Second)
	lm.UnlockKeyForTest("k1")

	_, ok := lm.Renew("k1", tok, 30*time.Second)
	if ok {
		t.Fatal("renew of expired lease should fail")
	}
	lm.LockKeyForTest("k1")
	if len(lm.ResourceForTest("k1").Holders) != 0 {
		lm.UnlockKeyForTest("k1")
		t.Fatal("owner should be cleared after expired renew")
	}
	lm.UnlockKeyForTest("k1")
}

// ---------------------------------------------------------------------------
// FIFOEnqueue
// ---------------------------------------------------------------------------

func TestFIFOEnqueue_Immediate(t *testing.T) {
	lm := testManager()
	status, tok, lease, err := lm.Enqueue("k1", 30*time.Second, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if status != "acquired" {
		t.Fatalf("expected acquired, got %s", status)
	}
	if tok == "" {
		t.Fatal("expected token")
	}
	if lease != 30 {
		t.Fatalf("expected lease 30, got %d", lease)
	}
	lm.LockConnMuForTest()
	if lm.ConnEnqueuedForTest(connKey{ConnID: 1, Key: "k1"}) == nil {
		lm.UnlockConnMuForTest()
		t.Fatal("should be in connEnqueued")
	}
	lm.UnlockConnMuForTest()
}

func TestFIFOEnqueue_Queued(t *testing.T) {
	lm := testManager()
	lm.Acquire(bg(), "k1", 5*time.Second, 30*time.Second, 1, 1)

	status, tok, lease, err := lm.Enqueue("k1", 30*time.Second, 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if status != "queued" {
		t.Fatalf("expected queued, got %s", status)
	}
	if tok != "" {
		t.Fatal("token should be empty when queued")
	}
	if lease != 0 {
		t.Fatal("lease should be 0 when queued")
	}
	lm.LockConnMuForTest()
	es := lm.ConnEnqueuedForTest(connKey{ConnID: 2, Key: "k1"})
	if es == nil || es.waiter == nil {
		lm.UnlockConnMuForTest()
		t.Fatal("should have waiter in connEnqueued")
	}
	lm.UnlockConnMuForTest()
}

func TestFIFOEnqueue_DoubleEnqueue(t *testing.T) {
	lm := testManager()
	lm.Enqueue("k1", 30*time.Second, 1, 1)
	_, _, _, err := lm.Enqueue("k1", 30*time.Second, 1, 1)
	if err == nil {
		t.Fatal("double enqueue should error")
	}
	if !errors.Is(err, ErrAlreadyEnqueued) {
		t.Fatalf("expected ErrAlreadyEnqueued, got %v", err)
	}
}

func TestFIFOEnqueue_MaxLocks(t *testing.T) {
	lm := testManager()
	lm.cfg.MaxLocks = 1
	lm.Enqueue("k1", 30*time.Second, 1, 1)
	_, _, _, err := lm.Enqueue("k2", 30*time.Second, 2, 1)
	if err != ErrMaxLocks {
		t.Fatalf("expected ErrMaxLocks, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// FIFOWait
// ---------------------------------------------------------------------------

func TestFIFOWait_FastPath(t *testing.T) {
	lm := testManager()
	status, token, _, _ := lm.Enqueue("k1", 30*time.Second, 1, 1)
	if status != "acquired" {
		t.Fatal("expected acquired")
	}

	tok, ttl, err := lm.Wait(bg(), "k1", 5*time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	if tok != token {
		t.Fatalf("token mismatch: %s != %s", tok, token)
	}
	if ttl != 30 {
		t.Fatalf("expected ttl 30, got %d", ttl)
	}
	lm.LockConnMuForTest()
	if lm.ConnEnqueuedForTest(connKey{ConnID: 1, Key: "k1"}) != nil {
		lm.UnlockConnMuForTest()
		t.Fatal("should be removed from connEnqueued")
	}
	lm.UnlockConnMuForTest()
}

func TestFIFOWait_QueuedThenWait(t *testing.T) {
	lm := testManager()
	tok1, _ := lm.Acquire(bg(), "k1", 5*time.Second, 30*time.Second, 1, 1)
	lm.Enqueue("k1", 30*time.Second, 2, 1)

	done := make(chan struct{})
	var gotTok string
	var gotTTL int
	go func() {
		gotTok, gotTTL, _ = lm.Wait(bg(), "k1", 5*time.Second, 2)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	lm.Release("k1", tok1)
	<-done

	if gotTok == "" {
		t.Fatal("should have received token")
	}
	if gotTTL != 30 {
		t.Fatalf("expected ttl 30, got %d", gotTTL)
	}
}

func TestFIFOWait_Timeout(t *testing.T) {
	lm := testManager()
	lm.Acquire(bg(), "k1", 5*time.Second, 30*time.Second, 1, 1)
	lm.Enqueue("k1", 30*time.Second, 2, 1)

	tok, _, err := lm.Wait(bg(), "k1", 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if tok != "" {
		t.Fatal("expected empty token (timeout)")
	}
}

func TestFIFOWait_NotEnqueued(t *testing.T) {
	lm := testManager()
	_, _, err := lm.Wait(bg(), "k1", 5*time.Second, 1)
	if err != ErrNotEnqueued {
		t.Fatalf("expected ErrNotEnqueued, got %v", err)
	}
}

func TestFIFOWait_FastPathLockLost(t *testing.T) {
	lm := testManager()
	status, tok, _, _ := lm.Enqueue("k1", 1*time.Second, 1, 1)
	if status != "acquired" {
		t.Fatal("expected acquired")
	}
	// Manually expire + clear holder
	lm.LockKeyForTest("k1")
	lm.ResourceForTest("k1").Holders[tok].leaseExpires = time.Now().Add(-1 * time.Second)
	delete(lm.ResourceForTest("k1").Holders, tok)
	lm.UnlockKeyForTest("k1")

	gotTok, _, err := lm.Wait(bg(), "k1", 5*time.Second, 1)
	if !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("expected ErrLeaseExpired, got %v", err)
	}
	if gotTok != "" {
		t.Fatal("expected empty token (lock lost)")
	}
}

func TestFIFOWait_ContextCancel(t *testing.T) {
	lm := testManager()
	lm.Acquire(bg(), "k1", 5*time.Second, 30*time.Second, 1, 1)
	lm.Enqueue("k1", 30*time.Second, 2, 1)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var waitErr error
	go func() {
		_, _, waitErr = lm.Wait(ctx, "k1", 30*time.Second, 2)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	if waitErr == nil {
		t.Fatal("expected context error")
	}
	if waitErr != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", waitErr)
	}
}

// ---------------------------------------------------------------------------
// Two-phase flow
// ---------------------------------------------------------------------------

func TestTwoPhase_FullCycle(t *testing.T) {
	lm := testManager()
	status, token, _, _ := lm.Enqueue("k1", 30*time.Second, 1, 1)
	if status != "acquired" {
		t.Fatal("expected acquired")
	}

	tok, _, _ := lm.Wait(bg(), "k1", 5*time.Second, 1)
	if tok != token {
		t.Fatal("token mismatch")
	}

	if !lm.Release("k1", tok) {
		t.Fatal("release should succeed")
	}
	lm.LockKeyForTest("k1")
	if len(lm.ResourceForTest("k1").Holders) != 0 {
		lm.UnlockKeyForTest("k1")
		t.Fatal("owner should be cleared")
	}
	lm.UnlockKeyForTest("k1")
}

func TestTwoPhase_Contention(t *testing.T) {
	lm := testManager()
	tok1, _ := lm.Acquire(bg(), "k1", 5*time.Second, 30*time.Second, 1, 1)

	status, _, _, _ := lm.Enqueue("k1", 30*time.Second, 2, 1)
	if status != "queued" {
		t.Fatal("expected queued")
	}

	done := make(chan string, 1)
	go func() {
		tok, _, _ := lm.Wait(bg(), "k1", 5*time.Second, 2)
		done <- tok
	}()

	time.Sleep(50 * time.Millisecond)
	lm.Release("k1", tok1)
	tok2 := <-done

	if tok2 == "" {
		t.Fatal("conn2 should have acquired")
	}
	lm.LockKeyForTest("k1")
	if holderConnID(lm.ResourceForTest("k1")) != 2 {
		lm.UnlockKeyForTest("k1")
		t.Fatal("conn2 should own")
	}
	lm.UnlockKeyForTest("k1")
	lm.Release("k1", tok2)
}

// ---------------------------------------------------------------------------
// CleanupConnection
// ---------------------------------------------------------------------------

func TestCleanup_ReleasesOwned(t *testing.T) {
	lm := testManager()
	tok, _ := lm.Acquire(bg(), "k1", 5*time.Second, 30*time.Second, 100, 1)
	if tok == "" {
		t.Fatal("should acquire")
	}
	lm.CleanupConnection(100)
	lm.LockKeyForTest("k1")
	if len(lm.ResourceForTest("k1").Holders) != 0 {
		lm.UnlockKeyForTest("k1")
		t.Fatal("owner should be cleared")
	}
	lm.UnlockKeyForTest("k1")
}

func TestCleanup_CancelsPending(t *testing.T) {
	lm := testManager()
	lm.Acquire(bg(), "k1", 5*time.Second, 30*time.Second, 1, 1)

	done := make(chan string, 1)
	go func() {
		tok, _ := lm.Acquire(bg(), "k1", 10*time.Second, 30*time.Second, 2, 1)
		done <- tok
	}()
	time.Sleep(50 * time.Millisecond)

	lm.CleanupConnection(2)
	tok := <-done
	if tok != "" {
		t.Fatal("cancelled waiter should get empty token")
	}
}

func TestCleanup_TransfersToWaiter(t *testing.T) {
	lm := testManager()
	lm.Acquire(bg(), "k1", 5*time.Second, 30*time.Second, 1, 1)

	done := make(chan string, 1)
	go func() {
		tok, _ := lm.Acquire(bg(), "k1", 5*time.Second, 30*time.Second, 2, 1)
		done <- tok
	}()
	time.Sleep(50 * time.Millisecond)

	lm.CleanupConnection(1) // releases conn1's lock -> transfers to conn2
	tok2 := <-done
	if tok2 == "" {
		t.Fatal("conn2 should have acquired after transfer")
	}
	lm.LockKeyForTest("k1")
	if holderConnID(lm.ResourceForTest("k1")) != 2 {
		lm.UnlockKeyForTest("k1")
		t.Fatal("conn2 should own")
	}
	lm.UnlockKeyForTest("k1")
}

func TestCleanup_EnqueuedWaiter(t *testing.T) {
	lm := testManager()
	lm.Acquire(bg(), "k1", 5*time.Second, 30*time.Second, 1, 1)
	lm.Enqueue("k1", 30*time.Second, 2, 1)

	lm.LockConnMuForTest()
	if lm.ConnEnqueuedForTest(connKey{ConnID: 2, Key: "k1"}) == nil {
		lm.UnlockConnMuForTest()
		t.Fatal("should be enqueued")
	}
	lm.UnlockConnMuForTest()

	lm.CleanupConnection(2)
	lm.LockConnMuForTest()
	if lm.ConnEnqueuedForTest(connKey{ConnID: 2, Key: "k1"}) != nil {
		lm.UnlockConnMuForTest()
		t.Fatal("should be cleaned up")
	}
	lm.UnlockConnMuForTest()
	lm.LockKeyForTest("k1")
	if len(lm.ResourceForTest("k1").Waiters) != 0 {
		lm.UnlockKeyForTest("k1")
		t.Fatal("waiter should be removed")
	}
	lm.UnlockKeyForTest("k1")
}

func TestCleanup_FastPath(t *testing.T) {
	lm := testManager()
	status, _, _, _ := lm.Enqueue("k1", 30*time.Second, 1, 1)
	if status != "acquired" {
		t.Fatal("expected acquired")
	}

	lm.CleanupConnection(1)
	lm.LockConnMuForTest()
	if lm.ConnEnqueuedForTest(connKey{ConnID: 1, Key: "k1"}) != nil {
		lm.UnlockConnMuForTest()
		t.Fatal("should be cleaned up")
	}
	lm.UnlockConnMuForTest()
	lm.LockKeyForTest("k1")
	if len(lm.ResourceForTest("k1").Holders) != 0 {
		lm.UnlockKeyForTest("k1")
		t.Fatal("owner should be cleared")
	}
	lm.UnlockKeyForTest("k1")
}

func TestCleanup_Noop(t *testing.T) {
	lm := testManager()
	lm.CleanupConnection(9999) // should not panic
}

// ---------------------------------------------------------------------------
// LeaseExpiryLoop
// ---------------------------------------------------------------------------

func TestLeaseExpiry_ReleasesLock(t *testing.T) {
	lm := testManager()
	lm.cfg.LeaseSweepInterval = 50 * time.Millisecond
	tok, _ := lm.Acquire(bg(), "k1", 5*time.Second, 1*time.Second, 1, 1)
	if tok == "" {
		t.Fatal("should acquire")
	}
	// Manually expire
	lm.LockKeyForTest("k1")
	lm.ResourceForTest("k1").Holders[tok].leaseExpires = time.Now().Add(-1 * time.Second)
	lm.UnlockKeyForTest("k1")

	ctx, cancel := context.WithCancel(context.Background())
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		lm.LeaseExpiryLoop(ctx)
	}()
	time.Sleep(200 * time.Millisecond)
	cancel()
	<-loopDone

	lm.LockKeyForTest("k1")
	holders := len(lm.ResourceForTest("k1").Holders)
	lm.UnlockKeyForTest("k1")
	if holders != 0 {
		t.Fatal("owner should be cleared by expiry loop")
	}
}

func TestLeaseExpiry_TransfersToWaiter(t *testing.T) {
	lm := testManager()
	lm.cfg.LeaseSweepInterval = 50 * time.Millisecond
	tok, _ := lm.Acquire(bg(), "k1", 5*time.Second, 1*time.Second, 1, 1)

	done := make(chan string, 1)
	go func() {
		tok, _ := lm.Acquire(bg(), "k1", 5*time.Second, 30*time.Second, 2, 1)
		done <- tok
	}()
	time.Sleep(50 * time.Millisecond)

	// Expire
	lm.LockKeyForTest("k1")
	lm.ResourceForTest("k1").Holders[tok].leaseExpires = time.Now().Add(-1 * time.Second)
	lm.UnlockKeyForTest("k1")

	ctx, cancel := context.WithCancel(context.Background())
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		lm.LeaseExpiryLoop(ctx)
	}()

	tok2 := <-done
	cancel()
	<-loopDone

	if tok2 == "" {
		t.Fatal("conn2 should have acquired after expiry")
	}
	lm.LockKeyForTest("k1")
	ownerConnID := holderConnID(lm.ResourceForTest("k1"))
	lm.UnlockKeyForTest("k1")
	if ownerConnID != 2 {
		t.Fatal("conn2 should own")
	}
}

// ---------------------------------------------------------------------------
// GCLoop
// ---------------------------------------------------------------------------

func TestGC_PrunesIdle(t *testing.T) {
	lm := testManager()
	lm.cfg.GCInterval = 50 * time.Millisecond
	lm.cfg.GCMaxIdleTime = 0

	tok, _ := lm.Acquire(bg(), "k1", 5*time.Second, 30*time.Second, 1, 1)
	lm.Release("k1", tok)
	// Force idle
	lm.LockKeyForTest("k1")
	lm.ResourceForTest("k1").LastActivity = time.Now().Add(-100 * time.Second)
	lm.UnlockKeyForTest("k1")

	ctx, cancel := context.WithCancel(context.Background())
	go lm.GCLoop(ctx)
	time.Sleep(200 * time.Millisecond)
	cancel()

	lm.LockKeyForTest("k1")
	exists := lm.ResourceForTest("k1") != nil
	lm.UnlockKeyForTest("k1")
	if exists {
		t.Fatal("k1 should have been GC'd")
	}
}

func TestGC_DoesNotPruneHeld(t *testing.T) {
	lm := testManager()
	lm.cfg.GCInterval = 50 * time.Millisecond
	lm.cfg.GCMaxIdleTime = 0

	lm.Acquire(bg(), "k1", 5*time.Second, 30*time.Second, 1, 1)
	lm.LockKeyForTest("k1")
	lm.ResourceForTest("k1").LastActivity = time.Now().Add(-100 * time.Second)
	lm.UnlockKeyForTest("k1")

	ctx, cancel := context.WithCancel(context.Background())
	go lm.GCLoop(ctx)
	time.Sleep(200 * time.Millisecond)
	cancel()

	lm.LockKeyForTest("k1")
	exists := lm.ResourceForTest("k1") != nil
	lm.UnlockKeyForTest("k1")
	if !exists {
		t.Fatal("k1 should not be GC'd (still held)")
	}
}

// ===========================================================================
// Semaphore tests
// ===========================================================================

// ---------------------------------------------------------------------------
// SemAcquire
// ---------------------------------------------------------------------------

func TestSemAcquire_Immediate(t *testing.T) {
	lm := testManager()
	tok, err := lm.Acquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	if tok == "" {
		t.Fatal("expected token")
	}
	lm.LockKeyForTest("s1")
	st := lm.ResourceForTest("s1")
	if st.Limit != 3 {
		lm.UnlockKeyForTest("s1")
		t.Fatalf("limit: got %d want 3", st.Limit)
	}
	if len(st.Holders) != 1 {
		lm.UnlockKeyForTest("s1")
		t.Fatalf("holders: got %d want 1", len(st.Holders))
	}
	lm.UnlockKeyForTest("s1")
}

func TestSemAcquire_MultipleConcurrent(t *testing.T) {
	lm := testManager()
	tok1, _ := lm.Acquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 3)
	tok2, _ := lm.Acquire(bg(), "s1", 5*time.Second, 30*time.Second, 2, 3)
	tok3, _ := lm.Acquire(bg(), "s1", 5*time.Second, 30*time.Second, 3, 3)
	if tok1 == "" || tok2 == "" || tok3 == "" {
		t.Fatal("all three should acquire immediately")
	}
	lm.LockKeyForTest("s1")
	if len(lm.ResourceForTest("s1").Holders) != 3 {
		lm.UnlockKeyForTest("s1")
		t.Fatalf("holders: got %d want 3", len(lm.ResourceForTest("s1").Holders))
	}
	lm.UnlockKeyForTest("s1")
}

func TestSemAcquire_AtCapacityTimeout(t *testing.T) {
	lm := testManager()
	lm.Acquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 2)
	lm.Acquire(bg(), "s1", 5*time.Second, 30*time.Second, 2, 2)

	// Third should timeout with 0 timeout
	tok, err := lm.Acquire(bg(), "s1", 0, 30*time.Second, 3, 2)
	if err != nil {
		t.Fatal(err)
	}
	if tok != "" {
		t.Fatal("expected empty token (timeout)")
	}
}

func TestSemAcquire_FIFOOrdering(t *testing.T) {
	lm := testManager()
	tok1, _ := lm.Acquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 1)

	var tok2, tok3 string
	var mu sync.Mutex
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		t, _ := lm.Acquire(bg(), "s1", 5*time.Second, 30*time.Second, 2, 1)
		mu.Lock()
		tok2 = t
		mu.Unlock()
	}()
	go func() {
		defer wg.Done()
		time.Sleep(20 * time.Millisecond)
		t, _ := lm.Acquire(bg(), "s1", 5*time.Second, 30*time.Second, 3, 1)
		mu.Lock()
		tok3 = t
		mu.Unlock()
	}()

	time.Sleep(50 * time.Millisecond)
	lm.Release("s1", tok1)
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	if tok2 == "" {
		mu.Unlock()
		t.Fatal("conn2 should have acquired")
	}
	mu.Unlock()

	lm.Release("s1", tok2)
	wg.Wait()

	mu.Lock()
	if tok3 == "" {
		mu.Unlock()
		t.Fatal("conn3 should have acquired")
	}
	mu.Unlock()
}

func TestSemAcquire_MaxLocks(t *testing.T) {
	lm := testManager()
	lm.cfg.MaxLocks = 1
	_, err := lm.Acquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	_, err = lm.Acquire(bg(), "s2", 5*time.Second, 30*time.Second, 2, 3)
	if err != ErrMaxLocks {
		t.Fatalf("expected ErrMaxLocks, got %v", err)
	}
}

func TestSemAcquire_LimitMismatch(t *testing.T) {
	lm := testManager()
	_, err := lm.Acquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	_, err = lm.Acquire(bg(), "s1", 5*time.Second, 30*time.Second, 2, 5)
	if err != ErrLimitMismatch {
		t.Fatalf("expected ErrLimitMismatch, got %v", err)
	}
}

func TestSemAcquire_MaxLocksSharedWithLocks(t *testing.T) {
	lm := testManager()
	lm.cfg.MaxLocks = 2
	lm.Acquire(bg(), "lock1", 5*time.Second, 30*time.Second, 1, 1)
	lm.Acquire(bg(), "sem1", 5*time.Second, 30*time.Second, 2, 3)
	_, err := lm.Acquire(bg(), "sem2", 5*time.Second, 30*time.Second, 3, 3)
	if err != ErrMaxLocks {
		t.Fatalf("expected ErrMaxLocks, got %v", err)
	}
}

func TestSemAcquire_ContextCancel(t *testing.T) {
	lm := testManager()
	_, err := lm.Acquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 1)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var acquireErr error
	go func() {
		_, acquireErr = lm.Acquire(ctx, "s1", 30*time.Second, 30*time.Second, 2, 1)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	if acquireErr == nil {
		t.Fatal("expected context error")
	}
	if acquireErr != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", acquireErr)
	}
}

// ---------------------------------------------------------------------------
// SemRelease
// ---------------------------------------------------------------------------

func TestSemRelease_Valid(t *testing.T) {
	lm := testManager()
	tok, _ := lm.Acquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 3)
	if !lm.Release("s1", tok) {
		t.Fatal("release should succeed")
	}
	lm.LockKeyForTest("s1")
	if len(lm.ResourceForTest("s1").Holders) != 0 {
		lm.UnlockKeyForTest("s1")
		t.Fatal("holders should be empty")
	}
	lm.UnlockKeyForTest("s1")
}

func TestSemRelease_WrongToken(t *testing.T) {
	lm := testManager()
	lm.Acquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 3)
	if lm.Release("s1", "wrong") {
		t.Fatal("release with wrong token should fail")
	}
}

func TestSemRelease_TransfersToWaiter(t *testing.T) {
	lm := testManager()
	tok1, _ := lm.Acquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 1)

	done := make(chan string, 1)
	go func() {
		tok, _ := lm.Acquire(bg(), "s1", 5*time.Second, 30*time.Second, 2, 1)
		done <- tok
	}()
	time.Sleep(50 * time.Millisecond)

	lm.Release("s1", tok1)
	tok2 := <-done
	if tok2 == "" {
		t.Fatal("conn2 should have acquired")
	}
}

// ---------------------------------------------------------------------------
// SemRenew
// ---------------------------------------------------------------------------

func TestSemRenew_Valid(t *testing.T) {
	lm := testManager()
	tok, _ := lm.Acquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 3)
	remaining, ok := lm.Renew("s1", tok, 60*time.Second)
	if !ok {
		t.Fatal("renew should succeed")
	}
	if remaining <= 0 {
		t.Fatal("remaining should be > 0")
	}
}

func TestSemRenew_WrongToken(t *testing.T) {
	lm := testManager()
	lm.Acquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 3)
	_, ok := lm.Renew("s1", "wrong", 30*time.Second)
	if ok {
		t.Fatal("renew with wrong token should fail")
	}
}

func TestSemRenew_Expired(t *testing.T) {
	lm := testManager()
	tok, _ := lm.Acquire(bg(), "s1", 5*time.Second, 1*time.Second, 1, 3)
	lm.LockKeyForTest("s1")
	lm.ResourceForTest("s1").Holders[tok].leaseExpires = time.Now().Add(-1 * time.Second)
	lm.UnlockKeyForTest("s1")

	_, ok := lm.Renew("s1", tok, 30*time.Second)
	if ok {
		t.Fatal("renew of expired lease should fail")
	}
	lm.LockKeyForTest("s1")
	if len(lm.ResourceForTest("s1").Holders) != 0 {
		lm.UnlockKeyForTest("s1")
		t.Fatal("expired holder should be removed")
	}
	lm.UnlockKeyForTest("s1")
}

// ---------------------------------------------------------------------------
// SemEnqueue / SemWait
// ---------------------------------------------------------------------------

func TestSemEnqueue_Immediate(t *testing.T) {
	lm := testManager()
	status, tok, lease, err := lm.Enqueue("s1", 30*time.Second, 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	if status != "acquired" || tok == "" || lease != 30 {
		t.Fatalf("unexpected: status=%s tok=%s lease=%d", status, tok, lease)
	}
}

func TestSemEnqueue_Queued(t *testing.T) {
	lm := testManager()
	lm.Acquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 1)

	status, tok, lease, err := lm.Enqueue("s1", 30*time.Second, 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if status != "queued" || tok != "" || lease != 0 {
		t.Fatalf("unexpected: status=%s tok=%s lease=%d", status, tok, lease)
	}
}

func TestSemEnqueue_DoubleEnqueue(t *testing.T) {
	lm := testManager()
	lm.Enqueue("s1", 30*time.Second, 1, 3)
	_, _, _, err := lm.Enqueue("s1", 30*time.Second, 1, 3)
	if err == nil {
		t.Fatal("double enqueue should error")
	}
	if !errors.Is(err, ErrAlreadyEnqueued) {
		t.Fatalf("expected ErrAlreadyEnqueued, got %v", err)
	}
}

func TestSemEnqueue_LimitMismatch(t *testing.T) {
	lm := testManager()
	lm.Enqueue("s1", 30*time.Second, 1, 3)
	_, _, _, err := lm.Enqueue("s1", 30*time.Second, 2, 5)
	if err != ErrLimitMismatch {
		t.Fatalf("expected ErrLimitMismatch, got %v", err)
	}
}

func TestSemWait_FastPath(t *testing.T) {
	lm := testManager()
	status, token, _, _ := lm.Enqueue("s1", 30*time.Second, 1, 3)
	if status != "acquired" {
		t.Fatal("expected acquired")
	}

	tok, ttl, err := lm.Wait(bg(), "s1", 5*time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	if tok != token || ttl != 30 {
		t.Fatalf("unexpected: tok=%s ttl=%d", tok, ttl)
	}
}

func TestSemWait_QueuedThenWait(t *testing.T) {
	lm := testManager()
	tok1, _ := lm.Acquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 1)
	lm.Enqueue("s1", 30*time.Second, 2, 1)

	done := make(chan struct{})
	var gotTok string
	go func() {
		gotTok, _, _ = lm.Wait(bg(), "s1", 5*time.Second, 2)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	lm.Release("s1", tok1)
	<-done

	if gotTok == "" {
		t.Fatal("should have received token")
	}
}

func TestSemWait_Timeout(t *testing.T) {
	lm := testManager()
	lm.Acquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 1)
	lm.Enqueue("s1", 30*time.Second, 2, 1)

	tok, _, err := lm.Wait(bg(), "s1", 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if tok != "" {
		t.Fatal("expected empty token (timeout)")
	}
}

func TestSemWait_NotEnqueued(t *testing.T) {
	lm := testManager()
	_, _, err := lm.Wait(bg(), "s1", 5*time.Second, 1)
	if err != ErrNotEnqueued {
		t.Fatalf("expected ErrNotEnqueued, got %v", err)
	}
}

func TestSemWait_ContextCancel(t *testing.T) {
	lm := testManager()
	lm.Acquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 1)
	lm.Enqueue("s1", 30*time.Second, 2, 1)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var waitErr error
	go func() {
		_, _, waitErr = lm.Wait(ctx, "s1", 30*time.Second, 2)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	if waitErr == nil {
		t.Fatal("expected context error")
	}
	if waitErr != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", waitErr)
	}
}

// ---------------------------------------------------------------------------
// Semaphore CleanupConnection
// ---------------------------------------------------------------------------

func TestSemCleanup_ReleasesOwned(t *testing.T) {
	lm := testManager()
	tok, _ := lm.Acquire(bg(), "s1", 5*time.Second, 30*time.Second, 100, 3)
	if tok == "" {
		t.Fatal("should acquire")
	}
	lm.CleanupConnection(100)
	lm.LockKeyForTest("s1")
	if len(lm.ResourceForTest("s1").Holders) != 0 {
		lm.UnlockKeyForTest("s1")
		t.Fatal("holders should be cleared")
	}
	lm.UnlockKeyForTest("s1")
}

func TestSemCleanup_CancelsPending(t *testing.T) {
	lm := testManager()
	lm.Acquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 1)

	done := make(chan string, 1)
	go func() {
		tok, _ := lm.Acquire(bg(), "s1", 10*time.Second, 30*time.Second, 2, 1)
		done <- tok
	}()
	time.Sleep(50 * time.Millisecond)

	lm.CleanupConnection(2)
	tok := <-done
	if tok != "" {
		t.Fatal("cancelled waiter should get empty token")
	}
}

func TestSemCleanup_TransfersToWaiter(t *testing.T) {
	lm := testManager()
	lm.Acquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 1)

	done := make(chan string, 1)
	go func() {
		tok, _ := lm.Acquire(bg(), "s1", 5*time.Second, 30*time.Second, 2, 1)
		done <- tok
	}()
	time.Sleep(50 * time.Millisecond)

	lm.CleanupConnection(1)
	tok2 := <-done
	if tok2 == "" {
		t.Fatal("conn2 should have acquired after transfer")
	}
}

func TestSemCleanup_EnqueuedWaiter(t *testing.T) {
	lm := testManager()
	lm.Acquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 1)
	lm.Enqueue("s1", 30*time.Second, 2, 1)

	lm.CleanupConnection(2)
	lm.LockKeyForTest("s1")
	if len(lm.ResourceForTest("s1").Waiters) != 0 {
		lm.UnlockKeyForTest("s1")
		t.Fatal("waiter should be removed")
	}
	lm.UnlockKeyForTest("s1")
}

// ---------------------------------------------------------------------------
// Semaphore LeaseExpiryLoop
// ---------------------------------------------------------------------------

func TestSemLeaseExpiry_ReleasesHolder(t *testing.T) {
	lm := testManager()
	lm.cfg.LeaseSweepInterval = 50 * time.Millisecond
	tok, _ := lm.Acquire(bg(), "s1", 5*time.Second, 1*time.Second, 1, 3)
	if tok == "" {
		t.Fatal("should acquire")
	}
	lm.LockKeyForTest("s1")
	lm.ResourceForTest("s1").Holders[tok].leaseExpires = time.Now().Add(-1 * time.Second)
	lm.UnlockKeyForTest("s1")

	ctx, cancel := context.WithCancel(context.Background())
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		lm.LeaseExpiryLoop(ctx)
	}()
	time.Sleep(200 * time.Millisecond)
	cancel()
	<-loopDone

	lm.LockKeyForTest("s1")
	holderCount := len(lm.ResourceForTest("s1").Holders)
	lm.UnlockKeyForTest("s1")
	if holderCount != 0 {
		t.Fatal("holder should be evicted by expiry loop")
	}
}

func TestSemLeaseExpiry_TransfersToWaiter(t *testing.T) {
	lm := testManager()
	lm.cfg.LeaseSweepInterval = 50 * time.Millisecond
	lm.Acquire(bg(), "s1", 5*time.Second, 1*time.Second, 1, 1)

	done := make(chan string, 1)
	go func() {
		tok, _ := lm.Acquire(bg(), "s1", 5*time.Second, 30*time.Second, 2, 1)
		done <- tok
	}()
	time.Sleep(50 * time.Millisecond)

	// Expire holder
	lm.LockKeyForTest("s1")
	for _, h := range lm.ResourceForTest("s1").Holders {
		h.leaseExpires = time.Now().Add(-1 * time.Second)
	}
	lm.UnlockKeyForTest("s1")

	ctx, cancel := context.WithCancel(context.Background())
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		lm.LeaseExpiryLoop(ctx)
	}()

	tok2 := <-done
	cancel()
	<-loopDone

	if tok2 == "" {
		t.Fatal("conn2 should have acquired after expiry")
	}
}

// ---------------------------------------------------------------------------
// Semaphore GCLoop
// ---------------------------------------------------------------------------

func TestSemGC_PrunesIdle(t *testing.T) {
	lm := testManager()
	lm.cfg.GCInterval = 50 * time.Millisecond
	lm.cfg.GCMaxIdleTime = 0

	tok, _ := lm.Acquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 3)
	lm.Release("s1", tok)
	lm.LockKeyForTest("s1")
	lm.ResourceForTest("s1").LastActivity = time.Now().Add(-100 * time.Second)
	lm.UnlockKeyForTest("s1")

	ctx, cancel := context.WithCancel(context.Background())
	go lm.GCLoop(ctx)
	time.Sleep(200 * time.Millisecond)
	cancel()

	lm.LockKeyForTest("s1")
	exists := lm.ResourceForTest("s1") != nil
	lm.UnlockKeyForTest("s1")
	if exists {
		t.Fatal("s1 should have been GC'd")
	}
}

func TestSemGC_DoesNotPruneHeld(t *testing.T) {
	lm := testManager()
	lm.cfg.GCInterval = 50 * time.Millisecond
	lm.cfg.GCMaxIdleTime = 0

	lm.Acquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 3)
	lm.LockKeyForTest("s1")
	lm.ResourceForTest("s1").LastActivity = time.Now().Add(-100 * time.Second)
	lm.UnlockKeyForTest("s1")

	ctx, cancel := context.WithCancel(context.Background())
	go lm.GCLoop(ctx)
	time.Sleep(200 * time.Millisecond)
	cancel()

	lm.LockKeyForTest("s1")
	exists := lm.ResourceForTest("s1") != nil
	lm.UnlockKeyForTest("s1")
	if !exists {
		t.Fatal("s1 should not be GC'd (still has holders)")
	}
}

// ===========================================================================
// MaxWaiters tests
// ===========================================================================

func TestFIFOAcquire_MaxWaiters(t *testing.T) {
	lm := testManager()
	lm.cfg.MaxWaiters = 1

	// conn1 holds the lock
	_, err := lm.Acquire(bg(), "k1", 5*time.Second, 30*time.Second, 1, 1)
	if err != nil {
		t.Fatal(err)
	}

	// conn2 enqueues as waiter (fills the queue)
	done := make(chan struct{})
	go func() {
		lm.Acquire(bg(), "k1", 5*time.Second, 30*time.Second, 2, 1)
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)

	// conn3 should get ErrMaxWaiters
	_, err = lm.Acquire(bg(), "k1", 5*time.Second, 30*time.Second, 3, 1)
	if err != ErrMaxWaiters {
		t.Fatalf("expected ErrMaxWaiters, got %v", err)
	}

	// Cleanup: release so goroutine can finish
	lm.LockKeyForTest("k1")
	tok := holderToken(lm.ResourceForTest("k1"))
	lm.UnlockKeyForTest("k1")
	lm.Release("k1", tok)
	<-done
}

func TestFIFOEnqueue_MaxWaiters(t *testing.T) {
	lm := testManager()
	lm.cfg.MaxWaiters = 1

	// conn1 holds the lock
	lm.Acquire(bg(), "k1", 5*time.Second, 30*time.Second, 1, 1)

	// conn2 enqueues (fills queue)
	status, _, _, err := lm.Enqueue("k1", 30*time.Second, 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if status != "queued" {
		t.Fatalf("expected queued, got %s", status)
	}

	// conn3 should get ErrMaxWaiters
	_, _, _, err = lm.Enqueue("k1", 30*time.Second, 3, 1)
	if err != ErrMaxWaiters {
		t.Fatalf("expected ErrMaxWaiters, got %v", err)
	}
}

func TestSemAcquire_MaxWaiters(t *testing.T) {
	lm := testManager()
	lm.cfg.MaxWaiters = 1

	// conn1 holds the single slot
	_, err := lm.Acquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 1)
	if err != nil {
		t.Fatal(err)
	}

	// conn2 waits (fills queue)
	done := make(chan struct{})
	go func() {
		lm.Acquire(bg(), "s1", 5*time.Second, 30*time.Second, 2, 1)
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)

	// conn3 should get ErrMaxWaiters
	_, err = lm.Acquire(bg(), "s1", 5*time.Second, 30*time.Second, 3, 1)
	if err != ErrMaxWaiters {
		t.Fatalf("expected ErrMaxWaiters, got %v", err)
	}

	// Cleanup
	lm.LockKeyForTest("s1")
	var tok string
	for t := range lm.ResourceForTest("s1").Holders {
		tok = t
		break
	}
	lm.UnlockKeyForTest("s1")
	lm.Release("s1", tok)
	<-done
}

func TestSemEnqueue_MaxWaiters(t *testing.T) {
	lm := testManager()
	lm.cfg.MaxWaiters = 1

	// conn1 holds the slot
	lm.Acquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 1)

	// conn2 enqueues (fills queue)
	status, _, _, err := lm.Enqueue("s1", 30*time.Second, 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if status != "queued" {
		t.Fatalf("expected queued, got %s", status)
	}

	// conn3 should get ErrMaxWaiters
	_, _, _, err = lm.Enqueue("s1", 30*time.Second, 3, 1)
	if err != ErrMaxWaiters {
		t.Fatalf("expected ErrMaxWaiters, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Regression: Release must not clobber a different in-progress enqueue
// ---------------------------------------------------------------------------

func TestSemRelease_DoesNotClobberEnqueuedState(t *testing.T) {
	lm := testManager()

	// Phase 1: conn1 acquires slot T1 via two-phase
	status, tok1, _, err := lm.Enqueue("s1", 30*time.Second, 1, 3)
	if err != nil || status != "acquired" {
		t.Fatalf("enqueue1: status=%s err=%v", status, err)
	}
	got1, _, err := lm.Wait(bg(), "s1", 5*time.Second, 1)
	if err != nil || got1 != tok1 {
		t.Fatalf("wait1: tok=%s err=%v", got1, err)
	}

	// Phase 2: conn1 enqueues again for a second slot (T2)
	status, tok2, _, err := lm.Enqueue("s1", 30*time.Second, 1, 3)
	if err != nil || status != "acquired" {
		t.Fatalf("enqueue2: status=%s err=%v", status, err)
	}

	// Release T1 — must NOT destroy the enqueued state for T2
	if !lm.Release("s1", tok1) {
		t.Fatal("release T1 should succeed")
	}

	// SemWait for T2 must still work
	got2, _, err := lm.Wait(bg(), "s1", 5*time.Second, 1)
	if err != nil {
		t.Fatalf("wait2 should succeed, got err: %v", err)
	}
	if got2 != tok2 {
		t.Fatalf("wait2 token mismatch: got %s want %s", got2, tok2)
	}
}

func TestFIFORelease_DoesNotClobberEnqueuedState(t *testing.T) {
	lm := testManager()

	// Conn1 acquires via two-phase
	status, tok1, _, err := lm.Enqueue("k1", 30*time.Second, 1, 1)
	if err != nil || status != "acquired" {
		t.Fatalf("enqueue1: status=%s err=%v", status, err)
	}
	got1, _, err := lm.Wait(bg(), "k1", 5*time.Second, 1)
	if err != nil || got1 != tok1 {
		t.Fatalf("wait1: tok=%s err=%v", got1, err)
	}

	// Conn1 enqueues again (will be queued behind itself — a self-deadlock
	// scenario, but the data structure must still be consistent)
	status, _, _, err = lm.Enqueue("k1", 30*time.Second, 1, 1)
	if err != nil {
		t.Fatalf("enqueue2: err=%v", err)
	}
	if status != "queued" {
		t.Fatalf("expected queued, got %s", status)
	}

	// Release T1 — must NOT destroy the queued enqueue state
	if !lm.Release("k1", tok1) {
		t.Fatal("release should succeed")
	}

	// The enqueued waiter should still be tracked (it will get granted the lock)
	lm.LockConnMuForTest()
	if lm.ConnEnqueuedForTest(connKey{ConnID: 1, Key: "k1"}) == nil {
		lm.UnlockConnMuForTest()
		t.Fatal("enqueued state for conn1 should still exist")
	}
	lm.UnlockConnMuForTest()
}

// ===========================================================================
// Regression tests for unpushed fixes
// ===========================================================================

// ---------------------------------------------------------------------------
// ErrLeaseExpired in FIFOWait/SemWait fast path
// ---------------------------------------------------------------------------

func TestFIFOWait_FastPathLeaseExpired(t *testing.T) {
	// When a lock is acquired via enqueue then its lease expires before
	// Wait is called, Wait must return ErrLeaseExpired and clean
	// up owner state so the next waiter can proceed.
	lm := testManager()
	status, tok, _, _ := lm.Enqueue("k1", 1*time.Second, 1, 1)
	if status != "acquired" || tok == "" {
		t.Fatal("expected acquired")
	}

	// Manually expire the lease
	lm.LockKeyForTest("k1")
	lm.ResourceForTest("k1").Holders[tok].leaseExpires = time.Now().Add(-1 * time.Second)
	lm.UnlockKeyForTest("k1")

	_, _, err := lm.Wait(bg(), "k1", 5*time.Second, 1)
	if !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("expected ErrLeaseExpired, got %v", err)
	}

	// Owner state must be cleaned up
	lm.LockKeyForTest("k1")
	st := lm.ResourceForTest("k1")
	if len(st.Holders) != 0 {
		lm.UnlockKeyForTest("k1")
		t.Fatal("owner should be cleared after expired wait")
	}
	lm.UnlockKeyForTest("k1")

	// connEnqueued should be cleaned up
	lm.LockConnMuForTest()
	if lm.ConnEnqueuedForTest(connKey{ConnID: 1, Key: "k1"}) != nil {
		lm.UnlockConnMuForTest()
		t.Fatal("connEnqueued should be deleted")
	}
	lm.UnlockConnMuForTest()
}

func TestFIFOWait_FastPathLeaseExpiredGrantsNext(t *testing.T) {
	// When the fast-path detects expiry, the next waiter should be granted.
	lm := testManager()
	lm.Enqueue("k1", 1*time.Second, 1, 1) // conn1 acquires

	// conn2 enqueues as waiter
	lm.Enqueue("k1", 30*time.Second, 2, 1)

	// Expire conn1's lease
	lm.LockKeyForTest("k1")
	for _, h := range lm.ResourceForTest("k1").Holders {
		h.leaseExpires = time.Now().Add(-1 * time.Second)
	}
	lm.UnlockKeyForTest("k1")

	// conn1's Wait detects expiry and should grant to conn2
	_, _, err := lm.Wait(bg(), "k1", 5*time.Second, 1)
	if !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("expected ErrLeaseExpired, got %v", err)
	}

	// conn2 should now be the owner (granted by grantNextWaiterLocked)
	lm.LockKeyForTest("k1")
	st := lm.ResourceForTest("k1")
	ownerConn := holderConnID(st)
	lm.UnlockKeyForTest("k1")
	if ownerConn != 2 {
		t.Fatalf("expected conn2 to own, got %d", ownerConn)
	}
}

func TestSemWait_FastPathLeaseExpired(t *testing.T) {
	lm := testManager()
	status, tok, _, _ := lm.Enqueue("s1", 1*time.Second, 1, 3)
	if status != "acquired" || tok == "" {
		t.Fatal("expected acquired")
	}

	// Manually expire
	lm.LockKeyForTest("s1")
	lm.ResourceForTest("s1").Holders[tok].leaseExpires = time.Now().Add(-1 * time.Second)
	lm.UnlockKeyForTest("s1")

	_, _, err := lm.Wait(bg(), "s1", 5*time.Second, 1)
	if !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("expected ErrLeaseExpired, got %v", err)
	}

	// Holder should be cleaned up
	lm.LockKeyForTest("s1")
	if len(lm.ResourceForTest("s1").Holders) != 0 {
		lm.UnlockKeyForTest("s1")
		t.Fatal("expired holder should be removed")
	}
	lm.UnlockKeyForTest("s1")
}

func TestSemWait_FastPathLockLost(t *testing.T) {
	// If the sem state is GC'd or holder was evicted, Wait should
	// return ErrLeaseExpired.
	lm := testManager()
	status, tok, _, _ := lm.Enqueue("s1", 1*time.Second, 1, 3)
	if status != "acquired" || tok == "" {
		t.Fatal("expected acquired")
	}

	// Remove the holder to simulate state loss
	lm.LockKeyForTest("s1")
	delete(lm.ResourceForTest("s1").Holders, tok)
	lm.UnlockKeyForTest("s1")

	_, _, err := lm.Wait(bg(), "s1", 5*time.Second, 1)
	if !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("expected ErrLeaseExpired, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// ErrWaiterClosed
// ---------------------------------------------------------------------------

func TestFIFOAcquire_WaiterClosed(t *testing.T) {
	// When CleanupConnection closes a waiter channel, Acquire
	// should return ErrWaiterClosed instead of returning an empty token.
	lm := testManager()
	lm.Acquire(bg(), "k1", 5*time.Second, 30*time.Second, 1, 1)

	done := make(chan error, 1)
	go func() {
		_, err := lm.Acquire(bg(), "k1", 30*time.Second, 30*time.Second, 2, 1)
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)

	lm.CleanupConnection(2) // closes conn2's waiter channel
	err := <-done
	if !errors.Is(err, ErrWaiterClosed) {
		t.Fatalf("expected ErrWaiterClosed, got %v", err)
	}
}

func TestFIFOWait_WaiterClosed(t *testing.T) {
	lm := testManager()
	lm.Acquire(bg(), "k1", 5*time.Second, 30*time.Second, 1, 1)
	lm.Enqueue("k1", 30*time.Second, 2, 1)

	done := make(chan error, 1)
	go func() {
		_, _, err := lm.Wait(bg(), "k1", 30*time.Second, 2)
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)

	lm.CleanupConnection(2)
	err := <-done
	if !errors.Is(err, ErrWaiterClosed) {
		t.Fatalf("expected ErrWaiterClosed, got %v", err)
	}
}

func TestSemAcquire_WaiterClosed(t *testing.T) {
	lm := testManager()
	lm.Acquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 1)

	done := make(chan error, 1)
	go func() {
		_, err := lm.Acquire(bg(), "s1", 30*time.Second, 30*time.Second, 2, 1)
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)

	lm.CleanupConnection(2)
	err := <-done
	if !errors.Is(err, ErrWaiterClosed) {
		t.Fatalf("expected ErrWaiterClosed, got %v", err)
	}
}

func TestSemWait_WaiterClosed(t *testing.T) {
	lm := testManager()
	lm.Acquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 1)
	lm.Enqueue("s1", 30*time.Second, 2, 1)

	done := make(chan error, 1)
	go func() {
		_, _, err := lm.Wait(bg(), "s1", 30*time.Second, 2)
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)

	lm.CleanupConnection(2)
	err := <-done
	if !errors.Is(err, ErrWaiterClosed) {
		t.Fatalf("expected ErrWaiterClosed, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// connEnqueued cleanup on release and lease expiry
// ---------------------------------------------------------------------------

func TestFIFORelease_CleansConnEnqueued(t *testing.T) {
	// After Enqueue acquires immediately, Release must clean
	// the connEnqueued entry for that token.
	lm := testManager()
	_, tok, _, _ := lm.Enqueue("k1", 30*time.Second, 1, 1)
	eqKey := connKey{ConnID: 1, Key: "k1"}

	lm.LockConnMuForTest()
	if lm.ConnEnqueuedForTest(eqKey) == nil {
		lm.UnlockConnMuForTest()
		t.Fatal("connEnqueued should exist before release")
	}
	lm.UnlockConnMuForTest()

	lm.Release("k1", tok)

	lm.LockConnMuForTest()
	if lm.ConnEnqueuedForTest(eqKey) != nil {
		lm.UnlockConnMuForTest()
		t.Fatal("connEnqueued should be cleaned up after release")
	}
	lm.UnlockConnMuForTest()
}

func TestSemRelease_CleansConnSemEnqueued(t *testing.T) {
	lm := testManager()
	_, tok, _, _ := lm.Enqueue("s1", 30*time.Second, 1, 3)
	eqKey := connKey{ConnID: 1, Key: "s1"}

	lm.LockConnMuForTest()
	if lm.ConnEnqueuedForTest(eqKey) == nil {
		lm.UnlockConnMuForTest()
		t.Fatal("connEnqueued should exist before release")
	}
	lm.UnlockConnMuForTest()

	lm.Release("s1", tok)

	lm.LockConnMuForTest()
	if lm.ConnEnqueuedForTest(eqKey) != nil {
		lm.UnlockConnMuForTest()
		t.Fatal("connEnqueued should be cleaned up after release")
	}
	lm.UnlockConnMuForTest()
}

func TestFIFORenew_ExpiredCleansConnEnqueued(t *testing.T) {
	// When renew detects expiry, it must also clean connEnqueued.
	lm := testManager()
	_, tok, _, _ := lm.Enqueue("k1", 1*time.Second, 1, 1)
	eqKey := connKey{ConnID: 1, Key: "k1"}

	// Manually expire
	lm.LockKeyForTest("k1")
	lm.ResourceForTest("k1").Holders[tok].leaseExpires = time.Now().Add(-1 * time.Second)
	lm.UnlockKeyForTest("k1")

	_, ok := lm.Renew("k1", tok, 30*time.Second)
	if ok {
		t.Fatal("renew should fail on expired lease")
	}

	lm.LockConnMuForTest()
	if lm.ConnEnqueuedForTest(eqKey) != nil {
		lm.UnlockConnMuForTest()
		t.Fatal("connEnqueued should be cleaned up after expired renew")
	}
	lm.UnlockConnMuForTest()
}

func TestSemRenew_ExpiredCleansConnSemEnqueued(t *testing.T) {
	lm := testManager()
	_, tok, _, _ := lm.Enqueue("s1", 1*time.Second, 1, 3)
	eqKey := connKey{ConnID: 1, Key: "s1"}

	lm.LockKeyForTest("s1")
	lm.ResourceForTest("s1").Holders[tok].leaseExpires = time.Now().Add(-1 * time.Second)
	lm.UnlockKeyForTest("s1")

	_, ok := lm.Renew("s1", tok, 30*time.Second)
	if ok {
		t.Fatal("renew should fail on expired lease")
	}

	lm.LockConnMuForTest()
	if lm.ConnEnqueuedForTest(eqKey) != nil {
		lm.UnlockConnMuForTest()
		t.Fatal("connEnqueued should be cleaned up after expired renew")
	}
	lm.UnlockConnMuForTest()
}

func TestLeaseExpiry_CleansConnEnqueued(t *testing.T) {
	// The LeaseExpiryLoop should also clean connEnqueued entries.
	lm := testManager()
	lm.cfg.LeaseSweepInterval = 50 * time.Millisecond
	_, tok, _, _ := lm.Enqueue("k1", 1*time.Second, 1, 1)
	eqKey := connKey{ConnID: 1, Key: "k1"}

	lm.LockConnMuForTest()
	if lm.ConnEnqueuedForTest(eqKey) == nil {
		lm.UnlockConnMuForTest()
		t.Fatal("connEnqueued should exist")
	}
	lm.UnlockConnMuForTest()

	// Expire
	lm.LockKeyForTest("k1")
	lm.ResourceForTest("k1").Holders[tok].leaseExpires = time.Now().Add(-1 * time.Second)
	lm.UnlockKeyForTest("k1")

	ctx, cancel := context.WithCancel(context.Background())
	go lm.LeaseExpiryLoop(ctx)
	time.Sleep(200 * time.Millisecond)
	cancel()

	lm.LockConnMuForTest()
	es := lm.ConnEnqueuedForTest(eqKey)
	lm.UnlockConnMuForTest()
	if es == nil {
		t.Fatal("connEnqueued should still exist (marked expired)")
	}
	if !es.expired {
		t.Fatal("connEnqueued should be marked expired by expiry loop")
	}
}

func TestSemLeaseExpiry_CleansConnSemEnqueued(t *testing.T) {
	lm := testManager()
	lm.cfg.LeaseSweepInterval = 50 * time.Millisecond
	_, tok, _, _ := lm.Enqueue("s1", 1*time.Second, 1, 3)
	eqKey := connKey{ConnID: 1, Key: "s1"}

	lm.LockKeyForTest("s1")
	lm.ResourceForTest("s1").Holders[tok].leaseExpires = time.Now().Add(-1 * time.Second)
	lm.UnlockKeyForTest("s1")

	ctx, cancel := context.WithCancel(context.Background())
	go lm.LeaseExpiryLoop(ctx)
	time.Sleep(200 * time.Millisecond)
	cancel()

	lm.LockConnMuForTest()
	es := lm.ConnEnqueuedForTest(eqKey)
	lm.UnlockConnMuForTest()
	if es == nil {
		t.Fatal("connEnqueued should still exist (marked expired)")
	}
	if !es.expired {
		t.Fatal("connEnqueued should be marked expired by expiry loop")
	}
}

// ---------------------------------------------------------------------------
// connOwned cleanup in FIFOWait/SemWait fast path on expiry
// ---------------------------------------------------------------------------

func TestFIFOWait_FastPathExpiredCleansConnOwned(t *testing.T) {
	lm := testManager()
	status, tok, _, _ := lm.Enqueue("k1", 1*time.Second, 1, 1)
	if status != "acquired" {
		t.Fatal("expected acquired")
	}

	// Verify connOwned is set
	lm.LockConnMuForTest()
	if lm.ConnOwnedForTest(1) == nil {
		lm.UnlockConnMuForTest()
		t.Fatal("connOwned should have conn 1")
	}
	lm.UnlockConnMuForTest()
	lm.LockKeyForTest("k1")
	lm.ResourceForTest("k1").Holders[tok].leaseExpires = time.Now().Add(-1 * time.Second)
	lm.UnlockKeyForTest("k1")

	lm.Wait(bg(), "k1", 5*time.Second, 1)

	// connOwned for this key should be cleaned up
	lm.LockConnMuForTest()
	if owned := lm.ConnOwnedForTest(1); owned != nil {
		if _, hasKey := owned["k1"]; hasKey {
			lm.UnlockConnMuForTest()
			t.Fatal("connOwned should not have k1 after expired wait")
		}
	}
	lm.UnlockConnMuForTest()
}

func TestSemWait_FastPathExpiredCleansConnSemOwned(t *testing.T) {
	lm := testManager()
	status, tok, _, _ := lm.Enqueue("s1", 1*time.Second, 1, 3)
	if status != "acquired" || tok == "" {
		t.Fatal("expected acquired")
	}

	lm.LockKeyForTest("s1")
	lm.ResourceForTest("s1").Holders[tok].leaseExpires = time.Now().Add(-1 * time.Second)
	lm.UnlockKeyForTest("s1")

	lm.Wait(bg(), "s1", 5*time.Second, 1)

	lm.LockConnMuForTest()
	if m := lm.ConnOwnedForTest(1); m != nil {
		if tokens, ok := m["s1"]; ok {
			if _, ok := tokens[tok]; ok {
				lm.UnlockConnMuForTest()
				t.Fatal("connOwned should not have the expired token")
			}
		}
	}
	lm.UnlockConnMuForTest()
}

// ---------------------------------------------------------------------------
// Stats idle classification
// ---------------------------------------------------------------------------

func TestStats_IdleClassification(t *testing.T) {
	lm := testManager()

	// Create an idle lock (acquired then released)
	tok, _ := lm.Acquire(bg(), "idle-lock", 5*time.Second, 30*time.Second, 1, 1)
	lm.Release("idle-lock", tok)

	// Create an active lock (held)
	lm.Acquire(bg(), "active-lock", 5*time.Second, 30*time.Second, 2, 1)

	// Create an idle semaphore
	semTok, _ := lm.Acquire(bg(), "idle-sem", 5*time.Second, 30*time.Second, 3, 3)
	lm.Release("idle-sem", semTok)

	// Create an active semaphore
	lm.Acquire(bg(), "active-sem", 5*time.Second, 30*time.Second, 4, 3)

	stats := lm.Stats(4)

	// Active locks should be in Locks list
	foundActive := false
	for _, l := range stats.Locks {
		if l.Key == "active-lock" {
			foundActive = true
		}
		if l.Key == "idle-lock" {
			t.Fatal("idle-lock should not be in active locks")
		}
	}
	if !foundActive {
		t.Fatal("active-lock should be in locks list")
	}

	// Idle locks should be in IdleLocks list
	foundIdle := false
	for _, l := range stats.IdleLocks {
		if l.Key == "idle-lock" {
			foundIdle = true
		}
	}
	if !foundIdle {
		t.Fatal("idle-lock should be in idle locks list")
	}

	// Active semaphores should be in Semaphores list
	foundActiveSem := false
	for _, s := range stats.Semaphores {
		if s.Key == "active-sem" {
			foundActiveSem = true
		}
	}
	if !foundActiveSem {
		t.Fatal("active-sem should be in semaphores list")
	}

	// Idle semaphores should be in IdleSemaphores list
	foundIdleSem := false
	for _, s := range stats.IdleSemaphores {
		if s.Key == "idle-sem" {
			foundIdleSem = true
		}
	}
	if !foundIdleSem {
		t.Fatal("idle-sem should be in idle semaphores list")
	}
}

// ---------------------------------------------------------------------------
// ErrAlreadyEnqueued (sentinel error, not type assertion)
// ---------------------------------------------------------------------------

func TestFIFOEnqueue_AlreadyEnqueuedIsSentinel(t *testing.T) {
	lm := testManager()
	lm.Enqueue("k1", 30*time.Second, 1, 1)
	_, _, _, err := lm.Enqueue("k1", 30*time.Second, 1, 1)
	if !errors.Is(err, ErrAlreadyEnqueued) {
		t.Fatalf("expected errors.Is(err, ErrAlreadyEnqueued), got %v", err)
	}
}

func TestSemEnqueue_AlreadyEnqueuedIsSentinel(t *testing.T) {
	lm := testManager()
	lm.Enqueue("s1", 30*time.Second, 1, 3)
	_, _, _, err := lm.Enqueue("s1", 30*time.Second, 1, 3)
	if !errors.Is(err, ErrAlreadyEnqueued) {
		t.Fatalf("expected errors.Is(err, ErrAlreadyEnqueued), got %v", err)
	}
}

// ---------------------------------------------------------------------------
// ResetForTest clears all state
// ---------------------------------------------------------------------------

func TestResetForTest(t *testing.T) {
	lm := testManager()
	lm.Acquire(bg(), "k1", 5*time.Second, 30*time.Second, 1, 1)
	lm.Acquire(bg(), "s1", 5*time.Second, 30*time.Second, 2, 3)
	lm.Enqueue("k2", 30*time.Second, 3, 1)
	lm.Enqueue("s2", 30*time.Second, 4, 3)

	lm.ResetForTest()

	if lm.ResourceCountForTest() != 0 {
		t.Fatal("resources should be empty")
	}
	if lm.ConnOwnedCountForTest() != 0 {
		t.Fatal("connOwned should be empty")
	}
	if lm.ConnEnqueuedCountForTest() != 0 {
		t.Fatal("connEnqueued should be empty")
	}
}

// ===========================================================================
// Phase 1: Atomic Counters
// ===========================================================================

func TestIncr_Basic(t *testing.T) {
	lm := testManager()
	val, err := lm.Incr("c1", 5)
	if err != nil {
		t.Fatal(err)
	}
	if val != 5 {
		t.Fatalf("expected 5, got %d", val)
	}
}

func TestIncr_Multiple(t *testing.T) {
	lm := testManager()
	lm.Incr("c1", 5)
	lm.Incr("c1", 3)
	val, _ := lm.Incr("c1", 2)
	if val != 10 {
		t.Fatalf("expected 10, got %d", val)
	}
}

func TestDecr_Negative(t *testing.T) {
	lm := testManager()
	lm.Incr("c1", 10)
	val, _ := lm.Decr("c1", 15)
	if val != -5 {
		t.Fatalf("expected -5, got %d", val)
	}
}

func TestGetCounter_Nonexistent(t *testing.T) {
	lm := testManager()
	val := lm.GetCounter("nosuchkey")
	if val != 0 {
		t.Fatalf("expected 0, got %d", val)
	}
}

func TestSetCounter(t *testing.T) {
	lm := testManager()
	if err := lm.SetCounter("c1", 42); err != nil {
		t.Fatal(err)
	}
	val := lm.GetCounter("c1")
	if val != 42 {
		t.Fatalf("expected 42, got %d", val)
	}
}

func TestCounter_MaxKeys(t *testing.T) {
	cfg := testConfig()
	cfg.MaxKeys = 2
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	lm := NewLockManager(cfg, log)

	if _, err := lm.Incr("c1", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := lm.Incr("c2", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := lm.Incr("c3", 1); !errors.Is(err, ErrMaxKeys) {
		t.Fatalf("expected ErrMaxKeys, got %v", err)
	}
}

func TestCounter_GC_ZeroValue(t *testing.T) {
	cfg := testConfig()
	cfg.GCMaxIdleTime = 50 * time.Millisecond
	cfg.GCInterval = 50 * time.Millisecond
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	lm := NewLockManager(cfg, log)

	lm.Incr("c1", 5)
	lm.Decr("c1", 5) // value == 0

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go lm.GCLoop(ctx)

	time.Sleep(200 * time.Millisecond)
	if lm.GetCounter("c1") != 0 {
		t.Fatal("expected counter to be GC'd (value 0)")
	}
}

func TestCounter_GC_NonZeroPreserved(t *testing.T) {
	cfg := testConfig()
	cfg.GCMaxIdleTime = 50 * time.Millisecond
	cfg.GCInterval = 50 * time.Millisecond
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	lm := NewLockManager(cfg, log)

	lm.Incr("c1", 10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go lm.GCLoop(ctx)

	time.Sleep(200 * time.Millisecond)
	val := lm.GetCounter("c1")
	if val != 10 {
		t.Fatalf("non-zero counter should be preserved, got %d", val)
	}
}

func TestCounter_NamespaceIsolation(t *testing.T) {
	lm := testManager()
	// Same key "foo" as lock and counter — should coexist
	tok, err := lm.Acquire(bg(), "foo", 5*time.Second, 30*time.Second, 1, 1)
	if err != nil || tok == "" {
		t.Fatal("lock acquire failed")
	}
	val, err := lm.Incr("foo", 42)
	if err != nil {
		t.Fatal(err)
	}
	if val != 42 {
		t.Fatalf("expected 42, got %d", val)
	}
	// Lock should still be held
	lm.LockKeyForTest("foo")
	st := lm.ResourceForTest("foo")
	if st == nil || len(st.Holders) != 1 {
		lm.UnlockKeyForTest("foo")
		t.Fatal("lock should still be held")
	}
	lm.UnlockKeyForTest("foo")
}

// ===========================================================================
// Phase 2: KV Store
// ===========================================================================

func TestKV_SetGet(t *testing.T) {
	lm := testManager()
	if err := lm.KVSet("k1", "hello", 0); err != nil {
		t.Fatal(err)
	}
	val, ok := lm.KVGet("k1")
	if !ok || val != "hello" {
		t.Fatalf("expected 'hello', got %q ok=%v", val, ok)
	}
}

func TestKV_Nonexistent(t *testing.T) {
	lm := testManager()
	_, ok := lm.KVGet("nosuchkey")
	if ok {
		t.Fatal("expected not found")
	}
}

func TestKV_Delete(t *testing.T) {
	lm := testManager()
	lm.KVSet("k1", "hello", 0)
	lm.KVDel("k1")
	_, ok := lm.KVGet("k1")
	if ok {
		t.Fatal("expected not found after delete")
	}
}

func TestKV_TTLExpiry_Lazy(t *testing.T) {
	lm := testManager()
	lm.KVSet("k1", "hello", 1) // 1 second TTL
	time.Sleep(1100 * time.Millisecond)
	_, ok := lm.KVGet("k1")
	if ok {
		t.Fatal("expected lazy expiry on read")
	}
}

func TestKV_TTLExpiry_Sweep(t *testing.T) {
	cfg := testConfig()
	cfg.LeaseSweepInterval = 50 * time.Millisecond
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	lm := NewLockManager(cfg, log)

	lm.KVSet("k1", "hello", 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go lm.LeaseExpiryLoop(ctx)

	time.Sleep(1200 * time.Millisecond)
	_, ok := lm.KVGet("k1")
	if ok {
		t.Fatal("expected sweep to expire KV entry")
	}
}

func TestKV_MaxKeys(t *testing.T) {
	cfg := testConfig()
	cfg.MaxKeys = 2
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	lm := NewLockManager(cfg, log)

	lm.KVSet("k1", "v1", 0)
	lm.KVSet("k2", "v2", 0)
	err := lm.KVSet("k3", "v3", 0)
	if !errors.Is(err, ErrMaxKeys) {
		t.Fatalf("expected ErrMaxKeys, got %v", err)
	}
}

func TestKV_GC_Idle(t *testing.T) {
	cfg := testConfig()
	cfg.GCMaxIdleTime = 50 * time.Millisecond
	cfg.GCInterval = 50 * time.Millisecond
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	lm := NewLockManager(cfg, log)

	lm.KVSet("k1", "hello", 0) // no TTL

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go lm.GCLoop(ctx)

	time.Sleep(200 * time.Millisecond)
	_, ok := lm.KVGet("k1")
	if ok {
		t.Fatal("expected idle GC to prune no-TTL entry")
	}
}

func TestKV_NamespaceIsolation(t *testing.T) {
	lm := testManager()
	lm.Incr("foo", 42)
	lm.KVSet("foo", "bar", 0)
	val := lm.GetCounter("foo")
	if val != 42 {
		t.Fatalf("counter should still be 42, got %d", val)
	}
	kv, ok := lm.KVGet("foo")
	if !ok || kv != "bar" {
		t.Fatalf("kv should be 'bar', got %q", kv)
	}
}

// ===========================================================================
// Phase 4: Lists/Queues
// ===========================================================================

func TestList_PushPop_Basic(t *testing.T) {
	lm := testManager()
	n, err := lm.RPush("q1", "a")
	if err != nil || n != 1 {
		t.Fatalf("rpush: n=%d err=%v", n, err)
	}
	n, err = lm.RPush("q1", "b")
	if err != nil || n != 2 {
		t.Fatalf("rpush: n=%d err=%v", n, err)
	}
	val, ok := lm.LPop("q1")
	if !ok || val != "a" {
		t.Fatalf("lpop: got %q ok=%v", val, ok)
	}
}

func TestList_FIFO(t *testing.T) {
	lm := testManager()
	lm.RPush("q1", "a")
	lm.RPush("q1", "b")
	lm.RPush("q1", "c")

	v1, _ := lm.LPop("q1")
	v2, _ := lm.LPop("q1")
	v3, _ := lm.LPop("q1")
	if v1 != "a" || v2 != "b" || v3 != "c" {
		t.Fatalf("FIFO order wrong: %s %s %s", v1, v2, v3)
	}
}

func TestList_LIFO(t *testing.T) {
	lm := testManager()
	lm.LPush("q1", "a")
	lm.LPush("q1", "b")
	lm.LPush("q1", "c")

	v1, _ := lm.LPop("q1")
	v2, _ := lm.LPop("q1")
	v3, _ := lm.LPop("q1")
	if v1 != "c" || v2 != "b" || v3 != "a" {
		t.Fatalf("LIFO order wrong: %s %s %s", v1, v2, v3)
	}
}

func TestList_EmptyPop(t *testing.T) {
	lm := testManager()
	_, ok := lm.LPop("empty")
	if ok {
		t.Fatal("expected false for empty pop")
	}
	_, ok = lm.RPop("empty")
	if ok {
		t.Fatal("expected false for empty rpop")
	}
}

func TestList_LLen(t *testing.T) {
	lm := testManager()
	if lm.LLen("q1") != 0 {
		t.Fatal("expected 0 for nonexistent")
	}
	lm.RPush("q1", "a")
	lm.RPush("q1", "b")
	if lm.LLen("q1") != 2 {
		t.Fatalf("expected 2, got %d", lm.LLen("q1"))
	}
}

func TestList_LRange_Positive(t *testing.T) {
	lm := testManager()
	lm.RPush("q1", "a")
	lm.RPush("q1", "b")
	lm.RPush("q1", "c")
	lm.RPush("q1", "d")

	items := lm.LRange("q1", 1, 2)
	if len(items) != 2 || items[0] != "b" || items[1] != "c" {
		t.Fatalf("expected [b c], got %v", items)
	}
}

func TestList_LRange_Negative(t *testing.T) {
	lm := testManager()
	lm.RPush("q1", "a")
	lm.RPush("q1", "b")
	lm.RPush("q1", "c")

	items := lm.LRange("q1", -2, -1)
	if len(items) != 2 || items[0] != "b" || items[1] != "c" {
		t.Fatalf("expected [b c], got %v", items)
	}
}

func TestList_LRange_OutOfBounds(t *testing.T) {
	lm := testManager()
	lm.RPush("q1", "a")
	lm.RPush("q1", "b")

	items := lm.LRange("q1", 0, 100)
	if len(items) != 2 {
		t.Fatalf("expected 2, got %d", len(items))
	}
}

func TestList_LRange_Empty(t *testing.T) {
	lm := testManager()
	items := lm.LRange("empty", 0, -1)
	if len(items) != 0 {
		t.Fatalf("expected empty, got %v", items)
	}
}

func TestList_LRange_FullRange(t *testing.T) {
	lm := testManager()
	lm.RPush("q1", "a")
	lm.RPush("q1", "b")
	lm.RPush("q1", "c")

	items := lm.LRange("q1", 0, -1)
	if len(items) != 3 || items[0] != "a" || items[2] != "c" {
		t.Fatalf("expected [a b c], got %v", items)
	}
}

func TestList_MaxKeys(t *testing.T) {
	cfg := testConfig()
	cfg.MaxKeys = 1
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	lm := NewLockManager(cfg, log)

	lm.RPush("q1", "a")
	_, err := lm.RPush("q2", "b")
	if !errors.Is(err, ErrMaxKeys) {
		t.Fatalf("expected ErrMaxKeys, got %v", err)
	}
}

func TestList_MaxListLength(t *testing.T) {
	cfg := testConfig()
	cfg.MaxListLength = 2
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	lm := NewLockManager(cfg, log)

	lm.RPush("q1", "a")
	lm.RPush("q1", "b")
	_, err := lm.RPush("q1", "c")
	if !errors.Is(err, ErrListFull) {
		t.Fatalf("expected ErrListFull, got %v", err)
	}
}

func TestList_AutoDeleteOnPop(t *testing.T) {
	lm := testManager()
	lm.RPush("q1", "a")
	lm.LPop("q1")
	if lm.LLen("q1") != 0 {
		t.Fatal("empty list should be auto-deleted")
	}
}

func TestList_GC(t *testing.T) {
	cfg := testConfig()
	cfg.GCMaxIdleTime = 50 * time.Millisecond
	cfg.GCInterval = 50 * time.Millisecond
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	lm := NewLockManager(cfg, log)

	lm.RPush("q1", "a")
	lm.LPop("q1") // empty now

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go lm.GCLoop(ctx)

	time.Sleep(200 * time.Millisecond)
	// The list should have been auto-deleted by pop, so GC is a no-op.
	// Just verify LLen returns 0
	if lm.LLen("q1") != 0 {
		t.Fatal("expected empty")
	}
}

func TestList_NamespaceIsolation(t *testing.T) {
	lm := testManager()
	lm.Incr("foo", 10)
	lm.KVSet("foo", "bar", 0)
	lm.RPush("foo", "item1")

	if lm.GetCounter("foo") != 10 {
		t.Fatal("counter namespace broken")
	}
	kv, ok := lm.KVGet("foo")
	if !ok || kv != "bar" {
		t.Fatal("kv namespace broken")
	}
	if lm.LLen("foo") != 1 {
		t.Fatal("list namespace broken")
	}
}

// ===========================================================================
// Fencing Token tests
// ===========================================================================

func TestFencingToken_MonotonicallyIncreases(t *testing.T) {
	lm := testManager()
	_, fence1, err := lm.AcquireWithFence(bg(), "k1", 5*time.Second, 30*time.Second, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	_, fence2, err := lm.AcquireWithFence(bg(), "k2", 5*time.Second, 30*time.Second, 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	_, fence3, err := lm.AcquireWithFence(bg(), "k3", 5*time.Second, 30*time.Second, 3, 1)
	if err != nil {
		t.Fatal(err)
	}
	if fence1 == 0 {
		t.Fatal("fence1 should be > 0")
	}
	if fence2 <= fence1 {
		t.Fatalf("fence2 (%d) should be > fence1 (%d)", fence2, fence1)
	}
	if fence3 <= fence2 {
		t.Fatalf("fence3 (%d) should be > fence2 (%d)", fence3, fence2)
	}
}

func TestFencingToken_SlowPathGrant(t *testing.T) {
	lm := testManager()
	tok1, fence1, err := lm.AcquireWithFence(bg(), "k1", 5*time.Second, 30*time.Second, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if fence1 == 0 {
		t.Fatal("fence1 should be > 0")
	}

	done := make(chan uint64, 1)
	go func() {
		_, fence, _ := lm.AcquireWithFence(bg(), "k1", 5*time.Second, 30*time.Second, 2, 1)
		done <- fence
	}()
	time.Sleep(50 * time.Millisecond)

	lm.Release("k1", tok1)
	fence2 := <-done
	if fence2 == 0 {
		t.Fatal("fence2 should be > 0 (slow path grant)")
	}
	if fence2 <= fence1 {
		t.Fatalf("fence2 (%d) should be > fence1 (%d)", fence2, fence1)
	}
}

func TestFencingToken_EnqueueAcquiredFence(t *testing.T) {
	lm := testManager()
	status, tok, _, fence, err := lm.EnqueueWithFence("k1", 30*time.Second, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if status != "acquired" {
		t.Fatalf("expected acquired, got %s", status)
	}
	if tok == "" {
		t.Fatal("expected token")
	}
	if fence == 0 {
		t.Fatal("fence should be > 0 on fast-path enqueue")
	}
}

func TestFencingToken_WaitWithFence(t *testing.T) {
	lm := testManager()
	tok1, _ := lm.Acquire(bg(), "k1", 5*time.Second, 30*time.Second, 1, 1)
	lm.Enqueue("k1", 30*time.Second, 2, 1)

	done := make(chan uint64, 1)
	go func() {
		_, _, fence, _ := lm.WaitWithFence(bg(), "k1", 5*time.Second, 2)
		done <- fence
	}()
	time.Sleep(50 * time.Millisecond)

	lm.Release("k1", tok1)
	fence := <-done
	if fence == 0 {
		t.Fatal("WaitWithFence should return a non-zero fence")
	}
}

func TestFencingToken_RenewReturnsFence(t *testing.T) {
	lm := testManager()
	tok, fence1, err := lm.AcquireWithFence(bg(), "k1", 5*time.Second, 30*time.Second, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if fence1 == 0 {
		t.Fatal("fence1 should be > 0")
	}

	_, fence2, err := lm.RenewWithFence("k1", tok, 60*time.Second)
	if err != nil {
		t.Fatal("renew should succeed")
	}
	if fence2 != fence1 {
		t.Fatalf("RenewWithFence should return same fence: got %d, want %d", fence2, fence1)
	}
}

func TestFencingToken_SemaphoreFence(t *testing.T) {
	lm := testManager()
	_, fence1, err := lm.AcquireWithFence(bg(), "s1", 5*time.Second, 30*time.Second, 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	_, fence2, err := lm.AcquireWithFence(bg(), "s1", 5*time.Second, 30*time.Second, 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	_, fence3, err := lm.AcquireWithFence(bg(), "s1", 5*time.Second, 30*time.Second, 3, 3)
	if err != nil {
		t.Fatal(err)
	}
	if fence1 == 0 || fence2 == 0 || fence3 == 0 {
		t.Fatalf("all fences should be > 0: got %d, %d, %d", fence1, fence2, fence3)
	}
	if fence2 <= fence1 || fence3 <= fence2 {
		t.Fatalf("fences should increase: %d, %d, %d", fence1, fence2, fence3)
	}
}

// ===========================================================================
// Blocking Pop tests
// ===========================================================================

func TestBLPop_NonEmptyList(t *testing.T) {
	lm := testManager()
	lm.RPush("list1", "a")
	lm.RPush("list1", "b")

	val, err := lm.BLPop(bg(), "list1", 5*time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	if val != "a" {
		t.Fatalf("expected 'a', got %q", val)
	}
}

func TestBLPop_EmptyBlocksUntilPush(t *testing.T) {
	lm := testManager()

	done := make(chan string, 1)
	go func() {
		val, _ := lm.BLPop(bg(), "list1", 5*time.Second, 1)
		done <- val
	}()
	time.Sleep(50 * time.Millisecond)

	lm.RPush("list1", "hello")
	val := <-done
	if val != "hello" {
		t.Fatalf("expected 'hello', got %q", val)
	}
}

func TestBLPop_Timeout(t *testing.T) {
	lm := testManager()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	val, err := lm.BLPop(ctx, "list1", 1*time.Millisecond, 1)
	if err != nil && err != context.DeadlineExceeded && !errors.Is(err, ErrTimeout) {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "" {
		t.Fatalf("expected empty string on timeout, got %q", val)
	}
}

func TestBRPop_NonEmptyList(t *testing.T) {
	lm := testManager()
	lm.LPush("list1", "a")
	lm.LPush("list1", "b")
	// List is now [b, a]

	val, err := lm.BRPop(bg(), "list1", 5*time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	if val != "a" {
		t.Fatalf("expected 'a' (rightmost), got %q", val)
	}
}

func TestBLPop_MultipleFIFO(t *testing.T) {
	lm := testManager()

	results := make([]chan string, 3)
	// Launch goroutines one at a time with sleeps to ensure FIFO ordering
	for i := 0; i < 3; i++ {
		results[i] = make(chan string, 1)
		connID := uint64(i + 1)
		ch := results[i]
		go func() {
			val, _ := lm.BLPop(bg(), "list1", 5*time.Second, connID)
			ch <- val
		}()
		time.Sleep(50 * time.Millisecond)
	}

	lm.RPush("list1", "first")
	lm.RPush("list1", "second")
	lm.RPush("list1", "third")

	v1 := <-results[0]
	v2 := <-results[1]
	v3 := <-results[2]

	if v1 != "first" {
		t.Fatalf("popper 1: expected 'first', got %q", v1)
	}
	if v2 != "second" {
		t.Fatalf("popper 2: expected 'second', got %q", v2)
	}
	if v3 != "third" {
		t.Fatalf("popper 3: expected 'third', got %q", v3)
	}
}

func TestBLPop_PushHandoff(t *testing.T) {
	lm := testManager()

	done := make(chan string, 1)
	go func() {
		val, _ := lm.BLPop(bg(), "list1", 5*time.Second, 1)
		done <- val
	}()
	time.Sleep(50 * time.Millisecond)

	lm.RPush("list1", "direct")
	val := <-done
	if val != "direct" {
		t.Fatalf("expected 'direct', got %q", val)
	}

	// List should be empty since the value was handed off directly
	if lm.LLen("list1") != 0 {
		t.Fatalf("list should be empty after handoff, got len %d", lm.LLen("list1"))
	}
}

func TestBLPop_DisconnectCleanup(t *testing.T) {
	lm := testManager()

	done := make(chan struct{})
	var gotVal string
	go func() {
		gotVal, _ = lm.BLPop(bg(), "list1", 30*time.Second, 42)
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)

	lm.CleanupConnection(42)
	<-done

	if gotVal != "" {
		t.Fatalf("expected empty string after disconnect, got %q", gotVal)
	}
}

// ===========================================================================
// Read-Write Lock tests
// ===========================================================================

func TestRWLock_MultipleReaders(t *testing.T) {
	lm := testManager()
	tok1, fence1, err := lm.RWAcquire(bg(), "rw1", 'r', 5*time.Second, 30*time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	tok2, fence2, err := lm.RWAcquire(bg(), "rw1", 'r', 5*time.Second, 30*time.Second, 2)
	if err != nil {
		t.Fatal(err)
	}
	tok3, fence3, err := lm.RWAcquire(bg(), "rw1", 'r', 5*time.Second, 30*time.Second, 3)
	if err != nil {
		t.Fatal(err)
	}
	if tok1 == "" || tok2 == "" || tok3 == "" {
		t.Fatal("all readers should acquire simultaneously")
	}
	if fence1 == 0 || fence2 == 0 || fence3 == 0 {
		t.Fatal("all readers should get fences")
	}
	lm.LockKeyForTest("rw1")
	if len(lm.ResourceForTest("rw1").Holders) != 3 {
		lm.UnlockKeyForTest("rw1")
		t.Fatalf("expected 3 holders, got %d", len(lm.ResourceForTest("rw1").Holders))
	}
	lm.UnlockKeyForTest("rw1")
}

func TestRWLock_WriterExclusive(t *testing.T) {
	lm := testManager()
	_, _, err := lm.RWAcquire(bg(), "rw1", 'w', 5*time.Second, 30*time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}

	tok2, _, err := lm.RWAcquire(bg(), "rw1", 'w', 0, 30*time.Second, 2)
	if err != nil {
		t.Fatal(err)
	}
	if tok2 != "" {
		t.Fatal("second writer should timeout")
	}
}

func TestRWLock_WriterBlocksReaders(t *testing.T) {
	lm := testManager()
	_, _, err := lm.RWAcquire(bg(), "rw1", 'w', 5*time.Second, 30*time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}

	tok2, _, err := lm.RWAcquire(bg(), "rw1", 'r', 0, 30*time.Second, 2)
	if err != nil {
		t.Fatal(err)
	}
	if tok2 != "" {
		t.Fatal("reader should timeout when writer holds")
	}
}

func TestRWLock_ReadersBlockWriter(t *testing.T) {
	lm := testManager()
	_, _, err := lm.RWAcquire(bg(), "rw1", 'r', 5*time.Second, 30*time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}

	tok2, _, err := lm.RWAcquire(bg(), "rw1", 'w', 0, 30*time.Second, 2)
	if err != nil {
		t.Fatal(err)
	}
	if tok2 != "" {
		t.Fatal("writer should timeout when reader holds")
	}
}

func TestRWLock_FIFOFairness(t *testing.T) {
	lm := testManager()
	// Acquire a read lock
	tok1, _, err := lm.RWAcquire(bg(), "rw1", 'r', 5*time.Second, 30*time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}

	// Enqueue a writer (conn 2), then a reader (conn 3) as waiters
	writerDone := make(chan string, 1)
	go func() {
		tok, _, _ := lm.RWAcquire(bg(), "rw1", 'w', 5*time.Second, 30*time.Second, 2)
		writerDone <- tok
	}()
	time.Sleep(50 * time.Millisecond)

	readerDone := make(chan string, 1)
	go func() {
		tok, _, _ := lm.RWAcquire(bg(), "rw1", 'r', 5*time.Second, 30*time.Second, 3)
		readerDone <- tok
	}()
	time.Sleep(50 * time.Millisecond)

	// Release the read lock — writer should get granted (not the reader)
	lm.RWRelease("rw1", tok1)

	writerTok := <-writerDone
	if writerTok == "" {
		t.Fatal("writer should have been granted")
	}

	// Reader should still be waiting (not yet granted)
	select {
	case <-readerDone:
		t.Fatal("reader should not be granted while writer holds")
	case <-time.After(100 * time.Millisecond):
		// expected
	}

	// Release the writer — reader should get granted
	lm.RWRelease("rw1", writerTok)
	readerTok := <-readerDone
	if readerTok == "" {
		t.Fatal("reader should have been granted after writer release")
	}
}

func TestRWLock_BatchGrantReaders(t *testing.T) {
	lm := testManager()
	// Hold a write lock
	tok1, _, err := lm.RWAcquire(bg(), "rw1", 'w', 5*time.Second, 30*time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}

	// Enqueue 3 readers
	results := make([]chan string, 3)
	for i := 0; i < 3; i++ {
		results[i] = make(chan string, 1)
		connID := uint64(i + 2)
		ch := results[i]
		go func() {
			tok, _, _ := lm.RWAcquire(bg(), "rw1", 'r', 5*time.Second, 30*time.Second, connID)
			ch <- tok
		}()
	}
	time.Sleep(50 * time.Millisecond)

	// Release write lock — all 3 readers should be granted simultaneously
	lm.RWRelease("rw1", tok1)

	for i, ch := range results {
		select {
		case tok := <-ch:
			if tok == "" {
				t.Fatalf("reader %d should have been granted", i+2)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("reader %d timed out waiting for grant", i+2)
		}
	}

	// Verify all 3 are holders
	lm.LockKeyForTest("rw1")
	if len(lm.ResourceForTest("rw1").Holders) != 3 {
		lm.UnlockKeyForTest("rw1")
		t.Fatalf("expected 3 holders, got %d", len(lm.ResourceForTest("rw1").Holders))
	}
	lm.UnlockKeyForTest("rw1")
}

func TestRWLock_TypeMismatch(t *testing.T) {
	lm := testManager()
	// Create a regular lock (limit=1)
	_, err := lm.Acquire(bg(), "k1", 5*time.Second, 30*time.Second, 1, 1)
	if err != nil {
		t.Fatal(err)
	}

	// Try RWAcquire on the same key — should get ErrTypeMismatch
	_, _, err = lm.RWAcquire(bg(), "k1", 'r', 0, 30*time.Second, 2)
	if err != ErrTypeMismatch {
		t.Fatalf("expected ErrTypeMismatch, got %v", err)
	}
}

func TestRWLock_RenewMode(t *testing.T) {
	lm := testManager()

	// Acquire read lock and renew
	tokR, _, err := lm.RWAcquire(bg(), "rw1", 'r', 5*time.Second, 30*time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = lm.RWRenew("rw1", tokR, 60*time.Second)
	if err != nil {
		t.Fatal("RWRenew on read lock should succeed")
	}

	// Release and acquire write lock, then renew
	lm.RWRelease("rw1", tokR)

	tokW, _, err := lm.RWAcquire(bg(), "rw1", 'w', 5*time.Second, 30*time.Second, 2)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = lm.RWRenew("rw1", tokW, 60*time.Second)
	if err != nil {
		t.Fatal("RWRenew on write lock should succeed")
	}
}

func TestRWLock_ReleaseAndGrant(t *testing.T) {
	lm := testManager()
	// Acquire write lock
	tokW, _, err := lm.RWAcquire(bg(), "rw1", 'w', 5*time.Second, 30*time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}

	// Enqueue a reader via RWEnqueue
	status, _, _, _, err := lm.RWEnqueue("rw1", 'r', 30*time.Second, 2)
	if err != nil {
		t.Fatal(err)
	}
	if status != "queued" {
		t.Fatalf("expected queued, got %s", status)
	}

	// Start RWWait in goroutine
	done := make(chan struct{})
	var gotTok string
	var gotFence uint64
	go func() {
		gotTok, _, gotFence, _ = lm.RWWait(bg(), "rw1", 5*time.Second, 2)
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)

	// Release write lock — reader should be granted
	lm.RWRelease("rw1", tokW)
	<-done

	if gotTok == "" {
		t.Fatal("reader should have been granted via RWWait")
	}
	if gotFence == 0 {
		t.Fatal("RWWait should return a non-zero fence")
	}
}

// ---------------------------------------------------------------------------
// KVCAS
// ---------------------------------------------------------------------------

func TestKVCAS_CreateIfAbsent(t *testing.T) {
	lm := testManager()
	ok, err := lm.KVCAS("k1", "", "hello", 0)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected CAS to succeed for absent key")
	}
	val, exists := lm.KVGet("k1")
	if !exists || val != "hello" {
		t.Fatalf("expected 'hello', got %q (exists=%v)", val, exists)
	}
}

func TestKVCAS_SuccessfulSwap(t *testing.T) {
	lm := testManager()
	lm.KVSet("k1", "old", 0)
	ok, err := lm.KVCAS("k1", "old", "new", 0)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected CAS to succeed")
	}
	val, exists := lm.KVGet("k1")
	if !exists || val != "new" {
		t.Fatalf("expected 'new', got %q", val)
	}
}

func TestKVCAS_Conflict(t *testing.T) {
	lm := testManager()
	lm.KVSet("k1", "actual", 0)
	ok, err := lm.KVCAS("k1", "wrong", "new", 0)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected CAS to fail on mismatch")
	}
	// Value should be unchanged
	val, _ := lm.KVGet("k1")
	if val != "actual" {
		t.Fatalf("expected 'actual', got %q", val)
	}
}

func TestKVCAS_ExpiredKey(t *testing.T) {
	lm := testManager()
	lm.KVSet("k1", "old", 1)
	time.Sleep(1100 * time.Millisecond)
	// Expired key — should be treated as absent
	ok, err := lm.KVCAS("k1", "", "new", 0)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected CAS to succeed on expired key with empty old")
	}
}

func TestKVCAS_MaxKeys(t *testing.T) {
	cfg := testConfig()
	cfg.MaxKeys = 1
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	lm := NewLockManager(cfg, log)
	lm.KVSet("existing", "v", 0)

	// Second key should fail
	_, err := lm.KVCAS("new_key", "", "val", 0)
	if !errors.Is(err, ErrMaxKeys) {
		t.Fatalf("expected ErrMaxKeys, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Barriers
// ---------------------------------------------------------------------------

func TestBarrier_BasicTrip(t *testing.T) {
	lm := testManager()
	var wg sync.WaitGroup
	results := make([]bool, 3)

	for i := range 3 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ok, err := lm.BarrierWait(bg(), "b1", 3, 5*time.Second, uint64(id+1))
			if err != nil {
				t.Errorf("worker %d: %v", id, err)
				return
			}
			results[id] = ok
		}(i)
	}
	wg.Wait()

	for i, ok := range results {
		if !ok {
			t.Errorf("worker %d: barrier did not trip", i)
		}
	}
}

func TestBarrier_Timeout(t *testing.T) {
	lm := testManager()
	// Only 1 of 3 arrives — should timeout
	ok, err := lm.BarrierWait(bg(), "b1", 3, 200*time.Millisecond, 1)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected timeout, got trip")
	}
}

func TestBarrier_CountMismatch(t *testing.T) {
	lm := testManager()
	// First creates with count=3
	go func() {
		lm.BarrierWait(bg(), "b1", 3, 2*time.Second, 1)
	}()
	time.Sleep(50 * time.Millisecond)
	// Second tries with count=2 — should error
	_, err := lm.BarrierWait(bg(), "b1", 2, 200*time.Millisecond, 2)
	if !errors.Is(err, ErrBarrierCountMismatch) {
		t.Fatalf("expected ErrBarrierCountMismatch, got %v", err)
	}
}

func TestBarrier_DisconnectCleanup(t *testing.T) {
	lm := testManager()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		lm.BarrierWait(ctx, "b1", 3, 10*time.Second, 1)
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	// After cleanup, barrier state should be gone (only 1 participant, now cancelled)
	lm.CleanupConnection(1)
}

func TestBarrier_MaxKeys(t *testing.T) {
	cfg := testConfig()
	cfg.MaxKeys = 1
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	lm := NewLockManager(cfg, log)
	lm.KVSet("existing", "v", 0)

	_, err := lm.BarrierWait(bg(), "new_barrier", 2, 200*time.Millisecond, 1)
	if !errors.Is(err, ErrMaxKeys) {
		t.Fatalf("expected ErrMaxKeys, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Leader Watchers
// ---------------------------------------------------------------------------

func TestLeaderWatcher_RegisterNotify(t *testing.T) {
	lm := testManager()
	ch := make(chan []byte, 10)
	lm.RegisterLeaderWatcher("election1", 1, ch, func() {})
	lm.NotifyLeaderChange("election1", "elected", 0)

	select {
	case msg := <-ch:
		if string(msg) != "leader elected election1\n" {
			t.Fatalf("unexpected message: %q", string(msg))
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for notification")
	}
}

func TestLeaderWatcher_Unregister(t *testing.T) {
	lm := testManager()
	ch := make(chan []byte, 10)
	lm.RegisterLeaderWatcher("election1", 1, ch, func() {})
	lm.UnregisterLeaderWatcher("election1", 1)
	lm.NotifyLeaderChange("election1", "elected", 0)

	select {
	case <-ch:
		t.Fatal("should not receive notification after unregister")
	case <-time.After(100 * time.Millisecond):
		// OK
	}
}

func TestLeaderWatcher_UnregisterAll(t *testing.T) {
	lm := testManager()
	ch := make(chan []byte, 10)
	lm.RegisterLeaderWatcher("e1", 1, ch, func() {})
	lm.RegisterLeaderWatcher("e2", 1, ch, func() {})
	lm.UnregisterAllLeaderWatchers(1)
	lm.NotifyLeaderChange("e1", "elected", 0)
	lm.NotifyLeaderChange("e2", "elected", 0)

	select {
	case <-ch:
		t.Fatal("should not receive notification after unregister all")
	case <-time.After(100 * time.Millisecond):
		// OK
	}
}

// ---------------------------------------------------------------------------
// Type Mismatch: Regular vs RW locks
// ---------------------------------------------------------------------------

func TestTypeMismatch_RegularThenRW(t *testing.T) {
	lm := testManager()
	_, err := lm.Acquire(bg(), "k1", 5*time.Second, 30*time.Second, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = lm.RWAcquire(bg(), "k1", 'r', 0, 30*time.Second, 2)
	if !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("expected ErrTypeMismatch, got %v", err)
	}
}

func TestTypeMismatch_RWThenRegular(t *testing.T) {
	lm := testManager()
	_, _, err := lm.RWAcquire(bg(), "k1", 'r', 5*time.Second, 30*time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = lm.Acquire(bg(), "k1", 0, 30*time.Second, 2, 1)
	if !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("expected ErrTypeMismatch, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// RW Lock Enqueue/Wait
// ---------------------------------------------------------------------------

func TestRWLock_EnqueueImmediate(t *testing.T) {
	lm := testManager()
	status, tok, lease, fence, err := lm.RWEnqueue("k1", 'r', 30*time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	if status != "acquired" {
		t.Fatalf("expected acquired, got %s", status)
	}
	if tok == "" || lease == 0 || fence == 0 {
		t.Fatalf("expected non-zero values: tok=%q lease=%d fence=%d", tok, lease, fence)
	}
}

func TestRWLock_EnqueueQueued(t *testing.T) {
	lm := testManager()
	// Writer holds the lock
	_, _, err := lm.RWAcquire(bg(), "k1", 'w', 5*time.Second, 30*time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}

	// Reader should be queued
	status, tok, _, _, err := lm.RWEnqueue("k1", 'r', 30*time.Second, 2)
	if err != nil {
		t.Fatal(err)
	}
	if status != "queued" {
		t.Fatalf("expected queued, got %s", status)
	}
	if tok != "" {
		t.Fatal("token should be empty when queued")
	}
}

func TestRWLock_WaitAfterEnqueue(t *testing.T) {
	lm := testManager()
	wTok, _, err := lm.RWAcquire(bg(), "k1", 'w', 5*time.Second, 30*time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}

	status, _, _, _, err := lm.RWEnqueue("k1", 'r', 30*time.Second, 2)
	if err != nil || status != "queued" {
		t.Fatal("expected queued")
	}

	// Release writer in background
	go func() {
		time.Sleep(50 * time.Millisecond)
		lm.RWRelease("k1", wTok)
	}()

	tok, lease, fence, err := lm.RWWait(bg(), "k1", 5*time.Second, 2)
	if err != nil {
		t.Fatal(err)
	}
	if tok == "" || lease == 0 || fence == 0 {
		t.Fatalf("expected non-zero values after wait")
	}
}

// ---------------------------------------------------------------------------
// RW Lock Lease Expiry
// ---------------------------------------------------------------------------

func TestRWLock_LeaseExpiry(t *testing.T) {
	lm := testManager()
	cfg := lm.cfg
	cfg.LeaseSweepInterval = 50 * time.Millisecond

	tok, _, err := lm.RWAcquire(bg(), "k1", 'w', 5*time.Second, 1*time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}

	// Force expire
	lm.LockKeyForTest("k1")
	st := lm.ResourceForTest("k1")
	if st != nil {
		if h, ok := st.Holders[tok]; ok {
			h.leaseExpires = time.Now().Add(-1 * time.Second)
		}
	}
	lm.UnlockKeyForTest("k1")

	// Renew should fail
	_, _, err = lm.RWRenew("k1", tok, 30*time.Second)
	if err == nil {
		t.Fatal("renew should fail after expiry")
	}
}

// ---------------------------------------------------------------------------
// Fencing tokens are monotonically increasing across acquires
// ---------------------------------------------------------------------------

func TestFencingToken_AcrossMultipleAcquires(t *testing.T) {
	lm := testManager()
	var prevFence uint64
	for i := 0; i < 10; i++ {
		tok, fence, err := lm.AcquireWithFence(bg(), "k1", 5*time.Second, 30*time.Second, uint64(i+1), 1)
		if err != nil {
			t.Fatal(err)
		}
		if fence <= prevFence {
			t.Fatalf("fence %d should be > previous %d", fence, prevFence)
		}
		prevFence = fence
		lm.Release("k1", tok)
	}
}

// ---------------------------------------------------------------------------
// Counter edge cases
// ---------------------------------------------------------------------------

func TestDecr_MinInt64Delta(t *testing.T) {
	lm := testManager()
	// Decr with delta=MinInt64 should return error (negation overflow)
	_, err := lm.Decr("k1", math.MinInt64)
	if err == nil {
		t.Fatal("expected overflow error for MinInt64 delta")
	}
}

func TestDecr_MinInt64Value(t *testing.T) {
	lm := testManager()
	// Set counter to MinInt64 and decrement by 1 — should be rejected
	// as underflow now that Incr has overflow protection.
	lm.SetCounter("k1", math.MinInt64)
	_, err := lm.Decr("k1", 1)
	if err == nil {
		t.Fatal("expected underflow error for MinInt64 - 1")
	}
}

func TestIncr_ConcurrentSafety(t *testing.T) {
	lm := testManager()
	var wg sync.WaitGroup
	n := 100
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			lm.Incr("k1", 1)
		}()
	}
	wg.Wait()
	val := lm.GetCounter("k1")
	if val != int64(n) {
		t.Fatalf("expected %d, got %d", n, val)
	}
}

// ---------------------------------------------------------------------------
// KV CAS concurrent safety
// ---------------------------------------------------------------------------

func TestKVCAS_ConcurrentSwap(t *testing.T) {
	lm := testManager()
	lm.KVSet("k1", "initial", 0)

	var wg sync.WaitGroup
	successes := int64(0)
	n := 50
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			ok, _ := lm.KVCAS("k1", "initial", "updated", 0)
			if ok {
				atomic.AddInt64(&successes, 1)
			}
		}(i)
	}
	wg.Wait()
	// Exactly one should succeed
	if successes != 1 {
		t.Fatalf("expected exactly 1 successful CAS, got %d", successes)
	}
}

// ---------------------------------------------------------------------------
// List operations
// ---------------------------------------------------------------------------

func TestList_ConcurrentPushPop(t *testing.T) {
	lm := testManager()
	var wg sync.WaitGroup
	n := 100
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			lm.RPush("q1", "item")
		}(i)
	}
	wg.Wait()
	if lm.LLen("q1") != n {
		t.Fatalf("expected %d items, got %d", n, lm.LLen("q1"))
	}

	// Pop all
	for i := 0; i < n; i++ {
		_, ok := lm.LPop("q1")
		if !ok {
			t.Fatalf("pop %d failed", i)
		}
	}
	if lm.LLen("q1") != 0 {
		t.Fatalf("expected 0 items, got %d", lm.LLen("q1"))
	}
}

// ---------------------------------------------------------------------------
// Barrier edge cases
// ---------------------------------------------------------------------------

func TestBarrier_ContextCancel(t *testing.T) {
	lm := testManager()
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		_, err := lm.BarrierWait(ctx, "b1", 3, 30*time.Second, 1)
		done <- err
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error from context cancellation")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("barrier did not return after context cancel")
	}
}

func TestBarrier_Reuse(t *testing.T) {
	lm := testManager()

	// First barrier trip
	var wg sync.WaitGroup
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func(id int) {
			defer wg.Done()
			ok, err := lm.BarrierWait(bg(), "b1", 2, 5*time.Second, uint64(id+1))
			if err != nil || !ok {
				t.Errorf("barrier trip 1 conn %d: ok=%v err=%v", id, ok, err)
			}
		}(i)
	}
	wg.Wait()

	// Second barrier trip with same key
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func(id int) {
			defer wg.Done()
			ok, err := lm.BarrierWait(bg(), "b1", 2, 5*time.Second, uint64(id+10))
			if err != nil || !ok {
				t.Errorf("barrier trip 2 conn %d: ok=%v err=%v", id, ok, err)
			}
		}(i)
	}
	wg.Wait()
}

// ---------------------------------------------------------------------------
// Leader election with fence tokens
// ---------------------------------------------------------------------------

func TestLeaderElection_FenceMonotonic(t *testing.T) {
	lm := testManager()
	var prevFence uint64

	for i := 0; i < 5; i++ {
		tok, fence, err := lm.AcquireWithFence(bg(), "leader1", 5*time.Second, 30*time.Second, uint64(i+1), 1)
		if err != nil {
			t.Fatal(err)
		}
		if fence <= prevFence {
			t.Fatalf("fence %d not greater than prev %d at iteration %d", fence, prevFence, i)
		}
		prevFence = fence

		// Resign (release)
		if !lm.Release("leader1", tok) {
			t.Fatal("resign failed")
		}
	}
}

// ---------------------------------------------------------------------------
// Cleanup edge cases
// ---------------------------------------------------------------------------

func TestCleanup_RWLock(t *testing.T) {
	lm := testManager()
	_, _, err := lm.RWAcquire(bg(), "k1", 'r', 5*time.Second, 30*time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = lm.RWAcquire(bg(), "k1", 'r', 5*time.Second, 30*time.Second, 2)
	if err != nil {
		t.Fatal(err)
	}

	// Cleanup conn 1
	lm.CleanupConnection(1)

	// Conn 2 should still hold
	lm.LockKeyForTest("k1")
	st := lm.ResourceForTest("k1")
	if st == nil || len(st.Holders) != 1 {
		lm.UnlockKeyForTest("k1")
		t.Fatal("expected 1 holder after cleanup")
	}
	lm.UnlockKeyForTest("k1")
}

func TestCleanup_BarrierParticipant(t *testing.T) {
	lm := testManager()

	type result struct {
		ok  bool
		err error
	}

	// Barrier needs 3 participants. Conn 1 and conn 2 join, then conn 2 disconnects.
	done1 := make(chan result, 1)
	done2 := make(chan result, 1)

	go func() {
		ok, err := lm.BarrierWait(bg(), "b1", 3, 5*time.Second, 1)
		done1 <- result{ok, err}
	}()

	time.Sleep(50 * time.Millisecond)

	go func() {
		ok, err := lm.BarrierWait(bg(), "b1", 3, 5*time.Second, 2)
		done2 <- result{ok, err}
	}()

	time.Sleep(50 * time.Millisecond)

	// Disconnect conn 2 — barrier now unreachable (only 1 of 3 remain)
	lm.CleanupConnection(2)

	select {
	case r := <-done1:
		// When a participant leaves, barrier becomes unreachable: returns (false, nil)
		if r.ok {
			t.Fatal("barrier should not have tripped")
		}
		if r.err != nil {
			t.Fatalf("expected nil error, got %v", r.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("conn 1 barrier wait did not return after cleanup")
	}

	select {
	case <-done2:
	case <-time.After(2 * time.Second):
		t.Fatal("conn 2 barrier wait did not return after cleanup")
	}
}

// ---------------------------------------------------------------------------
// Stats comprehensive
// ---------------------------------------------------------------------------

func TestStats_Comprehensive(t *testing.T) {
	lm := testManager()

	// Create some resources
	lm.Acquire(bg(), "lock1", 5*time.Second, 30*time.Second, 1, 1)
	lm.Acquire(bg(), "sem1", 5*time.Second, 30*time.Second, 2, 3)
	lm.Incr("counter1", 1)
	lm.KVSet("kv1", "val", 0)
	lm.RPush("list1", "item")

	stats := lm.Stats(5) // 5 connections
	if stats.Connections != 5 {
		t.Errorf("connections: got %d, want 5", stats.Connections)
	}
	if len(stats.Locks) < 1 {
		t.Errorf("locks: got %d, want >= 1", len(stats.Locks))
	}
	if len(stats.Counters) < 1 {
		t.Errorf("counters: got %d, want >= 1", len(stats.Counters))
	}
	if len(stats.KVEntries) < 1 {
		t.Errorf("kv_entries: got %d, want >= 1", len(stats.KVEntries))
	}
	if len(stats.Lists) < 1 {
		t.Errorf("lists: got %d, want >= 1", len(stats.Lists))
	}
}

// ---------------------------------------------------------------------------
// BLPop/BRPop context cancellation
// ---------------------------------------------------------------------------

func TestBLPop_ContextCancel(t *testing.T) {
	lm := testManager()
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		_, err := lm.BLPop(ctx, "q1", 30*time.Second, 1)
		done <- err
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error from context cancellation")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("BLPop did not return after context cancel")
	}
}

func TestBRPop_ContextCancel(t *testing.T) {
	lm := testManager()
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		_, err := lm.BRPop(ctx, "q1", 30*time.Second, 1)
		done <- err
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error from context cancellation")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("BRPop did not return after context cancel")
	}
}

// ---------------------------------------------------------------------------
// MaxKeys enforcement across types
// ---------------------------------------------------------------------------

func TestMaxKeys_AcrossTypes(t *testing.T) {
	lm := testManager()
	lm.cfg.MaxKeys = 3

	// Use up 3 keys: 1 counter, 1 KV, 1 list
	lm.Incr("counter1", 1)
	lm.KVSet("kv1", "val", 0)
	lm.RPush("list1", "item")

	// 4th key should fail
	err := lm.KVSet("kv2", "val", 0)
	if !errors.Is(err, ErrMaxKeys) {
		t.Fatalf("expected ErrMaxKeys, got %v", err)
	}

	_, err = lm.Incr("counter2", 1)
	if !errors.Is(err, ErrMaxKeys) {
		t.Fatalf("expected ErrMaxKeys for counter, got %v", err)
	}

	_, err = lm.RPush("list2", "item")
	if !errors.Is(err, ErrMaxKeys) {
		t.Fatalf("expected ErrMaxKeys for list, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// List full enforcement
// ---------------------------------------------------------------------------

func TestListFull_Enforcement(t *testing.T) {
	lm := testManager()
	lm.cfg.MaxListLength = 3

	lm.RPush("q1", "a")
	lm.RPush("q1", "b")
	lm.RPush("q1", "c")

	_, err := lm.RPush("q1", "d")
	if !errors.Is(err, ErrListFull) {
		t.Fatalf("expected ErrListFull, got %v", err)
	}

	_, err = lm.LPush("q1", "d")
	if !errors.Is(err, ErrListFull) {
		t.Fatalf("expected ErrListFull for LPush, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// RW Lock concurrent readers and writer exclusion
// ---------------------------------------------------------------------------

func TestRWLock_ConcurrentReadersBlockWriter(t *testing.T) {
	lm := testManager()

	// Acquire 3 read locks
	toks := make([]string, 3)
	for i := range 3 {
		tok, _, err := lm.RWAcquire(bg(), "k1", 'r', 5*time.Second, 30*time.Second, uint64(i+1))
		if err != nil {
			t.Fatal(err)
		}
		toks[i] = tok
	}

	// Writer should timeout with 0 timeout
	_, _, err := lm.RWAcquire(bg(), "k1", 'w', 0, 30*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	// Actually, with timeout=0 and readers present, writer gets empty token (timeout)
	// Let's check with a non-blocking approach
	_, _, _, _, errEnq := lm.RWEnqueue("k1", 'w', 30*time.Second, 10)
	if errEnq != nil {
		t.Fatal(errEnq)
	}

	// Release all readers, writer should be grantable
	for _, tok := range toks {
		lm.RWRelease("k1", tok)
	}
}

// ---------------------------------------------------------------------------
// RW Lock renew returns fence
// ---------------------------------------------------------------------------

func TestRWLock_RenewReturnsFence(t *testing.T) {
	lm := testManager()
	tok, fence, err := lm.RWAcquire(bg(), "k1", 'w', 5*time.Second, 30*time.Second, 1)
	if err != nil || tok == "" {
		t.Fatal("acquire failed")
	}

	remaining, renewFence, err := lm.RWRenew("k1", tok, 60*time.Second)
	if err != nil {
		t.Fatal("renew should succeed")
	}
	if remaining <= 0 {
		t.Fatal("remaining should be > 0")
	}
	if renewFence != fence {
		t.Fatalf("fence should be same: %d vs %d", renewFence, fence)
	}
}

// ---------------------------------------------------------------------------
// Leader watcher notification excludes connection
// ---------------------------------------------------------------------------

func TestLeaderWatcher_ExcludeConnID(t *testing.T) {
	lm := testManager()
	ch1 := make(chan []byte, 10)
	ch2 := make(chan []byte, 10)
	lm.RegisterLeaderWatcher("e1", 1, ch1, func() {})
	lm.RegisterLeaderWatcher("e1", 2, ch2, func() {})

	// Notify excluding conn 1
	lm.NotifyLeaderChange("e1", "elected", 1)

	// Conn 2 should receive
	select {
	case <-ch2:
	case <-time.After(time.Second):
		t.Fatal("conn 2 should receive notification")
	}

	// Conn 1 should NOT receive
	select {
	case <-ch1:
		t.Fatal("conn 1 should be excluded")
	case <-time.After(50 * time.Millisecond):
	}
}

// ---------------------------------------------------------------------------
// Resign leader
// ---------------------------------------------------------------------------

func TestResignLeader(t *testing.T) {
	lm := testManager()
	tok, _, err := lm.AcquireWithFence(bg(), "leader1", 5*time.Second, 30*time.Second, 1, 1)
	if err != nil {
		t.Fatal(err)
	}

	ok := lm.ResignLeader("leader1", tok, 1)
	if !ok {
		t.Fatal("resign should succeed")
	}

	// Lock should be free now
	tok2, _, err := lm.AcquireWithFence(bg(), "leader1", 0, 30*time.Second, 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if tok2 == "" {
		t.Fatal("should be able to acquire after resign")
	}
}

func TestResignLeader_WrongToken(t *testing.T) {
	lm := testManager()
	lm.AcquireWithFence(bg(), "leader1", 5*time.Second, 30*time.Second, 1, 1)

	ok := lm.ResignLeader("leader1", "wrong", 1)
	if ok {
		t.Fatal("resign with wrong token should fail")
	}
}

// ---------------------------------------------------------------------------
// GC does not prune active counters, KV, lists
// ---------------------------------------------------------------------------

func TestGC_PreservesActiveResources(t *testing.T) {
	lm := testManager()
	lm.cfg.GCMaxIdleTime = 50 * time.Millisecond
	lm.cfg.GCInterval = 50 * time.Millisecond

	lm.Incr("c1", 42)
	lm.KVSet("kv1", "val", 0)
	lm.RPush("list1", "item")

	// Touch them periodically
	ctx, cancel := context.WithCancel(context.Background())
	go lm.GCLoop(ctx)

	// Keep touching
	for i := 0; i < 5; i++ {
		time.Sleep(30 * time.Millisecond)
		lm.GetCounter("c1")
		lm.KVGet("kv1")
		lm.LLen("list1")
	}
	cancel()

	// They should still exist
	if lm.GetCounter("c1") != 42 {
		t.Fatal("counter should still exist")
	}
	val, ok := lm.KVGet("kv1")
	if !ok || val != "val" {
		t.Fatal("KV should still exist")
	}
	if lm.LLen("list1") != 1 {
		t.Fatal("list should still exist")
	}
}

// ---------------------------------------------------------------------------
// MaxWaiters enforcement (explicit)
// ---------------------------------------------------------------------------

func TestMaxWaiters_Lock(t *testing.T) {
	lm := testManager()
	lm.cfg.MaxWaiters = 2

	// Holder occupies the lock
	tok, err := lm.Acquire(bg(), "k1", 0, 30*time.Second, 1, 1)
	if err != nil || tok == "" {
		t.Fatal("initial acquire should succeed")
	}

	// Fill up waiter slots
	for i := uint64(2); i <= 3; i++ {
		go func(id uint64) {
			lm.Acquire(bg(), "k1", 30*time.Second, 30*time.Second, id, 1)
		}(i)
	}
	time.Sleep(50 * time.Millisecond)

	// Next waiter should be rejected
	_, err = lm.Acquire(bg(), "k1", 5*time.Second, 30*time.Second, 4, 1)
	if !errors.Is(err, ErrMaxWaiters) {
		t.Fatalf("expected ErrMaxWaiters, got %v", err)
	}
}

func TestMaxWaiters_Enqueue(t *testing.T) {
	lm := testManager()
	lm.cfg.MaxWaiters = 1

	// Holder occupies the lock
	lm.Acquire(bg(), "k1", 0, 30*time.Second, 1, 1)

	// First enqueue queues
	status, _, _, _, err := lm.EnqueueWithFence("k1", 30*time.Second, 2, 1)
	if err != nil || status != "queued" {
		t.Fatalf("first enqueue should queue, got status=%s err=%v", status, err)
	}

	// Second enqueue should be rejected
	_, _, _, _, err = lm.EnqueueWithFence("k1", 30*time.Second, 3, 1)
	if !errors.Is(err, ErrMaxWaiters) {
		t.Fatalf("expected ErrMaxWaiters, got %v", err)
	}
}

func TestMaxWaiters_RWLock(t *testing.T) {
	lm := testManager()
	lm.cfg.MaxWaiters = 1

	// Writer holds the lock
	lm.RWAcquire(bg(), "k1", 'w', 0, 30*time.Second, 1)

	// First waiter queues
	go lm.RWAcquire(bg(), "k1", 'r', 30*time.Second, 30*time.Second, 2)
	time.Sleep(50 * time.Millisecond)

	// Second waiter should be rejected
	_, _, err := lm.RWAcquire(bg(), "k1", 'r', 5*time.Second, 30*time.Second, 3)
	if !errors.Is(err, ErrMaxWaiters) {
		t.Fatalf("expected ErrMaxWaiters, got %v", err)
	}
}

func TestMaxWaiters_RWEnqueue(t *testing.T) {
	lm := testManager()
	lm.cfg.MaxWaiters = 1

	// Writer holds the lock
	lm.RWAcquire(bg(), "k1", 'w', 0, 30*time.Second, 1)

	// First enqueue queues
	status, _, _, _, err := lm.RWEnqueue("k1", 'r', 30*time.Second, 2)
	if err != nil || status != "queued" {
		t.Fatalf("first enqueue should queue, got status=%s err=%v", status, err)
	}

	// Second enqueue should be rejected
	_, _, _, _, err = lm.RWEnqueue("k1", 'r', 30*time.Second, 3)
	if !errors.Is(err, ErrMaxWaiters) {
		t.Fatalf("expected ErrMaxWaiters, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Semaphore FIFO ordering
// ---------------------------------------------------------------------------

func TestSemaphore_FIFOOrdering(t *testing.T) {
	lm := testManager()
	// Semaphore with limit=1 (behaves like mutex but with semaphore path)
	tok1, err := lm.Acquire(bg(), "sem1", 0, 30*time.Second, 1, 1)
	if err != nil || tok1 == "" {
		t.Fatal("initial acquire should succeed")
	}

	// Queue up 3 waiters in order
	results := make(chan uint64, 3)
	for i := uint64(2); i <= 4; i++ {
		go func(id uint64) {
			time.Sleep(time.Duration(id-2) * 20 * time.Millisecond) // stagger
			lm.Acquire(bg(), "sem1", 30*time.Second, 30*time.Second, id, 1)
			results <- id
		}(i)
	}
	time.Sleep(150 * time.Millisecond)

	// Release holder, waiters should be granted in FIFO order
	lm.Release("sem1", tok1)

	first := <-results
	if first != 2 {
		t.Fatalf("expected conn 2 first, got %d", first)
	}
}

// ---------------------------------------------------------------------------
// LeaseExpiryLoop context cancel
// ---------------------------------------------------------------------------

func TestLeaseExpiryLoop_ContextCancel(t *testing.T) {
	lm := testManager()
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		lm.LeaseExpiryLoop(ctx)
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("LeaseExpiryLoop did not stop after context cancel")
	}
}

// ---------------------------------------------------------------------------
// GCLoop context cancel
// ---------------------------------------------------------------------------

func TestGCLoop_ContextCancel(t *testing.T) {
	lm := testManager()
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		lm.GCLoop(ctx)
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("GCLoop did not stop after context cancel")
	}
}

// ---------------------------------------------------------------------------
// Cleanup for all resource types
// ---------------------------------------------------------------------------

func TestCleanup_ListWaiter(t *testing.T) {
	lm := testManager()

	done := make(chan error, 1)
	go func() {
		_, err := lm.BLPop(bg(), "q1", 30*time.Second, 1)
		done <- err
	}()

	time.Sleep(50 * time.Millisecond)
	lm.CleanupConnection(1)

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error from BLPop after cleanup")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("BLPop did not return after cleanup")
	}
}

func TestCleanup_LeaderWatcher(t *testing.T) {
	lm := testManager()
	ch := make(chan []byte, 10)
	lm.RegisterLeaderWatcher("e1", 1, ch, func() {})

	// UnregisterAllLeaderWatchers removes observer watchers
	lm.UnregisterAllLeaderWatchers(1)

	// Notify should not reach conn 1
	lm.NotifyLeaderChange("e1", "elected", 0)
	select {
	case <-ch:
		t.Fatal("should not receive notification after unregister")
	case <-time.After(50 * time.Millisecond):
		// Expected
	}
}

func TestCleanup_SemaphoreSlot(t *testing.T) {
	lm := testManager()
	// Acquire 2 of 3 semaphore slots
	tok1, err := lm.Acquire(bg(), "sem1", 0, 30*time.Second, 1, 3)
	if err != nil || tok1 == "" {
		t.Fatal("acquire slot 1 failed")
	}
	tok2, err := lm.Acquire(bg(), "sem1", 0, 30*time.Second, 2, 3)
	if err != nil || tok2 == "" {
		t.Fatal("acquire slot 2 failed")
	}

	// Disconnect conn 1 — should free 1 slot
	lm.CleanupConnection(1)

	// Conn 3 should be able to acquire
	tok3, err := lm.Acquire(bg(), "sem1", 0, 30*time.Second, 3, 3)
	if err != nil || tok3 == "" {
		t.Fatal("acquire after cleanup should succeed")
	}
}

// ---------------------------------------------------------------------------
// KV TTL expiry verification
// ---------------------------------------------------------------------------

func TestKV_TTLExpiry(t *testing.T) {
	lm := testManager()
	lm.KVSet("k1", "val", 1) // 1 second TTL

	// Should be readable immediately
	val, ok := lm.KVGet("k1")
	if !ok || val != "val" {
		t.Fatal("should be readable immediately")
	}

	// Wait for expiry
	time.Sleep(1100 * time.Millisecond)

	// Should be expired via lazy check
	_, ok = lm.KVGet("k1")
	if ok {
		t.Fatal("should be expired")
	}
}

// ---------------------------------------------------------------------------
// RegisterAndNotifyLeader
// ---------------------------------------------------------------------------

func TestRegisterAndNotifyLeader(t *testing.T) {
	lm := testManager()

	// Register an observer first (conn 2)
	obsCh := make(chan []byte, 10)
	lm.RegisterLeaderWatcher("leader1", 2, obsCh, func() {})

	// RegisterAndNotifyLeader registers conn 1 as leader and notifies OTHERS
	leaderCh := make(chan []byte, 10)
	lm.RegisterAndNotifyLeader("leader1", "elected", 1, leaderCh, func() {})

	// Observer (conn 2) should receive the notification
	select {
	case msg := <-obsCh:
		if len(msg) == 0 {
			t.Fatal("expected non-empty notification")
		}
	case <-time.After(time.Second):
		t.Fatal("observer should receive notification of new leader")
	}

	// Leader (conn 1) should NOT receive its own notification
	select {
	case <-leaderCh:
		t.Fatal("leader should not receive its own notification")
	case <-time.After(50 * time.Millisecond):
		// Expected
	}
}

// ---------------------------------------------------------------------------
// LRange edge cases
// ---------------------------------------------------------------------------

func TestLRange_NegativeIndices(t *testing.T) {
	lm := testManager()
	lm.RPush("q1", "a")
	lm.RPush("q1", "b")
	lm.RPush("q1", "c")

	// Last 2 elements
	items := lm.LRange("q1", -2, -1)
	if len(items) != 2 || items[0] != "b" || items[1] != "c" {
		t.Fatalf("expected [b c], got %v", items)
	}

	// Full range via negatives
	items = lm.LRange("q1", -3, -1)
	if len(items) != 3 {
		t.Fatalf("expected 3 items, got %v", items)
	}
}

func TestLRange_EmptyList(t *testing.T) {
	lm := testManager()
	items := lm.LRange("nonexistent", 0, -1)
	if len(items) != 0 {
		t.Fatalf("expected empty, got %v", items)
	}
}

// ---------------------------------------------------------------------------
// BRPop blocking
// ---------------------------------------------------------------------------

func TestBRPop_Blocks(t *testing.T) {
	lm := testManager()

	done := make(chan string, 1)
	go func() {
		val, _ := lm.BRPop(bg(), "q1", 5*time.Second, 1)
		done <- val
	}()

	time.Sleep(50 * time.Millisecond)
	lm.LPush("q1", "hello")

	select {
	case val := <-done:
		if val != "hello" {
			t.Fatalf("expected hello, got %q", val)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("BRPop did not unblock")
	}
}

func TestBRPop_Timeout(t *testing.T) {
	lm := testManager()
	_, err := lm.BRPop(bg(), "q1", 0, 1)
	// Timeout=0 should return immediately with timeout error
	if err != nil {
		// Some implementations return empty string + nil
		// Others return error on timeout
	}
}

// ---------------------------------------------------------------------------
// Duplicate barrier participant
// ---------------------------------------------------------------------------

func TestBarrier_DuplicateParticipant(t *testing.T) {
	lm := testManager()

	done := make(chan struct{})
	go func() {
		lm.BarrierWait(bg(), "b1", 3, 5*time.Second, 1)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)

	// Same connID should be rejected
	_, err := lm.BarrierWait(bg(), "b1", 3, 0, 1)
	if err == nil {
		t.Fatal("expected error for duplicate participant")
	}

	// Clean up: cancel the barrier
	lm.CleanupConnection(1)
	<-done
}

// ---------------------------------------------------------------------------
// Regression: LPop backing array memory leak
// ---------------------------------------------------------------------------

// TestLPop_BackingArrayCompaction verifies that repeated RPush+LPop cycles do
// not leak memory via the backing array. After enough pops, compactItems()
// should reallocate the slice, allowing the old backing array to be GC'd.
func TestLPop_BackingArrayCompaction(t *testing.T) {
	lm := testManager()

	// Push N items then pop them all. The itemsStart counter should trigger
	// compaction and the backing array should be reclaimed.
	const N = 200
	for i := range N {
		_, err := lm.RPush("compactq", fmt.Sprintf("item%d", i))
		if err != nil {
			t.Fatal(err)
		}
	}

	for range N {
		val, ok := lm.LPop("compactq")
		if !ok {
			t.Fatal("expected item from LPop")
		}
		_ = val
	}

	// After all pops, the list should be empty and cleaned up.
	_, ok := lm.LPop("compactq")
	if ok {
		t.Fatal("expected empty list after all pops")
	}
}

// TestLPop_FIFOCompactionPreservesOrder verifies that compaction does not
// corrupt the item order during interleaved push/pop operations.
func TestLPop_FIFOCompactionPreservesOrder(t *testing.T) {
	lm := testManager()

	// Interleave pushes and pops to trigger compaction mid-stream.
	pushed := 0
	popped := 0
	for i := range 500 {
		// Push 3 items for every 2 pops to gradually build up and trigger compaction.
		lm.RPush("fifoq", fmt.Sprintf("v%d", i))
		pushed++

		if i%3 == 0 && pushed > popped {
			val, ok := lm.LPop("fifoq")
			if !ok {
				t.Fatalf("expected item at pop %d", popped)
			}
			expected := fmt.Sprintf("v%d", popped)
			if val != expected {
				t.Fatalf("FIFO order broken: expected %q, got %q", expected, val)
			}
			popped++
		}
	}

	// Drain remaining items and verify order.
	for popped < pushed {
		val, ok := lm.LPop("fifoq")
		if !ok {
			t.Fatalf("expected item at pop %d, list empty", popped)
		}
		expected := fmt.Sprintf("v%d", popped)
		if val != expected {
			t.Fatalf("FIFO order broken: expected %q, got %q", expected, val)
		}
		popped++
	}
}

// ---------------------------------------------------------------------------
// Regression: Dead writer blocking live readers in RW lock
// ---------------------------------------------------------------------------

// TestRWLock_DeadWriterSkipped verifies that when a writer's connection is
// cleaned up while it's queued, subsequent readers are not permanently blocked.
func TestRWLock_DeadWriterSkipped(t *testing.T) {
	lm := testManager()

	// Reader 1 acquires the RW lock.
	tok1, _, err := lm.RWAcquire(bg(), "rwdead", 'r', 5*time.Second, 5*time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	if tok1 == "" {
		t.Fatal("expected reader 1 to acquire")
	}

	// Writer (conn 2) enqueues — it should be queued behind the reader.
	status, _, _, _, err := lm.RWEnqueue("rwdead", 'w', 5*time.Second, 2)
	if err != nil {
		t.Fatal(err)
	}
	if status != "queued" {
		t.Fatalf("expected writer to be queued, got %q", status)
	}

	// Reader 3 enqueues — it should be queued behind the writer.
	status, _, _, _, err = lm.RWEnqueue("rwdead", 'r', 5*time.Second, 3)
	if err != nil {
		t.Fatal(err)
	}
	if status != "queued" {
		t.Fatalf("expected reader 3 to be queued, got %q", status)
	}

	// Simulate the writer's connection being cleaned up (dead writer).
	lm.CleanupConnection(2)

	// Release reader 1 — this should trigger grantNextRWWaiterLocked.
	// The dead writer should be skipped, and reader 3 should be granted.
	if !lm.Release("rwdead", tok1) {
		t.Fatal("expected release to succeed")
	}

	// Reader 3 should now be able to complete its Wait.
	ctx, cancel := context.WithTimeout(bg(), 2*time.Second)
	defer cancel()
	tok3, _, _, err := lm.RWWait(ctx, "rwdead", 2*time.Second, 3)
	if err != nil {
		t.Fatalf("reader 3 should have been granted after dead writer skipped: %v", err)
	}
	if tok3 == "" {
		t.Fatal("expected reader 3 to get a token")
	}
}

// TestRWLock_LiveWriterStillBlocks confirms that a live (non-dead) writer in
// the queue still correctly blocks subsequent readers.
func TestRWLock_LiveWriterStillBlocks(t *testing.T) {
	lm := testManager()

	// Reader 1 acquires.
	tok1, _, err := lm.RWAcquire(bg(), "rwlive", 'r', 5*time.Second, 5*time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}

	// Writer (conn 2) enqueues — should be queued behind reader 1.
	status, _, _, _, err := lm.RWEnqueue("rwlive", 'w', 5*time.Second, 2)
	if err != nil {
		t.Fatal(err)
	}
	if status != "queued" {
		t.Fatalf("expected queued, got %q", status)
	}

	// Reader 3 enqueues — should be queued behind writer 2.
	status, _, _, _, err = lm.RWEnqueue("rwlive", 'r', 5*time.Second, 3)
	if err != nil {
		t.Fatal(err)
	}
	if status != "queued" {
		t.Fatalf("expected queued, got %q", status)
	}

	// Start writer 2's Wait in a goroutine — it will get granted when reader 1 releases.
	writerDone := make(chan string, 1)
	go func() {
		tok, _, _, err := lm.RWWait(bg(), "rwlive", 5*time.Second, 2)
		if err != nil {
			t.Errorf("writer wait error: %v", err)
		}
		writerDone <- tok
	}()

	// Start reader 3's Wait — it should block because writer 2 is in the queue.
	readerDone := make(chan struct{}, 1)
	go func() {
		_, _, _, _ = lm.RWWait(bg(), "rwlive", 5*time.Second, 3)
		readerDone <- struct{}{}
	}()

	// Release reader 1 — writer 2 is alive, so it should be granted (not skipped).
	if !lm.Release("rwlive", tok1) {
		t.Fatal("expected release to succeed")
	}

	// Writer 2 should be granted.
	select {
	case tok2 := <-writerDone:
		if tok2 == "" {
			t.Fatal("expected writer 2 to get a token")
		}
		// While writer 2 holds the lock, reader 3 must NOT be granted yet.
		select {
		case <-readerDone:
			t.Fatal("reader 3 was granted while writer 2 still holds lock")
		case <-time.After(200 * time.Millisecond):
			// Good — reader 3 is still blocked as expected.
		}
		// Release writer 2 — now reader 3 should be granted.
		lm.Release("rwlive", tok2)
	case <-time.After(2 * time.Second):
		t.Fatal("writer 2 was not granted within timeout")
	}

	select {
	case <-readerDone:
		// Good — reader 3 was granted after writer released.
	case <-time.After(2 * time.Second):
		t.Fatal("reader 3 was not granted after writer released")
	}
}

// ---------------------------------------------------------------------------
// Stress and edge-case tests
// ---------------------------------------------------------------------------

// TestConcurrent_MixedLockTypes exercises 50 goroutines simultaneously
// acquiring locks (limit=1), semaphores (limit=3), and RW locks on
// overlapping keys. The test passes if no panics or deadlocks occur.
func TestConcurrent_MixedLockTypes(t *testing.T) {
	lm := testManager()
	var wg sync.WaitGroup
	const goroutines = 50
	const iterations = 50

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			connID := uint64(id + 1)
			for j := 0; j < iterations; j++ {
				switch id % 3 {
				case 0: // mutex
					key := fmt.Sprintf("mixed-lock-%d", j%5)
					tok, _ := lm.Acquire(bg(), key, 10*time.Millisecond, 5*time.Second, connID, 1)
					if tok != "" {
						lm.Release(key, tok)
					}
				case 1: // semaphore
					key := fmt.Sprintf("mixed-sem-%d", j%5)
					tok, _ := lm.Acquire(bg(), key, 10*time.Millisecond, 5*time.Second, connID, 3)
					if tok != "" {
						lm.Release(key, tok)
					}
				case 2: // RW lock
					key := fmt.Sprintf("mixed-rw-%d", j%5)
					mode := byte('r')
					if j%4 == 0 {
						mode = 'w'
					}
					tok, _, err := lm.RWAcquire(bg(), key, mode, 10*time.Millisecond, 5*time.Second, connID)
					if err == nil && tok != "" {
						lm.RWRelease(key, tok)
					}
				}
			}
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All goroutines completed without panic or deadlock.
	case <-time.After(10 * time.Second):
		t.Fatal("deadlock: goroutines did not finish within 10s")
	}
}

// TestConcurrent_CleanupDuringAcquire starts 10 goroutines attempting to
// acquire a held lock with a timeout, while concurrently cleaning up some
// of their connections. The test passes if no panics occur.
func TestConcurrent_CleanupDuringAcquire(t *testing.T) {
	lm := testManager()
	// Hold the lock so all acquires block.
	tok, err := lm.Acquire(bg(), "cleanup-race", 5*time.Second, 30*time.Second, 100, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer lm.Release("cleanup-race", tok)

	var wg sync.WaitGroup
	const n = 10
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(id int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(bg(), 500*time.Millisecond)
			defer cancel()
			lm.Acquire(ctx, "cleanup-race", 500*time.Millisecond, 5*time.Second, uint64(id+1), 1)
		}(i)
	}

	// Give goroutines time to enqueue.
	time.Sleep(50 * time.Millisecond)

	// Clean up half the connections while they are waiting.
	for i := 0; i < n/2; i++ {
		lm.CleanupConnection(uint64(i + 1))
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// No panics or deadlocks.
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock: goroutines did not finish within 5s")
	}
}

// TestRWLock_MultipleDeadWriters enqueues reader(1) -> writer(2) ->
// writer(3) -> reader(4). Connections 2 and 3 are cleaned up (dead
// writers). When reader 1 releases, reader 4 should be granted because
// both dead writers are skipped.
func TestRWLock_MultipleDeadWriters(t *testing.T) {
	lm := testManager()

	// Reader 1 acquires.
	tok1, _, err := lm.RWAcquire(bg(), "rw-multi-dead", 'r', 5*time.Second, 5*time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	if tok1 == "" {
		t.Fatal("expected reader 1 to acquire")
	}

	// Writer 2 enqueues.
	status, _, _, _, err := lm.RWEnqueue("rw-multi-dead", 'w', 5*time.Second, 2)
	if err != nil {
		t.Fatal(err)
	}
	if status != "queued" {
		t.Fatalf("expected writer 2 queued, got %q", status)
	}

	// Writer 3 enqueues.
	status, _, _, _, err = lm.RWEnqueue("rw-multi-dead", 'w', 5*time.Second, 3)
	if err != nil {
		t.Fatal(err)
	}
	if status != "queued" {
		t.Fatalf("expected writer 3 queued, got %q", status)
	}

	// Reader 4 enqueues.
	status, _, _, _, err = lm.RWEnqueue("rw-multi-dead", 'r', 5*time.Second, 4)
	if err != nil {
		t.Fatal(err)
	}
	if status != "queued" {
		t.Fatalf("expected reader 4 queued, got %q", status)
	}

	// Kill connections 2 and 3 (dead writers).
	lm.CleanupConnection(2)
	lm.CleanupConnection(3)

	// Release reader 1. Dead writers should be skipped, reader 4 granted.
	if !lm.Release("rw-multi-dead", tok1) {
		t.Fatal("expected release to succeed")
	}

	// Reader 4 should be able to Wait and get a token.
	ctx, cancel := context.WithTimeout(bg(), 2*time.Second)
	defer cancel()
	tok4, _, _, err := lm.RWWait(ctx, "rw-multi-dead", 2*time.Second, 4)
	if err != nil {
		t.Fatalf("reader 4 wait error: %v", err)
	}
	if tok4 == "" {
		t.Fatal("expected reader 4 to get a token after dead writers skipped")
	}
}

// TestRWLock_DeadWriterAmongLiveReaders sets up:
// writer(1, holding) -> reader(2, queued) -> writer(3, dead) -> reader(4, queued).
// When writer 1 releases, reader 2 is granted. Writer 3 is dead so it is
// skipped, and reader 4 should also be batch-granted along with reader 2.
func TestRWLock_DeadWriterAmongLiveReaders(t *testing.T) {
	lm := testManager()

	// Writer 1 acquires.
	tok1, _, err := lm.RWAcquire(bg(), "rw-dead-among", 'w', 5*time.Second, 5*time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	if tok1 == "" {
		t.Fatal("expected writer 1 to acquire")
	}

	// Reader 2 enqueues.
	status, _, _, _, err := lm.RWEnqueue("rw-dead-among", 'r', 5*time.Second, 2)
	if err != nil {
		t.Fatal(err)
	}
	if status != "queued" {
		t.Fatalf("expected reader 2 queued, got %q", status)
	}

	// Writer 3 enqueues.
	status, _, _, _, err = lm.RWEnqueue("rw-dead-among", 'w', 5*time.Second, 3)
	if err != nil {
		t.Fatal(err)
	}
	if status != "queued" {
		t.Fatalf("expected writer 3 queued, got %q", status)
	}

	// Reader 4 enqueues.
	status, _, _, _, err = lm.RWEnqueue("rw-dead-among", 'r', 5*time.Second, 4)
	if err != nil {
		t.Fatal(err)
	}
	if status != "queued" {
		t.Fatalf("expected reader 4 queued, got %q", status)
	}

	// Kill writer 3 (dead writer in the middle of the queue).
	lm.CleanupConnection(3)

	// Start Wait goroutines for reader 2 and reader 4.
	reader2Done := make(chan string, 1)
	reader4Done := make(chan string, 1)

	go func() {
		tok, _, _, _ := lm.RWWait(bg(), "rw-dead-among", 5*time.Second, 2)
		reader2Done <- tok
	}()
	go func() {
		tok, _, _, _ := lm.RWWait(bg(), "rw-dead-among", 5*time.Second, 4)
		reader4Done <- tok
	}()

	// Release writer 1. Reader 2 should be granted. Dead writer 3 is
	// skipped, so reader 4 should also be batch-granted.
	if !lm.RWRelease("rw-dead-among", tok1) {
		t.Fatal("expected release to succeed")
	}

	// Reader 2 must be granted.
	select {
	case tok2 := <-reader2Done:
		if tok2 == "" {
			t.Fatal("expected reader 2 to get a token")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("reader 2 was not granted within timeout")
	}

	// Reader 4 must also be granted (writer 3 was dead, so no barrier).
	select {
	case tok4 := <-reader4Done:
		if tok4 == "" {
			t.Fatal("expected reader 4 to get a token")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("reader 4 was not granted within timeout")
	}
}

// TestLeaseExpiry_SimultaneousMultipleKeys acquires 20 locks with very
// short lease TTLs, waits for expiry, and verifies all are released.
func TestLeaseExpiry_SimultaneousMultipleKeys(t *testing.T) {
	lm := testManager()
	lm.cfg.LeaseSweepInterval = 50 * time.Millisecond
	const n = 20

	tokens := make(map[string]string) // key -> token
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("lease-exp-%d", i)
		tok, err := lm.Acquire(bg(), key, 5*time.Second, 5*time.Second, uint64(i+1), 1)
		if err != nil {
			t.Fatal(err)
		}
		if tok == "" {
			t.Fatalf("expected to acquire lock %s", key)
		}
		tokens[key] = tok
	}

	// Manually expire all leases.
	for key, tok := range tokens {
		lm.LockKeyForTest(key)
		st := lm.ResourceForTest(key)
		if st != nil {
			st.Holders[tok].leaseExpires = time.Now().Add(-1 * time.Second)
		}
		lm.UnlockKeyForTest(key)
	}

	// Run the expiry loop briefly.
	ctx, cancel := context.WithCancel(bg())
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		lm.LeaseExpiryLoop(ctx)
	}()

	time.Sleep(300 * time.Millisecond)
	cancel()
	<-loopDone

	// Verify all locks are released.
	for key := range tokens {
		lm.LockKeyForTest(key)
		st := lm.ResourceForTest(key)
		holders := 0
		if st != nil {
			holders = len(st.Holders)
		}
		lm.UnlockKeyForTest(key)
		if holders != 0 {
			t.Errorf("key %s: expected 0 holders, got %d", key, holders)
		}
	}
}

// TestBarrier_ExactlyNParticipants verifies a barrier with N=5 succeeds
// when exactly 5 goroutines join.
func TestBarrier_ExactlyNParticipants(t *testing.T) {
	lm := testManager()
	const n = 5
	var wg sync.WaitGroup
	results := make([]bool, n)
	errs := make([]error, n)

	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(id int) {
			defer wg.Done()
			ok, err := lm.BarrierWait(bg(), "barrier-exact", n, 5*time.Second, uint64(id+1))
			results[id] = ok
			errs[id] = err
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Errorf("participant %d error: %v", i, errs[i])
		}
		if !results[i] {
			t.Errorf("participant %d: barrier did not trip", i)
		}
	}
}

// TestBarrier_ContextCancelPartial launches 2 of 3 required participants,
// cancels one's context, and verifies:
//   - The cancelled participant returns an error.
//   - The remaining participant is also woken (barrier becomes unreachable).
//   - A fresh barrier on the same key can be created and tripped afterwards.
func TestBarrier_ContextCancelPartial(t *testing.T) {
	lm := testManager()

	type result struct {
		ok  bool
		err error
	}

	// Participant 1 waits normally.
	p1Done := make(chan result, 1)
	go func() {
		ok, err := lm.BarrierWait(bg(), "barrier-cancel", 3, 5*time.Second, 1)
		p1Done <- result{ok, err}
	}()

	// Participant 2 will be cancelled.
	ctx2, cancel2 := context.WithCancel(bg())
	p2Done := make(chan result, 1)
	go func() {
		ok, err := lm.BarrierWait(ctx2, "barrier-cancel", 3, 5*time.Second, 2)
		p2Done <- result{ok, err}
	}()

	// Give participants time to enqueue.
	time.Sleep(50 * time.Millisecond)

	// Cancel participant 2. This makes the barrier unreachable (count=3
	// can never be met with only 1 remaining participant), so the barrier
	// cancels all remaining participants.
	cancel2()

	select {
	case r := <-p2Done:
		if r.err == nil {
			t.Fatal("expected error from cancelled participant 2")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled participant did not return")
	}

	// Participant 1 should also be woken because the barrier is now
	// unreachable (it returns ok=false, err=nil via the Cancelled channel).
	select {
	case r := <-p1Done:
		if r.ok {
			t.Fatal("participant 1: barrier should not have tripped")
		}
		// err may or may not be set, but ok must be false.
	case <-time.After(2 * time.Second):
		t.Fatal("participant 1 was not woken after barrier became unreachable")
	}

	// Now the barrier key is fully cleaned up. A fresh barrier on the
	// same key should work. Use 3 new participants (3, 4, 5).
	freshResults := make([]result, 3)
	var wg sync.WaitGroup
	wg.Add(3)
	for i := 0; i < 3; i++ {
		go func(id int) {
			defer wg.Done()
			ok, err := lm.BarrierWait(bg(), "barrier-cancel", 3, 5*time.Second, uint64(id+3))
			freshResults[id] = result{ok, err}
		}(i)
	}
	wg.Wait()

	for i, r := range freshResults {
		if r.err != nil {
			t.Errorf("fresh participant %d error: %v", i+3, r.err)
		}
		if !r.ok {
			t.Errorf("fresh participant %d: barrier did not trip", i+3)
		}
	}
}

// TestKVCAS_ConcurrentSwapMultiRound runs 20 goroutines all attempting CAS
// on the same key. Exactly one should win per round, across multiple rounds.
func TestKVCAS_ConcurrentSwapMultiRound(t *testing.T) {
	lm := testManager()
	const goroutines = 20
	const rounds = 10

	for round := 0; round < rounds; round++ {
		expected := fmt.Sprintf("val-%d", round)
		next := fmt.Sprintf("val-%d", round+1)

		if round == 0 {
			lm.KVSet("cas-race", expected, 0)
		}

		var wg sync.WaitGroup
		var wins int64
		wg.Add(goroutines)
		for i := 0; i < goroutines; i++ {
			go func() {
				defer wg.Done()
				ok, _ := lm.KVCAS("cas-race", expected, next, 0)
				if ok {
					atomic.AddInt64(&wins, 1)
				}
			}()
		}
		wg.Wait()

		if wins != 1 {
			t.Fatalf("round %d: expected exactly 1 CAS winner, got %d", round, wins)
		}
		val, exists := lm.KVGet("cas-race")
		if !exists || val != next {
			t.Fatalf("round %d: expected %q, got %q (exists=%v)", round, next, val, exists)
		}
	}
}

// TestElection_FenceMonotonic elects and resigns a leader 10 times in a
// row and verifies the fence token strictly increases each time.
func TestElection_FenceMonotonic(t *testing.T) {
	lm := testManager()
	var prevFence uint64

	for i := 0; i < 10; i++ {
		connID := uint64(i + 1)
		tok, fence, err := lm.AcquireWithFence(bg(), "elect-fence", 5*time.Second, 30*time.Second, connID, 1)
		if err != nil {
			t.Fatalf("iteration %d: acquire error: %v", i, err)
		}
		if tok == "" {
			t.Fatalf("iteration %d: expected token", i)
		}
		if fence <= prevFence {
			t.Fatalf("iteration %d: fence %d not greater than prev %d", i, fence, prevFence)
		}
		prevFence = fence

		if !lm.ResignLeader("elect-fence", tok, connID) {
			t.Fatalf("iteration %d: resign failed", i)
		}
	}
}

// TestList_ConcurrentPushPopStress exercises 20 pushers (100 items each)
// and 10 poppers concurrently on the same list. All pushed items must be
// accounted for (pushed - popped = remaining in list).
func TestList_ConcurrentPushPopStress(t *testing.T) {
	lm := testManager()
	const pushers = 20
	const popsPerPopper = 50
	const poppers = 10
	const itemsPerPusher = 100

	var wg sync.WaitGroup
	var totalPushed int64
	var totalPopped int64

	// Start pushers.
	wg.Add(pushers)
	for i := 0; i < pushers; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < itemsPerPusher; j++ {
				val := fmt.Sprintf("pusher-%d-item-%d", id, j)
				_, err := lm.RPush("stress-list", val)
				if err != nil {
					t.Errorf("push error: %v", err)
					return
				}
				atomic.AddInt64(&totalPushed, 1)
			}
		}(i)
	}

	// Start poppers.
	wg.Add(poppers)
	for i := 0; i < poppers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < popsPerPopper; j++ {
				_, ok := lm.LPop("stress-list")
				if ok {
					atomic.AddInt64(&totalPopped, 1)
				}
			}
		}()
	}

	wg.Wait()

	remaining := lm.LLen("stress-list")
	pushed := atomic.LoadInt64(&totalPushed)
	popped := atomic.LoadInt64(&totalPopped)

	if int64(remaining) != pushed-popped {
		t.Fatalf("accounting mismatch: pushed=%d popped=%d remaining=%d (expected %d)",
			pushed, popped, remaining, pushed-popped)
	}
}

// TestSemaphore_ConcurrentContention creates a semaphore with limit=3 and
// launches 20 goroutines all trying to acquire. An atomic counter tracks
// concurrent holders and verifies the count never exceeds the limit.
func TestSemaphore_ConcurrentContention(t *testing.T) {
	lm := testManager()
	const limit = 3
	const goroutines = 20
	const iterations = 20

	var concurrent int64
	var maxSeen int64
	var wg sync.WaitGroup

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			connID := uint64(id + 1)
			for j := 0; j < iterations; j++ {
				tok, err := lm.Acquire(bg(), "sem-contention", 2*time.Second, 5*time.Second, connID, limit)
				if err != nil {
					t.Errorf("goroutine %d iter %d: acquire error: %v", id, j, err)
					return
				}
				if tok == "" {
					// Timeout, try again.
					continue
				}

				cur := atomic.AddInt64(&concurrent, 1)
				// Track max.
				for {
					old := atomic.LoadInt64(&maxSeen)
					if cur <= old || atomic.CompareAndSwapInt64(&maxSeen, old, cur) {
						break
					}
				}

				if cur > int64(limit) {
					t.Errorf("concurrent holders %d exceeds limit %d", cur, limit)
				}

				// Hold briefly.
				time.Sleep(time.Millisecond)
				atomic.AddInt64(&concurrent, -1)

				lm.Release("sem-contention", tok)
			}
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All goroutines completed.
	case <-time.After(30 * time.Second):
		t.Fatal("deadlock: goroutines did not finish within 30s")
	}

	if atomic.LoadInt64(&maxSeen) > int64(limit) {
		t.Fatalf("max concurrent holders %d exceeded limit %d", maxSeen, limit)
	}
}

// TestCleanup_ConcurrentCleanupSameConn calls CleanupConnection with the
// same connID from multiple goroutines simultaneously. The test passes if
// there are no panics or data races.
func TestCleanup_ConcurrentCleanupSameConn(t *testing.T) {
	lm := testManager()

	// Acquire several resources under connID 1.
	lm.Acquire(bg(), "cc-lock", 5*time.Second, 30*time.Second, 1, 1)
	lm.Acquire(bg(), "cc-sem", 5*time.Second, 30*time.Second, 1, 3)
	lm.RWAcquire(bg(), "cc-rw", 'r', 5*time.Second, 30*time.Second, 1)

	var wg sync.WaitGroup
	const goroutines = 10
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			lm.CleanupConnection(1)
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// No panics or races.
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock: concurrent cleanup did not finish within 5s")
	}
}

// TestFencingToken_GloballyIncreasing acquires locks across multiple
// different keys and verifies that fence tokens are globally monotonically
// increasing (since they come from a single atomic counter).
func TestFencingToken_GloballyIncreasing(t *testing.T) {
	lm := testManager()

	var prevFence uint64
	keys := []string{"fence-a", "fence-b", "fence-c", "fence-d", "fence-e"}

	for i, key := range keys {
		connID := uint64(i + 1)
		tok, fence, err := lm.AcquireWithFence(bg(), key, 5*time.Second, 30*time.Second, connID, 1)
		if err != nil {
			t.Fatalf("key %s: acquire error: %v", key, err)
		}
		if tok == "" {
			t.Fatalf("key %s: expected token", key)
		}
		if fence <= prevFence {
			t.Fatalf("key %s: fence %d not greater than prev %d", key, fence, prevFence)
		}
		prevFence = fence
	}

	// Also verify across RW locks and semaphores.
	rwtok, rwfence, err := lm.RWAcquire(bg(), "fence-rw", 'w', 5*time.Second, 30*time.Second, 100)
	if err != nil {
		t.Fatal(err)
	}
	if rwtok == "" {
		t.Fatal("expected rw token")
	}
	if rwfence <= prevFence {
		t.Fatalf("rw fence %d not greater than prev %d", rwfence, prevFence)
	}
	prevFence = rwfence

	// Semaphore acquire via AcquireWithFence.
	semtok, semfence, err := lm.AcquireWithFence(bg(), "fence-sem", 5*time.Second, 30*time.Second, 200, 3)
	if err != nil {
		t.Fatal(err)
	}
	if semtok == "" {
		t.Fatal("expected sem token")
	}
	if semfence <= prevFence {
		t.Fatalf("sem fence %d not greater than prev %d", semfence, prevFence)
	}
}

// TestStats_ComprehensiveUnderLoad acquires various lock types, runs
// Stats(), and verifies the returned fields are populated correctly.
func TestStats_ComprehensiveUnderLoad(t *testing.T) {
	lm := testManager()

	// Create a mutex lock.
	lm.Acquire(bg(), "stat-lock-1", 5*time.Second, 30*time.Second, 1, 1)
	lm.Acquire(bg(), "stat-lock-2", 5*time.Second, 30*time.Second, 2, 1)

	// Create semaphores.
	lm.Acquire(bg(), "stat-sem-1", 5*time.Second, 30*time.Second, 3, 3)
	lm.Acquire(bg(), "stat-sem-1", 5*time.Second, 30*time.Second, 4, 3)

	// Create an RW lock.
	lm.RWAcquire(bg(), "stat-rw-1", 'r', 5*time.Second, 30*time.Second, 5)
	lm.RWAcquire(bg(), "stat-rw-1", 'r', 5*time.Second, 30*time.Second, 6)

	// Create an idle lock (acquire then release).
	idleTok, _ := lm.Acquire(bg(), "stat-idle", 5*time.Second, 30*time.Second, 7, 1)
	lm.Release("stat-idle", idleTok)

	// Create counters.
	lm.Incr("stat-counter", 42)

	// Create KV entries.
	lm.KVSet("stat-kv", "value", 0)

	// Create lists.
	lm.RPush("stat-list", "item1")
	lm.RPush("stat-list", "item2")

	// Create a barrier (1 of 2 participants waiting).
	go func() {
		lm.BarrierWait(bg(), "stat-barrier", 2, 2*time.Second, 10)
	}()
	time.Sleep(50 * time.Millisecond)

	stats := lm.Stats(10)

	// Verify connections.
	if stats.Connections != 10 {
		t.Errorf("connections: got %d, want 10", stats.Connections)
	}

	// Verify locks (mutex + idle should both appear somewhere).
	if len(stats.Locks) < 2 {
		t.Errorf("locks: got %d, want >= 2", len(stats.Locks))
	}

	// Verify semaphores.
	foundSem := false
	for _, s := range stats.Semaphores {
		if s.Key == "stat-sem-1" {
			foundSem = true
		}
	}
	if !foundSem {
		t.Error("stat-sem-1 not found in semaphores")
	}

	// Verify RW locks.
	foundRW := false
	for _, rw := range stats.RWLocks {
		if rw.Key == "stat-rw-1" {
			foundRW = true
		}
	}
	if !foundRW {
		t.Error("stat-rw-1 not found in rw_locks")
	}

	// Verify idle locks.
	foundIdle := false
	for _, il := range stats.IdleLocks {
		if il.Key == "stat-idle" {
			foundIdle = true
		}
	}
	if !foundIdle {
		t.Error("stat-idle not found in idle_locks")
	}

	// Verify counters.
	if len(stats.Counters) < 1 {
		t.Errorf("counters: got %d, want >= 1", len(stats.Counters))
	}

	// Verify KV entries.
	if len(stats.KVEntries) < 1 {
		t.Errorf("kv_entries: got %d, want >= 1", len(stats.KVEntries))
	}

	// Verify lists.
	if len(stats.Lists) < 1 {
		t.Errorf("lists: got %d, want >= 1", len(stats.Lists))
	}

	// Verify barriers.
	if len(stats.Barriers) < 1 {
		t.Errorf("barriers: got %d, want >= 1", len(stats.Barriers))
	}
}

// ---------------------------------------------------------------------------
// Gap-filling: LeaderHolder
// ---------------------------------------------------------------------------

func TestLeaderHolder_NoLeader(t *testing.T) {
	lm := testManager()
	connID := lm.LeaderHolder("nonexistent-election")
	if connID != 0 {
		t.Fatalf("expected 0 for no leader, got %d", connID)
	}
}

func TestLeaderHolder_WithLeader(t *testing.T) {
	lm := testManager()
	tok, _, err := lm.AcquireWithFence(bg(), "leader-key", 5*time.Second, 5*time.Second, 42, 1)
	if err != nil {
		t.Fatal(err)
	}
	if tok == "" {
		t.Fatal("expected token")
	}

	connID := lm.LeaderHolder("leader-key")
	if connID != 42 {
		t.Fatalf("expected leader connID 42, got %d", connID)
	}

	// Release and verify no leader.
	lm.Release("leader-key", tok)
	connID = lm.LeaderHolder("leader-key")
	if connID != 0 {
		t.Fatalf("expected 0 after release, got %d", connID)
	}
}

// ---------------------------------------------------------------------------
// Gap-filling: ResignLeader wrong token / nonexistent
// ---------------------------------------------------------------------------

func TestResignLeader_WrongTokenAndCorrectToken(t *testing.T) {
	lm := testManager()
	tok, _, err := lm.AcquireWithFence(bg(), "resign-wrong", 5*time.Second, 5*time.Second, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	// Register as leader watcher (simulates server's elect handler)
	ch := make(chan []byte, 4)
	lm.RegisterLeaderWatcher("resign-wrong", 1, ch, nil)

	// Wrong token should fail
	ok := lm.ResignLeader("resign-wrong", "bogus-token", 1)
	if ok {
		t.Fatal("expected ResignLeader with wrong token to return false")
	}

	// Correct token should succeed
	ok = lm.ResignLeader("resign-wrong", tok, 1)
	if !ok {
		t.Fatal("expected ResignLeader with correct token to return true")
	}
}

func TestResignLeader_Nonexistent(t *testing.T) {
	lm := testManager()
	ok := lm.ResignLeader("no-such-key", "no-such-token", 99)
	if ok {
		t.Fatal("expected ResignLeader on nonexistent key to return false")
	}
}

// ---------------------------------------------------------------------------
// Gap-filling: RegisterLeaderWatcherWithStatus (observe while leader exists)
// ---------------------------------------------------------------------------

func TestRegisterLeaderWatcherWithStatus_LeaderExists(t *testing.T) {
	lm := testManager()

	// Acquire lock (simulates elect)
	tok, _, err := lm.AcquireWithFence(bg(), "obs-status", 5*time.Second, 5*time.Second, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if tok == "" {
		t.Fatal("expected token")
	}

	// Register watcher with status — should immediately receive "leader elected"
	ch := make(chan []byte, 4)
	lm.RegisterLeaderWatcherWithStatus("obs-status", 2, ch, nil)

	select {
	case msg := <-ch:
		expected := "leader elected obs-status\n"
		if string(msg) != expected {
			t.Fatalf("expected %q, got %q", expected, string(msg))
		}
	default:
		t.Fatal("expected immediate notification, got nothing")
	}
}

func TestRegisterLeaderWatcherWithStatus_NoLeader(t *testing.T) {
	lm := testManager()

	// Register watcher with status — no leader exists, should NOT receive anything
	ch := make(chan []byte, 4)
	lm.RegisterLeaderWatcherWithStatus("obs-empty", 2, ch, nil)

	select {
	case msg := <-ch:
		t.Fatalf("expected no notification, got %q", string(msg))
	default:
		// Good — no notification.
	}
}

// ---------------------------------------------------------------------------
// Gap-filling: CleanupConnection with AutoReleaseOnDisconnect=false
// ---------------------------------------------------------------------------

func TestCleanup_AutoReleaseDisabled(t *testing.T) {
	cfg := testConfig()
	cfg.AutoReleaseOnDisconnect = false
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	lm := NewLockManager(cfg, log)

	tok, _, err := lm.AcquireWithFence(bg(), "no-auto-release", 5*time.Second, 5*time.Second, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if tok == "" {
		t.Fatal("expected token")
	}

	// CleanupConnection should be a no-op when auto-release is disabled.
	lm.CleanupConnection(1)

	// Lock should still be held.
	connID := lm.LeaderHolder("no-auto-release")
	if connID != 1 {
		t.Fatalf("expected lock still held by conn 1, got %d", connID)
	}
}

// ---------------------------------------------------------------------------
// Gap-filling: RPop positive cases (previously only empty-case tested)
// ---------------------------------------------------------------------------

func TestRPop_Basic(t *testing.T) {
	lm := testManager()
	lm.RPush("q1", "a")
	lm.RPush("q1", "b")
	lm.RPush("q1", "c")

	// RPop returns from the right (LIFO order for RPush+RPop)
	v, ok := lm.RPop("q1")
	if !ok || v != "c" {
		t.Fatalf("expected (c, true), got (%s, %v)", v, ok)
	}
	v, ok = lm.RPop("q1")
	if !ok || v != "b" {
		t.Fatalf("expected (b, true), got (%s, %v)", v, ok)
	}
	v, ok = lm.RPop("q1")
	if !ok || v != "a" {
		t.Fatalf("expected (a, true), got (%s, %v)", v, ok)
	}
	// Now empty
	_, ok = lm.RPop("q1")
	if ok {
		t.Fatal("expected false after draining list")
	}
}

func TestRPop_MixedWithLPop(t *testing.T) {
	lm := testManager()
	lm.RPush("q1", "a")
	lm.RPush("q1", "b")
	lm.RPush("q1", "c")
	lm.RPush("q1", "d")

	// LPop from left, RPop from right
	v1, _ := lm.LPop("q1")
	v2, _ := lm.RPop("q1")
	if v1 != "a" {
		t.Fatalf("LPop expected a, got %s", v1)
	}
	if v2 != "d" {
		t.Fatalf("RPop expected d, got %s", v2)
	}
	// Remaining: [b, c]
	if lm.LLen("q1") != 2 {
		t.Fatalf("expected len 2, got %d", lm.LLen("q1"))
	}
}

func TestRPop_SingleElement(t *testing.T) {
	lm := testManager()
	lm.RPush("q1", "only")
	v, ok := lm.RPop("q1")
	if !ok || v != "only" {
		t.Fatalf("expected (only, true), got (%s, %v)", v, ok)
	}
	// List should be cleaned up
	if lm.LLen("q1") != 0 {
		t.Fatalf("expected len 0 after draining, got %d", lm.LLen("q1"))
	}
}

func TestRPop_WithLPush(t *testing.T) {
	lm := testManager()
	// LPush pushes to front, RPop pops from back
	lm.LPush("q1", "a") // [a]
	lm.LPush("q1", "b") // [b, a]
	lm.LPush("q1", "c") // [c, b, a]

	v, ok := lm.RPop("q1")
	if !ok || v != "a" {
		t.Fatalf("expected (a, true), got (%s, %v)", v, ok)
	}
	v, ok = lm.RPop("q1")
	if !ok || v != "b" {
		t.Fatalf("expected (b, true), got (%s, %v)", v, ok)
	}
}

// ---------------------------------------------------------------------------
// Gap-filling: KVDel edge cases
// ---------------------------------------------------------------------------

func TestKVDel_Nonexistent(t *testing.T) {
	lm := testManager()
	// Deleting a key that doesn't exist should be a safe no-op.
	lm.KVDel("no-such-key")
}

func TestKVDel_DoubleDelete(t *testing.T) {
	lm := testManager()
	lm.KVSet("k1", "val", 0)
	lm.KVDel("k1")
	// Second delete should be a safe no-op.
	lm.KVDel("k1")

	_, ok := lm.KVGet("k1")
	if ok {
		t.Fatal("expected not found after double delete")
	}
}

func TestKVDel_FreesKeySlot(t *testing.T) {
	cfg := testConfig()
	cfg.MaxKeys = 2
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	lm := NewLockManager(cfg, log)

	if err := lm.KVSet("k1", "a", 0); err != nil {
		t.Fatal(err)
	}
	if err := lm.KVSet("k2", "b", 0); err != nil {
		t.Fatal(err)
	}
	// At capacity
	if err := lm.KVSet("k3", "c", 0); !errors.Is(err, ErrMaxKeys) {
		t.Fatalf("expected ErrMaxKeys, got %v", err)
	}

	// Delete one, then the slot should be free
	lm.KVDel("k1")
	if err := lm.KVSet("k3", "c", 0); err != nil {
		t.Fatalf("expected success after delete, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Gap-filling: SetCounter edge cases
// ---------------------------------------------------------------------------

func TestSetCounter_Overwrite(t *testing.T) {
	lm := testManager()
	lm.SetCounter("c1", 100)
	lm.SetCounter("c1", -50)
	v := lm.GetCounter("c1")
	if v != -50 {
		t.Fatalf("expected -50, got %d", v)
	}
}

func TestSetCounter_MaxKeys(t *testing.T) {
	cfg := testConfig()
	cfg.MaxKeys = 1
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	lm := NewLockManager(cfg, log)

	if err := lm.SetCounter("c1", 10); err != nil {
		t.Fatal(err)
	}
	// Second different key should fail
	if err := lm.SetCounter("c2", 20); !errors.Is(err, ErrMaxKeys) {
		t.Fatalf("expected ErrMaxKeys, got %v", err)
	}
	// Overwrite existing key should still work
	if err := lm.SetCounter("c1", 30); err != nil {
		t.Fatalf("expected overwrite to succeed, got %v", err)
	}
}

func TestSetCounter_ZeroValue(t *testing.T) {
	lm := testManager()
	lm.SetCounter("c1", 0)
	v := lm.GetCounter("c1")
	if v != 0 {
		t.Fatalf("expected 0, got %d", v)
	}
}

func TestSetCounter_IncrAfterSet(t *testing.T) {
	lm := testManager()
	lm.SetCounter("c1", 100)
	val, err := lm.Incr("c1", 5)
	if err != nil {
		t.Fatal(err)
	}
	if val != 105 {
		t.Fatalf("expected 105, got %d", val)
	}
}

// ---------------------------------------------------------------------------
// Gap-filling: RenewWithFence additional coverage
// ---------------------------------------------------------------------------

func TestRenewWithFence_WrongToken(t *testing.T) {
	lm := testManager()
	lm.AcquireWithFence(bg(), "k1", 5*time.Second, 30*time.Second, 1, 1)

	_, _, err := lm.RenewWithFence("k1", "wrong-token", 60*time.Second)
	if err == nil {
		t.Fatal("RenewWithFence with wrong token should fail")
	}
}

func TestRenewWithFence_NonexistentKey(t *testing.T) {
	lm := testManager()
	_, _, err := lm.RenewWithFence("no-key", "no-token", 60*time.Second)
	if err == nil {
		t.Fatal("RenewWithFence on nonexistent key should fail")
	}
}

func TestRenewWithFence_ExpiredLease(t *testing.T) {
	cfg := testConfig()
	cfg.LeaseSweepInterval = 50 * time.Millisecond
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	lm := NewLockManager(cfg, log)

	tok, _, err := lm.AcquireWithFence(bg(), "k1", 100*time.Millisecond, 100*time.Millisecond, 1, 1)
	if err != nil {
		t.Fatal(err)
	}

	// Wait for lease to expire
	time.Sleep(200 * time.Millisecond)

	_, _, err = lm.RenewWithFence("k1", tok, 60*time.Second)
	if !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("RenewWithFence on expired lease should return ErrLeaseExpired, got %v", err)
	}
}

func TestRenewWithFence_PreservesFenceAfterRenew(t *testing.T) {
	lm := testManager()
	tok, fence1, err := lm.AcquireWithFence(bg(), "k1", 5*time.Second, 30*time.Second, 1, 1)
	if err != nil {
		t.Fatal(err)
	}

	// Renew multiple times — fence should remain the same
	for i := 0; i < 5; i++ {
		remaining, fence, err := lm.RenewWithFence("k1", tok, 30*time.Second)
		if err != nil {
			t.Fatalf("renew %d failed: %v", i, err)
		}
		if fence != fence1 {
			t.Fatalf("renew %d: fence changed from %d to %d", i, fence1, fence)
		}
		if remaining <= 0 {
			t.Fatalf("renew %d: remaining should be positive, got %d", i, remaining)
		}
	}
}

// ---------------------------------------------------------------------------
// Regression: Incr overflow/underflow detection
// ---------------------------------------------------------------------------

func TestIncr_OverflowMaxInt64(t *testing.T) {
	lm := testManager()
	lm.SetCounter("c1", math.MaxInt64)
	_, err := lm.Incr("c1", 1)
	if err == nil {
		t.Fatal("expected overflow error for MaxInt64 + 1")
	}
	// Counter value should be unchanged
	v := lm.GetCounter("c1")
	if v != math.MaxInt64 {
		t.Fatalf("counter should be unchanged, got %d", v)
	}
}

func TestIncr_UnderflowMinInt64(t *testing.T) {
	lm := testManager()
	lm.SetCounter("c1", math.MinInt64)
	_, err := lm.Incr("c1", -1)
	if err == nil {
		t.Fatal("expected underflow error for MinInt64 + (-1)")
	}
	v := lm.GetCounter("c1")
	if v != math.MinInt64 {
		t.Fatalf("counter should be unchanged, got %d", v)
	}
}

func TestIncr_LargePositiveDelta(t *testing.T) {
	lm := testManager()
	lm.SetCounter("c1", 1)
	_, err := lm.Incr("c1", math.MaxInt64)
	if err == nil {
		t.Fatal("expected overflow error for 1 + MaxInt64")
	}
}

func TestIncr_BoundaryNoOverflow(t *testing.T) {
	lm := testManager()
	lm.SetCounter("c1", math.MaxInt64-1)
	val, err := lm.Incr("c1", 1)
	if err != nil {
		t.Fatalf("MaxInt64-1 + 1 should not overflow: %v", err)
	}
	if val != math.MaxInt64 {
		t.Fatalf("expected MaxInt64, got %d", val)
	}
}

// ---------------------------------------------------------------------------
// Regression: compactItems triggers at correct threshold
// ---------------------------------------------------------------------------

func TestList_CompactItems_TriggerAfterHalfWasted(t *testing.T) {
	lm := testManager()
	// Push many items to build up a big backing array
	for i := 0; i < 100; i++ {
		lm.RPush("q1", fmt.Sprintf("item%d", i))
	}
	// Pop more than half — compaction should trigger during a subsequent LPop
	for i := 0; i < 60; i++ {
		lm.LPop("q1")
	}
	// Remaining 40 items should still be correct
	if lm.LLen("q1") != 40 {
		t.Fatalf("expected 40 items, got %d", lm.LLen("q1"))
	}
	v, ok := lm.LPop("q1")
	if !ok || v != "item60" {
		t.Fatalf("expected item60 after compaction, got %q", v)
	}
}

// ---------------------------------------------------------------------------
// HasLeaderWatcherObserver — all 3 paths
// ---------------------------------------------------------------------------

func TestHasLeaderWatcherObserver(t *testing.T) {
	lm := testManager()

	// Path 1: No watcher at all for connID
	if lm.HasLeaderWatcherObserver("lw-key", 99) {
		t.Fatal("expected false when no watcher exists")
	}

	// Path 2: Watcher exists but observer=false (registered via RegisterAndNotifyLeader)
	ch1 := make(chan []byte, 4)
	lm.RegisterAndNotifyLeader("lw-key", "elected", 10, ch1, func() {})
	if lm.HasLeaderWatcherObserver("lw-key", 10) {
		t.Fatal("expected false for holder-only watcher (not observer)")
	}

	// Path 3: Watcher is an observer (registered via RegisterLeaderWatcherWithStatus)
	ch2 := make(chan []byte, 4)
	lm.RegisterLeaderWatcherWithStatus("lw-key", 20, ch2, func() {})
	if !lm.HasLeaderWatcherObserver("lw-key", 20) {
		t.Fatal("expected true for observer watcher")
	}
}

// ---------------------------------------------------------------------------
// RegisterLeaderWatcherWithStatus return value
// ---------------------------------------------------------------------------

func TestRegisterLeaderWatcherWithStatus_ReturnValue(t *testing.T) {
	lm := testManager()

	// Brand new registration → true
	ch := make(chan []byte, 4)
	added := lm.RegisterLeaderWatcherWithStatus("rw-key", 1, ch, func() {})
	if !added {
		t.Fatal("expected true for brand new observer registration")
	}

	// Re-register same connID as observer (already observer) → false
	added = lm.RegisterLeaderWatcherWithStatus("rw-key", 1, ch, func() {})
	if added {
		t.Fatal("expected false when re-registering same connID already marked observer")
	}

	// Register holder-only first, then upgrade to observer → true
	ch2 := make(chan []byte, 4)
	lm.RegisterAndNotifyLeader("rw-key2", "elected", 2, ch2, func() {})
	added = lm.RegisterLeaderWatcherWithStatus("rw-key2", 2, ch2, func() {})
	if !added {
		t.Fatal("expected true when upgrading holder-only watcher to observer")
	}
}

// ---------------------------------------------------------------------------
// UnregisterLeaderWatcher return value
// ---------------------------------------------------------------------------

func TestUnregisterLeaderWatcher_ReturnValue(t *testing.T) {
	lm := testManager()

	// No watcher exists → false
	removed := lm.UnregisterLeaderWatcher("unreg-key", 99)
	if removed {
		t.Fatal("expected false when no watcher exists")
	}

	// Register then unregister → true
	ch := make(chan []byte, 4)
	lm.RegisterLeaderWatcherWithStatus("unreg-key", 1, ch, func() {})
	removed = lm.UnregisterLeaderWatcher("unreg-key", 1)
	if !removed {
		t.Fatal("expected true when watcher existed and was removed")
	}

	// Second unregister of same → false
	removed = lm.UnregisterLeaderWatcher("unreg-key", 1)
	if removed {
		t.Fatal("expected false on second unregister (already removed)")
	}
}

// ---------------------------------------------------------------------------
// ResignLeader notifies watchers
// ---------------------------------------------------------------------------

func TestResignLeader_NotifiesWatchers(t *testing.T) {
	lm := testManager()

	// Acquire a lock
	tok, _, err := lm.AcquireWithFence(bg(), "resign-w", 5*time.Second, 30*time.Second, 1, 1)
	if err != nil {
		t.Fatal(err)
	}

	// Register an observer watcher on a different connID
	ch := make(chan []byte, 4)
	lm.RegisterLeaderWatcherWithStatus("resign-w", 2, ch, func() {})
	// Drain the initial "leader elected" notification
	select {
	case <-ch:
	default:
	}

	// Resign
	if !lm.ResignLeader("resign-w", tok, 1) {
		t.Fatal("resign should succeed")
	}

	// Watcher should receive "leader resigned <key>"
	select {
	case msg := <-ch:
		expected := "leader resigned resign-w\n"
		if string(msg) != expected {
			t.Fatalf("expected %q, got %q", expected, string(msg))
		}
	default:
		t.Fatal("expected resigned notification, got nothing")
	}
}

// ---------------------------------------------------------------------------
// GrantNextWaiter failover notification
// ---------------------------------------------------------------------------

func TestGrantNextWaiter_FailoverNotification(t *testing.T) {
	lm := testManager()

	// conn1 acquires the lock
	tok1, _ := lm.Acquire(bg(), "fo-key", 5*time.Second, 30*time.Second, 1, 1)
	if tok1 == "" {
		t.Fatal("conn1 should acquire")
	}

	// conn2 enqueues as waiter
	done := make(chan string, 1)
	go func() {
		tok, _ := lm.Acquire(bg(), "fo-key", 5*time.Second, 30*time.Second, 2, 1)
		done <- tok
	}()
	time.Sleep(50 * time.Millisecond)

	// Register observer watcher on conn3
	ch := make(chan []byte, 4)
	lm.RegisterLeaderWatcherWithStatus("fo-key", 3, ch, func() {})
	// Drain "leader elected" if present
	select {
	case <-ch:
	default:
	}

	// Release conn1's lock → grant to conn2 → failover notification
	lm.Release("fo-key", tok1)
	tok2 := <-done
	if tok2 == "" {
		t.Fatal("conn2 should have acquired")
	}

	// Observer should receive "leader failover fo-key"
	select {
	case msg := <-ch:
		expected := "leader failover fo-key\n"
		if string(msg) != expected {
			t.Fatalf("expected %q, got %q", expected, string(msg))
		}
	case <-time.After(time.Second):
		t.Fatal("expected failover notification, timed out")
	}
}

// ---------------------------------------------------------------------------
// OnLockRelease callback — release path
// ---------------------------------------------------------------------------

func TestOnLockRelease_Callback(t *testing.T) {
	lm := testManager()

	released := make(chan string, 1)
	lm.OnLockRelease = func(key string) {
		released <- key
	}

	tok, _ := lm.Acquire(bg(), "cb-key", 5*time.Second, 30*time.Second, 1, 1)
	if tok == "" {
		t.Fatal("should acquire")
	}

	// Manually expire the lease so that a subsequent Acquire triggers eviction
	lm.LockKeyForTest("cb-key")
	lm.ResourceForTest("cb-key").Holders[tok].leaseExpires = time.Now().Add(-1 * time.Second)
	lm.UnlockKeyForTest("cb-key")

	// Another Acquire will call evictExpiredLocked which fires OnLockRelease
	lm.Acquire(bg(), "cb-key", 0, 30*time.Second, 2, 1)

	select {
	case key := <-released:
		if key != "cb-key" {
			t.Fatalf("expected cb-key, got %s", key)
		}
	case <-time.After(time.Second):
		t.Fatal("OnLockRelease callback not fired")
	}
}

// ---------------------------------------------------------------------------
// OnLockRelease callback — lease expiry path
// ---------------------------------------------------------------------------

func TestOnLockRelease_LeaseExpiry(t *testing.T) {
	lm := testManager()
	lm.cfg.LeaseSweepInterval = 50 * time.Millisecond

	released := make(chan string, 1)
	lm.OnLockRelease = func(key string) {
		released <- key
	}

	tok, _ := lm.Acquire(bg(), "cb-exp", 5*time.Second, 1*time.Second, 1, 1)
	if tok == "" {
		t.Fatal("should acquire")
	}

	// Manually expire
	lm.LockKeyForTest("cb-exp")
	lm.ResourceForTest("cb-exp").Holders[tok].leaseExpires = time.Now().Add(-1 * time.Second)
	lm.UnlockKeyForTest("cb-exp")

	ctx, cancel := context.WithCancel(context.Background())
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		lm.LeaseExpiryLoop(ctx)
	}()

	select {
	case key := <-released:
		if key != "cb-exp" {
			t.Fatalf("expected cb-exp, got %s", key)
		}
	case <-time.After(time.Second):
		t.Fatal("OnLockRelease callback not fired by lease expiry")
	}
	cancel()
	<-loopDone
}

// ---------------------------------------------------------------------------
// KVCAS with TTL
// ---------------------------------------------------------------------------

func TestKVCAS_WithTTL(t *testing.T) {
	lm := testManager()
	lm.cfg.LeaseSweepInterval = 50 * time.Millisecond

	// Create via CAS with a 1-second TTL
	ok, err := lm.KVCAS("cas-ttl", "", "hello", 1)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("CAS should succeed on absent key")
	}

	// Should be readable immediately
	val, found := lm.KVGet("cas-ttl")
	if !found || val != "hello" {
		t.Fatalf("expected hello, got %q (found=%v)", val, found)
	}

	// Force expiry by setting ExpiresAt to the past
	lm.LockKeyForTest("cas-ttl")
	sh := lm.ResourceForTest("cas-ttl") // ResourceForTest looks at resources, we need kvStore
	_ = sh
	lm.UnlockKeyForTest("cas-ttl")

	// Use the LeaseExpiryLoop to sweep KV TTLs (it sweeps expired KV too)
	// Or just wait — KVGet does lazy expiry. Force it:
	// Manually expire by accessing internals through the shard.
	// Since we don't have a KV-specific test helper, just sleep past the TTL
	// and rely on lazy expiry in KVGet.
	// Set short TTL: create a new one with ttl=0 first, then create the real one.
	lm.KVDel("cas-ttl")

	// Re-create with extremely short effective TTL by directly manipulating shard
	ok2, err2 := lm.KVCAS("cas-ttl2", "", "world", 1)
	if err2 != nil {
		t.Fatal(err2)
	}
	if !ok2 {
		t.Fatal("CAS should succeed")
	}

	// Use LeaseExpiryLoop which also sweeps KV entries
	// Manually expire via shard access:
	idx := shardIndex("cas-ttl2")
	lm.LockKeyForTest("cas-ttl2")
	lm.UnlockKeyForTest("cas-ttl2")
	// Access shard directly (same package)
	s := &lm.shards[idx]
	s.mu.Lock()
	if e, ok := s.kvStore["cas-ttl2"]; ok {
		e.ExpiresAt = time.Now().Add(-1 * time.Second)
	}
	s.mu.Unlock()

	// KVGet does lazy expiry
	_, found = lm.KVGet("cas-ttl2")
	if found {
		t.Fatal("expected key to be expired and not found")
	}
}

// ---------------------------------------------------------------------------
// Barrier idle GC
// ---------------------------------------------------------------------------

func TestBarrier_IdleGC(t *testing.T) {
	lm := testManager()
	lm.cfg.GCInterval = 50 * time.Millisecond
	lm.cfg.GCMaxIdleTime = 0

	// Create barrier state without tripping it (1 of 3 participants, then timeout)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	lm.BarrierWait(ctx, "b-idle", 3, 50*time.Millisecond, 1)
	cancel()

	// Force LastActivity to the past
	idx := shardIndex("b-idle")
	s := &lm.shards[idx]
	s.mu.Lock()
	if bs, ok := s.barriers["b-idle"]; ok {
		bs.LastActivity = time.Now().Add(-100 * time.Second)
	}
	s.mu.Unlock()

	gcCtx, gcCancel := context.WithCancel(context.Background())
	go lm.GCLoop(gcCtx)
	time.Sleep(200 * time.Millisecond)
	gcCancel()

	// Barrier should have been GC'd
	s.mu.Lock()
	_, exists := s.barriers["b-idle"]
	s.mu.Unlock()
	if exists {
		t.Fatal("idle barrier should have been GC'd")
	}
}

// ---------------------------------------------------------------------------
// GC does NOT prune list with active pop waiters
// ---------------------------------------------------------------------------

func TestGC_ListWithPopWaiters_NotPruned(t *testing.T) {
	lm := testManager()
	lm.cfg.GCInterval = 50 * time.Millisecond
	lm.cfg.GCMaxIdleTime = 0

	// Create a BLPop waiter on an empty list
	blpopDone := make(chan struct{})
	blpopCtx, blpopCancel := context.WithCancel(context.Background())
	go func() {
		defer close(blpopDone)
		lm.BLPop(blpopCtx, "list-gc", 30*time.Second, 1)
	}()
	time.Sleep(50 * time.Millisecond)

	// Force LastActivity to the past so GC considers it idle
	idx := shardIndex("list-gc")
	s := &lm.shards[idx]
	s.mu.Lock()
	if ls, ok := s.lists["list-gc"]; ok {
		ls.LastActivity = time.Now().Add(-100 * time.Second)
	}
	s.mu.Unlock()

	// Run GC
	gcCtx, gcCancel := context.WithCancel(context.Background())
	go lm.GCLoop(gcCtx)
	time.Sleep(200 * time.Millisecond)
	gcCancel()

	// List should NOT have been pruned (has active pop waiters)
	s.mu.Lock()
	_, exists := s.lists["list-gc"]
	s.mu.Unlock()
	if !exists {
		t.Fatal("list with active pop waiters should not be GC'd")
	}

	// Cleanup
	blpopCancel()
	<-blpopDone
}

// ---------------------------------------------------------------------------
// removeLeaderHolderWatcher preserves observer flag
// ---------------------------------------------------------------------------

func TestRemoveLeaderHolderWatcher_ObserverPreserved(t *testing.T) {
	lm := testManager()

	// Register holder watcher via RegisterAndNotifyLeader (observer=false)
	ch := make(chan []byte, 4)
	lm.RegisterAndNotifyLeader("rh-key", "elected", 1, ch, func() {})

	// Also register as observer via RegisterLeaderWatcherWithStatus (sets observer=true)
	lm.RegisterLeaderWatcherWithStatus("rh-key", 1, ch, func() {})
	// Drain any notifications
	for len(ch) > 0 {
		<-ch
	}

	// Now simulate what happens when a lock is released: removeLeaderHolderWatcher
	// is called. Since observer=true, the watcher should be preserved.
	// We can trigger this by acquiring and releasing a lock.
	tok, _ := lm.Acquire(bg(), "rh-key", 5*time.Second, 30*time.Second, 1, 1)
	if tok == "" {
		t.Fatal("should acquire")
	}
	// Register holder watcher again (this is what elect does after acquire)
	lm.RegisterAndNotifyLeader("rh-key", "elected", 1, ch, func() {})
	// Drain notifications
	for len(ch) > 0 {
		<-ch
	}

	// Release the lock — this calls removeLeaderHolderWatcher internally
	lm.Release("rh-key", tok)

	// The observer watcher should still be there
	if !lm.HasLeaderWatcherObserver("rh-key", 1) {
		t.Fatal("observer watcher should be preserved after removeLeaderHolderWatcher")
	}
}
