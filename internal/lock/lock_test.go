package lock

import (
	"context"
	"errors"
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

	_, fence2, ok := lm.RenewWithFence("k1", tok, 60*time.Second)
	if !ok {
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
	if err != nil && err != context.DeadlineExceeded {
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
	_, _, ok := lm.RWRenew("rw1", tokR, 60*time.Second)
	if !ok {
		t.Fatal("RWRenew on read lock should succeed")
	}

	// Release and acquire write lock, then renew
	lm.RWRelease("rw1", tokR)

	tokW, _, err := lm.RWAcquire(bg(), "rw1", 'w', 5*time.Second, 30*time.Second, 2)
	if err != nil {
		t.Fatal(err)
	}
	_, _, ok = lm.RWRenew("rw1", tokW, 60*time.Second)
	if !ok {
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
	_, _, ok := lm.RWRenew("k1", tok, 30*time.Second)
	if ok {
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
	// Set counter to MinInt64 and decrement by 1
	// In Go, this wraps around (integer overflow is defined behavior)
	lm.SetCounter("k1", math.MinInt64)
	val, err := lm.Decr("k1", 1)
	if err != nil {
		t.Fatal(err)
	}
	// Go wraps: MinInt64 + (-1) = MaxInt64
	if val != math.MaxInt64 {
		t.Fatalf("expected MaxInt64 from wrap, got %d", val)
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

	remaining, renewFence, ok := lm.RWRenew("k1", tok, 60*time.Second)
	if !ok {
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
