package lock

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
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
	exists := lm.ConnEnqueuedForTest(eqKey) != nil
	lm.UnlockConnMuForTest()
	if exists {
		t.Fatal("connEnqueued should be cleaned up by expiry loop")
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
	exists := lm.ConnEnqueuedForTest(eqKey) != nil
	lm.UnlockConnMuForTest()
	if exists {
		t.Fatal("connEnqueued should be cleaned up by expiry loop")
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
