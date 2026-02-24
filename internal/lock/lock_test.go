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

// ---------------------------------------------------------------------------
// FIFOAcquire
// ---------------------------------------------------------------------------

func TestFIFOAcquire_Immediate(t *testing.T) {
	lm := testManager()
	tok, err := lm.FIFOAcquire(bg(), "k1", 5*time.Second, 30*time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	if tok == "" {
		t.Fatal("expected token")
	}
	st := lm.locks["k1"]
	if st.OwnerToken != tok {
		t.Fatalf("owner_token mismatch: %s != %s", st.OwnerToken, tok)
	}
	if st.OwnerConnID != 1 {
		t.Fatalf("owner_conn_id: got %d want 1", st.OwnerConnID)
	}
}

func TestFIFOAcquire_Timeout(t *testing.T) {
	lm := testManager()
	_, err := lm.FIFOAcquire(bg(), "k1", 5*time.Second, 30*time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	tok, err := lm.FIFOAcquire(bg(), "k1", 0, 30*time.Second, 2)
	if err != nil {
		t.Fatal(err)
	}
	if tok != "" {
		t.Fatal("expected empty token (timeout)")
	}
}

func TestFIFOAcquire_FIFOOrdering(t *testing.T) {
	lm := testManager()
	tok1, _ := lm.FIFOAcquire(bg(), "k1", 5*time.Second, 30*time.Second, 1)

	var tok2, tok3 string
	var mu sync.Mutex
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		t, _ := lm.FIFOAcquire(bg(), "k1", 5*time.Second, 30*time.Second, 2)
		mu.Lock()
		tok2 = t
		mu.Unlock()
	}()
	go func() {
		defer wg.Done()
		time.Sleep(20 * time.Millisecond) // ensure conn2 enqueues first
		t, _ := lm.FIFOAcquire(bg(), "k1", 5*time.Second, 30*time.Second, 3)
		mu.Lock()
		tok3 = t
		mu.Unlock()
	}()

	time.Sleep(50 * time.Millisecond)
	lm.FIFORelease("k1", tok1)
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	if tok2 == "" {
		mu.Unlock()
		t.Fatal("conn2 should have acquired")
	}
	mu.Unlock()

	if lm.locks["k1"].OwnerConnID != 2 {
		t.Fatalf("expected conn 2 to own, got %d", lm.locks["k1"].OwnerConnID)
	}

	lm.FIFORelease("k1", tok2)
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
	_, err := lm.FIFOAcquire(bg(), "k1", 5*time.Second, 30*time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = lm.FIFOAcquire(bg(), "k2", 5*time.Second, 30*time.Second, 2)
	if err != ErrMaxLocks {
		t.Fatalf("expected ErrMaxLocks, got %v", err)
	}
}

func TestFIFOAcquire_ContextCancel(t *testing.T) {
	lm := testManager()
	_, err := lm.FIFOAcquire(bg(), "k1", 5*time.Second, 30*time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var acquireErr error
	go func() {
		_, acquireErr = lm.FIFOAcquire(ctx, "k1", 30*time.Second, 30*time.Second, 2)
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
	tok, _ := lm.FIFOAcquire(bg(), "k1", 5*time.Second, 30*time.Second, 1)
	if !lm.FIFORelease("k1", tok) {
		t.Fatal("release should succeed")
	}
	if lm.locks["k1"].OwnerToken != "" {
		t.Fatal("owner should be cleared")
	}
}

func TestFIFORelease_WrongToken(t *testing.T) {
	lm := testManager()
	lm.FIFOAcquire(bg(), "k1", 5*time.Second, 30*time.Second, 1)
	if lm.FIFORelease("k1", "wrong") {
		t.Fatal("release with wrong token should fail")
	}
}

func TestFIFORelease_Nonexistent(t *testing.T) {
	lm := testManager()
	if lm.FIFORelease("nope", "tok") {
		t.Fatal("release of nonexistent key should fail")
	}
}

func TestFIFORelease_TransfersToWaiter(t *testing.T) {
	lm := testManager()
	tok1, _ := lm.FIFOAcquire(bg(), "k1", 5*time.Second, 30*time.Second, 1)

	done := make(chan string, 1)
	go func() {
		tok, _ := lm.FIFOAcquire(bg(), "k1", 5*time.Second, 30*time.Second, 2)
		done <- tok
	}()
	time.Sleep(50 * time.Millisecond)

	lm.FIFORelease("k1", tok1)
	tok2 := <-done
	if tok2 == "" {
		t.Fatal("conn2 should have acquired")
	}
	if lm.locks["k1"].OwnerConnID != 2 {
		t.Fatal("conn2 should own the lock")
	}
}

// ---------------------------------------------------------------------------
// FIFORenew
// ---------------------------------------------------------------------------

func TestFIFORenew_Valid(t *testing.T) {
	lm := testManager()
	tok, _ := lm.FIFOAcquire(bg(), "k1", 5*time.Second, 30*time.Second, 1)
	remaining, ok := lm.FIFORenew("k1", tok, 60*time.Second)
	if !ok {
		t.Fatal("renew should succeed")
	}
	if remaining <= 0 {
		t.Fatal("remaining should be > 0")
	}
}

func TestFIFORenew_WrongToken(t *testing.T) {
	lm := testManager()
	lm.FIFOAcquire(bg(), "k1", 5*time.Second, 30*time.Second, 1)
	_, ok := lm.FIFORenew("k1", "wrong", 30*time.Second)
	if ok {
		t.Fatal("renew with wrong token should fail")
	}
}

func TestFIFORenew_Nonexistent(t *testing.T) {
	lm := testManager()
	_, ok := lm.FIFORenew("nope", "tok", 30*time.Second)
	if ok {
		t.Fatal("renew of nonexistent key should fail")
	}
}

func TestFIFORenew_Expired(t *testing.T) {
	lm := testManager()
	tok, _ := lm.FIFOAcquire(bg(), "k1", 5*time.Second, 1*time.Second, 1)
	// Manually expire
	lm.mu.Lock()
	lm.locks["k1"].LeaseExpires = time.Now().Add(-1 * time.Second)
	lm.mu.Unlock()

	_, ok := lm.FIFORenew("k1", tok, 30*time.Second)
	if ok {
		t.Fatal("renew of expired lease should fail")
	}
	if lm.locks["k1"].OwnerToken != "" {
		t.Fatal("owner should be cleared after expired renew")
	}
}

// ---------------------------------------------------------------------------
// FIFOEnqueue
// ---------------------------------------------------------------------------

func TestFIFOEnqueue_Immediate(t *testing.T) {
	lm := testManager()
	status, tok, lease, err := lm.FIFOEnqueue("k1", 30*time.Second, 1)
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
	if _, ok := lm.connEnqueued[connKey{ConnID: 1, Key: "k1"}]; !ok {
		t.Fatal("should be in connEnqueued")
	}
}

func TestFIFOEnqueue_Queued(t *testing.T) {
	lm := testManager()
	lm.FIFOAcquire(bg(), "k1", 5*time.Second, 30*time.Second, 1)

	status, tok, lease, err := lm.FIFOEnqueue("k1", 30*time.Second, 2)
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
	es := lm.connEnqueued[connKey{ConnID: 2, Key: "k1"}]
	if es == nil || es.waiter == nil {
		t.Fatal("should have waiter in connEnqueued")
	}
}

func TestFIFOEnqueue_DoubleEnqueue(t *testing.T) {
	lm := testManager()
	lm.FIFOEnqueue("k1", 30*time.Second, 1)
	_, _, _, err := lm.FIFOEnqueue("k1", 30*time.Second, 1)
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
	lm.FIFOEnqueue("k1", 30*time.Second, 1)
	_, _, _, err := lm.FIFOEnqueue("k2", 30*time.Second, 2)
	if err != ErrMaxLocks {
		t.Fatalf("expected ErrMaxLocks, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// FIFOWait
// ---------------------------------------------------------------------------

func TestFIFOWait_FastPath(t *testing.T) {
	lm := testManager()
	status, token, _, _ := lm.FIFOEnqueue("k1", 30*time.Second, 1)
	if status != "acquired" {
		t.Fatal("expected acquired")
	}

	tok, ttl, err := lm.FIFOWait(bg(), "k1", 5*time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	if tok != token {
		t.Fatalf("token mismatch: %s != %s", tok, token)
	}
	if ttl != 30 {
		t.Fatalf("expected ttl 30, got %d", ttl)
	}
	if _, ok := lm.connEnqueued[connKey{ConnID: 1, Key: "k1"}]; ok {
		t.Fatal("should be removed from connEnqueued")
	}
}

func TestFIFOWait_QueuedThenWait(t *testing.T) {
	lm := testManager()
	tok1, _ := lm.FIFOAcquire(bg(), "k1", 5*time.Second, 30*time.Second, 1)
	lm.FIFOEnqueue("k1", 30*time.Second, 2)

	done := make(chan struct{})
	var gotTok string
	var gotTTL int
	go func() {
		gotTok, gotTTL, _ = lm.FIFOWait(bg(), "k1", 5*time.Second, 2)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	lm.FIFORelease("k1", tok1)
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
	lm.FIFOAcquire(bg(), "k1", 5*time.Second, 30*time.Second, 1)
	lm.FIFOEnqueue("k1", 30*time.Second, 2)

	tok, _, err := lm.FIFOWait(bg(), "k1", 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if tok != "" {
		t.Fatal("expected empty token (timeout)")
	}
}

func TestFIFOWait_NotEnqueued(t *testing.T) {
	lm := testManager()
	_, _, err := lm.FIFOWait(bg(), "k1", 5*time.Second, 1)
	if err != ErrNotEnqueued {
		t.Fatalf("expected ErrNotEnqueued, got %v", err)
	}
}

func TestFIFOWait_FastPathLockLost(t *testing.T) {
	lm := testManager()
	status, _, _, _ := lm.FIFOEnqueue("k1", 1*time.Second, 1)
	if status != "acquired" {
		t.Fatal("expected acquired")
	}
	// Manually expire + clear owner
	lm.mu.Lock()
	lm.locks["k1"].LeaseExpires = time.Now().Add(-1 * time.Second)
	lm.locks["k1"].OwnerToken = ""
	lm.locks["k1"].OwnerConnID = 0
	lm.mu.Unlock()

	tok, _, err := lm.FIFOWait(bg(), "k1", 5*time.Second, 1)
	if !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("expected ErrLeaseExpired, got %v", err)
	}
	if tok != "" {
		t.Fatal("expected empty token (lock lost)")
	}
}

func TestFIFOWait_ContextCancel(t *testing.T) {
	lm := testManager()
	lm.FIFOAcquire(bg(), "k1", 5*time.Second, 30*time.Second, 1)
	lm.FIFOEnqueue("k1", 30*time.Second, 2)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var waitErr error
	go func() {
		_, _, waitErr = lm.FIFOWait(ctx, "k1", 30*time.Second, 2)
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
	status, token, _, _ := lm.FIFOEnqueue("k1", 30*time.Second, 1)
	if status != "acquired" {
		t.Fatal("expected acquired")
	}

	tok, _, _ := lm.FIFOWait(bg(), "k1", 5*time.Second, 1)
	if tok != token {
		t.Fatal("token mismatch")
	}

	if !lm.FIFORelease("k1", tok) {
		t.Fatal("release should succeed")
	}
	if lm.locks["k1"].OwnerToken != "" {
		t.Fatal("owner should be cleared")
	}
}

func TestTwoPhase_Contention(t *testing.T) {
	lm := testManager()
	tok1, _ := lm.FIFOAcquire(bg(), "k1", 5*time.Second, 30*time.Second, 1)

	status, _, _, _ := lm.FIFOEnqueue("k1", 30*time.Second, 2)
	if status != "queued" {
		t.Fatal("expected queued")
	}

	done := make(chan string, 1)
	go func() {
		tok, _, _ := lm.FIFOWait(bg(), "k1", 5*time.Second, 2)
		done <- tok
	}()

	time.Sleep(50 * time.Millisecond)
	lm.FIFORelease("k1", tok1)
	tok2 := <-done

	if tok2 == "" {
		t.Fatal("conn2 should have acquired")
	}
	if lm.locks["k1"].OwnerConnID != 2 {
		t.Fatal("conn2 should own")
	}
	lm.FIFORelease("k1", tok2)
}

// ---------------------------------------------------------------------------
// CleanupConnection
// ---------------------------------------------------------------------------

func TestCleanup_ReleasesOwned(t *testing.T) {
	lm := testManager()
	tok, _ := lm.FIFOAcquire(bg(), "k1", 5*time.Second, 30*time.Second, 100)
	if tok == "" {
		t.Fatal("should acquire")
	}
	lm.CleanupConnection(100)
	if lm.locks["k1"].OwnerToken != "" {
		t.Fatal("owner should be cleared")
	}
}

func TestCleanup_CancelsPending(t *testing.T) {
	lm := testManager()
	lm.FIFOAcquire(bg(), "k1", 5*time.Second, 30*time.Second, 1)

	done := make(chan string, 1)
	go func() {
		tok, _ := lm.FIFOAcquire(bg(), "k1", 10*time.Second, 30*time.Second, 2)
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
	lm.FIFOAcquire(bg(), "k1", 5*time.Second, 30*time.Second, 1)

	done := make(chan string, 1)
	go func() {
		tok, _ := lm.FIFOAcquire(bg(), "k1", 5*time.Second, 30*time.Second, 2)
		done <- tok
	}()
	time.Sleep(50 * time.Millisecond)

	lm.CleanupConnection(1) // releases conn1's lock -> transfers to conn2
	tok2 := <-done
	if tok2 == "" {
		t.Fatal("conn2 should have acquired after transfer")
	}
	if lm.locks["k1"].OwnerConnID != 2 {
		t.Fatal("conn2 should own")
	}
}

func TestCleanup_EnqueuedWaiter(t *testing.T) {
	lm := testManager()
	lm.FIFOAcquire(bg(), "k1", 5*time.Second, 30*time.Second, 1)
	lm.FIFOEnqueue("k1", 30*time.Second, 2)

	if _, ok := lm.connEnqueued[connKey{ConnID: 2, Key: "k1"}]; !ok {
		t.Fatal("should be enqueued")
	}

	lm.CleanupConnection(2)
	if _, ok := lm.connEnqueued[connKey{ConnID: 2, Key: "k1"}]; ok {
		t.Fatal("should be cleaned up")
	}
	if len(lm.locks["k1"].Waiters) != 0 {
		t.Fatal("waiter should be removed")
	}
}

func TestCleanup_FastPath(t *testing.T) {
	lm := testManager()
	status, _, _, _ := lm.FIFOEnqueue("k1", 30*time.Second, 1)
	if status != "acquired" {
		t.Fatal("expected acquired")
	}

	lm.CleanupConnection(1)
	if _, ok := lm.connEnqueued[connKey{ConnID: 1, Key: "k1"}]; ok {
		t.Fatal("should be cleaned up")
	}
	if lm.locks["k1"].OwnerToken != "" {
		t.Fatal("owner should be cleared")
	}
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
	tok, _ := lm.FIFOAcquire(bg(), "k1", 5*time.Second, 1*time.Second, 1)
	if tok == "" {
		t.Fatal("should acquire")
	}
	// Manually expire
	lm.mu.Lock()
	lm.locks["k1"].LeaseExpires = time.Now().Add(-1 * time.Second)
	lm.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		lm.LeaseExpiryLoop(ctx)
	}()
	time.Sleep(200 * time.Millisecond)
	cancel()
	<-loopDone

	lm.mu.Lock()
	ownerToken := lm.locks["k1"].OwnerToken
	lm.mu.Unlock()
	if ownerToken != "" {
		t.Fatal("owner should be cleared by expiry loop")
	}
}

func TestLeaseExpiry_TransfersToWaiter(t *testing.T) {
	lm := testManager()
	lm.cfg.LeaseSweepInterval = 50 * time.Millisecond
	lm.FIFOAcquire(bg(), "k1", 5*time.Second, 1*time.Second, 1)

	done := make(chan string, 1)
	go func() {
		tok, _ := lm.FIFOAcquire(bg(), "k1", 5*time.Second, 30*time.Second, 2)
		done <- tok
	}()
	time.Sleep(50 * time.Millisecond)

	// Expire
	lm.mu.Lock()
	lm.locks["k1"].LeaseExpires = time.Now().Add(-1 * time.Second)
	lm.mu.Unlock()

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
	lm.mu.Lock()
	ownerConnID := lm.locks["k1"].OwnerConnID
	lm.mu.Unlock()
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

	tok, _ := lm.FIFOAcquire(bg(), "k1", 5*time.Second, 30*time.Second, 1)
	lm.FIFORelease("k1", tok)
	// Force idle
	lm.mu.Lock()
	lm.locks["k1"].LastActivity = time.Now().Add(-100 * time.Second)
	lm.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	go lm.GCLoop(ctx)
	time.Sleep(200 * time.Millisecond)
	cancel()

	lm.mu.Lock()
	_, exists := lm.locks["k1"]
	lm.mu.Unlock()
	if exists {
		t.Fatal("k1 should have been GC'd")
	}
}

func TestGC_DoesNotPruneHeld(t *testing.T) {
	lm := testManager()
	lm.cfg.GCInterval = 50 * time.Millisecond
	lm.cfg.GCMaxIdleTime = 0

	lm.FIFOAcquire(bg(), "k1", 5*time.Second, 30*time.Second, 1)
	lm.mu.Lock()
	lm.locks["k1"].LastActivity = time.Now().Add(-100 * time.Second)
	lm.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	go lm.GCLoop(ctx)
	time.Sleep(200 * time.Millisecond)
	cancel()

	lm.mu.Lock()
	_, exists := lm.locks["k1"]
	lm.mu.Unlock()
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
	tok, err := lm.SemAcquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	if tok == "" {
		t.Fatal("expected token")
	}
	st := lm.sems["s1"]
	if st.Limit != 3 {
		t.Fatalf("limit: got %d want 3", st.Limit)
	}
	if len(st.Holders) != 1 {
		t.Fatalf("holders: got %d want 1", len(st.Holders))
	}
}

func TestSemAcquire_MultipleConcurrent(t *testing.T) {
	lm := testManager()
	tok1, _ := lm.SemAcquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 3)
	tok2, _ := lm.SemAcquire(bg(), "s1", 5*time.Second, 30*time.Second, 2, 3)
	tok3, _ := lm.SemAcquire(bg(), "s1", 5*time.Second, 30*time.Second, 3, 3)
	if tok1 == "" || tok2 == "" || tok3 == "" {
		t.Fatal("all three should acquire immediately")
	}
	if len(lm.sems["s1"].Holders) != 3 {
		t.Fatalf("holders: got %d want 3", len(lm.sems["s1"].Holders))
	}
}

func TestSemAcquire_AtCapacityTimeout(t *testing.T) {
	lm := testManager()
	lm.SemAcquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 2)
	lm.SemAcquire(bg(), "s1", 5*time.Second, 30*time.Second, 2, 2)

	// Third should timeout with 0 timeout
	tok, err := lm.SemAcquire(bg(), "s1", 0, 30*time.Second, 3, 2)
	if err != nil {
		t.Fatal(err)
	}
	if tok != "" {
		t.Fatal("expected empty token (timeout)")
	}
}

func TestSemAcquire_FIFOOrdering(t *testing.T) {
	lm := testManager()
	tok1, _ := lm.SemAcquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 1)

	var tok2, tok3 string
	var mu sync.Mutex
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		t, _ := lm.SemAcquire(bg(), "s1", 5*time.Second, 30*time.Second, 2, 1)
		mu.Lock()
		tok2 = t
		mu.Unlock()
	}()
	go func() {
		defer wg.Done()
		time.Sleep(20 * time.Millisecond)
		t, _ := lm.SemAcquire(bg(), "s1", 5*time.Second, 30*time.Second, 3, 1)
		mu.Lock()
		tok3 = t
		mu.Unlock()
	}()

	time.Sleep(50 * time.Millisecond)
	lm.SemRelease("s1", tok1)
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	if tok2 == "" {
		mu.Unlock()
		t.Fatal("conn2 should have acquired")
	}
	mu.Unlock()

	lm.SemRelease("s1", tok2)
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
	_, err := lm.SemAcquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	_, err = lm.SemAcquire(bg(), "s2", 5*time.Second, 30*time.Second, 2, 3)
	if err != ErrMaxLocks {
		t.Fatalf("expected ErrMaxLocks, got %v", err)
	}
}

func TestSemAcquire_LimitMismatch(t *testing.T) {
	lm := testManager()
	_, err := lm.SemAcquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	_, err = lm.SemAcquire(bg(), "s1", 5*time.Second, 30*time.Second, 2, 5)
	if err != ErrLimitMismatch {
		t.Fatalf("expected ErrLimitMismatch, got %v", err)
	}
}

func TestSemAcquire_MaxLocksSharedWithLocks(t *testing.T) {
	lm := testManager()
	lm.cfg.MaxLocks = 2
	lm.FIFOAcquire(bg(), "lock1", 5*time.Second, 30*time.Second, 1)
	lm.SemAcquire(bg(), "sem1", 5*time.Second, 30*time.Second, 2, 3)
	_, err := lm.SemAcquire(bg(), "sem2", 5*time.Second, 30*time.Second, 3, 3)
	if err != ErrMaxLocks {
		t.Fatalf("expected ErrMaxLocks, got %v", err)
	}
}

func TestSemAcquire_ContextCancel(t *testing.T) {
	lm := testManager()
	_, err := lm.SemAcquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 1)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var acquireErr error
	go func() {
		_, acquireErr = lm.SemAcquire(ctx, "s1", 30*time.Second, 30*time.Second, 2, 1)
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
	tok, _ := lm.SemAcquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 3)
	if !lm.SemRelease("s1", tok) {
		t.Fatal("release should succeed")
	}
	if len(lm.sems["s1"].Holders) != 0 {
		t.Fatal("holders should be empty")
	}
}

func TestSemRelease_WrongToken(t *testing.T) {
	lm := testManager()
	lm.SemAcquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 3)
	if lm.SemRelease("s1", "wrong") {
		t.Fatal("release with wrong token should fail")
	}
}

func TestSemRelease_TransfersToWaiter(t *testing.T) {
	lm := testManager()
	tok1, _ := lm.SemAcquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 1)

	done := make(chan string, 1)
	go func() {
		tok, _ := lm.SemAcquire(bg(), "s1", 5*time.Second, 30*time.Second, 2, 1)
		done <- tok
	}()
	time.Sleep(50 * time.Millisecond)

	lm.SemRelease("s1", tok1)
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
	tok, _ := lm.SemAcquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 3)
	remaining, ok := lm.SemRenew("s1", tok, 60*time.Second)
	if !ok {
		t.Fatal("renew should succeed")
	}
	if remaining <= 0 {
		t.Fatal("remaining should be > 0")
	}
}

func TestSemRenew_WrongToken(t *testing.T) {
	lm := testManager()
	lm.SemAcquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 3)
	_, ok := lm.SemRenew("s1", "wrong", 30*time.Second)
	if ok {
		t.Fatal("renew with wrong token should fail")
	}
}

func TestSemRenew_Expired(t *testing.T) {
	lm := testManager()
	tok, _ := lm.SemAcquire(bg(), "s1", 5*time.Second, 1*time.Second, 1, 3)
	lm.mu.Lock()
	lm.sems["s1"].Holders[tok].leaseExpires = time.Now().Add(-1 * time.Second)
	lm.mu.Unlock()

	_, ok := lm.SemRenew("s1", tok, 30*time.Second)
	if ok {
		t.Fatal("renew of expired lease should fail")
	}
	if len(lm.sems["s1"].Holders) != 0 {
		t.Fatal("expired holder should be removed")
	}
}

// ---------------------------------------------------------------------------
// SemEnqueue / SemWait
// ---------------------------------------------------------------------------

func TestSemEnqueue_Immediate(t *testing.T) {
	lm := testManager()
	status, tok, lease, err := lm.SemEnqueue("s1", 30*time.Second, 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	if status != "acquired" || tok == "" || lease != 30 {
		t.Fatalf("unexpected: status=%s tok=%s lease=%d", status, tok, lease)
	}
}

func TestSemEnqueue_Queued(t *testing.T) {
	lm := testManager()
	lm.SemAcquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 1)

	status, tok, lease, err := lm.SemEnqueue("s1", 30*time.Second, 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if status != "queued" || tok != "" || lease != 0 {
		t.Fatalf("unexpected: status=%s tok=%s lease=%d", status, tok, lease)
	}
}

func TestSemEnqueue_DoubleEnqueue(t *testing.T) {
	lm := testManager()
	lm.SemEnqueue("s1", 30*time.Second, 1, 3)
	_, _, _, err := lm.SemEnqueue("s1", 30*time.Second, 1, 3)
	if err == nil {
		t.Fatal("double enqueue should error")
	}
	if !errors.Is(err, ErrAlreadyEnqueued) {
		t.Fatalf("expected ErrAlreadyEnqueued, got %v", err)
	}
}

func TestSemEnqueue_LimitMismatch(t *testing.T) {
	lm := testManager()
	lm.SemEnqueue("s1", 30*time.Second, 1, 3)
	_, _, _, err := lm.SemEnqueue("s1", 30*time.Second, 2, 5)
	if err != ErrLimitMismatch {
		t.Fatalf("expected ErrLimitMismatch, got %v", err)
	}
}

func TestSemWait_FastPath(t *testing.T) {
	lm := testManager()
	status, token, _, _ := lm.SemEnqueue("s1", 30*time.Second, 1, 3)
	if status != "acquired" {
		t.Fatal("expected acquired")
	}

	tok, ttl, err := lm.SemWait(bg(), "s1", 5*time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	if tok != token || ttl != 30 {
		t.Fatalf("unexpected: tok=%s ttl=%d", tok, ttl)
	}
}

func TestSemWait_QueuedThenWait(t *testing.T) {
	lm := testManager()
	tok1, _ := lm.SemAcquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 1)
	lm.SemEnqueue("s1", 30*time.Second, 2, 1)

	done := make(chan struct{})
	var gotTok string
	go func() {
		gotTok, _, _ = lm.SemWait(bg(), "s1", 5*time.Second, 2)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	lm.SemRelease("s1", tok1)
	<-done

	if gotTok == "" {
		t.Fatal("should have received token")
	}
}

func TestSemWait_Timeout(t *testing.T) {
	lm := testManager()
	lm.SemAcquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 1)
	lm.SemEnqueue("s1", 30*time.Second, 2, 1)

	tok, _, err := lm.SemWait(bg(), "s1", 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if tok != "" {
		t.Fatal("expected empty token (timeout)")
	}
}

func TestSemWait_NotEnqueued(t *testing.T) {
	lm := testManager()
	_, _, err := lm.SemWait(bg(), "s1", 5*time.Second, 1)
	if err != ErrNotEnqueued {
		t.Fatalf("expected ErrNotEnqueued, got %v", err)
	}
}

func TestSemWait_ContextCancel(t *testing.T) {
	lm := testManager()
	lm.SemAcquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 1)
	lm.SemEnqueue("s1", 30*time.Second, 2, 1)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var waitErr error
	go func() {
		_, _, waitErr = lm.SemWait(ctx, "s1", 30*time.Second, 2)
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
	tok, _ := lm.SemAcquire(bg(), "s1", 5*time.Second, 30*time.Second, 100, 3)
	if tok == "" {
		t.Fatal("should acquire")
	}
	lm.CleanupConnection(100)
	if len(lm.sems["s1"].Holders) != 0 {
		t.Fatal("holders should be cleared")
	}
}

func TestSemCleanup_CancelsPending(t *testing.T) {
	lm := testManager()
	lm.SemAcquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 1)

	done := make(chan string, 1)
	go func() {
		tok, _ := lm.SemAcquire(bg(), "s1", 10*time.Second, 30*time.Second, 2, 1)
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
	lm.SemAcquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 1)

	done := make(chan string, 1)
	go func() {
		tok, _ := lm.SemAcquire(bg(), "s1", 5*time.Second, 30*time.Second, 2, 1)
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
	lm.SemAcquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 1)
	lm.SemEnqueue("s1", 30*time.Second, 2, 1)

	lm.CleanupConnection(2)
	if len(lm.sems["s1"].Waiters) != 0 {
		t.Fatal("waiter should be removed")
	}
}

// ---------------------------------------------------------------------------
// Semaphore LeaseExpiryLoop
// ---------------------------------------------------------------------------

func TestSemLeaseExpiry_ReleasesHolder(t *testing.T) {
	lm := testManager()
	lm.cfg.LeaseSweepInterval = 50 * time.Millisecond
	tok, _ := lm.SemAcquire(bg(), "s1", 5*time.Second, 1*time.Second, 1, 3)
	if tok == "" {
		t.Fatal("should acquire")
	}
	lm.mu.Lock()
	lm.sems["s1"].Holders[tok].leaseExpires = time.Now().Add(-1 * time.Second)
	lm.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		lm.LeaseExpiryLoop(ctx)
	}()
	time.Sleep(200 * time.Millisecond)
	cancel()
	<-loopDone

	lm.mu.Lock()
	holderCount := len(lm.sems["s1"].Holders)
	lm.mu.Unlock()
	if holderCount != 0 {
		t.Fatal("holder should be evicted by expiry loop")
	}
}

func TestSemLeaseExpiry_TransfersToWaiter(t *testing.T) {
	lm := testManager()
	lm.cfg.LeaseSweepInterval = 50 * time.Millisecond
	lm.SemAcquire(bg(), "s1", 5*time.Second, 1*time.Second, 1, 1)

	done := make(chan string, 1)
	go func() {
		tok, _ := lm.SemAcquire(bg(), "s1", 5*time.Second, 30*time.Second, 2, 1)
		done <- tok
	}()
	time.Sleep(50 * time.Millisecond)

	// Expire holder
	lm.mu.Lock()
	for _, h := range lm.sems["s1"].Holders {
		h.leaseExpires = time.Now().Add(-1 * time.Second)
	}
	lm.mu.Unlock()

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

	tok, _ := lm.SemAcquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 3)
	lm.SemRelease("s1", tok)
	lm.mu.Lock()
	lm.sems["s1"].LastActivity = time.Now().Add(-100 * time.Second)
	lm.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	go lm.GCLoop(ctx)
	time.Sleep(200 * time.Millisecond)
	cancel()

	lm.mu.Lock()
	_, exists := lm.sems["s1"]
	lm.mu.Unlock()
	if exists {
		t.Fatal("s1 should have been GC'd")
	}
}

func TestSemGC_DoesNotPruneHeld(t *testing.T) {
	lm := testManager()
	lm.cfg.GCInterval = 50 * time.Millisecond
	lm.cfg.GCMaxIdleTime = 0

	lm.SemAcquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 3)
	lm.mu.Lock()
	lm.sems["s1"].LastActivity = time.Now().Add(-100 * time.Second)
	lm.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	go lm.GCLoop(ctx)
	time.Sleep(200 * time.Millisecond)
	cancel()

	lm.mu.Lock()
	_, exists := lm.sems["s1"]
	lm.mu.Unlock()
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
	_, err := lm.FIFOAcquire(bg(), "k1", 5*time.Second, 30*time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}

	// conn2 enqueues as waiter (fills the queue)
	done := make(chan struct{})
	go func() {
		lm.FIFOAcquire(bg(), "k1", 5*time.Second, 30*time.Second, 2)
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)

	// conn3 should get ErrMaxWaiters
	_, err = lm.FIFOAcquire(bg(), "k1", 5*time.Second, 30*time.Second, 3)
	if err != ErrMaxWaiters {
		t.Fatalf("expected ErrMaxWaiters, got %v", err)
	}

	// Cleanup: release so goroutine can finish
	lm.mu.Lock()
	tok := lm.locks["k1"].OwnerToken
	lm.mu.Unlock()
	lm.FIFORelease("k1", tok)
	<-done
}

func TestFIFOEnqueue_MaxWaiters(t *testing.T) {
	lm := testManager()
	lm.cfg.MaxWaiters = 1

	// conn1 holds the lock
	lm.FIFOAcquire(bg(), "k1", 5*time.Second, 30*time.Second, 1)

	// conn2 enqueues (fills queue)
	status, _, _, err := lm.FIFOEnqueue("k1", 30*time.Second, 2)
	if err != nil {
		t.Fatal(err)
	}
	if status != "queued" {
		t.Fatalf("expected queued, got %s", status)
	}

	// conn3 should get ErrMaxWaiters
	_, _, _, err = lm.FIFOEnqueue("k1", 30*time.Second, 3)
	if err != ErrMaxWaiters {
		t.Fatalf("expected ErrMaxWaiters, got %v", err)
	}
}

func TestSemAcquire_MaxWaiters(t *testing.T) {
	lm := testManager()
	lm.cfg.MaxWaiters = 1

	// conn1 holds the single slot
	_, err := lm.SemAcquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 1)
	if err != nil {
		t.Fatal(err)
	}

	// conn2 waits (fills queue)
	done := make(chan struct{})
	go func() {
		lm.SemAcquire(bg(), "s1", 5*time.Second, 30*time.Second, 2, 1)
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)

	// conn3 should get ErrMaxWaiters
	_, err = lm.SemAcquire(bg(), "s1", 5*time.Second, 30*time.Second, 3, 1)
	if err != ErrMaxWaiters {
		t.Fatalf("expected ErrMaxWaiters, got %v", err)
	}

	// Cleanup
	lm.mu.Lock()
	var tok string
	for t := range lm.sems["s1"].Holders {
		tok = t
		break
	}
	lm.mu.Unlock()
	lm.SemRelease("s1", tok)
	<-done
}

func TestSemEnqueue_MaxWaiters(t *testing.T) {
	lm := testManager()
	lm.cfg.MaxWaiters = 1

	// conn1 holds the slot
	lm.SemAcquire(bg(), "s1", 5*time.Second, 30*time.Second, 1, 1)

	// conn2 enqueues (fills queue)
	status, _, _, err := lm.SemEnqueue("s1", 30*time.Second, 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if status != "queued" {
		t.Fatalf("expected queued, got %s", status)
	}

	// conn3 should get ErrMaxWaiters
	_, _, _, err = lm.SemEnqueue("s1", 30*time.Second, 3, 1)
	if err != ErrMaxWaiters {
		t.Fatalf("expected ErrMaxWaiters, got %v", err)
	}
}
