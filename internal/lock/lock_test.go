package lock

import (
	"context"
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
		GCMaxIdleTime:           0,
		MaxLocks:                1024,
		ReadTimeout:             5 * time.Second,
		AutoReleaseOnDisconnect: true,
	}
}

func testManager() *LockManager {
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	return NewLockManager(testConfig(), log)
}

// ---------------------------------------------------------------------------
// FIFOAcquire
// ---------------------------------------------------------------------------

func TestFIFOAcquire_Immediate(t *testing.T) {
	lm := testManager()
	tok, err := lm.FIFOAcquire("k1", 5*time.Second, 30*time.Second, 1)
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
	_, err := lm.FIFOAcquire("k1", 5*time.Second, 30*time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	tok, err := lm.FIFOAcquire("k1", 0, 30*time.Second, 2)
	if err != nil {
		t.Fatal(err)
	}
	if tok != "" {
		t.Fatal("expected empty token (timeout)")
	}
}

func TestFIFOAcquire_FIFOOrdering(t *testing.T) {
	lm := testManager()
	tok1, _ := lm.FIFOAcquire("k1", 5*time.Second, 30*time.Second, 1)

	var tok2, tok3 string
	var mu sync.Mutex
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		t, _ := lm.FIFOAcquire("k1", 5*time.Second, 30*time.Second, 2)
		mu.Lock()
		tok2 = t
		mu.Unlock()
	}()
	go func() {
		defer wg.Done()
		time.Sleep(20 * time.Millisecond) // ensure conn2 enqueues first
		t, _ := lm.FIFOAcquire("k1", 5*time.Second, 30*time.Second, 3)
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
	_, err := lm.FIFOAcquire("k1", 5*time.Second, 30*time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = lm.FIFOAcquire("k2", 5*time.Second, 30*time.Second, 2)
	if err != ErrMaxLocks {
		t.Fatalf("expected ErrMaxLocks, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// FIFORelease
// ---------------------------------------------------------------------------

func TestFIFORelease_Valid(t *testing.T) {
	lm := testManager()
	tok, _ := lm.FIFOAcquire("k1", 5*time.Second, 30*time.Second, 1)
	if !lm.FIFORelease("k1", tok) {
		t.Fatal("release should succeed")
	}
	if lm.locks["k1"].OwnerToken != "" {
		t.Fatal("owner should be cleared")
	}
}

func TestFIFORelease_WrongToken(t *testing.T) {
	lm := testManager()
	lm.FIFOAcquire("k1", 5*time.Second, 30*time.Second, 1)
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
	tok1, _ := lm.FIFOAcquire("k1", 5*time.Second, 30*time.Second, 1)

	done := make(chan string, 1)
	go func() {
		tok, _ := lm.FIFOAcquire("k1", 5*time.Second, 30*time.Second, 2)
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
	tok, _ := lm.FIFOAcquire("k1", 5*time.Second, 30*time.Second, 1)
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
	lm.FIFOAcquire("k1", 5*time.Second, 30*time.Second, 1)
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
	tok, _ := lm.FIFOAcquire("k1", 5*time.Second, 1*time.Second, 1)
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
	lm.FIFOAcquire("k1", 5*time.Second, 30*time.Second, 1)

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
	if es == nil || es.Waiter == nil {
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
	if !IsAlreadyEnqueued(err) {
		t.Fatalf("expected already-enqueued error, got %v", err)
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

	tok, ttl, err := lm.FIFOWait("k1", 5*time.Second, 1)
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
	tok1, _ := lm.FIFOAcquire("k1", 5*time.Second, 30*time.Second, 1)
	lm.FIFOEnqueue("k1", 30*time.Second, 2)

	done := make(chan struct{})
	var gotTok string
	var gotTTL int
	go func() {
		gotTok, gotTTL, _ = lm.FIFOWait("k1", 5*time.Second, 2)
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
	lm.FIFOAcquire("k1", 5*time.Second, 30*time.Second, 1)
	lm.FIFOEnqueue("k1", 30*time.Second, 2)

	tok, _, err := lm.FIFOWait("k1", 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if tok != "" {
		t.Fatal("expected empty token (timeout)")
	}
}

func TestFIFOWait_NotEnqueued(t *testing.T) {
	lm := testManager()
	_, _, err := lm.FIFOWait("k1", 5*time.Second, 1)
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

	tok, _, err := lm.FIFOWait("k1", 5*time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	if tok != "" {
		t.Fatal("expected empty token (lock lost)")
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

	tok, _, _ := lm.FIFOWait("k1", 5*time.Second, 1)
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
	tok1, _ := lm.FIFOAcquire("k1", 5*time.Second, 30*time.Second, 1)

	status, _, _, _ := lm.FIFOEnqueue("k1", 30*time.Second, 2)
	if status != "queued" {
		t.Fatal("expected queued")
	}

	done := make(chan string, 1)
	go func() {
		tok, _, _ := lm.FIFOWait("k1", 5*time.Second, 2)
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
	tok, _ := lm.FIFOAcquire("k1", 5*time.Second, 30*time.Second, 100)
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
	lm.FIFOAcquire("k1", 5*time.Second, 30*time.Second, 1)

	done := make(chan string, 1)
	go func() {
		tok, _ := lm.FIFOAcquire("k1", 10*time.Second, 30*time.Second, 2)
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
	lm.FIFOAcquire("k1", 5*time.Second, 30*time.Second, 1)

	done := make(chan string, 1)
	go func() {
		tok, _ := lm.FIFOAcquire("k1", 5*time.Second, 30*time.Second, 2)
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
	lm.FIFOAcquire("k1", 5*time.Second, 30*time.Second, 1)
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
	tok, _ := lm.FIFOAcquire("k1", 5*time.Second, 1*time.Second, 1)
	if tok == "" {
		t.Fatal("should acquire")
	}
	// Manually expire
	lm.mu.Lock()
	lm.locks["k1"].LeaseExpires = time.Now().Add(-1 * time.Second)
	lm.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	go lm.LeaseExpiryLoop(ctx)
	time.Sleep(200 * time.Millisecond)
	cancel()

	if lm.locks["k1"].OwnerToken != "" {
		t.Fatal("owner should be cleared by expiry loop")
	}
}

func TestLeaseExpiry_TransfersToWaiter(t *testing.T) {
	lm := testManager()
	lm.cfg.LeaseSweepInterval = 50 * time.Millisecond
	lm.FIFOAcquire("k1", 5*time.Second, 1*time.Second, 1)

	done := make(chan string, 1)
	go func() {
		tok, _ := lm.FIFOAcquire("k1", 5*time.Second, 30*time.Second, 2)
		done <- tok
	}()
	time.Sleep(50 * time.Millisecond)

	// Expire
	lm.mu.Lock()
	lm.locks["k1"].LeaseExpires = time.Now().Add(-1 * time.Second)
	lm.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	go lm.LeaseExpiryLoop(ctx)

	tok2 := <-done
	cancel()

	if tok2 == "" {
		t.Fatal("conn2 should have acquired after expiry")
	}
	if lm.locks["k1"].OwnerConnID != 2 {
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

	tok, _ := lm.FIFOAcquire("k1", 5*time.Second, 30*time.Second, 1)
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

	lm.FIFOAcquire("k1", 5*time.Second, 30*time.Second, 1)
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
