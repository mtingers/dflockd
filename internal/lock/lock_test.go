package lock

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/mtingers/dflockd/internal/config"
)

func newTestManager(t *testing.T, autoRelease bool) *LockManager {
	t.Helper()
	cfg := &config.Config{
		MaxLocks:                1024,
		MaxWaiters:              0,
		DefaultLeaseTTL:         33 * time.Second,
		LeaseSweepInterval:      time.Second,
		GCInterval:              time.Second,
		GCMaxIdleTime:           time.Minute,
		AutoReleaseOnDisconnect: autoRelease,
	}
	return NewLockManager(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// ---------------------------------------------------------------------------
// Basic Acquire / Release
// ---------------------------------------------------------------------------

func TestAcquire_FastPath(t *testing.T) {
	lm := newTestManager(t, true)
	ctx := context.Background()
	tok, err := lm.Acquire(ctx, "k", time.Second, time.Second, 1, 1)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if tok == "" {
		t.Fatal("empty token")
	}
}

func TestAcquire_SecondCallerWaits(t *testing.T) {
	lm := newTestManager(t, true)
	ctx := context.Background()

	tok1, err := lm.Acquire(ctx, "k", time.Second, 5*time.Second, 1, 1)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	gotCh := make(chan string, 1)
	go func() {
		tok, err := lm.Acquire(ctx, "k", 2*time.Second, 5*time.Second, 2, 1)
		if err != nil {
			t.Errorf("second acquire: %v", err)
			gotCh <- ""
			return
		}
		gotCh <- tok
	}()

	// Give the second caller time to enqueue.
	time.Sleep(50 * time.Millisecond)
	if !lm.Release("k", tok1) {
		t.Fatal("release returned false")
	}

	select {
	case tok2 := <-gotCh:
		if tok2 == "" {
			t.Fatal("second caller got empty token")
		}
		if tok2 == tok1 {
			t.Fatal("second caller got the same token")
		}
	case <-time.After(time.Second):
		t.Fatal("second caller didn't get grant after release")
	}
}

func TestAcquire_FIFOOrdering(t *testing.T) {
	lm := newTestManager(t, true)
	ctx := context.Background()

	tok0, _ := lm.Acquire(ctx, "k", time.Second, 30*time.Second, 1, 1)

	// Each waiter goroutine posts its caller-id to `order` once it gets
	// the grant, then immediately releases so the next waiter can run.
	const N = 5
	order := make(chan int, N)
	var wg sync.WaitGroup
	for i := 1; i <= N; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			tok, err := lm.Acquire(ctx, "k", 5*time.Second, 30*time.Second, uint64(i+1), 1)
			if err != nil || tok == "" {
				return
			}
			order <- i
			lm.Release("k", tok)
		}()
		// Stagger so goroutines enqueue in deterministic order.
		time.Sleep(10 * time.Millisecond)
	}

	// Drop tok0; the chain takes over from there.
	lm.Release("k", tok0)

	for i := 1; i <= N; i++ {
		select {
		case got := <-order:
			if got != i {
				t.Fatalf("FIFO violation: position %d went to caller %d (want %d)", i, got, i)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("position %d never arrived", i)
		}
	}
	wg.Wait()
}

// resetLeasesForTest forces all holders of key to expire immediately.
func (lm *LockManager) resetLeasesForTest(key string) {
	sh := lm.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if st, ok := sh.resources[key]; ok {
		past := time.Now().Add(-time.Second)
		for _, h := range st.Holders {
			h.leaseExpires = past
		}
	}
}

// ---------------------------------------------------------------------------
// Acquire timeout
// ---------------------------------------------------------------------------

func TestAcquire_Timeout(t *testing.T) {
	lm := newTestManager(t, true)
	ctx := context.Background()
	_, _ = lm.Acquire(ctx, "k", time.Second, 30*time.Second, 1, 1)

	tok, err := lm.Acquire(ctx, "k", 50*time.Millisecond, 30*time.Second, 2, 1)
	if err != nil {
		t.Fatalf("expected timeout (empty token, nil err), got err %v", err)
	}
	if tok != "" {
		t.Fatalf("expected empty token, got %q", tok)
	}
}

// ---------------------------------------------------------------------------
// Renew
// ---------------------------------------------------------------------------

func TestRenew_Extends(t *testing.T) {
	lm := newTestManager(t, true)
	tok, _ := lm.Acquire(context.Background(), "k", time.Second, 2*time.Second, 1, 1)
	remaining, ok := lm.Renew("k", tok, 60*time.Second)
	if !ok {
		t.Fatal("renew returned false")
	}
	if remaining != 60 {
		t.Errorf("got remaining %d, want 60", remaining)
	}
}

func TestRenew_BadToken(t *testing.T) {
	lm := newTestManager(t, true)
	_, _ = lm.Acquire(context.Background(), "k", time.Second, 2*time.Second, 1, 1)
	_, ok := lm.Renew("k", "bogus", 30*time.Second)
	if ok {
		t.Fatal("renew should fail on bad token")
	}
}

func TestRenew_ExpiredLease(t *testing.T) {
	lm := newTestManager(t, true)
	tok, _ := lm.Acquire(context.Background(), "k", time.Second, time.Second, 1, 1)
	lm.resetLeasesForTest("k")
	_, ok := lm.Renew("k", tok, 30*time.Second)
	if ok {
		t.Fatal("renew should fail on expired lease")
	}
}

// ---------------------------------------------------------------------------
// Two-phase Enqueue / Wait
// ---------------------------------------------------------------------------

func TestEnqueue_FastPath(t *testing.T) {
	lm := newTestManager(t, true)
	status, tok, lease, err := lm.Enqueue("k", 30*time.Second, 1, 1)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if status != "acquired" {
		t.Errorf("got status %q, want acquired", status)
	}
	if tok == "" || lease == 0 {
		t.Errorf("got %q %d", tok, lease)
	}
}

func TestEnqueue_Queued(t *testing.T) {
	lm := newTestManager(t, true)
	_, _, _, _ = lm.Enqueue("k", 30*time.Second, 1, 1)
	status, _, _, err := lm.Enqueue("k", 30*time.Second, 2, 1)
	if err != nil {
		t.Fatalf("second enqueue: %v", err)
	}
	if status != "queued" {
		t.Errorf("got status %q, want queued", status)
	}
}

func TestEnqueue_AlreadyEnqueued(t *testing.T) {
	lm := newTestManager(t, true)
	_, _, _, _ = lm.Enqueue("k", 30*time.Second, 1, 1)
	_, _, _, err := lm.Enqueue("k", 30*time.Second, 1, 1)
	if !errors.Is(err, ErrAlreadyEnqueued) {
		t.Fatalf("got %v, want ErrAlreadyEnqueued", err)
	}
}

func TestWait_FastPath(t *testing.T) {
	lm := newTestManager(t, true)
	status, _, _, _ := lm.Enqueue("k", 30*time.Second, 1, 1)
	if status != "acquired" {
		t.Fatal("expected acquired")
	}
	tok, lease, err := lm.Wait(context.Background(), "k", time.Second, 1)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if tok == "" || lease == 0 {
		t.Errorf("got %q %d", tok, lease)
	}
}

func TestWait_NotEnqueued(t *testing.T) {
	lm := newTestManager(t, true)
	_, _, err := lm.Wait(context.Background(), "k", time.Second, 1)
	if !errors.Is(err, ErrNotEnqueued) {
		t.Fatalf("got %v, want ErrNotEnqueued", err)
	}
}

func TestWait_Timeout(t *testing.T) {
	lm := newTestManager(t, true)
	_, _, _, _ = lm.Enqueue("k", 30*time.Second, 1, 1) // holder
	status, _, _, _ := lm.Enqueue("k", 30*time.Second, 2, 1)
	if status != "queued" {
		t.Fatal("expected queued")
	}
	tok, _, err := lm.Wait(context.Background(), "k", 50*time.Millisecond, 2)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if tok != "" {
		t.Errorf("expected empty token on timeout, got %q", tok)
	}
}

func TestWait_GrantArrives(t *testing.T) {
	lm := newTestManager(t, true)
	_, holderTok, _, _ := lm.Enqueue("k", 30*time.Second, 1, 1)
	_, _, _, _ = lm.Enqueue("k", 30*time.Second, 2, 1)

	gotCh := make(chan string, 1)
	go func() {
		tok, _, err := lm.Wait(context.Background(), "k", 5*time.Second, 2)
		if err != nil {
			t.Errorf("wait: %v", err)
			gotCh <- ""
			return
		}
		gotCh <- tok
	}()

	time.Sleep(20 * time.Millisecond)
	lm.Release("k", holderTok)

	select {
	case tok := <-gotCh:
		if tok == "" {
			t.Fatal("got empty token")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("wait never returned after release")
	}
}

// ---------------------------------------------------------------------------
// Semaphores
// ---------------------------------------------------------------------------

func TestSemaphore_LimitEnforced(t *testing.T) {
	lm := newTestManager(t, true)
	ctx := context.Background()

	tok1, err := lm.Acquire(ctx, "sem", time.Second, 5*time.Second, 1, 3)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	tok2, err := lm.Acquire(ctx, "sem", time.Second, 5*time.Second, 2, 3)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	tok3, err := lm.Acquire(ctx, "sem", time.Second, 5*time.Second, 3, 3)
	if err != nil {
		t.Fatalf("third: %v", err)
	}
	// Fourth should time out.
	tok4, err := lm.Acquire(ctx, "sem", 50*time.Millisecond, 5*time.Second, 4, 3)
	if err != nil {
		t.Fatalf("fourth: %v", err)
	}
	if tok4 != "" {
		t.Fatal("fourth acquire shouldn't have gotten a token")
	}
	for _, tok := range []string{tok1, tok2, tok3} {
		if tok == "" {
			t.Fatal("empty token from semaphore acquire")
		}
	}
}

func TestSemaphore_LimitMismatch(t *testing.T) {
	lm := newTestManager(t, true)
	_, _ = lm.Acquire(context.Background(), "sem", time.Second, 5*time.Second, 1, 3)
	_, err := lm.Acquire(context.Background(), "sem", time.Second, 5*time.Second, 2, 5)
	if !errors.Is(err, ErrLimitMismatch) {
		t.Fatalf("got %v, want ErrLimitMismatch", err)
	}
}

// ---------------------------------------------------------------------------
// MaxLocks / MaxWaiters
// ---------------------------------------------------------------------------

func TestMaxLocks(t *testing.T) {
	lm := newTestManager(t, true)
	lm.cfg.MaxLocks = 2
	ctx := context.Background()
	_, err := lm.Acquire(ctx, "a", time.Second, time.Second, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = lm.Acquire(ctx, "b", time.Second, time.Second, 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = lm.Acquire(ctx, "c", time.Second, time.Second, 3, 1)
	if !errors.Is(err, ErrMaxLocks) {
		t.Fatalf("got %v, want ErrMaxLocks", err)
	}
}

func TestMaxWaiters(t *testing.T) {
	lm := newTestManager(t, true)
	lm.cfg.MaxWaiters = 1
	ctx := context.Background()

	_, _ = lm.Acquire(ctx, "k", time.Second, 10*time.Second, 1, 1)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = lm.Acquire(ctx, "k", 5*time.Second, 10*time.Second, 2, 1)
	}()
	time.Sleep(50 * time.Millisecond) // let waiter enqueue

	_, err := lm.Acquire(ctx, "k", 5*time.Second, 10*time.Second, 3, 1)
	if !errors.Is(err, ErrMaxWaiters) {
		t.Fatalf("got %v, want ErrMaxWaiters", err)
	}
	// Don't leak the goroutine.
	lm.CleanupConnection(2)
	wg.Wait()
}

// ---------------------------------------------------------------------------
// CleanupConnection
// ---------------------------------------------------------------------------

func TestCleanup_AutoReleasesHolders(t *testing.T) {
	lm := newTestManager(t, true)
	ctx := context.Background()
	_, _ = lm.Acquire(ctx, "k", time.Second, 30*time.Second, 1, 1)
	lm.CleanupConnection(1)

	tok2, err := lm.Acquire(ctx, "k", time.Second, 30*time.Second, 2, 1)
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	if tok2 == "" {
		t.Fatal("expected new grant after cleanup")
	}
}

func TestCleanup_NoAutoRelease_KeepsHolders(t *testing.T) {
	lm := newTestManager(t, false)
	ctx := context.Background()
	_, _ = lm.Acquire(ctx, "k", time.Second, 30*time.Second, 1, 1)
	lm.CleanupConnection(1)

	// Second caller should be made to wait — fast path is closed.
	tok, err := lm.Acquire(ctx, "k", 50*time.Millisecond, 30*time.Second, 2, 1)
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	if tok != "" {
		t.Fatal("expected timeout (no auto-release), got grant")
	}
}

func TestCleanup_DropsPendingWaiters(t *testing.T) {
	lm := newTestManager(t, true)
	ctx := context.Background()
	_, _ = lm.Acquire(ctx, "k", time.Second, 30*time.Second, 1, 1)

	done := make(chan error, 1)
	go func() {
		_, err := lm.Acquire(ctx, "k", 5*time.Second, 30*time.Second, 2, 1)
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)

	lm.CleanupConnection(2) // disconnect the waiter

	select {
	case err := <-done:
		if !errors.Is(err, ErrWaiterClosed) {
			t.Fatalf("got %v, want ErrWaiterClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiter goroutine never returned after cleanup")
	}
}

func TestCleanup_DropsEnqueuedState(t *testing.T) {
	lm := newTestManager(t, true)
	_, _, _, _ = lm.Enqueue("k", 30*time.Second, 1, 1) // holder
	_, _, _, _ = lm.Enqueue("k", 30*time.Second, 2, 1) // queued

	lm.CleanupConnection(2)

	// connID 2 should now be able to enqueue again (state was cleared).
	_, _, _, err := lm.Enqueue("k", 30*time.Second, 2, 1)
	if errors.Is(err, ErrAlreadyEnqueued) {
		t.Fatal("enqueued state survived cleanup")
	}
}

// ---------------------------------------------------------------------------
// Lease expiry sweep
// ---------------------------------------------------------------------------

func TestSweepLeases_GrantsNextWaiter(t *testing.T) {
	lm := newTestManager(t, true)
	ctx := context.Background()
	tok1, _ := lm.Acquire(ctx, "k", time.Second, 30*time.Second, 1, 1)
	_ = tok1

	got := make(chan string, 1)
	go func() {
		tok, err := lm.Acquire(ctx, "k", 5*time.Second, 30*time.Second, 2, 1)
		if err != nil {
			t.Errorf("acquire: %v", err)
			got <- ""
			return
		}
		got <- tok
	}()
	time.Sleep(50 * time.Millisecond)

	// Force expiry, then run the sweep.
	lm.resetLeasesForTest("k")
	lm.sweepLeases(time.Now())

	select {
	case tok := <-got:
		if tok == "" {
			t.Fatal("expected grant after sweep")
		}
	case <-time.After(time.Second):
		t.Fatal("waiter never granted")
	}
}

// ---------------------------------------------------------------------------
// GC
// ---------------------------------------------------------------------------

func TestGC_PrunesIdle(t *testing.T) {
	lm := newTestManager(t, true)
	lm.cfg.GCMaxIdleTime = 10 * time.Millisecond

	tok, _ := lm.Acquire(context.Background(), "k", time.Second, time.Second, 1, 1)
	lm.Release("k", tok)

	time.Sleep(20 * time.Millisecond)
	lm.gcOnce(time.Now())

	if c := lm.resourceTotal.Load(); c != 0 {
		t.Errorf("resource count after GC = %d, want 0", c)
	}
}

func TestGC_KeepsHeld(t *testing.T) {
	lm := newTestManager(t, true)
	lm.cfg.GCMaxIdleTime = time.Millisecond

	_, _ = lm.Acquire(context.Background(), "k", time.Second, time.Second, 1, 1)
	time.Sleep(10 * time.Millisecond)
	lm.gcOnce(time.Now())

	if c := lm.resourceTotal.Load(); c != 1 {
		t.Errorf("held resource was GC'd")
	}
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

func TestStats_HeldLock(t *testing.T) {
	lm := newTestManager(t, true)
	_, _ = lm.Acquire(context.Background(), LockPrefix+"k", time.Second, 60*time.Second, 7, 1)
	st := lm.Stats(0)
	if len(st.Locks) != 1 {
		t.Fatalf("got %d locks, want 1", len(st.Locks))
	}
	if st.Locks[0].OwnerConnID != 7 {
		t.Errorf("owner conn = %d, want 7", st.Locks[0].OwnerConnID)
	}
}

func TestStats_Semaphore(t *testing.T) {
	lm := newTestManager(t, true)
	_, _ = lm.Acquire(context.Background(), SemPrefix+"k", time.Second, 60*time.Second, 1, 3)
	_, _ = lm.Acquire(context.Background(), SemPrefix+"k", time.Second, 60*time.Second, 2, 3)
	st := lm.Stats(0)
	if len(st.Semaphores) != 1 {
		t.Fatalf("got %d sems, want 1", len(st.Semaphores))
	}
	if st.Semaphores[0].Holders != 2 {
		t.Errorf("holders = %d, want 2", st.Semaphores[0].Holders)
	}
}

// ---------------------------------------------------------------------------
// Context cancellation
// ---------------------------------------------------------------------------

func TestAcquire_ContextCancel(t *testing.T) {
	lm := newTestManager(t, true)
	_, _ = lm.Acquire(context.Background(), "k", time.Second, 30*time.Second, 1, 1)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := lm.Acquire(ctx, "k", 5*time.Second, 30*time.Second, 2, 1)
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("acquire didn't return after cancel")
	}
}

// ---------------------------------------------------------------------------
// StripKeyPrefix
// ---------------------------------------------------------------------------

func TestStripKeyPrefix(t *testing.T) {
	cases := []struct{ in, out string }{
		{"lock:foo", "foo"},
		{"sem:bar", "bar"},
		{"plain", "plain"},
	}
	for _, c := range cases {
		if got := StripKeyPrefix(c.in); got != c.out {
			t.Errorf("%q: got %q, want %q", c.in, got, c.out)
		}
	}
}

// ---------------------------------------------------------------------------
// Lease expiry grants next waiter (end-to-end via Acquire path)
// ---------------------------------------------------------------------------

func TestExpiredLease_GrantedToNextOnAcquire(t *testing.T) {
	lm := newTestManager(t, true)
	ctx := context.Background()

	// First conn holds the lock with a short lease.
	_, _ = lm.Acquire(ctx, "k", time.Second, 30*time.Second, 1, 1)

	// Force expiry; a fresh acquire must opportunistically evict.
	lm.resetLeasesForTest("k")

	tok2, err := lm.Acquire(ctx, "k", 50*time.Millisecond, 30*time.Second, 2, 1)
	if err != nil {
		t.Fatalf("acquire after expiry: %v", err)
	}
	if tok2 == "" {
		t.Fatal("expected grant, got timeout")
	}
}

// ---------------------------------------------------------------------------
// Stress: many concurrent acquires don't deadlock
// ---------------------------------------------------------------------------

func TestStress_ManyConcurrent(t *testing.T) {
	lm := newTestManager(t, true)
	ctx := context.Background()

	const N = 50
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			tok, err := lm.Acquire(ctx, "stress", 5*time.Second, 30*time.Second, uint64(i+1), 1)
			if err != nil || tok == "" {
				return
			}
			lm.Release("stress", tok)
		}()
	}
	wg.Wait()
}

// ---------------------------------------------------------------------------
// Wait(timeout=0) returns a token if the waiter was promoted to holder
// before our cleanup arrived. Used by the HTTP queued-cleanup path —
// the handler must capture this token and release it.
// ---------------------------------------------------------------------------

func TestWait_ZeroTimeoutAfterPromote_ReturnsToken(t *testing.T) {
	lm := newTestManager(t, true)

	// Holder on conn 1 occupies the slot.
	tok1, _ := lm.Acquire(context.Background(), "k", time.Second, 30*time.Second, 1, 1)

	// Conn 2 enqueues — gets "queued".
	status, _, _, _ := lm.Enqueue("k", 30*time.Second, 2, 1)
	if status != "queued" {
		t.Fatalf("expected queued, got %q", status)
	}

	// Holder releases — conn 2's waiter is promoted (preToken set on
	// the enqueued state).
	lm.Release("k", tok1)

	// Now Wait(timeout=0) MUST return the granted token, not silently
	// drop it. The HTTP cleanup path relies on this so it can release
	// instead of stranding.
	tok2, _, err := lm.Wait(context.Background(), "k", 0, 2)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if tok2 == "" {
		t.Fatal("Wait(0) must return the promoted grant — silent drop would leak a held slot")
	}
}

// ---------------------------------------------------------------------------
// Wait fast path: enqueue acquired immediately, wait should return token
// ---------------------------------------------------------------------------

func TestWait_FastPathReturnsToken(t *testing.T) {
	lm := newTestManager(t, true)
	status, gotTok, _, _ := lm.Enqueue("k", 30*time.Second, 1, 1)
	if status != "acquired" {
		t.Fatal("expected acquired")
	}
	tok, _, err := lm.Wait(context.Background(), "k", time.Second, 1)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if tok != gotTok {
		t.Errorf("got token %q, want %q (same as enqueue)", tok, gotTok)
	}
}

// ---------------------------------------------------------------------------
// shardIndex distribution
// ---------------------------------------------------------------------------

func TestShardIndex_Distribution(t *testing.T) {
	counts := make([]int, numShards)
	for i := 0; i < 10000; i++ {
		counts[shardIndex("key"+string(rune(i)))]++
	}
	zero := 0
	for _, c := range counts {
		if c == 0 {
			zero++
		}
	}
	if zero > numShards/4 {
		t.Errorf("%d/%d shards saw zero hits — distribution looks degenerate", zero, numShards)
	}
}
