package client_test

import (
	"testing"
	"time"

	"github.com/mtingers/dflockd/client"
)

// Cover the low-level SemRenew / SemEnqueue / SemWait entry points
// (all three were at 0% — only SemAcquire and SemRelease had direct
// tests via TestDial_Semaphore).

func TestSemRenew_BumpsLease(t *testing.T) {
	addr, stop := startServer(t)
	defer stop()

	c, _ := client.Dial(addr)
	defer c.Close()

	tok, _, err := client.SemAcquire(c, "sem-renew", time.Second, 1, client.WithLeaseTTL(15))
	if err != nil {
		t.Fatalf("SemAcquire: %v", err)
	}
	remaining, err := client.SemRenew(c, "sem-renew", tok, client.WithLeaseTTL(60))
	if err != nil {
		t.Fatalf("SemRenew: %v", err)
	}
	if remaining != 60 {
		t.Fatalf("remaining=%d, want 60", remaining)
	}
	if err := client.SemRelease(c, "sem-renew", tok); err != nil {
		t.Fatalf("SemRelease: %v", err)
	}
}

func TestSemEnqueueWait_GrantsAfterRelease(t *testing.T) {
	addr, stop := startServer(t)
	defer stop()

	holder, _ := client.Dial(addr)
	defer holder.Close()
	htok, _, err := client.SemAcquire(holder, "sem-eq", time.Second, 1, client.WithLeaseTTL(30))
	if err != nil {
		t.Fatalf("holder SemAcquire: %v", err)
	}

	queuer, _ := client.Dial(addr)
	defer queuer.Close()
	status, _, _, err := client.SemEnqueue(queuer, "sem-eq", 1, client.WithLeaseTTL(30))
	if err != nil {
		t.Fatalf("SemEnqueue: %v", err)
	}
	if status != "queued" {
		t.Fatalf("status=%q, want queued", status)
	}

	done := make(chan error, 1)
	go func() {
		_, _, werr := client.SemWait(queuer, "sem-eq", 5*time.Second)
		done <- werr
	}()

	time.Sleep(50 * time.Millisecond)
	if err := client.SemRelease(holder, "sem-eq", htok); err != nil {
		t.Fatalf("holder SemRelease: %v", err)
	}

	select {
	case werr := <-done:
		if werr != nil {
			t.Fatalf("SemWait: %v", werr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("SemWait did not return after holder release")
	}
}
