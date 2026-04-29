package client_test

import (
	"context"
	"testing"
	"time"

	"github.com/mtingers/dflockd/client"
)

// Release on a queued (but not yet granted) Lock used to surface a
// "dflockd: empty value" error because the token was empty. The connection
// close already abandons the waiter at the protocol level, so Release
// should now return nil and clean up local state.
func TestLockReleaseAfterQueued(t *testing.T) {
	_, addr := startServer(t, testConfig())

	l1 := &client.Lock{Key: "rq-lock", Servers: []string{addr}, AcquireTimeout: 5 * time.Second, LeaseTTL: 30}
	if _, err := l1.Acquire(context.Background()); err != nil {
		t.Fatalf("l1 Acquire: %v", err)
	}
	t.Cleanup(func() { _ = l1.Release(context.Background()) })

	l2 := &client.Lock{Key: "rq-lock", Servers: []string{addr}, AcquireTimeout: 5 * time.Second, LeaseTTL: 30}
	status, err := l2.Enqueue(context.Background())
	if err != nil {
		t.Fatalf("l2 Enqueue: %v", err)
	}
	if status != "queued" {
		t.Fatalf("expected queued, got %q", status)
	}
	if err := l2.Release(context.Background()); err != nil {
		t.Fatalf("Release after queued: %v", err)
	}
	if tok := l2.Token(); tok != "" {
		t.Fatalf("Token should be empty after Release, got %q", tok)
	}
}

func TestSemaphoreReleaseAfterQueued(t *testing.T) {
	_, addr := startServer(t, testConfig())

	s1 := &client.Semaphore{Key: "rq-sem", Limit: 1, Servers: []string{addr}, AcquireTimeout: 5 * time.Second, LeaseTTL: 30}
	if _, err := s1.Acquire(context.Background()); err != nil {
		t.Fatalf("s1 Acquire: %v", err)
	}
	t.Cleanup(func() { _ = s1.Release(context.Background()) })

	s2 := &client.Semaphore{Key: "rq-sem", Limit: 1, Servers: []string{addr}, AcquireTimeout: 5 * time.Second, LeaseTTL: 30}
	status, err := s2.Enqueue(context.Background())
	if err != nil {
		t.Fatalf("s2 Enqueue: %v", err)
	}
	if status != "queued" {
		t.Fatalf("expected queued, got %q", status)
	}
	if err := s2.Release(context.Background()); err != nil {
		t.Fatalf("Release after queued: %v", err)
	}
	if tok := s2.Token(); tok != "" {
		t.Fatalf("Token should be empty after Release, got %q", tok)
	}
}
