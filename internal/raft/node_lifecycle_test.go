package raft

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestNodeLifecycleCloseBeforeStart(t *testing.T) {
	n, transport := newUnstartedNode(t, "a", "a")
	defer transport.Close()
	ctx := context.Background()
	if _, err := n.Propose(ctx, []byte("x")); !errors.Is(err, ErrNotStarted) {
		t.Fatalf("Propose before Start = %v, want ErrNotStarted", err)
	}
	if _, err := n.AddVoter(ctx, "b", "b"); !errors.Is(err, ErrNotStarted) {
		t.Fatalf("AddVoter before Start = %v, want ErrNotStarted", err)
	}
	if err := n.TransferLeadership(ctx); !errors.Is(err, ErrNotStarted) {
		t.Fatalf("TransferLeadership before Start = %v, want ErrNotStarted", err)
	}

	done := make(chan error, 1)
	go func() { done <- n.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close before Start: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close before Start blocked")
	}
	if err := n.Start(); !errors.Is(err, ErrStopped) {
		t.Fatalf("Start after Close = %v, want ErrStopped", err)
	}
}

func TestNodeLifecycleRejectsDuplicateStartAndConcurrentClose(t *testing.T) {
	n, transport := newUnstartedNode(t, "a", "a")
	defer transport.Close()
	if n.Ready() {
		t.Fatal("unstarted node reports ready")
	}
	if err := n.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if !n.Ready() {
		t.Fatal("running voter does not report ready")
	}
	if err := n.Start(); !errors.Is(err, ErrAlreadyStarted) {
		t.Fatalf("second Start = %v, want ErrAlreadyStarted", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- n.Close()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent Close: %v", err)
		}
	}
	if _, err := n.Propose(context.Background(), []byte("x")); !errors.Is(err, ErrStopped) {
		t.Fatalf("Propose after Close = %v, want ErrStopped", err)
	}
	if n.Ready() {
		t.Fatal("closed node reports ready")
	}
}
