package client

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/mtingers/dflockd/internal/config"
	"github.com/mtingers/dflockd/internal/lock"
	"github.com/mtingers/dflockd/internal/server"
)

func startLaneTestServer(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	cfg := &config.Config{
		MaxLocks: 128, DefaultLeaseTTL: time.Minute,
		LeaseSweepInterval: 10 * time.Millisecond,
		GCInterval:         time.Second, GCMaxIdleTime: time.Minute,
		ReadTimeout: 5 * time.Second, WriteTimeout: time.Second,
		ShutdownTimeout: time.Second, AutoReleaseOnDisconnect: true,
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	lm, err := lock.NewLockManager(cfg, log)
	if err != nil {
		_ = listener.Close()
		t.Fatalf("NewLockManager: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	srv := server.New(lm, cfg, log)
	go func() { done <- srv.RunOnListener(ctx, listener) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("server did not stop")
		}
		_ = lm.Close()
	})
	return listener.Addr().String()
}

// A caller holding lock A while blocked acquiring lock B must still be able to
// renew A. If every operation shared one connection the renew would queue
// behind the blocked acquire and A's lease would expire under the caller.
func TestClusterRenewIsNotStarvedByBlockingAcquire(t *testing.T) {
	addr := startLaneTestServer(t)

	holder, err := NewCluster([]string{addr})
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close()
	blocker, err := NewCluster([]string{addr})
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()

	// Park key B so the holder's Acquire(B) has to block.
	if _, _, err := blocker.Acquire(context.Background(), "B", 0); err != nil {
		t.Fatalf("blocker Acquire(B): %v", err)
	}
	tokenA, _, err := holder.Acquire(context.Background(), "A", 0)
	if err != nil {
		t.Fatalf("Acquire(A): %v", err)
	}

	acquireDone := make(chan struct{})
	go func() {
		defer close(acquireDone)
		_, _, _ = holder.Acquire(context.Background(), "B", 3*time.Second)
	}()
	time.Sleep(200 * time.Millisecond) // let Acquire(B) occupy the session lane

	renewed := make(chan error, 1)
	go func() {
		_, err := holder.Renew(context.Background(), "A", tokenA)
		renewed <- err
	}()

	select {
	case err := <-renewed:
		if err != nil {
			t.Fatalf("Renew(A) while Acquire(B) blocked: %v", err)
		}
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("Renew(A) was starved by the blocked Acquire(B): a held lease cannot " +
			"be renewed while another call on the same Cluster is waiting for a grant")
	}
	<-acquireDone
}

// Release must also stay clear of the session lane, so a caller can hand back
// a lock it already holds while another key is still being waited on.
func TestClusterReleaseIsNotStarvedByBlockingAcquire(t *testing.T) {
	addr := startLaneTestServer(t)

	holder, err := NewCluster([]string{addr})
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close()
	blocker, err := NewCluster([]string{addr})
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()

	if _, _, err := blocker.Acquire(context.Background(), "B", 0); err != nil {
		t.Fatalf("blocker Acquire(B): %v", err)
	}
	tokenA, _, err := holder.Acquire(context.Background(), "A", 0)
	if err != nil {
		t.Fatalf("Acquire(A): %v", err)
	}

	acquireDone := make(chan struct{})
	go func() {
		defer close(acquireDone)
		_, _, _ = holder.Acquire(context.Background(), "B", 3*time.Second)
	}()
	time.Sleep(200 * time.Millisecond)

	released := make(chan error, 1)
	go func() { released <- holder.Release(context.Background(), "A", tokenA) }()

	select {
	case err := <-released:
		if err != nil {
			t.Fatalf("Release(A) while Acquire(B) blocked: %v", err)
		}
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("Release(A) was starved by the blocked Acquire(B)")
	}
	<-acquireDone
}
