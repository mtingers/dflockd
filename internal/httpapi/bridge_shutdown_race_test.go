package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/mtingers/dflockd/internal/lock"
	"github.com/mtingers/dflockd/internal/server"
)

// TestCreateSessionAfterShutdown verifies CreateSession refuses to create
// a session once the bridge has been shut down. Without this check the
// session would be added to a map nobody owns, and its sessionCtx (already
// cancelled) wouldn't translate to ServeConn promptly exiting.
func TestCreateSessionAfterShutdown(t *testing.T) {
	cfg := testConfig()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	lm := lock.NewLockManager(cfg, log)
	srv := server.New(lm, cfg, log)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bridge := NewBridge(ctx, srv, cfg, log, cfg.HTTPSessionIdleTimeout, cfg.HTTPMaxSessions)

	// First create succeeds normally.
	id1, err := bridge.CreateSession()
	if err != nil {
		t.Fatalf("first CreateSession: %v", err)
	}
	if id1 == "" {
		t.Fatal("first CreateSession returned empty id")
	}

	bridge.Shutdown()

	// After Shutdown, new CreateSession calls must fail with ErrBridgeShutdown.
	id2, err := bridge.CreateSession()
	if !errors.Is(err, ErrBridgeShutdown) {
		t.Fatalf("after Shutdown: expected ErrBridgeShutdown, got id=%q err=%v", id2, err)
	}
	if id2 != "" {
		t.Fatalf("after Shutdown: expected empty id, got %q", id2)
	}

	// The bridge's session count must remain zero — no leaked entry.
	if got := bridge.SessionCount(); got != 0 {
		t.Fatalf("session count after rejected create: %d, want 0", got)
	}
}

func TestCanceledRequestBeforeCommandDoesNotDeleteSession(t *testing.T) {
	h := newHarness(t, testConfig())
	id := h.createSession(t)
	s, err := h.bridge.LookupSession(id)
	if err != nil {
		t.Fatal(err)
	}

	// Hold the per-session command gate so the request is cancelled before it
	// can write any protocol bytes. That must not tear down unrelated session
	// state.
	<-s.reqMu
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	resp := h.doWithContext(t, ctx, "POST", "/v1/locks/cancel-before-send", id, acquireRequest{AcquireTimeoutS: 1})
	resp.Body.Close()
	s.reqMu <- struct{}{}

	if _, err := h.bridge.LookupSession(id); err != nil {
		t.Fatalf("session was deleted even though canceled request sent no command: %v", err)
	}
}

func TestSignalOverflowClosesHTTPSession(t *testing.T) {
	h := newHarness(t, testConfig())
	id := h.createSession(t)
	s, err := h.bridge.LookupSession(id)
	if err != nil {
		t.Fatal(err)
	}
	if resp, err := s.command("listen", "overflow.test", ""); err != nil || resp != "ok" {
		t.Fatalf("listen: resp=%q err=%v", resp, err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for !s.dead.Load() && time.Now().Before(deadline) {
		for i := 0; i < sigChBuffer+1; i++ {
			h.bridge.Signals().Signal("overflow.test", "x")
		}
		time.Sleep(time.Millisecond)
	}
	if !s.dead.Load() {
		t.Fatal("session stayed alive after overflowing the bridge signal buffer")
	}
}
