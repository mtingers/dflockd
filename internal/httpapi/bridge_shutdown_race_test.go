package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"

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
