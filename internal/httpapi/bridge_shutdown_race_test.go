package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
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
	// blocking endpoints honor request context: the cancelled request bails
	// out before the command is sent and the session must be preserved
	// (errCommandNotSent suppresses the destruction path).
	//
	// non-blocking endpoints (ping, release, renew, enqueue) intentionally
	// run with a background context so a HTTP-level cancel can never close
	// the virtual conn between command-send and response-recv — that close
	// would auto-release every other lock on the session via
	// CleanupConnection. For these endpoints the cancelled request blocks
	// on reqMu until the gate is released, then runs to completion. Either
	// way the session must remain alive.
	cases := []struct {
		name     string
		path     func(string) string
		body     any
		blocking bool
	}{
		{
			name: "ping",
			path: func(id string) string {
				return "/v1/sessions/" + id + "/ping"
			},
		},
		{
			name:     "lock acquire",
			path:     func(string) string { return "/v1/locks/cancel-before-send" },
			body:     acquireRequest{AcquireTimeoutS: 1},
			blocking: true,
		},
		{
			name: "lock enqueue",
			path: func(string) string { return "/v1/locks/cancel-before-send/enqueue" },
			body: enqueueRequest{},
		},
		{
			name: "lock release",
			path: func(string) string { return "/v1/locks/cancel-before-send/release" },
			body: releaseRequest{Token: "abc123"},
		},
		{
			name: "lock renew",
			path: func(string) string { return "/v1/locks/cancel-before-send/renew" },
			body: renewRequest{Token: "abc123", LeaseTTLS: 30},
		},
		{
			name:     "lock wait",
			path:     func(string) string { return "/v1/locks/cancel-before-send/wait" },
			body:     waitRequest{TimeoutS: 1},
			blocking: true,
		},
		{
			name:     "sem acquire",
			path:     func(string) string { return "/v1/semaphores/cancel-before-send" },
			body:     semAcquireRequest{AcquireTimeoutS: 1, Limit: 1},
			blocking: true,
		},
		{
			name: "sem enqueue",
			path: func(string) string { return "/v1/semaphores/cancel-before-send/enqueue" },
			body: semEnqueueRequest{Limit: 1},
		},
		{
			name: "sem release",
			path: func(string) string { return "/v1/semaphores/cancel-before-send/release" },
			body: releaseRequest{Token: "abc123"},
		},
		{
			name: "sem renew",
			path: func(string) string { return "/v1/semaphores/cancel-before-send/renew" },
			body: renewRequest{Token: "abc123", LeaseTTLS: 30},
		},
		{
			name:     "sem wait",
			path:     func(string) string { return "/v1/semaphores/cancel-before-send/wait" },
			body:     waitRequest{TimeoutS: 1},
			blocking: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, testConfig())
			id := h.createSession(t)
			s, err := h.bridge.LookupSession(id)
			if err != nil {
				t.Fatal(err)
			}

			// Hold the per-session command gate so the request can't write
			// any protocol bytes until we release it.
			<-s.reqMu
			var releasedGate bool
			releaseGate := func() {
				if !releasedGate {
					s.reqMu <- struct{}{}
					releasedGate = true
				}
			}
			defer releaseGate()

			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			done := make(chan *http.Response, 1)
			go func() {
				done <- h.doWithContext(t, ctx, "POST", tc.path(id), id, tc.body)
			}()

			if tc.blocking {
				// Blocking endpoints (acquire/wait) propagate r.Context()
				// into commandContext, so the cancelled request must return
				// promptly via errCommandNotSent without sending anything.
				select {
				case resp := <-done:
					resp.Body.Close()
				case <-time.After(time.Second):
					t.Fatal("blocking endpoint did not bail before sending command")
				}
			} else {
				// Non-blocking endpoints run with a background context, so
				// the request blocks on the gate until we release it and
				// then completes normally. The cancellation must not destroy
				// the session in either path.
				select {
				case resp := <-done:
					resp.Body.Close()
					t.Fatal("non-blocking endpoint returned before gate released; expected to block")
				case <-time.After(50 * time.Millisecond):
				}
				releaseGate()
				select {
				case resp := <-done:
					resp.Body.Close()
				case <-time.After(time.Second):
					t.Fatal("non-blocking endpoint did not complete after gate released")
				}
			}

			if _, err := h.bridge.LookupSession(id); err != nil {
				t.Fatalf("session was deleted even though canceled request sent no command: %v", err)
			}
		})
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
