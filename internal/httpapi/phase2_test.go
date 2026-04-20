package httpapi

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mtingers/dflockd/internal/lock"
	"github.com/mtingers/dflockd/internal/server"
)

// ---------------------------------------------------------------------------
// Two-phase enqueue / wait
// ---------------------------------------------------------------------------

func TestEnqueue_FreeLockReturnsAcquired(t *testing.T) {
	h := newHarness(t, testConfig())
	id := h.createSession(t)
	resp := h.do(t, "POST", "/v1/locks/ep-free/enqueue", id, enqueueRequest{LeaseTTLS: 30})
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("enqueue: %d %s", resp.StatusCode, string(b))
	}
	var body enqueueResponse
	decodeBody(t, resp, &body)
	if body.Status != "acquired" || body.Token == "" {
		t.Fatalf("body: %+v, want acquired with token", body)
	}
}

func TestEnqueueWait_Contention(t *testing.T) {
	h := newHarness(t, testConfig())
	idA := h.createSession(t)
	idB := h.createSession(t)

	// A grabs the lock via single-phase acquire with a short lease.
	resp := h.do(t, "POST", "/v1/locks/ep-wait", idA, acquireRequest{AcquireTimeoutS: 1, LeaseTTLS: 30})
	var aAck acquireResponse
	decodeBody(t, resp, &aAck)
	if aAck.Token == "" {
		t.Fatalf("A acquire failed: %+v", aAck)
	}

	// B enqueues → queued (since A holds).
	resp = h.do(t, "POST", "/v1/locks/ep-wait/enqueue", idB, enqueueRequest{LeaseTTLS: 30})
	var bEnq enqueueResponse
	decodeBody(t, resp, &bEnq)
	if bEnq.Status != "queued" {
		t.Fatalf("B enqueue: %+v, want queued", bEnq)
	}

	// B issues a long-ish wait; A releases partway through.
	waitDone := make(chan waitResponse, 1)
	go func() {
		r := h.do(t, "POST", "/v1/locks/ep-wait/wait", idB, waitRequest{TimeoutS: 5})
		var wr waitResponse
		decodeBody(t, r, &wr)
		waitDone <- wr
	}()
	time.Sleep(100 * time.Millisecond)

	h.do(t, "POST", "/v1/locks/ep-wait/release", idA, releaseRequest{Token: aAck.Token})

	select {
	case wr := <-waitDone:
		if wr.Status != "ok" || wr.Token == "" {
			t.Fatalf("B wait: %+v, want ok with token", wr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("B wait never returned")
	}
}

func TestCanceledLockEnqueueRemovesQueuedWaiter(t *testing.T) {
	h := newHarness(t, testConfig())
	idA := h.createSession(t)
	idB := h.createSession(t)
	idC := h.createSession(t)

	resp := h.do(t, "POST", "/v1/locks/ep-cancel-enqueue", idA, acquireRequest{AcquireTimeoutS: 1, LeaseTTLS: 30})
	var aAck acquireResponse
	decodeBody(t, resp, &aAck)
	if aAck.Token == "" {
		t.Fatalf("A acquire failed: %+v", aAck)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	resp = h.doWithContext(t, ctx, "POST", "/v1/locks/ep-cancel-enqueue/enqueue", idB, enqueueRequest{LeaseTTLS: 30})
	resp.Body.Close()

	resp = h.do(t, "POST", "/v1/locks/ep-cancel-enqueue/release", idA, releaseRequest{Token: aAck.Token})
	resp.Body.Close()

	resp = h.do(t, "POST", "/v1/locks/ep-cancel-enqueue", idC, acquireRequest{AcquireTimeoutS: 0, LeaseTTLS: 30})
	var cAck acquireResponse
	decodeBody(t, resp, &cAck)
	if cAck.Status != "ok" || cAck.Token == "" {
		t.Fatalf("C acquire: %+v, want ok with token after canceled waiter cleanup", cAck)
	}
}

func TestCanceledSemaphoreEnqueueRemovesQueuedWaiter(t *testing.T) {
	h := newHarness(t, testConfig())
	idA := h.createSession(t)
	idB := h.createSession(t)
	idC := h.createSession(t)

	resp := h.do(t, "POST", "/v1/semaphores/ep-sem-cancel-enqueue", idA, semAcquireRequest{AcquireTimeoutS: 1, Limit: 1, LeaseTTLS: 30})
	var aAck acquireResponse
	decodeBody(t, resp, &aAck)
	if aAck.Token == "" {
		t.Fatalf("A acquire failed: %+v", aAck)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	resp = h.doWithContext(t, ctx, "POST", "/v1/semaphores/ep-sem-cancel-enqueue/enqueue", idB, semEnqueueRequest{Limit: 1, LeaseTTLS: 30})
	resp.Body.Close()

	resp = h.do(t, "POST", "/v1/semaphores/ep-sem-cancel-enqueue/release", idA, releaseRequest{Token: aAck.Token})
	resp.Body.Close()

	resp = h.do(t, "POST", "/v1/semaphores/ep-sem-cancel-enqueue", idC, semAcquireRequest{AcquireTimeoutS: 0, Limit: 1, LeaseTTLS: 30})
	var cAck acquireResponse
	decodeBody(t, resp, &cAck)
	if cAck.Status != "ok" || cAck.Token == "" {
		t.Fatalf("C acquire: %+v, want ok with token after canceled waiter cleanup", cAck)
	}
}

func TestWait_WithoutEnqueueReturns409(t *testing.T) {
	h := newHarness(t, testConfig())
	id := h.createSession(t)
	resp := h.do(t, "POST", "/v1/locks/ep-none/wait", id, waitRequest{TimeoutS: 1})
	if resp.StatusCode != 409 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: %d want 409 (body: %s)", resp.StatusCode, string(b))
	}
}

func TestEnqueue_AlreadyEnqueuedReturns409(t *testing.T) {
	h := newHarness(t, testConfig())
	idA := h.createSession(t)
	idB := h.createSession(t)

	// A holds.
	resp := h.do(t, "POST", "/v1/locks/ep-dupe", idA, acquireRequest{AcquireTimeoutS: 1, LeaseTTLS: 30})
	var aAck acquireResponse
	decodeBody(t, resp, &aAck)

	// B enqueues: queued.
	resp = h.do(t, "POST", "/v1/locks/ep-dupe/enqueue", idB, enqueueRequest{LeaseTTLS: 30})
	var bEnq enqueueResponse
	decodeBody(t, resp, &bEnq)
	if bEnq.Status != "queued" {
		t.Fatalf("B first enqueue: %+v", bEnq)
	}

	// B enqueues again: should be rejected.
	resp = h.do(t, "POST", "/v1/locks/ep-dupe/enqueue", idB, enqueueRequest{LeaseTTLS: 30})
	if resp.StatusCode != 409 {
		t.Fatalf("B second enqueue: %d want 409", resp.StatusCode)
	}
}

func TestSemaphore_EnqueueWait(t *testing.T) {
	h := newHarness(t, testConfig())
	idA := h.createSession(t)

	// Fast path: no existing semaphore, limit 2, we get one slot.
	resp := h.do(t, "POST", "/v1/semaphores/ep-sem/enqueue", idA, semEnqueueRequest{Limit: 2, LeaseTTLS: 30})
	var ack enqueueResponse
	decodeBody(t, resp, &ack)
	if ack.Status != "acquired" {
		t.Fatalf("A enqueue: %+v", ack)
	}
}

// ---------------------------------------------------------------------------
// Signal publish (sessionless)
// ---------------------------------------------------------------------------

func TestSignalPublish_NoListeners(t *testing.T) {
	h := newHarness(t, testConfig())
	resp := h.do(t, "POST", "/v1/signals/events.foo", "", signalRequest{Payload: `{"x":1}`})
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("publish: %d %s", resp.StatusCode, string(b))
	}
	var body signalResponse
	decodeBody(t, resp, &body)
	if body.Delivered != 0 {
		t.Fatalf("delivered: got %d want 0", body.Delivered)
	}
}

func TestSignalPublish_WildcardChannelReturns400(t *testing.T) {
	h := newHarness(t, testConfig())
	resp := h.do(t, "POST", "/v1/signals/events.*.login", "", signalRequest{Payload: "x"})
	if resp.StatusCode != 400 {
		t.Fatalf("status: %d want 400", resp.StatusCode)
	}
}

func TestSignalPublish_EmptyPayloadReturns400(t *testing.T) {
	h := newHarness(t, testConfig())
	resp := h.do(t, "POST", "/v1/signals/events.foo", "", signalRequest{Payload: ""})
	if resp.StatusCode != 400 {
		t.Fatalf("status: %d want 400", resp.StatusCode)
	}
}

func TestSignalPublish_NewlineInPayloadReturns400(t *testing.T) {
	h := newHarness(t, testConfig())
	resp := h.do(t, "POST", "/v1/signals/events.foo", "", signalRequest{Payload: "a\nb"})
	if resp.StatusCode != 400 {
		t.Fatalf("status: %d want 400", resp.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// Cross-transport: TCP + HTTP on shared LockManager
// ---------------------------------------------------------------------------

// TestCrossTransport_FIFOPreservation spins up a full TCP server alongside
// the HTTP bridge and verifies that a waiter queued over TCP is served
// before a later HTTP waiter.
func TestCrossTransport_FIFOPreservation(t *testing.T) {
	if testing.Short() {
		t.Skip("5s test; skip under -short")
	}
	cfg := testConfig()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	lm := lock.NewLockManager(cfg, log)
	srv := server.New(lm, cfg, log)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start a real TCP listener bound to the same LockManager.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tcpAddr := ln.Addr().String()
	var srvWg sync.WaitGroup
	srvWg.Add(1)
	go func() {
		defer srvWg.Done()
		_ = srv.RunOnListener(ctx, ln)
	}()

	// Start background loops so lease expiry works in test.
	srvWg.Add(2)
	go func() { defer srvWg.Done(); lm.LeaseExpiryLoop(ctx) }()
	go func() { defer srvWg.Done(); lm.GCLoop(ctx) }()

	// Start the HTTP bridge using the same server.
	bridge := NewBridge(ctx, srv, cfg, log, cfg.HTTPSessionIdleTimeout, cfg.HTTPMaxSessions)
	defer bridge.Shutdown()

	hs := &httpServer{bridge: bridge, cfg: cfg, log: log}
	hs.cfg = cfg

	// We don't need a real HTTP listener for this — we'll use session
	// commands directly against the bridge.

	// 1. A TCP client acquires the lock.
	tcpConn, err := net.Dial("tcp", tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer tcpConn.Close()
	aToken := sendProto(t, tcpConn, "l\nfifo-test\n5 60\n")
	if !strings.HasPrefix(aToken, "ok ") {
		t.Fatalf("A acquire: %q", aToken)
	}

	// 2. A second TCP client enqueues (becomes waiter B).
	tcpConnB, err := net.Dial("tcp", tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer tcpConnB.Close()
	bResp := sendProto(t, tcpConnB, "e\nfifo-test\n60\n")
	if bResp != "queued" {
		t.Fatalf("B enqueue: %q", bResp)
	}

	// 3. An HTTP session enqueues (becomes waiter C, should be AFTER B).
	cID, err := bridge.CreateSession()
	if err != nil {
		t.Fatal(err)
	}
	cSess, _ := bridge.LookupSession(cID)
	// Use the raw key — the protocol handler adds the "lock:" prefix.
	cResp, err := cSess.command("e", "fifo-test", "60")
	if err != nil {
		t.Fatal(err)
	}
	if cResp != "queued" {
		t.Fatalf("C enqueue: %q", cResp)
	}

	// 4. A releases. Then B should get the lock via its Wait, not C.
	//    We race B and C waiting; B must win.

	bDone := make(chan string, 1)
	cDone := make(chan string, 1)
	go func() {
		bDone <- sendProto(t, tcpConnB, "w\nfifo-test\n3\n")
	}()
	go func() {
		r, _ := cSess.command("w", "fifo-test", "3")
		cDone <- r
	}()
	time.Sleep(200 * time.Millisecond)

	// Release A.
	tokenParts := strings.Fields(aToken)
	if len(tokenParts) < 2 {
		t.Fatalf("parse A token: %q", aToken)
	}
	rel := sendProto(t, tcpConn, fmt.Sprintf("r\nfifo-test\n%s\n", tokenParts[1]))
	if rel != "ok" {
		t.Fatalf("A release: %q", rel)
	}

	// B must complete; C must still be waiting.
	select {
	case bResp = <-bDone:
		if !strings.HasPrefix(bResp, "ok ") {
			t.Fatalf("B wait: %q", bResp)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("B wait never completed")
	}

	select {
	case cResp = <-cDone:
		// If C completed first, that's a FIFO violation.
		if strings.HasPrefix(cResp, "ok ") {
			t.Fatalf("C got lock before B — FIFO violated")
		}
		// C timed out or errored — acceptable as long as B got it first
	case <-time.After(100 * time.Millisecond):
		// C is still waiting — correct.
	}

	// Stop.
	cancel()
	ln.Close()
	srvWg.Wait()
}

// sendProto sends a raw protocol message and reads one response line.
func sendProto(t *testing.T, c net.Conn, msg string) string {
	t.Helper()
	if _, err := c.Write([]byte(msg)); err != nil {
		t.Fatalf("write: %v", err)
	}
	c.SetReadDeadline(time.Now().Add(5 * time.Second))
	r := bufio.NewReader(c)
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return strings.TrimRight(line, "\r\n")
}

// ---------------------------------------------------------------------------
// Leak check
// ---------------------------------------------------------------------------

// TestSessionLeak opens and closes many sessions; verifies the bridge map
// drains and no connEnqueued state leaks in the LockManager.
func TestSessionLeak_CleanShutdown(t *testing.T) {
	cfg := testConfig()
	h := newHarness(t, cfg)

	const n = 100
	for i := 0; i < n; i++ {
		id := h.createSession(t)
		// Acquire + release.
		resp := h.do(t, "POST", fmt.Sprintf("/v1/locks/leak-%d", i), id, acquireRequest{AcquireTimeoutS: 1, LeaseTTLS: 10})
		var ack acquireResponse
		decodeBody(t, resp, &ack)
		if ack.Token == "" {
			t.Fatalf("acquire %d: %+v", i, ack)
		}
		h.do(t, "POST", fmt.Sprintf("/v1/locks/leak-%d/release", i), id, releaseRequest{Token: ack.Token})
		h.do(t, "DELETE", "/v1/sessions/"+id, "", nil)
	}
	if got := h.bridge.SessionCount(); got != 0 {
		t.Fatalf("sessions remaining: %d", got)
	}
	if got := h.lm.ConnEnqueuedCountForTest(); got != 0 {
		t.Fatalf("connEnqueued entries remaining: %d", got)
	}
}
