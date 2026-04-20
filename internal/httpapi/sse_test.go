package httpapi

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// SSE helpers
// ---------------------------------------------------------------------------

// sseEvent is one parsed SSE frame: a set of field lines terminated by an
// empty line.
type sseEvent struct {
	Event string
	Data  string
}

// startSSE opens an SSE stream and returns a channel of parsed events.
// Reading from the channel blocks until the next event or the stream
// closes. The returned cancel closes the underlying HTTP request.
func startSSE(t *testing.T, h *testHarness, sessionID, pattern, group string) (<-chan sseEvent, context.CancelFunc) {
	t.Helper()
	q := url.Values{}
	q.Set("pattern", pattern)
	if group != "" {
		q.Set("group", group)
	}
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, "GET", h.http.URL+"/v1/signals?"+q.Encode(), nil)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if sessionID != "" {
		req.Header.Set("X-Dflockd-Session", sessionID)
	}
	if h.bridge.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+h.bridge.authToken)
	}
	resp, err := h.http.Client().Do(req)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		cancel()
		t.Fatalf("SSE open failed: status %d body %s", resp.StatusCode, string(body))
	}

	events := make(chan sseEvent, 16)
	go func() {
		defer close(events)
		defer resp.Body.Close()
		r := bufio.NewReader(resp.Body)
		var ev sseEvent
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				return
			}
			line = strings.TrimRight(line, "\r\n")
			if line == "" {
				if ev.Data != "" {
					events <- ev
				}
				ev = sseEvent{}
				continue
			}
			if strings.HasPrefix(line, ":") {
				continue // comment
			}
			if strings.HasPrefix(line, "event: ") {
				ev.Event = strings.TrimPrefix(line, "event: ")
			} else if strings.HasPrefix(line, "data: ") {
				ev.Data = strings.TrimPrefix(line, "data: ")
			}
		}
	}()
	return events, cancel
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestSSE_PublishDelivers(t *testing.T) {
	h := newHarness(t, testConfig())

	events, cancel := startSSE(t, h, "", "events.user.login", "")
	defer cancel()

	// Give the listen subscription time to register before publishing.
	time.Sleep(100 * time.Millisecond)

	// Publish a signal.
	resp := h.do(t, "POST", "/v1/signals/events.user.login", "", signalRequest{Payload: `{"user":"alice"}`})
	if resp.StatusCode != 200 {
		t.Fatalf("publish: %d", resp.StatusCode)
	}
	var pub signalResponse
	decodeBody(t, resp, &pub)
	if pub.Delivered != 1 {
		t.Fatalf("delivered: got %d want 1", pub.Delivered)
	}

	// Receive.
	select {
	case ev, ok := <-events:
		if !ok {
			t.Fatal("SSE closed without event")
		}
		if ev.Event != "sig" {
			t.Fatalf("event: %q want sig", ev.Event)
		}
		var payload struct {
			Channel string `json:"channel"`
			Payload string `json:"payload"`
		}
		if err := json.Unmarshal([]byte(ev.Data), &payload); err != nil {
			t.Fatalf("parse data %q: %v", ev.Data, err)
		}
		if payload.Channel != "events.user.login" {
			t.Fatalf("channel: %q", payload.Channel)
		}
		if payload.Payload != `{"user":"alice"}` {
			t.Fatalf("payload: %q", payload.Payload)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no event received")
	}
}

func TestSSE_Wildcard(t *testing.T) {
	h := newHarness(t, testConfig())

	events, cancel := startSSE(t, h, "", "events.>", "")
	defer cancel()
	time.Sleep(100 * time.Millisecond)

	h.do(t, "POST", "/v1/signals/events.login.success", "", signalRequest{Payload: "x"})
	h.do(t, "POST", "/v1/signals/events.order.created", "", signalRequest{Payload: "y"})

	received := 0
	deadline := time.After(3 * time.Second)
	for received < 2 {
		select {
		case _, ok := <-events:
			if !ok {
				t.Fatal("stream closed early")
			}
			received++
		case <-deadline:
			t.Fatalf("received %d/2 events", received)
		}
	}
}

func TestSSE_QueueGroupRoundRobin(t *testing.T) {
	h := newHarness(t, testConfig())

	evA, cancelA := startSSE(t, h, "", "events.tasks", "workers")
	defer cancelA()
	evB, cancelB := startSSE(t, h, "", "events.tasks", "workers")
	defer cancelB()
	time.Sleep(150 * time.Millisecond)

	// Publish 4 signals; each should go to exactly one subscriber.
	for i := 0; i < 4; i++ {
		h.do(t, "POST", "/v1/signals/events.tasks", "", signalRequest{Payload: "job"})
	}

	count := 0
	deadline := time.After(3 * time.Second)
collect:
	for count < 4 {
		select {
		case _, ok := <-evA:
			if ok {
				count++
			}
		case _, ok := <-evB:
			if ok {
				count++
			}
		case <-deadline:
			break collect
		}
	}
	if count != 4 {
		t.Fatalf("received %d/4 events", count)
	}
}

func TestSSE_IdleSurvivesReadTimeout(t *testing.T) {
	cfg := testConfig()
	// Force the server-side read timeout to fire quickly so we can observe
	// the internal pinger keeping the virtual conn alive.
	cfg.ReadTimeout = 400 * time.Millisecond
	// Ping every 100ms (inside ReadTimeout).
	cfg.HTTPSSEPingInterval = 100 * time.Millisecond
	h := newHarness(t, cfg)

	events, cancel := startSSE(t, h, "", "idle.test", "")
	defer cancel()

	// Wait longer than ReadTimeout; the pinger should keep the virtual
	// conn alive.
	time.Sleep(1 * time.Second)

	// Confirm the stream still works by publishing.
	h.do(t, "POST", "/v1/signals/idle.test", "", signalRequest{Payload: "alive"})

	select {
	case ev, ok := <-events:
		if !ok {
			t.Fatal("SSE closed (pinger failed to keep conn alive)")
		}
		if !strings.Contains(ev.Data, "alive") {
			t.Fatalf("unexpected event: %q", ev.Data)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no event after idle period")
	}
}

func TestSSE_MissingPatternReturns400(t *testing.T) {
	h := newHarness(t, testConfig())
	resp := h.do(t, "GET", "/v1/signals", "", nil)
	if resp.StatusCode != 400 {
		t.Fatalf("status: %d want 400", resp.StatusCode)
	}
}

func TestSSE_InvalidPatternReturns400(t *testing.T) {
	h := newHarness(t, testConfig())
	req, err := http.NewRequest("GET", h.http.URL+"/v1/signals?pattern=events.%3E.bad", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := h.http.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d want 400 body=%s", resp.StatusCode, string(body))
	}
}

// TestSSE_PayloadWithControlCharProducesValidJSON covers a bug found in
// re-analysis: using Go's %q verb to build the SSE data field produced
// Go-syntax escapes (\xNN, \a, \v, ...) that are not valid JSON. Any
// payload with a control char would yield an SSE frame that no JSON
// parser could consume. The fix uses json.Marshal for both fields.
func TestSSE_PayloadWithControlCharProducesValidJSON(t *testing.T) {
	h := newHarness(t, testConfig())

	events, cancel := startSSE(t, h, "", "ctrl.test", "")
	defer cancel()
	time.Sleep(100 * time.Millisecond)

	// Payload containing a control char (\x01) that Go's %q would
	// escape as "\x01" — valid Go, invalid JSON.
	payload := "before\x01after"
	resp := h.do(t, "POST", "/v1/signals/ctrl.test", "", signalRequest{Payload: payload})
	if resp.StatusCode != 200 {
		t.Fatalf("publish: %d", resp.StatusCode)
	}

	select {
	case ev, ok := <-events:
		if !ok {
			t.Fatal("SSE closed without event")
		}
		// The data field must parse as valid JSON.
		var body struct {
			Channel string `json:"channel"`
			Payload string `json:"payload"`
		}
		if err := json.Unmarshal([]byte(ev.Data), &body); err != nil {
			t.Fatalf("SSE data is not valid JSON (bug regressed): %v\ndata: %q", err, ev.Data)
		}
		if body.Channel != "ctrl.test" {
			t.Fatalf("channel: got %q want ctrl.test", body.Channel)
		}
		if body.Payload != payload {
			t.Fatalf("payload roundtrip failed: got %q want %q", body.Payload, payload)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no event received")
	}
}

func TestSSE_CancelTriggersUnlisten(t *testing.T) {
	h := newHarness(t, testConfig())

	sessionCountBefore := h.bridge.SessionCount()

	_, cancel := startSSE(t, h, "", "events.cleanup", "")
	time.Sleep(100 * time.Millisecond)

	if h.bridge.SessionCount() != sessionCountBefore+1 {
		t.Fatalf("session not created for SSE stream")
	}

	cancel()

	// Wait for the server-side to notice the disconnect and clean up.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if h.bridge.SessionCount() == sessionCountBefore {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got := h.bridge.SessionCount(); got != sessionCountBefore {
		t.Fatalf("session count after cancel: %d, want %d", got, sessionCountBefore)
	}

	// Verify signal stats shows no listener for events.cleanup.
	sigStats := h.bridge.Signals().Stats()
	for _, s := range sigStats {
		if s.Pattern == "events.cleanup" {
			t.Fatalf("listener still registered after cancel: %+v", s)
		}
	}
}
