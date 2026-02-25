package signal

import (
	"testing"
	"time"
)

// makeListener creates a Listener with a buffered WriteCh for testing.
func makeListener(connID uint64, pattern string) *Listener {
	return &Listener{
		ConnID:  connID,
		Pattern: pattern,
		WriteCh: make(chan []byte, 16),
	}
}

// recvWithin reads from ch with a short timeout, returning the value and true
// on success, or nil and false if nothing arrived in time.
func recvWithin(ch <-chan []byte, d time.Duration) ([]byte, bool) {
	select {
	case v := <-ch:
		return v, true
	case <-time.After(d):
		return nil, false
	}
}

const timeout = 50 * time.Millisecond

// ---------------------------------------------------------------------------
// 1. MatchPattern
// ---------------------------------------------------------------------------

func TestMatchPattern(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		channel string
		want    bool
	}{
		// Exact match
		{"exact match", "alerts.fire", "alerts.fire", true},
		{"exact no match", "alerts.fire", "alerts.flood", false},
		{"exact shorter pattern", "alerts", "alerts.fire", false},
		{"exact longer pattern", "alerts.fire", "alerts", false},

		// Single-token wildcard *
		{"star middle", "alerts.*.critical", "alerts.fire.critical", true},
		{"star first", "*.fire", "alerts.fire", true},
		{"star last", "alerts.*", "alerts.fire", true},
		{"star no match too many", "alerts.*", "alerts.fire.critical", false},
		{"star no match too few", "alerts.*.critical", "alerts.fire", false},
		{"star mismatch literal", "alerts.*.critical", "alerts.fire.warn", false},
		{"multiple stars", "*.*", "a.b", true},
		{"multiple stars mismatch count", "*.*", "a.b.c", false},

		// Multi-token wildcard >
		{"gt matches one trailing", "alerts.>", "alerts.fire", true},
		{"gt matches many trailing", "alerts.>", "alerts.fire.critical.now", true},
		{"gt as catch-all", ">", "anything.at.all", true},
		{"gt single token", ">", "anything", true},
		{"gt no match zero trailing", "alerts.>", "alerts", false},

		// Mixed wildcards
		{"star then gt", "alerts.*.>", "alerts.fire.critical", true},
		{"star then gt multi", "alerts.*.>", "alerts.fire.critical.now", true},
		{"star then gt too few", "alerts.*.>", "alerts.fire", false},

		// Edge cases
		{"empty tokens", "a..b", "a..b", true},
		{"single token exact", "a", "a", true},
		{"single token mismatch", "a", "b", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MatchPattern(tt.pattern, tt.channel)
			if got != tt.want {
				t.Errorf("MatchPattern(%q, %q) = %v, want %v",
					tt.pattern, tt.channel, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 2. Listen + Signal exact channel
// ---------------------------------------------------------------------------

func TestListenSignalExact(t *testing.T) {
	m := NewManager()
	l := makeListener(1, "alerts.fire")
	m.Listen(l)

	n := m.Signal("alerts.fire", "hot")
	if n != 1 {
		t.Fatalf("Signal returned %d, want 1", n)
	}

	msg, ok := recvWithin(l.WriteCh, timeout)
	if !ok {
		t.Fatal("expected message but channel was empty")
	}
	want := "sig alerts.fire hot\n"
	if string(msg) != want {
		t.Errorf("got %q, want %q", msg, want)
	}
}

// ---------------------------------------------------------------------------
// 3. Wildcard * delivery
// ---------------------------------------------------------------------------

func TestWildcardStar(t *testing.T) {
	m := NewManager()
	l := makeListener(1, "alerts.*")
	m.Listen(l)

	n := m.Signal("alerts.fire", "hot")
	if n != 1 {
		t.Fatalf("Signal returned %d, want 1", n)
	}

	msg, ok := recvWithin(l.WriteCh, timeout)
	if !ok {
		t.Fatal("expected message but channel was empty")
	}
	want := "sig alerts.fire hot\n"
	if string(msg) != want {
		t.Errorf("got %q, want %q", msg, want)
	}

	// Should NOT match a two-level suffix.
	n = m.Signal("alerts.fire.critical", "very hot")
	if n != 0 {
		t.Errorf("Signal returned %d for non-matching channel, want 0", n)
	}
}

// ---------------------------------------------------------------------------
// 4. Wildcard > delivery
// ---------------------------------------------------------------------------

func TestWildcardGT(t *testing.T) {
	m := NewManager()
	l := makeListener(1, "alerts.>")
	m.Listen(l)

	n := m.Signal("alerts.fire.critical", "bad")
	if n != 1 {
		t.Fatalf("Signal returned %d, want 1", n)
	}

	msg, ok := recvWithin(l.WriteCh, timeout)
	if !ok {
		t.Fatal("expected message but channel was empty")
	}
	want := "sig alerts.fire.critical bad\n"
	if string(msg) != want {
		t.Errorf("got %q, want %q", msg, want)
	}
}

// ---------------------------------------------------------------------------
// 5. Dedup: same connID on overlapping patterns receives signal only once
// ---------------------------------------------------------------------------

func TestDedup(t *testing.T) {
	m := NewManager()
	ch := make(chan []byte, 16)

	l1 := &Listener{ConnID: 1, Pattern: "a.*", WriteCh: ch}
	l2 := &Listener{ConnID: 1, Pattern: "a.>", WriteCh: ch}
	m.Listen(l1)
	m.Listen(l2)

	n := m.Signal("a.b", "payload")
	if n != 1 {
		t.Fatalf("Signal returned %d, want 1 (dedup)", n)
	}

	// Exactly one message should be on the channel.
	_, ok := recvWithin(ch, timeout)
	if !ok {
		t.Fatal("expected one message")
	}
	_, ok = recvWithin(ch, timeout)
	if ok {
		t.Fatal("got a second message; dedup failed")
	}
}

// ---------------------------------------------------------------------------
// 6. Multiple distinct listeners all receive the signal
// ---------------------------------------------------------------------------

func TestMultipleListeners(t *testing.T) {
	m := NewManager()
	l1 := makeListener(1, "ch")
	l2 := makeListener(2, "ch")
	l3 := makeListener(3, "ch")
	m.Listen(l1)
	m.Listen(l2)
	m.Listen(l3)

	n := m.Signal("ch", "data")
	if n != 3 {
		t.Fatalf("Signal returned %d, want 3", n)
	}

	for _, l := range []*Listener{l1, l2, l3} {
		_, ok := recvWithin(l.WriteCh, timeout)
		if !ok {
			t.Errorf("listener connID=%d did not receive message", l.ConnID)
		}
	}
}

// ---------------------------------------------------------------------------
// 7. Unlisten exact stops delivery
// ---------------------------------------------------------------------------

func TestUnlistenExact(t *testing.T) {
	m := NewManager()
	l := makeListener(1, "ch")
	m.Listen(l)

	// Verify delivery before unlisten.
	n := m.Signal("ch", "before")
	if n != 1 {
		t.Fatalf("Signal returned %d before unlisten, want 1", n)
	}
	_, ok := recvWithin(l.WriteCh, timeout)
	if !ok {
		t.Fatal("expected message before unlisten")
	}

	m.Unlisten("ch", 1)

	n = m.Signal("ch", "after")
	if n != 0 {
		t.Fatalf("Signal returned %d after unlisten, want 0", n)
	}
	_, ok = recvWithin(l.WriteCh, timeout)
	if ok {
		t.Fatal("received message after unlisten")
	}
}

// ---------------------------------------------------------------------------
// 8. Unlisten wildcard stops delivery
// ---------------------------------------------------------------------------

func TestUnlistenWildcard(t *testing.T) {
	m := NewManager()
	l := makeListener(1, "ch.*")
	m.Listen(l)

	n := m.Signal("ch.a", "before")
	if n != 1 {
		t.Fatalf("Signal returned %d before unlisten, want 1", n)
	}
	_, ok := recvWithin(l.WriteCh, timeout)
	if !ok {
		t.Fatal("expected message before unlisten")
	}

	m.Unlisten("ch.*", 1)

	n = m.Signal("ch.a", "after")
	if n != 0 {
		t.Fatalf("Signal returned %d after unlisten, want 0", n)
	}
	_, ok = recvWithin(l.WriteCh, timeout)
	if ok {
		t.Fatal("received message after unlisten")
	}
}

// ---------------------------------------------------------------------------
// 9. UnlistenAll removes every registration for a connection
// ---------------------------------------------------------------------------

func TestUnlistenAll(t *testing.T) {
	m := NewManager()
	l1 := makeListener(1, "exact.ch")
	l2 := makeListener(1, "wild.*")
	l3 := makeListener(1, "deep.>")
	m.Listen(l1)
	m.Listen(l2)
	m.Listen(l3)

	m.UnlistenAll(1)

	if n := m.Signal("exact.ch", "x"); n != 0 {
		t.Errorf("exact channel: Signal returned %d, want 0", n)
	}
	if n := m.Signal("wild.a", "x"); n != 0 {
		t.Errorf("wildcard * channel: Signal returned %d, want 0", n)
	}
	if n := m.Signal("deep.a.b", "x"); n != 0 {
		t.Errorf("wildcard > channel: Signal returned %d, want 0", n)
	}
}

// ---------------------------------------------------------------------------
// 10. Signal with no listeners returns 0
// ---------------------------------------------------------------------------

func TestNoListeners(t *testing.T) {
	m := NewManager()
	n := m.Signal("nobody.home", "hello")
	if n != 0 {
		t.Fatalf("Signal returned %d, want 0", n)
	}
}

// ---------------------------------------------------------------------------
// 11. Full buffer does not block the sender
// ---------------------------------------------------------------------------

func TestFullBufferDrop(t *testing.T) {
	m := NewManager()
	// Create a listener with a buffer of 1.
	l := &Listener{
		ConnID:  1,
		Pattern: "ch",
		WriteCh: make(chan []byte, 1),
	}
	m.Listen(l)

	// Fill the buffer.
	m.Signal("ch", "first")

	// Second signal should not block even though the buffer is full.
	done := make(chan struct{})
	go func() {
		m.Signal("ch", "second")
		close(done)
	}()

	select {
	case <-done:
		// Success: Signal did not block.
	case <-time.After(time.Second):
		t.Fatal("Signal blocked on full buffer")
	}
}

// ---------------------------------------------------------------------------
// 12. Signal returns the correct count of unique recipients
// ---------------------------------------------------------------------------

func TestSignalReturnsCount(t *testing.T) {
	m := NewManager()

	// No listeners.
	if n := m.Signal("ch", "x"); n != 0 {
		t.Errorf("no listeners: got %d, want 0", n)
	}

	// One exact listener.
	l1 := makeListener(1, "ch")
	m.Listen(l1)
	if n := m.Signal("ch", "x"); n != 1 {
		t.Errorf("one exact: got %d, want 1", n)
	}

	// Add a wildcard listener with a different connID.
	l2 := makeListener(2, "c*")
	m.Listen(l2)
	// "c*" is treated as a wildcard (contains *), but MatchPattern("c*","ch")
	// would split on '.' giving tokens ["c*"] vs ["ch"]. "c*" != "*" and != "ch",
	// so it won't match. Use a proper dotted wildcard instead.
	m.Unlisten("c*", 2)

	l2b := makeListener(2, "*")
	m.Listen(l2b)
	if n := m.Signal("ch", "x"); n != 2 {
		t.Errorf("exact + wildcard: got %d, want 2", n)
	}

	// Add a third listener with yet another connID on a > wildcard.
	l3 := makeListener(3, ">")
	m.Listen(l3)
	if n := m.Signal("ch", "x"); n != 3 {
		t.Errorf("exact + two wildcards: got %d, want 3", n)
	}

	// Drain channels so they don't interfere with subsequent assertions.
	for range l1.WriteCh {
		break
	}
}

// ---------------------------------------------------------------------------
// 13. Stats returns correct channel/pattern info
// ---------------------------------------------------------------------------

func TestStats(t *testing.T) {
	m := NewManager()

	// Empty manager.
	stats := m.Stats()
	if len(stats) != 0 {
		t.Fatalf("empty manager: got %d stats, want 0", len(stats))
	}

	// Add some listeners.
	m.Listen(makeListener(1, "exact.ch"))
	m.Listen(makeListener(2, "exact.ch"))
	m.Listen(makeListener(3, "wild.*"))

	stats = m.Stats()
	if len(stats) != 2 {
		t.Fatalf("got %d stat entries, want 2", len(stats))
	}

	// Build a lookup for deterministic checks.
	lookup := make(map[string]int, len(stats))
	for _, ci := range stats {
		lookup[ci.Pattern] = ci.Listeners
	}

	if got, ok := lookup["exact.ch"]; !ok || got != 2 {
		t.Errorf("exact.ch listeners = %d, want 2 (found=%v)", got, ok)
	}
	if got, ok := lookup["wild.*"]; !ok || got != 1 {
		t.Errorf("wild.* listeners = %d, want 1 (found=%v)", got, ok)
	}

	// After removing one exact listener, count should update.
	m.Unlisten("exact.ch", 1)
	stats = m.Stats()
	lookup = make(map[string]int, len(stats))
	for _, ci := range stats {
		lookup[ci.Pattern] = ci.Listeners
	}
	if got, ok := lookup["exact.ch"]; !ok || got != 1 {
		t.Errorf("after unlisten: exact.ch listeners = %d, want 1 (found=%v)", got, ok)
	}

	// After removing all exact listeners, the pattern should disappear.
	m.Unlisten("exact.ch", 2)
	stats = m.Stats()
	lookup = make(map[string]int, len(stats))
	for _, ci := range stats {
		lookup[ci.Pattern] = ci.Listeners
	}
	if _, ok := lookup["exact.ch"]; ok {
		t.Error("exact.ch should be absent after all listeners removed")
	}
}
