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

	m.Unlisten("ch", 1, "")

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

	m.Unlisten("ch.*", 1, "")

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
	m.Unlisten("c*", 2, "")

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
	m.Unlisten("exact.ch", 1, "")
	stats = m.Stats()
	lookup = make(map[string]int, len(stats))
	for _, ci := range stats {
		lookup[ci.Pattern] = ci.Listeners
	}
	if got, ok := lookup["exact.ch"]; !ok || got != 1 {
		t.Errorf("after unlisten: exact.ch listeners = %d, want 1 (found=%v)", got, ok)
	}

	// After removing all exact listeners, the pattern should disappear.
	m.Unlisten("exact.ch", 2, "")
	stats = m.Stats()
	lookup = make(map[string]int, len(stats))
	for _, ci := range stats {
		lookup[ci.Pattern] = ci.Listeners
	}
	if _, ok := lookup["exact.ch"]; ok {
		t.Error("exact.ch should be absent after all listeners removed")
	}
}

// ---------------------------------------------------------------------------
// Queue Group Tests
// ---------------------------------------------------------------------------

func makeGroupListener(connID uint64, pattern, group string) *Listener {
	return &Listener{
		ConnID:  connID,
		Pattern: pattern,
		Group:   group,
		WriteCh: make(chan []byte, 16),
	}
}

// 14. BasicRoundRobin — 2 grouped listeners, signal twice, each gets one
func TestQueueGroup_BasicRoundRobin(t *testing.T) {
	m := NewManager()
	a := makeGroupListener(1, "tasks.email", "workers")
	b := makeGroupListener(2, "tasks.email", "workers")
	m.Listen(a)
	m.Listen(b)

	n := m.Signal("tasks.email", "job1")
	if n != 1 {
		t.Fatalf("Signal returned %d, want 1", n)
	}

	n = m.Signal("tasks.email", "job2")
	if n != 1 {
		t.Fatalf("Signal returned %d, want 1", n)
	}

	// Each should have received exactly one message
	msgA, okA := recvWithin(a.WriteCh, timeout)
	msgB, okB := recvWithin(b.WriteCh, timeout)
	if !okA || !okB {
		t.Fatalf("expected both to receive one message: A=%v B=%v", okA, okB)
	}
	// Should not receive a second message
	_, extraA := recvWithin(a.WriteCh, timeout)
	_, extraB := recvWithin(b.WriteCh, timeout)
	if extraA || extraB {
		t.Fatal("one listener got more than one message")
	}
	_ = msgA
	_ = msgB
}

// 15. MixedGroupAndIndividual — A,B grouped + C different group + D non-grouped = 3 deliveries
func TestQueueGroup_MixedGroupAndIndividual(t *testing.T) {
	m := NewManager()
	a := makeGroupListener(1, "tasks.email", "worker-pool")
	b := makeGroupListener(2, "tasks.email", "worker-pool")
	c := makeGroupListener(3, "tasks.email", "audit-logger")
	d := makeListener(4, "tasks.email")
	m.Listen(a)
	m.Listen(b)
	m.Listen(c)
	m.Listen(d)

	n := m.Signal("tasks.email", "payload")
	// D (individual) + one of A/B (round-robin) + C (audit group) = 3
	if n != 3 {
		t.Fatalf("Signal returned %d, want 3", n)
	}

	// D should always receive
	_, okD := recvWithin(d.WriteCh, timeout)
	if !okD {
		t.Fatal("non-grouped listener D did not receive")
	}

	// C should always receive (only member of audit-logger group)
	_, okC := recvWithin(c.WriteCh, timeout)
	if !okC {
		t.Fatal("audit-logger group member C did not receive")
	}

	// Exactly one of A or B should receive
	_, okA := recvWithin(a.WriteCh, timeout)
	_, okB := recvWithin(b.WriteCh, timeout)
	if okA == okB {
		t.Fatalf("expected exactly one of A/B to receive: A=%v B=%v", okA, okB)
	}
}

// 16. DedupWithIndividual — connID in group + non-grouped, receives once, group advances
func TestQueueGroup_IndependentFromNonGrouped(t *testing.T) {
	m := NewManager()
	ch := make(chan []byte, 16)
	// Same connID, same WriteCh — registered as both non-grouped and grouped.
	// Groups deliver independently from non-grouped subscriptions, so the
	// connection should receive 2 messages: one from the individual subscription
	// and one from the queue group.
	individual := &Listener{ConnID: 1, Pattern: "ch", WriteCh: ch}
	grouped := &Listener{ConnID: 1, Pattern: "ch", Group: "g1", WriteCh: ch}
	m.Listen(individual)
	m.Listen(grouped)

	m.Signal("ch", "payload")

	// Should have two messages: one from non-grouped, one from the group.
	_, ok := recvWithin(ch, timeout)
	if !ok {
		t.Fatal("expected first message (non-grouped)")
	}
	_, ok = recvWithin(ch, timeout)
	if !ok {
		t.Fatal("expected second message (group)")
	}
	_, ok = recvWithin(ch, timeout)
	if ok {
		t.Fatal("got unexpected third message")
	}
}

// 17. WildcardGroup — grouped wildcard listeners, only one receives
func TestQueueGroup_Wildcard(t *testing.T) {
	m := NewManager()
	a := makeGroupListener(1, "tasks.*", "workers")
	b := makeGroupListener(2, "tasks.*", "workers")
	m.Listen(a)
	m.Listen(b)

	n := m.Signal("tasks.email", "job1")
	if n != 1 {
		t.Fatalf("Signal returned %d, want 1", n)
	}

	_, okA := recvWithin(a.WriteCh, timeout)
	_, okB := recvWithin(b.WriteCh, timeout)
	if okA == okB {
		t.Fatalf("expected exactly one of A/B: A=%v B=%v", okA, okB)
	}
}

// 18. Unlisten — remove one grouped member, remaining gets all signals
func TestQueueGroup_Unlisten(t *testing.T) {
	m := NewManager()
	a := makeGroupListener(1, "ch", "g1")
	b := makeGroupListener(2, "ch", "g1")
	m.Listen(a)
	m.Listen(b)

	m.Unlisten("ch", 1, "g1")

	// Only B remains in group
	n := m.Signal("ch", "payload")
	if n != 1 {
		t.Fatalf("Signal returned %d, want 1", n)
	}
	_, ok := recvWithin(b.WriteCh, timeout)
	if !ok {
		t.Fatal("remaining member B did not receive")
	}
	_, ok = recvWithin(a.WriteCh, timeout)
	if ok {
		t.Fatal("removed member A should not receive")
	}
}

// 19. UnlistenAll — cleanup removes grouped registrations
func TestQueueGroup_UnlistenAll(t *testing.T) {
	m := NewManager()
	a := makeGroupListener(1, "ch", "g1")
	b := makeGroupListener(1, "wild.*", "g2")
	m.Listen(a)
	m.Listen(b)

	m.UnlistenAll(1)

	if n := m.Signal("ch", "x"); n != 0 {
		t.Errorf("exact grouped: Signal returned %d, want 0", n)
	}
	if n := m.Signal("wild.a", "x"); n != 0 {
		t.Errorf("wild grouped: Signal returned %d, want 0", n)
	}
}

// 20. MultipleGroups — two groups on same pattern, one delivery per group
func TestQueueGroup_MultipleGroups(t *testing.T) {
	m := NewManager()
	a := makeGroupListener(1, "ch", "g1")
	b := makeGroupListener(2, "ch", "g1")
	c := makeGroupListener(3, "ch", "g2")
	d := makeGroupListener(4, "ch", "g2")
	m.Listen(a)
	m.Listen(b)
	m.Listen(c)
	m.Listen(d)

	n := m.Signal("ch", "payload")
	// One from g1 + one from g2 = 2
	if n != 2 {
		t.Fatalf("Signal returned %d, want 2", n)
	}
}

// 21. DuplicateSubscription — same connID+pattern+group ignored
func TestQueueGroup_DuplicateSubscription(t *testing.T) {
	m := NewManager()
	a := makeGroupListener(1, "ch", "g1")
	m.Listen(a)

	// Duplicate
	a2 := makeGroupListener(1, "ch", "g1")
	m.Listen(a2)

	n := m.Signal("ch", "payload")
	if n != 1 {
		t.Fatalf("Signal returned %d, want 1 (duplicate should be ignored)", n)
	}
}

// 22. SamePatternDifferentGroups — same connID, same pattern, two groups = both registered
func TestQueueGroup_SamePatternDifferentGroups(t *testing.T) {
	m := NewManager()
	a := makeGroupListener(1, "ch", "g1")
	b := makeGroupListener(1, "ch", "g2")
	m.Listen(a)
	m.Listen(b)

	n := m.Signal("ch", "payload")
	// ConnID 1 is in both groups. First group delivers to connID 1 (marked delivered).
	// Second group tries connID 1, finds it already delivered, skips.
	// So only 1 delivery total.
	if n != 1 {
		t.Fatalf("Signal returned %d, want 1 (dedup across groups)", n)
	}
}

// 23. Stats — group info in stats output
func TestQueueGroup_Stats(t *testing.T) {
	m := NewManager()
	m.Listen(makeGroupListener(1, "ch", "g1"))
	m.Listen(makeGroupListener(2, "ch", "g1"))
	m.Listen(makeGroupListener(3, "ch", "g2"))
	m.Listen(makeListener(4, "ch"))

	stats := m.Stats()
	// Should have: ch (non-grouped, 1 listener), ch/g1 (2), ch/g2 (1)
	type key struct {
		pattern string
		group   string
	}
	lookup := make(map[key]int)
	for _, ci := range stats {
		lookup[key{ci.Pattern, ci.Group}] = ci.Listeners
	}

	if got := lookup[key{"ch", ""}]; got != 1 {
		t.Errorf("non-grouped ch: got %d, want 1", got)
	}
	if got := lookup[key{"ch", "g1"}]; got != 2 {
		t.Errorf("ch/g1: got %d, want 2", got)
	}
	if got := lookup[key{"ch", "g2"}]; got != 1 {
		t.Errorf("ch/g2: got %d, want 1", got)
	}
}

// 24. RoundRobinDistribution — 99 signals to 3 members, 33 each
func TestQueueGroup_RoundRobinDistribution(t *testing.T) {
	m := NewManager()
	const total = 99
	listeners := make([]*Listener, 3)
	for i := range listeners {
		listeners[i] = &Listener{
			ConnID:  uint64(i + 1),
			Pattern: "ch",
			Group:   "workers",
			WriteCh: make(chan []byte, total), // large enough to not drop
		}
		m.Listen(listeners[i])
	}

	for i := 0; i < total; i++ {
		m.Signal("ch", "job")
	}

	counts := make([]int, 3)
	for i, l := range listeners {
		for {
			_, ok := recvWithin(l.WriteCh, timeout)
			if !ok {
				break
			}
			counts[i]++
		}
	}

	for i, c := range counts {
		if c != total/3 {
			t.Errorf("listener %d got %d, want %d", i, c, total/3)
		}
	}
}

// 25. EmptyGroupBackwardCompat — empty group = non-grouped
func TestQueueGroup_EmptyGroupBackwardCompat(t *testing.T) {
	m := NewManager()
	l := makeGroupListener(1, "ch", "")
	m.Listen(l)

	n := m.Signal("ch", "payload")
	if n != 1 {
		t.Fatalf("Signal returned %d, want 1", n)
	}
	_, ok := recvWithin(l.WriteCh, timeout)
	if !ok {
		t.Fatal("expected message")
	}
}

// 26. WildcardMixedGroupAndIndividual — non-grouped wild + grouped wild
func TestQueueGroup_WildcardMixedGroupAndIndividual(t *testing.T) {
	m := NewManager()
	individual := makeListener(1, "tasks.*")
	groupA := makeGroupListener(2, "tasks.*", "workers")
	groupB := makeGroupListener(3, "tasks.*", "workers")
	m.Listen(individual)
	m.Listen(groupA)
	m.Listen(groupB)

	n := m.Signal("tasks.email", "job")
	// individual + one of groupA/groupB = 2
	if n != 2 {
		t.Fatalf("Signal returned %d, want 2", n)
	}

	_, okInd := recvWithin(individual.WriteCh, timeout)
	if !okInd {
		t.Fatal("individual listener did not receive")
	}

	_, okA := recvWithin(groupA.WriteCh, timeout)
	_, okB := recvWithin(groupB.WriteCh, timeout)
	if okA == okB {
		t.Fatalf("expected exactly one of A/B: A=%v B=%v", okA, okB)
	}
}
