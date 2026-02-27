package watch

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestWatch_ExactMatch(t *testing.T) {
	m := NewManager()
	ch := make(chan []byte, 10)
	m.Watch(&Watcher{ConnID: 1, Pattern: "mykey", WriteCh: ch, CancelConn: func() {}})
	m.Notify("kset", "mykey")

	select {
	case msg := <-ch:
		expected := "watch kset mykey\n"
		if string(msg) != expected {
			t.Fatalf("expected %q, got %q", expected, string(msg))
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

func TestWatch_Wildcard(t *testing.T) {
	m := NewManager()
	ch := make(chan []byte, 10)
	m.Watch(&Watcher{ConnID: 1, Pattern: "prefix.*", WriteCh: ch, CancelConn: func() {}})
	m.Notify("kset", "prefix.foo")

	select {
	case msg := <-ch:
		expected := "watch kset prefix.foo\n"
		if string(msg) != expected {
			t.Fatalf("expected %q, got %q", expected, string(msg))
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}

	// Should not match a different prefix
	m.Notify("kset", "other.foo")
	select {
	case <-ch:
		t.Fatal("should not match other.foo")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestWatch_Unwatch(t *testing.T) {
	m := NewManager()
	ch := make(chan []byte, 10)
	m.Watch(&Watcher{ConnID: 1, Pattern: "mykey", WriteCh: ch, CancelConn: func() {}})
	m.Unwatch("mykey", 1)
	m.Notify("kset", "mykey")

	select {
	case <-ch:
		t.Fatal("should not receive after unwatch")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestWatch_UnwatchAll(t *testing.T) {
	m := NewManager()
	ch := make(chan []byte, 10)
	m.Watch(&Watcher{ConnID: 1, Pattern: "key1", WriteCh: ch, CancelConn: func() {}})
	m.Watch(&Watcher{ConnID: 1, Pattern: "key2", WriteCh: ch, CancelConn: func() {}})
	m.UnwatchAll(1)
	m.Notify("kset", "key1")
	m.Notify("kset", "key2")

	select {
	case <-ch:
		t.Fatal("should not receive after unwatch all")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestWatch_DuplicateIgnored(t *testing.T) {
	m := NewManager()
	ch := make(chan []byte, 10)
	m.Watch(&Watcher{ConnID: 1, Pattern: "mykey", WriteCh: ch, CancelConn: func() {}})
	m.Watch(&Watcher{ConnID: 1, Pattern: "mykey", WriteCh: ch, CancelConn: func() {}})
	m.Notify("kset", "mykey")

	// Should only receive one notification
	<-ch
	select {
	case <-ch:
		t.Fatal("should not receive duplicate notification")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestWatch_Stats(t *testing.T) {
	m := NewManager()
	ch1 := make(chan []byte, 10)
	ch2 := make(chan []byte, 10)
	m.Watch(&Watcher{ConnID: 1, Pattern: "key1", WriteCh: ch1, CancelConn: func() {}})
	m.Watch(&Watcher{ConnID: 2, Pattern: "key1", WriteCh: ch2, CancelConn: func() {}})

	stats := m.Stats()
	if len(stats) != 1 {
		t.Fatalf("expected 1 stat entry, got %d", len(stats))
	}
	if stats[0].Pattern != "key1" || stats[0].Watchers != 2 {
		t.Fatalf("unexpected stats: %+v", stats[0])
	}
}

func TestWatch_Dedup(t *testing.T) {
	// A connection watching both exact and wildcard should receive only once
	m := NewManager()
	ch := make(chan []byte, 10)
	m.Watch(&Watcher{ConnID: 1, Pattern: "mykey", WriteCh: ch, CancelConn: func() {}})
	m.Watch(&Watcher{ConnID: 1, Pattern: "*", WriteCh: ch, CancelConn: func() {}})
	m.Notify("kset", "mykey")

	<-ch
	select {
	case <-ch:
		t.Fatal("should deduplicate across exact and wildcard")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestWatch_PatternValidation(t *testing.T) {
	m := NewManager()
	ch := make(chan []byte, 10)

	tests := []struct {
		name    string
		pattern string
		wantErr bool
	}{
		{"valid exact", "mykey", false},
		{"valid star", "prefix.*", false},
		{"valid gt", "prefix.>", false},
		{"valid star then gt", "prefix.*.>", false},
		{"valid multi star", "*.*", false},
		{"valid catch-all gt", ">", false},
		{"valid single star", "*", false},
		{"invalid gt not last", ">.foo", true},
		{"invalid gt middle", "a.>.b", true},
		{"invalid partial star", "a.c*", true},
		{"invalid partial gt", "a.b>", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := m.Watch(&Watcher{ConnID: 1, Pattern: tc.pattern, WriteCh: ch, CancelConn: func() {}})
			if tc.wantErr && err == nil {
				t.Errorf("expected error for pattern %q", tc.pattern)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error for pattern %q: %v", tc.pattern, err)
			}
		})
	}
}

func TestWatch_MatchPattern(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		key     string
		want    bool
	}{
		{"exact match", "foo", "foo", true},
		{"exact no match", "foo", "bar", false},
		{"star match", "a.*", "a.b", true},
		{"star no match extra depth", "a.*", "a.b.c", false},
		{"star no match wrong prefix", "a.*", "b.c", false},
		{"gt match one", "a.>", "a.b", true},
		{"gt match many", "a.>", "a.b.c.d", true},
		{"gt no match zero", "a.>", "a", false},
		{"catch-all", ">", "anything", true},
		{"catch-all multi", ">", "a.b.c", true},
		{"star then gt", "a.*.>", "a.b.c", true},
		{"star then gt multi", "a.*.>", "a.b.c.d", true},
		{"star then gt too few", "a.*.>", "a.b", false},
		{"multiple stars", "*.*.*", "a.b.c", true},
		{"multiple stars wrong count", "*.*", "a.b.c", false},
		{"empty tokens", "a..b", "a..b", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := matchPattern(tc.pattern, tc.key)
			if got != tc.want {
				t.Errorf("matchPattern(%q, %q) = %v, want %v", tc.pattern, tc.key, got, tc.want)
			}
		})
	}
}

func TestWatch_MultiTokenWildcard(t *testing.T) {
	m := NewManager()
	ch := make(chan []byte, 10)
	m.Watch(&Watcher{ConnID: 1, Pattern: "prefix.>", WriteCh: ch, CancelConn: func() {}})

	m.Notify("kset", "prefix.a.b.c")
	select {
	case msg := <-ch:
		expected := "watch kset prefix.a.b.c\n"
		if string(msg) != expected {
			t.Fatalf("expected %q, got %q", expected, string(msg))
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}

	// Should NOT match just "prefix" (> requires at least one more token)
	m.Notify("kset", "prefix")
	select {
	case <-ch:
		t.Fatal("should not match 'prefix' with 'prefix.>'")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestWatch_SlowConsumer(t *testing.T) {
	m := NewManager()
	ch := make(chan []byte) // unbuffered
	cancelled := make(chan struct{})
	cancel := func() { close(cancelled) }

	m.Watch(&Watcher{ConnID: 1, Pattern: "mykey", WriteCh: ch, CancelConn: cancel})

	done := make(chan struct{})
	go func() {
		m.Notify("kset", "mykey")
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Notify blocked on slow consumer")
	}

	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("CancelConn was not called for slow consumer")
	}
}

func TestWatch_SlowConsumerWildcard(t *testing.T) {
	m := NewManager()
	ch := make(chan []byte) // unbuffered
	cancelled := make(chan struct{})
	cancel := func() { close(cancelled) }

	m.Watch(&Watcher{ConnID: 1, Pattern: "*", WriteCh: ch, CancelConn: cancel})

	done := make(chan struct{})
	go func() {
		m.Notify("kset", "anykey")
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Notify blocked on slow wildcard consumer")
	}

	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("CancelConn was not called for slow wildcard consumer")
	}
}

func TestWatch_MultipleConnsSamePattern(t *testing.T) {
	m := NewManager()
	ch1 := make(chan []byte, 10)
	ch2 := make(chan []byte, 10)
	ch3 := make(chan []byte, 10)

	m.Watch(&Watcher{ConnID: 1, Pattern: "mykey", WriteCh: ch1, CancelConn: func() {}})
	m.Watch(&Watcher{ConnID: 2, Pattern: "mykey", WriteCh: ch2, CancelConn: func() {}})
	m.Watch(&Watcher{ConnID: 3, Pattern: "mykey", WriteCh: ch3, CancelConn: func() {}})

	m.Notify("kset", "mykey")

	for i, ch := range []chan []byte{ch1, ch2, ch3} {
		select {
		case msg := <-ch:
			expected := "watch kset mykey\n"
			if string(msg) != expected {
				t.Fatalf("conn %d: expected %q, got %q", i+1, expected, string(msg))
			}
		case <-time.After(time.Second):
			t.Fatalf("conn %d: timeout waiting for notification", i+1)
		}
	}
}

func TestWatch_UnwatchWildcard(t *testing.T) {
	m := NewManager()
	ch := make(chan []byte, 10)
	m.Watch(&Watcher{ConnID: 1, Pattern: "prefix.*", WriteCh: ch, CancelConn: func() {}})

	m.Notify("kset", "prefix.foo")
	<-ch

	m.Unwatch("prefix.*", 1)

	m.Notify("kset", "prefix.bar")
	select {
	case <-ch:
		t.Fatal("should not receive after unwatch wildcard")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestWatch_UnwatchAllMixed(t *testing.T) {
	m := NewManager()
	ch := make(chan []byte, 10)

	m.Watch(&Watcher{ConnID: 1, Pattern: "exact", WriteCh: ch, CancelConn: func() {}})
	m.Watch(&Watcher{ConnID: 1, Pattern: "wild.*", WriteCh: ch, CancelConn: func() {}})
	m.Watch(&Watcher{ConnID: 1, Pattern: "deep.>", WriteCh: ch, CancelConn: func() {}})

	m.UnwatchAll(1)

	m.Notify("kset", "exact")
	m.Notify("kset", "wild.foo")
	m.Notify("kset", "deep.a.b")

	select {
	case <-ch:
		t.Fatal("should not receive after unwatch all")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestWatch_NoWatchers(t *testing.T) {
	m := NewManager()
	// Should not panic
	m.Notify("kset", "noone")
}

func TestWatch_StatsWildcard(t *testing.T) {
	m := NewManager()
	ch := make(chan []byte, 10)
	m.Watch(&Watcher{ConnID: 1, Pattern: "wild.*", WriteCh: ch, CancelConn: func() {}})
	m.Watch(&Watcher{ConnID: 2, Pattern: "wild.*", WriteCh: ch, CancelConn: func() {}})
	m.Watch(&Watcher{ConnID: 3, Pattern: "exact", WriteCh: ch, CancelConn: func() {}})

	stats := m.Stats()
	if len(stats) != 2 {
		t.Fatalf("expected 2 stat entries, got %d", len(stats))
	}

	lookup := make(map[string]int)
	for _, s := range stats {
		lookup[s.Pattern] = s.Watchers
	}

	if lookup["wild.*"] != 2 {
		t.Fatalf("expected 2 watchers for wild.*, got %d", lookup["wild.*"])
	}
	if lookup["exact"] != 1 {
		t.Fatalf("expected 1 watcher for exact, got %d", lookup["exact"])
	}
}

func TestWatch_DedupGT(t *testing.T) {
	m := NewManager()
	ch := make(chan []byte, 10)
	m.Watch(&Watcher{ConnID: 1, Pattern: "mykey", WriteCh: ch, CancelConn: func() {}})
	m.Watch(&Watcher{ConnID: 1, Pattern: ">", WriteCh: ch, CancelConn: func() {}})
	m.Notify("kset", "mykey")

	<-ch
	select {
	case <-ch:
		t.Fatal("should deduplicate across exact and > wildcard")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestWatch_MultipleEventTypes(t *testing.T) {
	m := NewManager()
	ch := make(chan []byte, 10)
	m.Watch(&Watcher{ConnID: 1, Pattern: "k1", WriteCh: ch, CancelConn: func() {}})

	events := []string{"kset", "kdel", "acquire", "release", "incr", "lpush"}
	for _, ev := range events {
		m.Notify(ev, "k1")
		msg := <-ch
		expected := "watch " + ev + " k1\n"
		if string(msg) != expected {
			t.Fatalf("event %s: expected %q, got %q", ev, expected, string(msg))
		}
	}
}

func TestWatch_UnwatchNonexistent(t *testing.T) {
	m := NewManager()
	// Should not panic
	m.Unwatch("nonexistent", 999)
	m.UnwatchAll(999)
}

func TestWatch_UnwatchPreservesOthers(t *testing.T) {
	m := NewManager()
	ch1 := make(chan []byte, 10)
	ch2 := make(chan []byte, 10)

	m.Watch(&Watcher{ConnID: 1, Pattern: "mykey", WriteCh: ch1, CancelConn: func() {}})
	m.Watch(&Watcher{ConnID: 2, Pattern: "mykey", WriteCh: ch2, CancelConn: func() {}})

	m.Unwatch("mykey", 1)
	m.Notify("kset", "mykey")

	select {
	case <-ch1:
		t.Fatal("conn1 should not receive after unwatch")
	case <-time.After(50 * time.Millisecond):
	}

	select {
	case msg := <-ch2:
		expected := "watch kset mykey\n"
		if string(msg) != expected {
			t.Fatalf("conn2: expected %q, got %q", expected, string(msg))
		}
	case <-time.After(time.Second):
		t.Fatal("conn2: timeout")
	}
}

func TestWatch_StatsEmpty(t *testing.T) {
	m := NewManager()
	stats := m.Stats()
	if len(stats) != 0 {
		t.Fatalf("expected 0 stats, got %d", len(stats))
	}
}

func TestWatch_StatsAfterUnwatch(t *testing.T) {
	m := NewManager()
	ch := make(chan []byte, 10)
	m.Watch(&Watcher{ConnID: 1, Pattern: "mykey", WriteCh: ch, CancelConn: func() {}})
	m.Unwatch("mykey", 1)

	stats := m.Stats()
	if len(stats) != 0 {
		t.Fatalf("expected 0 stats after unwatch, got %d", len(stats))
	}
}

func TestWatch_ConcurrentWatchUnwatch(t *testing.T) {
	m := NewManager()
	const N = 100
	var wg sync.WaitGroup

	// Concurrently watch many patterns from many connections
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ch := make(chan []byte, 10)
			pattern := fmt.Sprintf("key%d", id)
			m.Watch(&Watcher{ConnID: uint64(id), Pattern: pattern, WriteCh: ch, CancelConn: func() {}})
		}(i)
	}
	wg.Wait()

	// Concurrently unwatch all
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			pattern := fmt.Sprintf("key%d", id)
			m.Unwatch(pattern, uint64(id))
		}(i)
	}
	wg.Wait()

	stats := m.Stats()
	if len(stats) != 0 {
		t.Fatalf("expected 0 stats after concurrent unwatch, got %d", len(stats))
	}
}

func TestWatch_ConcurrentNotify(t *testing.T) {
	m := NewManager()
	const watchers = 10
	const notifies = 50
	channels := make([]chan []byte, watchers)

	for i := 0; i < watchers; i++ {
		channels[i] = make(chan []byte, notifies*2)
		m.Watch(&Watcher{
			ConnID:     uint64(i),
			Pattern:    "shared",
			WriteCh:    channels[i],
			CancelConn: func() {},
		})
	}

	// Concurrently send many notifications
	var wg sync.WaitGroup
	for i := 0; i < notifies; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.Notify("kset", "shared")
		}()
	}
	wg.Wait()

	// Each watcher should have received exactly notifies messages
	for i, ch := range channels {
		got := len(ch)
		if got != notifies {
			t.Errorf("watcher %d: expected %d notifications, got %d", i, notifies, got)
		}
	}
}

func TestWatch_ConcurrentWatchAndNotify(t *testing.T) {
	m := NewManager()
	const N = 50
	var wg sync.WaitGroup

	// Concurrently watch and notify at the same time
	for i := 0; i < N; i++ {
		wg.Add(2)
		go func(id int) {
			defer wg.Done()
			ch := make(chan []byte, 100)
			m.Watch(&Watcher{ConnID: uint64(id), Pattern: "race", WriteCh: ch, CancelConn: func() {}})
		}(i)
		go func() {
			defer wg.Done()
			m.Notify("kset", "race")
		}()
	}
	wg.Wait()
	// No panics or races = pass
}

func TestWatch_ConcurrentUnwatchAll(t *testing.T) {
	m := NewManager()
	const N = 50

	// Set up watchers with both exact and wildcard patterns
	for i := 0; i < N; i++ {
		ch := make(chan []byte, 10)
		m.Watch(&Watcher{ConnID: uint64(i), Pattern: "exact", WriteCh: ch, CancelConn: func() {}})
		m.Watch(&Watcher{ConnID: uint64(i), Pattern: "wild.*", WriteCh: ch, CancelConn: func() {}})
	}

	// Concurrently UnwatchAll for every connection
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			m.UnwatchAll(uint64(id))
		}(i)
	}
	wg.Wait()

	stats := m.Stats()
	if len(stats) != 0 {
		t.Fatalf("expected 0 stats after concurrent unwatch all, got %d", len(stats))
	}
}

func TestWatch_ConcurrentStats(t *testing.T) {
	m := NewManager()
	ch := make(chan []byte, 100)
	m.Watch(&Watcher{ConnID: 1, Pattern: "k1", WriteCh: ch, CancelConn: func() {}})
	m.Watch(&Watcher{ConnID: 2, Pattern: "k2.*", WriteCh: ch, CancelConn: func() {}})

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = m.Stats()
		}()
	}
	wg.Wait()
	// No panics or races = pass
}
