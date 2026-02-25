package watch

import (
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
