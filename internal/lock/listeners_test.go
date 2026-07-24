package lock

import (
	"testing"
	"time"

	"github.com/mtingers/dflockd/internal/config"
)

func managerWithCfg(t *testing.T, cfg *config.Config) *LockManager {
	t.Helper()
	lm, err := NewLockManager(cfg, nil)
	if err != nil {
		t.Fatalf("NewLockManager: %v", err)
	}
	t.Cleanup(func() { _ = lm.Close() })
	return lm
}

func listenerCfg() *config.Config {
	return &config.Config{MaxLocks: 100, MaxWaiters: 100, GCMaxIdleTime: time.Hour}
}

// One connection can hold several listeners on the same ref at once —
// a queued Enqueue stashes one for the whole gap until its Wait, while
// any later command on that connection registers its own. Cancelling
// either must leave the other able to receive.
func TestCancelOneListenerLeavesSiblingLive(t *testing.T) {
	lm := managerWithCfg(t, listenerCfg())

	_, cancelA := lm.WatchGrants("ref-1")
	chB, cancelB := lm.WatchGrants("ref-1")
	defer cancelB()
	cancelA()

	lm.RouteGrants([]Grant{{Key: "lock:k", Ref: "ref-1", Token: "tok", LeaseSec: 30}})

	select {
	case g := <-chB:
		if g.Token != "tok" {
			t.Fatalf("token = %q, want %q", g.Token, "tok")
		}
	default:
		t.Fatal("grant dropped: cancelling the first listener unregistered the live one")
	}
}

// Cancel is idempotent and must not disturb a listener registered after
// it for the same ref.
func TestDoubleCancelIsHarmless(t *testing.T) {
	lm := managerWithCfg(t, listenerCfg())

	_, cancelA := lm.WatchGrants("ref-1")
	cancelA()
	cancelA()

	chB, cancelB := lm.WatchGrants("ref-1")
	defer cancelB()
	lm.RouteGrants([]Grant{{Key: "lock:k", Ref: "ref-1", Token: "tok"}})

	select {
	case <-chB:
	default:
		t.Fatal("grant dropped after a repeated cancel of an unrelated listener")
	}
}

// A key-scoped listener only receives grants for its own key, so two
// outstanding operations on one ref can't steal each other's token.
func TestKeyScopedListenerOnlyGetsItsKey(t *testing.T) {
	lm := managerWithCfg(t, listenerCfg())

	chA, cancelA := lm.WatchGrantsFor("ref-1", "lock:a")
	defer cancelA()
	chB, cancelB := lm.WatchGrantsFor("ref-1", "lock:b")
	defer cancelB()

	lm.RouteGrants([]Grant{{Key: "lock:b", Ref: "ref-1", Token: "tok-b"}})

	select {
	case g := <-chA:
		t.Fatalf("listener for lock:a received a lock:b grant: %+v", g)
	default:
	}
	select {
	case g := <-chB:
		if g.Token != "tok-b" {
			t.Fatalf("token = %q, want tok-b", g.Token)
		}
	default:
		t.Fatal("key-scoped listener did not receive its own grant")
	}
}

// An unscoped watcher still receives any grant for its ref (the HTTP
// wait path and single-node callers rely on this).
func TestUnscopedListenerReceivesAnyKey(t *testing.T) {
	lm := managerWithCfg(t, listenerCfg())

	ch, cancel := lm.WatchGrants("ref-1")
	defer cancel()

	lm.RouteGrants([]Grant{{Key: "sem:whatever", Ref: "ref-1", Token: "tok"}})

	select {
	case <-ch:
	default:
		t.Fatal("unscoped listener missed a grant for its ref")
	}
}

// A grant for a ref nobody is watching is dropped, not delivered to
// some other ref's listener.
func TestGrantForUnknownRefIsDropped(t *testing.T) {
	lm := managerWithCfg(t, listenerCfg())

	ch, cancel := lm.WatchGrants("ref-1")
	defer cancel()

	lm.RouteGrants([]Grant{{Key: "lock:k", Ref: "ref-2", Token: "tok"}})

	select {
	case g := <-ch:
		t.Fatalf("ref-1 listener received a ref-2 grant: %+v", g)
	default:
	}
}
