package server

import (
	"testing"
)

// TestSetStableRefOnceOnly verifies the per-connection lock: a second
// stable-ref on the same connID returns false (the handler maps that
// to an error response).
func TestSetStableRefOnceOnly(t *testing.T) {
	srv, _ := newTestServer(t)
	if !srv.BindStableRef(7, "alice") {
		t.Fatalf("first SetStableRef: want true (fresh), got false")
	}
	if srv.BindStableRef(7, "mallory") {
		t.Fatalf("second SetStableRef on same connID: want false (locked), got true")
	}
	if got := srv.stableRefFor(7); got != "alice" {
		t.Fatalf("stableRefFor(7) = %q, want alice (second call must not overwrite)", got)
	}
}

// TestEffectiveRefFallsBackToConnID: with no stable ref set, the
// effective ref is the cluster cid (decimal). With a stable ref set,
// it returns that ref regardless of cid.
func TestEffectiveRefFallsBackToConnID(t *testing.T) {
	srv, _ := newTestServer(t)
	if got := srv.effectiveRef(1, 42); got != "42" {
		t.Fatalf("no stable ref: effectiveRef = %q, want %q", got, "42")
	}
	srv.BindStableRef(1, "session-abc")
	if got := srv.effectiveRef(1, 42); got != "session-abc" {
		t.Fatalf("with stable ref: effectiveRef = %q, want %q", got, "session-abc")
	}
}

// TestClearStableRefReleasesSlot: ClearStableRef must drop the entry
// so a future BindStableRef on the same connID succeeds (this matters
// in tests / single-conn-id reuse; in prod connIDs are monotonic).
func TestClearStableRefReleasesSlot(t *testing.T) {
	srv, _ := newTestServer(t)
	_ = srv.BindStableRef(9, "first")
	srv.ClearStableRef(9)
	if got := srv.stableRefFor(9); got != "" {
		t.Fatalf("stableRefFor after clear = %q, want empty", got)
	}
	if !srv.BindStableRef(9, "second") {
		t.Fatalf("re-set after clear: want true, got false")
	}
}
