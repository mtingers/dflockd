package replication

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

// fakeApply records calls to the Apply interface so tests can assert
// what state the secondary side received.
type fakeApply struct {
	mu       sync.Mutex
	holders  map[string]Holder // token → holder fields
	enqueued map[string]Enqueued
}

func newFakeApply() *fakeApply {
	return &fakeApply{
		holders:  make(map[string]Holder),
		enqueued: make(map[string]Enqueued),
	}
}

func (f *fakeApply) ApplyReplicatedHolderAdd(key string, _ int, token string, connID uint64, leaseExpires time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.holders[key+"|"+token] = Holder{Token: token, ConnID: connID, LeaseExpiresUnixNS: leaseExpires.UnixNano()}
}
func (f *fakeApply) ApplyReplicatedHolderRemove(key string, token string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.holders, key+"|"+token)
}
func (f *fakeApply) ApplyReplicatedHolderRenew(key string, token string, leaseExpires time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if h, ok := f.holders[key+"|"+token]; ok {
		h.LeaseExpiresUnixNS = leaseExpires.UnixNano()
		f.holders[key+"|"+token] = h
	}
}
func (f *fakeApply) ApplyReplicatedEnqueuedAdd(key string, connID uint64, token string, leaseTTL time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.enqueued[key+"|"+fmt.Sprint(connID)] = Enqueued{ConnID: connID, Token: token, LeaseTTLNS: int64(leaseTTL)}
}
func (f *fakeApply) ApplyReplicatedEnqueuedRemove(key string, connID uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.enqueued, key+"|"+fmt.Sprint(connID))
}

func (f *fakeApply) holderCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.holders)
}

// pickFreeAddr returns an immediately-available 127.0.0.1 address.
func pickFreeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// quietLog returns a logger that drops debug output but surfaces warnings.
func quietLog(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

func TestReplicator_FrameRoundTrip(t *testing.T) {
	// Smoke test: encode and decode every frame type.
	cases := []*Frame{
		{Type: FrameHello, Hello: &Hello{Role: RolePrimary, Epoch: 1, ProtoVer: ProtoVersion, NodeID: "a"}},
		{Type: FrameHeartbeat, Heartbeat: &Heartbeat{Epoch: 1, Now: time.Now().UnixNano()}},
		{Type: FrameOp, Op: &Op{Seq: 7, Epoch: 1, Kind: OpHolderAdd, Key: "lock:k", Token: "t", ConnID: 9, Limit: 1}},
		{Type: FrameOpAck, OpAck: &OpAck{Seq: 7, Epoch: 1}},
		{Type: FrameSnapshotEnd, SnapshotEnd: &SnapshotEnd{Epoch: 1, LastSeq: 99}},
	}
	for _, c := range cases {
		var buf bytesPipe
		if err := WriteFrame(&buf, c); err != nil {
			t.Fatalf("write %s: %v", c.Type, err)
		}
		got, err := ReadFrame(&buf)
		if err != nil {
			t.Fatalf("read %s: %v", c.Type, err)
		}
		if got.Type != c.Type {
			t.Fatalf("type: got %s want %s", got.Type, c.Type)
		}
	}
}

// TestReplicator_PrimarySecondaryEndToEnd brings up two in-process
// replicators, attaches a fake Apply to the secondary, and verifies
// that mutations Captured on the primary reach the secondary.
func TestReplicator_PrimarySecondaryEndToEnd(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	secAddr := pickFreeAddr(t)
	apply := newFakeApply()

	// Secondary: starts listener.
	sec := NewReplicator(Config{
		Role:       RoleSecondary,
		NodeID:     "sec",
		ListenAddr: secAddr,
		Apply:      apply,
		Log:        quietLog(t),
	})
	if err := sec.Start(ctx); err != nil {
		t.Fatalf("sec start: %v", err)
	}
	defer sec.Stop()

	// Primary: dials secondary.
	pri := NewReplicator(Config{
		Role:     RolePrimary,
		NodeID:   "pri",
		PeerAddr: secAddr,
		Log:      quietLog(t),
	})
	if err := pri.Start(ctx); err != nil {
		t.Fatalf("pri start: %v", err)
	}
	defer pri.Stop()

	// Wait for primary to reach Active.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && pri.State() != StateActive {
		time.Sleep(20 * time.Millisecond)
	}
	if pri.State() != StateActive {
		t.Fatalf("primary state: got %s want active", pri.State())
	}

	// Capture a holder add and wait for ack.
	expires := time.Now().Add(30 * time.Second).UnixNano()
	seq := pri.Capture(Mutation{
		Kind: OpHolderAdd, Key: "lock:test", Token: "tok-1",
		ConnID: 42, Limit: 1, LeaseExpiresUnixNS: expires,
	})
	awaitCtx, cancelAwait := context.WithTimeout(ctx, 2*time.Second)
	defer cancelAwait()
	if err := pri.AwaitAcked(awaitCtx, seq); err != nil {
		t.Fatalf("await ack: %v", err)
	}

	if got := apply.holderCount(); got != 1 {
		t.Fatalf("secondary holder count: got %d want 1", got)
	}
}

// TestReplicator_RefuseMutationsOnSecondary verifies that the
// ShouldRefuseMutations gate is consistent with role.
func TestReplicator_RefuseMutationsOnSecondary(t *testing.T) {
	apply := newFakeApply()
	sec := NewReplicator(Config{
		Role:       RoleSecondary,
		NodeID:     "sec",
		ListenAddr: pickFreeAddr(t),
		Apply:      apply,
		Log:        quietLog(t),
	})
	if !sec.ShouldRefuseMutations() {
		t.Fatal("secondary should always refuse mutations")
	}
	if sec.IsPrimary() {
		t.Fatal("secondary should not report IsPrimary")
	}

	pri := NewReplicator(Config{
		Role:     RolePrimary,
		NodeID:   "pri",
		PeerAddr: "127.0.0.1:0", // never reachable
		Log:      quietLog(t),
	})
	if pri.ShouldRefuseMutations() {
		t.Fatal("primary in init should not refuse mutations")
	}
	if !pri.IsPrimary() {
		t.Fatal("primary should report IsPrimary")
	}
}

// bytesPipe is a tiny in-memory pipe for Frame round-trip tests.
type bytesPipe struct {
	mu  sync.Mutex
	buf []byte
}

func (b *bytesPipe) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	return len(p), nil
}

func (b *bytesPipe) Read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.buf) == 0 {
		return 0, nil
	}
	n := copy(p, b.buf)
	b.buf = b.buf[n:]
	return n, nil
}
