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

func (f *fakeApply) ClearAll() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.holders = make(map[string]Holder)
	f.enqueued = make(map[string]Enqueued)
}

// fakeSnapshotter returns a fixed snapshot for tests of the catch-up path.
type fakeSnapshotter struct {
	entries []SnapshotEntry
}

func (f fakeSnapshotter) Snapshot() []SnapshotEntry { return f.entries }

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

	// Primary: dials secondary. Empty snapshot since we'll capture
	// mutations directly during the test.
	pri := NewReplicator(Config{
		Role:        RolePrimary,
		NodeID:      "pri",
		PeerAddr:    secAddr,
		Snapshotter: fakeSnapshotter{},
		Log:         quietLog(t),
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

// TestReplicator_SnapshotOnReconnect verifies the catch-up flow: a
// secondary that connects to a primary with non-trivial state ends up
// with that state mirrored locally after the snapshot exchange.
func TestReplicator_SnapshotOnReconnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	secAddr := pickFreeAddr(t)
	apply := newFakeApply()

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

	// Primary has pre-existing state from before secondary connected.
	preExisting := []SnapshotEntry{
		{
			Key:   "lock:k1",
			Limit: 1,
			Holders: []SnapshotHolder{
				{Token: "tok-pre", ConnID: 1, LeaseExpiresUnixNS: time.Now().Add(time.Minute).UnixNano()},
			},
		},
		{
			Key:   "sem:s1",
			Limit: 3,
			Holders: []SnapshotHolder{
				{Token: "sem-tok-1", ConnID: 2, LeaseExpiresUnixNS: time.Now().Add(time.Minute).UnixNano()},
				{Token: "sem-tok-2", ConnID: 3, LeaseExpiresUnixNS: time.Now().Add(time.Minute).UnixNano()},
			},
		},
	}

	pri := NewReplicator(Config{
		Role:        RolePrimary,
		NodeID:      "pri",
		PeerAddr:    secAddr,
		Snapshotter: fakeSnapshotter{entries: preExisting},
		Log:         quietLog(t),
	})
	if err := pri.Start(ctx); err != nil {
		t.Fatalf("pri start: %v", err)
	}
	defer pri.Stop()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && apply.holderCount() < 3 {
		time.Sleep(20 * time.Millisecond)
	}
	if got := apply.holderCount(); got != 3 {
		t.Fatalf("post-snapshot holder count: got %d want 3", got)
	}

	// Active state should be reached after snapshot complete.
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) && sec.State() != StateActive {
		time.Sleep(20 * time.Millisecond)
	}
	if sec.State() != StateActive {
		t.Fatalf("secondary state after snapshot: got %s want active", sec.State())
	}
}

// TestReplicator_PrimarySelfPromoteOnPeerLoss verifies that the
// primary transitions to Solo (with bumped epoch) after max-pause-ms
// elapses with no peer contact, and that AwaitAcked returns nil
// (treating Solo as "OK to proceed") in that state.
func TestReplicator_PrimarySelfPromoteOnPeerLoss(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pri := NewReplicator(Config{
		Role:        RolePrimary,
		NodeID:      "pri",
		PeerAddr:    "127.0.0.1:1", // refused immediately
		MaxPause:    150 * time.Millisecond,
		Snapshotter: fakeSnapshotter{},
		Log:         quietLog(t),
	})
	startEpoch := pri.Epoch()
	if err := pri.Start(ctx); err != nil {
		t.Fatalf("pri start: %v", err)
	}
	defer pri.Stop()

	// Capture a mutation while peer unreachable.
	seq := pri.Capture(Mutation{Kind: OpHolderAdd, Key: "lock:k", Token: "t"})

	// Should transition to Solo within ~MaxPause + a session-establish
	// delay. Generous deadline to avoid flake.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && pri.State() != StateSolo {
		time.Sleep(20 * time.Millisecond)
	}
	if pri.State() != StateSolo {
		t.Fatalf("primary state: got %s want solo", pri.State())
	}
	if pri.Epoch() <= startEpoch {
		t.Fatalf("epoch did not advance: start=%d now=%d", startEpoch, pri.Epoch())
	}

	// AwaitAcked should return nil (Solo treated as proceed-OK).
	awaitCtx, cancelAwait := context.WithTimeout(ctx, time.Second)
	defer cancelAwait()
	if err := pri.AwaitAcked(awaitCtx, seq); err != nil {
		t.Fatalf("AwaitAcked in Solo: got %v want nil", err)
	}
}

// TestReplicator_PromoteSecondary verifies the operator-driven
// failover: a secondary that has lost its primary can be promoted to
// primary, after which it accepts client mutations and runs at a
// bumped epoch.
func TestReplicator_PromoteSecondary(t *testing.T) {
	apply := newFakeApply()
	sec := NewReplicator(Config{
		Role:       RoleSecondary,
		NodeID:     "sec",
		ListenAddr: pickFreeAddr(t),
		Apply:      apply,
		Log:        quietLog(t),
	})
	if !sec.ShouldRefuseMutations() {
		t.Fatal("secondary should refuse before promote")
	}
	startEpoch := sec.Epoch()
	if err := sec.Promote(); err != nil {
		t.Fatalf("promote: %v", err)
	}
	if sec.ShouldRefuseMutations() {
		t.Fatal("promoted node should not refuse mutations")
	}
	if !sec.IsPrimary() {
		t.Fatal("promoted node should report IsPrimary")
	}
	if sec.Epoch() <= startEpoch {
		t.Fatalf("epoch did not advance: start=%d now=%d", startEpoch, sec.Epoch())
	}
	// Idempotency: a second Promote on a primary returns an error.
	if err := sec.Promote(); err == nil {
		t.Fatal("second Promote should error")
	}
}

// TestReplicator_PromoteAndRejoin verifies the one-step
// failover-and-reattach path: a secondary is promoted to primary AND
// reconfigured to dial a fresh secondary, all in one call. The new
// primary should be able to replicate to the new secondary.
func TestReplicator_PromoteAndRejoin(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Original secondary (about to be promoted).
	origSecAddr := pickFreeAddr(t)
	origApply := newFakeApply()
	orig := NewReplicator(Config{
		Role:        RoleSecondary,
		NodeID:      "orig",
		ListenAddr:  origSecAddr,
		Apply:       origApply,
		Snapshotter: fakeSnapshotter{},
		Log:         quietLog(t),
	})
	if err := orig.Start(ctx); err != nil {
		t.Fatalf("orig start: %v", err)
	}
	defer orig.Stop()

	// Bring up the brand-new secondary that will receive the
	// promoted node's replication stream.
	freshSecAddr := pickFreeAddr(t)
	freshApply := newFakeApply()
	fresh := NewReplicator(Config{
		Role:        RoleSecondary,
		NodeID:      "fresh",
		ListenAddr:  freshSecAddr,
		Apply:       freshApply,
		Snapshotter: fakeSnapshotter{},
		Log:         quietLog(t),
	})
	if err := fresh.Start(ctx); err != nil {
		t.Fatalf("fresh start: %v", err)
	}
	defer fresh.Stop()

	// Promote the original secondary AND tell it to peer with the fresh one.
	if err := orig.PromoteAndRejoin(freshSecAddr); err != nil {
		t.Fatalf("PromoteAndRejoin: %v", err)
	}

	// The promoted node should now be a primary; the fresh secondary
	// should reach Active after the snapshot exchange.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && (orig.State() != StateActive || fresh.State() != StateActive) {
		time.Sleep(20 * time.Millisecond)
	}
	if !orig.IsPrimary() {
		t.Fatal("orig should be primary after PromoteAndRejoin")
	}
	if orig.State() != StateActive {
		t.Fatalf("orig state: got %s want active", orig.State())
	}
	if fresh.State() != StateActive {
		t.Fatalf("fresh state: got %s want active", fresh.State())
	}

	// A mutation captured on the new primary should propagate to the fresh secondary.
	expires := time.Now().Add(30 * time.Second).UnixNano()
	seq := orig.Capture(Mutation{
		Kind: OpHolderAdd, Key: "lock:rejoin", Token: "tok-rejoin",
		ConnID: 7, Limit: 1, LeaseExpiresUnixNS: expires,
	})
	awaitCtx, cancelAwait := context.WithTimeout(ctx, 2*time.Second)
	defer cancelAwait()
	if err := orig.AwaitAcked(awaitCtx, seq); err != nil {
		t.Fatalf("await ack: %v", err)
	}
	if got := freshApply.holderCount(); got != 1 {
		t.Fatalf("fresh secondary holder count: got %d want 1", got)
	}
}

// TestWitness_AutoPromoteOnPeerLoss is the headline witness test:
//   1. Witness daemon starts.
//   2. Primary + secondary connect to the witness, peer with each other.
//   3. Primary "dies" (we Stop it).
//   4. Secondary's peer link drops; secondary consults witness.
//   5. Witness, after WitnessLivenessThreshold without primary heartbeats,
//      reports primary not alive.
//   6. Secondary auto-promotes (without operator action).
func TestWitness_AutoPromoteOnPeerLoss(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	witnessAddr := pickFreeAddr(t)
	ws := NewWitnessServer(quietLog(t))
	if err := ws.Start(ctx, witnessAddr, nil); err != nil {
		t.Fatalf("witness start: %v", err)
	}
	defer ws.Stop()

	secAddr := pickFreeAddr(t)
	apply := newFakeApply()
	sec := NewReplicator(Config{
		Role:        RoleSecondary,
		NodeID:      "sec-node",
		ListenAddr:  secAddr,
		Apply:       apply,
		Snapshotter: fakeSnapshotter{},
		WitnessAddr: witnessAddr,
		Log:         quietLog(t),
	})
	if err := sec.Start(ctx); err != nil {
		t.Fatalf("sec start: %v", err)
	}
	defer sec.Stop()

	pri := NewReplicator(Config{
		Role:        RolePrimary,
		NodeID:      "pri-node",
		PeerAddr:    secAddr,
		Snapshotter: fakeSnapshotter{},
		WitnessAddr: witnessAddr,
		Log:         quietLog(t),
	})
	if err := pri.Start(ctx); err != nil {
		t.Fatalf("pri start: %v", err)
	}

	// Wait for primary↔secondary peering to come up.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && (pri.State() != StateActive || sec.State() != StateActive) {
		time.Sleep(20 * time.Millisecond)
	}
	if pri.State() != StateActive || sec.State() != StateActive {
		t.Fatalf("peer link not active: pri=%s sec=%s", pri.State(), sec.State())
	}

	// Kill the primary. The secondary's peer link will drop; the
	// witness will (after WitnessLivenessThreshold) stop seeing
	// primary heartbeats; the secondary will auto-promote.
	pri.Stop()

	// Allow up to ~2× WitnessLivenessThreshold for the auto-promote
	// to fire. (The secondary polls the witness every 200ms during
	// its tryWitnessAutoPromote window.)
	deadline = time.Now().Add(WitnessLivenessThreshold + 4*time.Second)
	for time.Now().Before(deadline) && !sec.IsPrimary() {
		time.Sleep(50 * time.Millisecond)
	}
	if !sec.IsPrimary() {
		t.Fatalf("secondary did not auto-promote; state=%s", sec.State())
	}
	if sec.Epoch() == 0 {
		t.Fatalf("epoch did not advance after auto-promote: %d", sec.Epoch())
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
