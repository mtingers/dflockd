package replication

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// State is the replicator's current high-level mode. Transitions are
// driven by peer connectivity, max-pause-ms, and (for failover-class
// transitions) operator action / witness votes.
type State int

const (
	StateInit       State = iota // before any peer contact
	StateSyncing                 // catch-up in progress (snapshot apply)
	StateActive                  // peer reachable, sync replication operating normally
	StatePaused                  // peer unreachable; primary will self-promote on max-pause-ms; secondary will FAILED
	StateSolo                    // primary self-promoted; serving alone at higher epoch
	StateFailed                  // secondary in failure mode; refuses client traffic
)

func (s State) String() string {
	switch s {
	case StateInit:
		return "init"
	case StateSyncing:
		return "syncing"
	case StateActive:
		return "active"
	case StatePaused:
		return "paused"
	case StateSolo:
		return "solo"
	case StateFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// Apply is the lock-manager-side callback. The secondary uses these
// methods to install state arriving from the primary. Implemented by
// *lock.LockManager; declared as an interface here so the replication
// package does not import lock (avoiding an import cycle).
type Apply interface {
	ApplyReplicatedHolderAdd(key string, limit int, token string, connID uint64, leaseExpires time.Time)
	ApplyReplicatedHolderRemove(key string, token string)
	ApplyReplicatedHolderRenew(key string, token string, leaseExpires time.Time)
	ApplyReplicatedEnqueuedAdd(key string, connID uint64, token string, leaseTTL time.Duration)
	ApplyReplicatedEnqueuedRemove(key string, connID uint64)
	ClearAll()
}

// Snapshotter is the primary-side state walker. Implemented by
// *lock.LockManager (via Snapshot()). The replicator calls it on
// receipt of a SnapshotReq frame.
type Snapshotter interface {
	Snapshot() []SnapshotEntry
}

// SnapshotEntry mirrors lock.SnapshotEntry. Repeated here as a thin
// type so replication doesn't import lock.
type SnapshotEntry struct {
	Key      string
	Limit    int
	Holders  []SnapshotHolder
	Enqueued []SnapshotEnqueued
}

// SnapshotHolder mirrors lock.SnapshotHolder.
type SnapshotHolder struct {
	Token              string
	ConnID             uint64
	LeaseExpiresUnixNS int64
}

// SnapshotEnqueued mirrors lock.SnapshotEnqueued (note: distinct
// from the wire-frame Enqueued in protocol.go).
type SnapshotEnqueued struct {
	ConnID     uint64
	Token      string
	LeaseTTLNS int64
}

// Config is the wiring the replicator needs at construction time.
// Filled in from cmd/dflockd flags.
type Config struct {
	Role        Role          // primary or secondary
	NodeID      string        // free-form identifier for logs / hello
	PeerAddr    string        // primary: where the secondary will connect; secondary: where to dial
	ListenAddr  string        // secondary listens here for the primary's connection (filled iff Role==secondary)
	TLSConfig   *tls.Config   // optional; same on both sides
	MaxPause    time.Duration // 0 → DefaultMaxPause
	DialTimeout time.Duration

	// Apply is required on the secondary (incoming state). Optional
	// on the primary (only needed if a single LockManager backs both
	// roles, e.g. tests). nil on a pure primary.
	Apply Apply

	// Snapshotter is required on the primary (it serves SnapshotReq).
	// Optional on the secondary. nil on a pure secondary.
	Snapshotter Snapshotter

	// WitnessAddr, when non-empty, enables witness-mediated
	// auto-failover. Both primary and secondary connect to the
	// witness and heartbeat. On peer loss, the secondary asks the
	// witness for liveness; if the witness agrees the primary is
	// gone, the secondary auto-promotes (without operator action).
	WitnessAddr string

	Log *slog.Logger
}

// outboundBufSize bounds the in-flight pre-ack queue. Sized to absorb
// a multi-second peer-loss spike at high mutation rate; if it fills
// we are in trouble (peer is gone and the replication gate hasn't
// caught up). Capture logs loudly and the mutation is dropped — the
// primary will self-promote after max-pause-ms anyway.
const outboundBufSize = 4096

// Replicator is the long-lived coordinator that owns the peer link,
// the outbound op queue, and the state machine. Construct one with
// NewReplicator, call Start to spawn its goroutines, and Stop on
// shutdown. It implements the Hook interface so the lock manager can
// publish mutations directly to it.
//
// Lifecycle: Start() spawns a "loop" appropriate to the configured
// role (primary dial loop, or secondary listener accept loop).
// Promote(newPeer) tears down the current loop and starts a fresh
// primary loop with the supplied peer address (or no loop if
// newPeer=="" — Solo mode). All loops run under a context derived
// from the rootCtx supplied to Start; Stop() cancels it and waits.
type Replicator struct {
	cfg Config
	log *slog.Logger

	mu       sync.Mutex
	state    State
	epoch    uint64
	seq      uint64
	peer     *peerConn
	listener net.Listener
	outbound chan *Op

	// Sync replication: callers waiting for a specific seq to be acked.
	ackedSeq atomic.Uint64

	// Loop lifecycle. loopMu serialises Start / Promote / Stop so
	// concurrent calls don't double-spawn. loopCancel terminates the
	// currently-running loop's context; loopWg waits for it to exit.
	loopMu     sync.Mutex
	rootCtx    context.Context
	loopCancel context.CancelFunc
	loopWg     sync.WaitGroup

	// witness is the optional client to a witness daemon. When set,
	// the secondary's failover path consults it before auto-promoting.
	witness *witnessClient

	stopOnce sync.Once
	stop     chan struct{}
}

// NewReplicator creates a configured replicator without starting it.
func NewReplicator(cfg Config) *Replicator {
	if cfg.MaxPause <= 0 {
		cfg.MaxPause = DefaultMaxPause
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 5 * time.Second
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	r := &Replicator{
		cfg:      cfg,
		log:      cfg.Log,
		state:    StateInit,
		outbound: make(chan *Op, outboundBufSize),
		stop:     make(chan struct{}),
	}
	return r
}

// Start spawns the replicator's worker goroutines for the configured
// role. The supplied ctx is the root context — it bounds the
// replicator's overall lifetime. Loop lifecycles within that root
// (e.g. across Promote calls) use child contexts and are tracked by
// the internal WaitGroup.
func (r *Replicator) Start(ctx context.Context) error {
	r.loopMu.Lock()
	defer r.loopMu.Unlock()
	r.rootCtx = ctx
	r.mu.Lock()
	witnessAddr := r.cfg.WitnessAddr
	tlsCfg := r.cfg.TLSConfig
	role := r.cfg.Role
	nodeID := r.cfg.NodeID
	epoch := r.epoch
	r.mu.Unlock()
	if witnessAddr != "" {
		wc := newWitnessClient(witnessAddr, tlsCfg, role, nodeID, epoch, r.log)
		wc.Start(ctx)
		r.mu.Lock()
		r.witness = wc
		r.mu.Unlock()
	}
	return r.startLoopLocked()
}

// startLoopLocked spawns a fresh loop matching the current cfg.Role.
// Caller MUST hold loopMu. Returns an error if role is unsupported
// or the secondary's listen address is taken.
func (r *Replicator) startLoopLocked() error {
	if r.loopCancel != nil {
		// A loop is already running; don't double-spawn.
		return nil
	}
	parent := r.rootCtx
	if parent == nil {
		// Promote called before Start (e.g. in unit tests that drive
		// the state machine directly). Use Background — Stop will
		// still cancel this loop's child context cleanly.
		parent = context.Background()
	}
	loopCtx, cancel := context.WithCancel(parent)
	// Snapshot mutable cfg fields under r.mu so the race detector
	// sees synchronised reads (Promote writes them under r.mu).
	r.mu.Lock()
	role := r.cfg.Role
	peerAddr := r.cfg.PeerAddr
	listenAddr := r.cfg.ListenAddr
	r.mu.Unlock()
	switch role {
	case RolePrimary:
		if peerAddr == "" {
			// Solo primary: no peer to dial, no loop. The state was
			// already set by Promote (or by the caller before Start).
			cancel()
			return nil
		}
		r.loopCancel = cancel
		r.loopWg.Add(1)
		go func() {
			defer r.loopWg.Done()
			r.runPrimary(loopCtx)
		}()
	case RoleSecondary:
		lis, err := net.Listen("tcp", listenAddr)
		if err != nil {
			cancel()
			return fmt.Errorf("listen %s: %w", listenAddr, err)
		}
		if r.cfg.TLSConfig != nil {
			lis = tls.NewListener(lis, r.cfg.TLSConfig)
		}
		r.mu.Lock()
		r.listener = lis
		r.mu.Unlock()
		r.loopCancel = cancel
		r.loopWg.Add(1)
		go func() {
			defer r.loopWg.Done()
			r.runSecondary(loopCtx, lis)
		}()
	default:
		cancel()
		return fmt.Errorf("unsupported role %q", r.cfg.Role)
	}
	return nil
}

// stopLoopLocked tears down the currently-running loop. Caller MUST
// hold loopMu. Releases loopMu briefly to let the loop drain, then
// re-acquires before returning so callers see consistent state.
// Internally synchronous: returns only after the loop's goroutines
// have exited.
func (r *Replicator) stopLoopLocked() {
	if r.loopCancel != nil {
		r.loopCancel()
		r.loopCancel = nil
	}
	r.mu.Lock()
	if r.listener != nil {
		_ = r.listener.Close()
		r.listener = nil
	}
	if r.peer != nil {
		r.peer.Close(nil)
		r.peer = nil
	}
	r.mu.Unlock()
	// Releasing loopMu while waiting prevents a deadlock if the loop
	// itself acquires loopMu indirectly (it shouldn't, but defensive).
	r.loopMu.Unlock()
	r.loopWg.Wait()
	r.loopMu.Lock()
}

// Stop terminates the replicator cleanly. Idempotent.
func (r *Replicator) Stop() {
	r.stopOnce.Do(func() { close(r.stop) })
	r.loopMu.Lock()
	r.stopLoopLocked()
	r.mu.Lock()
	w := r.witness
	r.witness = nil
	r.mu.Unlock()
	if w != nil {
		w.Stop()
	}
	r.loopMu.Unlock()
}

// State returns the current high-level state. Primarily used by
// stats / metrics / tests.
func (r *Replicator) State() State {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state
}

// Epoch implements Hook.Epoch.
func (r *Replicator) Epoch() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.epoch
}

// Capture implements Hook.Capture. Called under the lock manager's
// shard lock. Must be O(1) — assign a seq, push onto the outbound
// channel, and return. The sender goroutine drains the channel and
// writes to the peer; the request handler waits on AwaitAcked.
func (r *Replicator) Capture(m Mutation) uint64 {
	r.mu.Lock()
	r.seq++
	op := &Op{
		Seq:                r.seq,
		Epoch:              r.epoch,
		Kind:               m.Kind,
		Key:                m.Key,
		Token:              m.Token,
		ConnID:             m.ConnID,
		Limit:              m.Limit,
		LeaseExpiresUnixNS: m.LeaseExpiresUnixNS,
		LeaseTTLNS:         m.LeaseTTLNS,
	}
	seq := r.seq
	r.mu.Unlock()
	select {
	case r.outbound <- op:
	default:
		// Channel full means peer has been gone for a while and the
		// pause/promote machinery is about to fire. Drop the op (it's
		// already applied locally) and log. The primary will go SOLO
		// shortly; the secondary will need a snapshot to recover.
		r.log.Error("replication: outbound channel full, mutation dropped",
			"seq", seq, "kind", m.Kind, "key", m.Key)
	}
	return seq
}

// AwaitAcked blocks until the peer has acked at least seq, OR the
// primary self-promotes to Solo (in which case the caller is free to
// proceed because the primary is now authoritative alone).
//
// The two "OK to proceed" cases collapse to a nil return so callers
// can stay schema-light: nil = response to client, non-nil = surface
// an error. Returns ctx.Err() on cancellation, ErrStopped on shutdown,
// ErrLostPeer if the peer link broke during ACTIVE mode (the caller
// should reject the mutation with error_paused — the operation has
// already been applied locally but is not durable across failover).
func (r *Replicator) AwaitAcked(ctx context.Context, seq uint64) error {
	if seq == 0 {
		return nil
	}
	for {
		if r.ackedSeq.Load() >= seq {
			return nil
		}
		st := r.State()
		switch st {
		case StateSolo:
			// Primary is now authoritative alone. Local apply is the
			// system of record. Treat as "acked".
			return nil
		case StateFailed:
			// We're a secondary that lost the primary. Should never
			// reach here for client-side mutations (gate refuses).
			return ErrLostPeer
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.stop:
			return ErrStopped
		case <-time.After(5 * time.Millisecond):
			// Short polling. A condvar would be tighter but couples
			// awkwardly with ctx cancellation. 5ms keeps median sync
			// latency well under 10ms in healthy conditions.
		}
	}
}

// Promote transitions a secondary into a primary at a bumped epoch.
// Invoked by the operator (via SIGUSR1 or an admin endpoint) after
// the original primary has been confirmed dead.
//
// Idempotent: returns "already primary" error if called on a node
// already in primary role. Operators can call PromoteAndRejoin
// instead if they want to point at a fresh secondary.
//
// Safety:
//   - Bumping the epoch fences any returning original-primary; if it
//     comes back, it's at the old epoch and will fail to peer with
//     any secondary that joined the new primary.
//   - The peer link (if any) is closed.
//   - State transitions to Solo. Mutation gate opens.
func (r *Replicator) Promote() error {
	return r.promoteAndConfigurePeer("")
}

// PromoteAndRejoin is Promote followed by reconfiguring this node as
// a primary that dials newPeer as the fresh secondary. Lets an
// operator promote a former secondary AND immediately re-establish
// replication against a brand-new secondary process.
//
// newPeer must be a "host:port" address reachable from this node.
// If empty, behaves identically to Promote (Solo).
func (r *Replicator) PromoteAndRejoin(newPeer string) error {
	return r.promoteAndConfigurePeer(newPeer)
}

func (r *Replicator) promoteAndConfigurePeer(newPeer string) error {
	r.loopMu.Lock()
	defer r.loopMu.Unlock()

	r.mu.Lock()
	if r.cfg.Role == RolePrimary {
		r.mu.Unlock()
		return errors.New("replication: already primary")
	}
	r.mu.Unlock()

	// Tear down the secondary loop. This blocks until its goroutines
	// have exited.
	r.stopLoopLocked()

	// Flip role and bump epoch under the state lock.
	r.mu.Lock()
	r.epoch++
	r.cfg.Role = RolePrimary
	r.cfg.PeerAddr = newPeer
	r.cfg.ListenAddr = ""
	r.state = StateSolo
	newEpoch := r.epoch
	r.mu.Unlock()

	if newPeer != "" {
		r.log.Warn("replication: PROMOTED to primary, dialling new secondary",
			"new_epoch", newEpoch, "new_peer", newPeer)
	} else {
		r.log.Warn("replication: PROMOTED to primary (Solo, no peer)",
			"new_epoch", newEpoch)
	}

	// Start a fresh primary loop (or no loop, if newPeer is empty).
	return r.startLoopLocked()
}

// HighWaterMark returns the largest seq Capture has assigned so far.
// The server handler reads this AFTER calling into the lock manager
// to learn the high-water of mutations its operation produced, then
// waits for that seq to be acked before responding to the client.
//
// Reading is racy with concurrent Captures from other goroutines, but
// safely conservative: a higher hwm just means we wait for unrelated
// mutations to ack too — they will, since they're being replicated
// anyway, and the wait time is bounded by the slowest concurrent op.
func (r *Replicator) HighWaterMark() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.seq
}

// ErrLostPeer is returned by AwaitAcked when the peer link broke
// before the supplied seq was acked and the primary did not (yet)
// self-promote. The caller's mutation has been applied locally but is
// not durable across failover.
var ErrLostPeer = errors.New("replication: peer lost before ack")

// ErrStopped is returned by AwaitAcked when the replicator is shutting down.
var ErrStopped = errors.New("replication: stopped")

// ---------------------------------------------------------------------------
// Primary loop
// ---------------------------------------------------------------------------

// runPrimary owns the primary-side state machine: dial the peer, send
// hello, drain outbound ops, watch heartbeats, transition to paused
// → solo on peer loss.
func (r *Replicator) runPrimary(ctx context.Context) {
	for {
		select {
		case <-r.stop:
			return
		case <-ctx.Done():
			return
		default:
		}
		r.runPrimarySession(ctx)
		// Brief backoff before reconnect attempt
		select {
		case <-r.stop:
			return
		case <-ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (r *Replicator) runPrimarySession(ctx context.Context) {
	r.log.Info("replication: primary dialling peer", "peer", r.cfg.PeerAddr)
	pc, err := dialPeer(ctx, r.cfg.PeerAddr, r.cfg.TLSConfig, r.cfg.DialTimeout)
	if err != nil {
		r.log.Warn("replication: dial failed", "err", err)
		r.enterPauseOrSolo(ctx)
		return
	}
	defer pc.Close(nil)

	if err := r.handshakePrimary(pc); err != nil {
		r.log.Warn("replication: handshake failed", "err", err)
		return
	}
	r.setState(StateActive)
	r.attachPeer(pc)
	defer r.detachPeer(pc)

	// Spawn reader, sender, heartbeat. Block on whichever exits first.
	sessCtx, sessCancel := context.WithCancel(ctx)
	defer sessCancel()
	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); r.primaryReader(sessCtx, pc) }()
	go func() { defer wg.Done(); r.primarySender(sessCtx, pc) }()
	go func() { defer wg.Done(); r.heartbeatLoop(sessCtx, pc) }()
	<-pc.Done()
	sessCancel()
	wg.Wait()
	r.setState(StatePaused)
	r.enterPauseOrSolo(ctx)
}

// enterPauseOrSolo waits up to MaxPause for the peer to come back. If
// the timer fires first, the primary self-promotes to Solo at a
// bumped epoch.
func (r *Replicator) enterPauseOrSolo(ctx context.Context) {
	r.setState(StatePaused)
	t := time.NewTimer(r.cfg.MaxPause)
	defer t.Stop()
	select {
	case <-r.stop:
		return
	case <-ctx.Done():
		return
	case <-t.C:
		// Peer did not return in time. Self-promote.
		r.mu.Lock()
		r.epoch++
		r.log.Warn("replication: max-pause-ms exceeded, self-promoting to SOLO",
			"epoch", r.epoch)
		r.state = StateSolo
		r.mu.Unlock()
		// AwaitAcked checks State() each tick and returns ErrSolo
		// when it sees the new state — no broadcast needed.
	}
}

// handshakePrimary sends Hello and waits for the secondary's Hello.
// Validates role and proto version.
func (r *Replicator) handshakePrimary(pc *peerConn) error {
	r.mu.Lock()
	hello := &Frame{Type: FrameHello, Hello: &Hello{
		Role:     RolePrimary,
		Epoch:    r.epoch,
		ProtoVer: ProtoVersion,
		NodeID:   r.cfg.NodeID,
		StartedUnix: time.Now().Unix(),
	}}
	r.mu.Unlock()
	if err := pc.WriteFrame(hello); err != nil {
		return fmt.Errorf("send hello: %w", err)
	}
	resp, err := pc.ReadFrame()
	if err != nil {
		return fmt.Errorf("read peer hello: %w", err)
	}
	if resp.Type != FrameHello || resp.Hello == nil {
		return fmt.Errorf("expected hello, got %s", resp.Type)
	}
	if resp.Hello.Role != RoleSecondary {
		return fmt.Errorf("peer role %q is not secondary", resp.Hello.Role)
	}
	if resp.Hello.ProtoVer != ProtoVersion {
		return fmt.Errorf("proto mismatch: ours %d theirs %d", ProtoVersion, resp.Hello.ProtoVer)
	}
	return nil
}

func (r *Replicator) primaryReader(ctx context.Context, pc *peerConn) {
	drainFramesUntilClose(ctx, pc, func(f *Frame) error {
		switch f.Type {
		case FrameHeartbeat:
			return nil
		case FrameOpAck:
			if f.OpAck != nil && f.OpAck.Seq > r.ackedSeq.Load() {
				r.ackedSeq.Store(f.OpAck.Seq)
			}
			return nil
		case FrameSnapshotReq:
			return r.sendSnapshot(pc)
		default:
			r.log.Debug("replication: unexpected frame on primary reader", "type", f.Type)
			return nil
		}
	})
}

// sendSnapshot serialises the lock manager's current state and pushes
// SnapshotPart frames + SnapshotEnd. Live ops captured during
// snapshot generation will follow naturally on the outbound channel
// because Snapshot() reads under shard locks (so no new ops can be
// captured for a shard while we're reading it). Across shards the
// snapshot is "soft" — but per-key consistency is what matters for
// lock semantics.
func (r *Replicator) sendSnapshot(pc *peerConn) error {
	if r.cfg.Snapshotter == nil {
		r.log.Error("replication: snapshot requested but no Snapshotter configured")
		return pc.WriteFrame(&Frame{
			Type:        FrameSnapshotEnd,
			SnapshotEnd: &SnapshotEnd{Epoch: r.Epoch(), LastSeq: r.ackedSeq.Load()},
		})
	}
	entries := r.cfg.Snapshotter.Snapshot()
	r.mu.Lock()
	snapSeq := r.seq // high-water at snapshot completion
	epoch := r.epoch
	r.mu.Unlock()
	r.log.Info("replication: sending snapshot",
		"entries", len(entries), "snap_seq", snapSeq, "epoch", epoch)
	for _, e := range entries {
		part := &Frame{Type: FrameSnapshotPart, SnapshotPart: &SnapshotPart{
			Epoch:    epoch,
			Key:      e.Key,
			Limit:    e.Limit,
			Holders:  toWireHolders(e.Holders),
			Enqueued: toWireEnqueued(e.Enqueued),
		}}
		if err := pc.WriteFrame(part); err != nil {
			return err
		}
	}
	end := &Frame{Type: FrameSnapshotEnd, SnapshotEnd: &SnapshotEnd{Epoch: epoch, LastSeq: snapSeq}}
	return pc.WriteFrame(end)
}

func toWireHolders(in []SnapshotHolder) []Holder {
	if len(in) == 0 {
		return nil
	}
	out := make([]Holder, len(in))
	for i, h := range in {
		out[i] = Holder{Token: h.Token, ConnID: h.ConnID, LeaseExpiresUnixNS: h.LeaseExpiresUnixNS}
	}
	return out
}

func toWireEnqueued(in []SnapshotEnqueued) []Enqueued {
	if len(in) == 0 {
		return nil
	}
	out := make([]Enqueued, len(in))
	for i, e := range in {
		out[i] = Enqueued{ConnID: e.ConnID, Token: e.Token, LeaseTTLNS: e.LeaseTTLNS}
	}
	return out
}

func (r *Replicator) primarySender(ctx context.Context, pc *peerConn) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-pc.Done():
			return
		case op := <-r.outbound:
			if err := pc.WriteFrame(&Frame{Type: FrameOp, Op: op}); err != nil {
				pc.Close(err)
				return
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Secondary loop
// ---------------------------------------------------------------------------

func (r *Replicator) runSecondary(ctx context.Context, lis net.Listener) {
	err := runPeerListener(ctx, lis, func(pc *peerConn) {
		r.handleSecondarySession(ctx, pc)
	})
	if err != nil {
		r.log.Error("replication: secondary listener error", "err", err)
	}
}

func (r *Replicator) handleSecondarySession(ctx context.Context, pc *peerConn) {
	defer pc.Close(nil)
	if err := r.handshakeSecondary(pc); err != nil {
		r.log.Warn("replication: secondary handshake failed", "err", err)
		return
	}
	r.setState(StateSyncing)
	r.attachPeer(pc)
	defer func() {
		r.detachPeer(pc)
		// Peer disconnect on the secondary side → secondary can no
		// longer mirror, so it enters FAILED. It refuses client
		// traffic. If a witness is configured, kick off auto-failover
		// in a SEPARATE goroutine — Promote() needs this loop to
		// exit (it stops the current loop before starting the new
		// primary loop), and the auto-promote work must outlive this
		// session's defer chain. Otherwise an operator must call
		// Promote / SIGUSR1 / the admin HTTP endpoint to advance.
		r.setState(StateFailed)
		r.mu.Lock()
		hasWitness := r.witness != nil
		r.mu.Unlock()
		if hasWitness {
			go r.tryWitnessAutoPromote(ctx)
		}
	}()

	// Wipe local state and request a fresh snapshot. Stale state
	// from a previous connection epoch is unsafe to keep — the peer
	// may have pruned things we never heard about.
	if r.cfg.Apply != nil {
		r.cfg.Apply.ClearAll()
	}
	if err := pc.WriteFrame(&Frame{
		Type:        FrameSnapshotReq,
		SnapshotReq: &SnapshotReq{Epoch: r.Epoch()},
	}); err != nil {
		r.log.Warn("replication: snapshot request failed", "err", err)
		return
	}

	sessCtx, sessCancel := context.WithCancel(ctx)
	defer sessCancel()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); r.secondaryReader(sessCtx, pc) }()
	go func() { defer wg.Done(); r.heartbeatLoop(sessCtx, pc) }()
	<-pc.Done()
	sessCancel()
	wg.Wait()
}

func (r *Replicator) handshakeSecondary(pc *peerConn) error {
	resp, err := pc.ReadFrame()
	if err != nil {
		return fmt.Errorf("read peer hello: %w", err)
	}
	if resp.Type != FrameHello || resp.Hello == nil {
		return fmt.Errorf("expected hello, got %s", resp.Type)
	}
	if resp.Hello.Role != RolePrimary {
		return fmt.Errorf("peer role %q is not primary", resp.Hello.Role)
	}
	if resp.Hello.ProtoVer != ProtoVersion {
		return fmt.Errorf("proto mismatch: ours %d theirs %d", ProtoVersion, resp.Hello.ProtoVer)
	}
	r.mu.Lock()
	r.epoch = resp.Hello.Epoch
	r.mu.Unlock()
	hello := &Frame{Type: FrameHello, Hello: &Hello{
		Role:     RoleSecondary,
		Epoch:    resp.Hello.Epoch,
		ProtoVer: ProtoVersion,
		NodeID:   r.cfg.NodeID,
		StartedUnix: time.Now().Unix(),
	}}
	return pc.WriteFrame(hello)
}

func (r *Replicator) secondaryReader(ctx context.Context, pc *peerConn) {
	drainFramesUntilClose(ctx, pc, func(f *Frame) error {
		switch f.Type {
		case FrameHeartbeat:
			return nil
		case FrameOp:
			if f.Op == nil {
				return errors.New("op frame with nil payload")
			}
			r.applyOp(f.Op)
			ack := &Frame{Type: FrameOpAck, OpAck: &OpAck{Seq: f.Op.Seq, Epoch: f.Op.Epoch}}
			return pc.WriteFrame(ack)
		case FrameSnapshotPart:
			r.applySnapshotPart(f.SnapshotPart)
			return nil
		case FrameSnapshotEnd:
			r.log.Info("replication: snapshot complete",
				"epoch", f.SnapshotEnd.Epoch, "last_seq", f.SnapshotEnd.LastSeq)
			r.setState(StateActive)
			return nil
		default:
			r.log.Debug("replication: unexpected frame on secondary reader", "type", f.Type)
			return nil
		}
	})
}

// tryWitnessAutoPromote queries the witness; if the witness reports
// the primary is no longer alive, the secondary auto-promotes itself
// (no operator action). Best-effort: if the witness is unreachable
// or disagrees, the secondary stays in StateFailed and the existing
// SIGUSR1 / HTTP-admin paths remain the failover route.
//
// Safety: this function calls Promote() which closes the peer link
// (already closed) and bumps epoch. The witness's record is updated
// via Endorse so any returning original-primary will see a higher
// endorsed epoch on its next witness query.
func (r *Replicator) tryWitnessAutoPromote(ctx context.Context) {
	r.mu.Lock()
	wc := r.witness
	r.mu.Unlock()
	if wc == nil {
		return
	}
	// Give the witness a moment to update its own view (it's bounded
	// by WitnessLivenessThreshold).
	deadline := time.Now().Add(WitnessLivenessThreshold + time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return
		case <-r.stop:
			return
		default:
		}
		st, err := wc.Query()
		if err != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if !st.PrimaryAlive {
			r.log.Warn("witness: primary confirmed dead, auto-promoting",
				"endorsed_epoch", st.EndorsedEpoch)
			if err := r.Promote(); err != nil {
				r.log.Warn("witness auto-promote: Promote failed", "err", err)
				return
			}
			// Inform the witness of the new authority.
			r.mu.Lock()
			wc2 := r.witness
			r.mu.Unlock()
			if wc2 != nil {
				_, _ = wc2.Endorse(r.Epoch())
				wc2.SetEpoch(r.Epoch())
			}
			return
		}
		// Witness still sees primary alive; wait and re-check.
		time.Sleep(200 * time.Millisecond)
	}
	r.log.Warn("witness: gave up waiting for primary-dead confirmation; staying in FAILED")
}

// applySnapshotPart installs one resource's worth of state on the
// secondary. Holders and Enqueued are added via the same Apply
// methods used for live ops — idempotent and order-tolerant.
func (r *Replicator) applySnapshotPart(p *SnapshotPart) {
	if p == nil || r.cfg.Apply == nil {
		return
	}
	for _, h := range p.Holders {
		expires := time.Time{}
		if h.LeaseExpiresUnixNS != 0 {
			expires = time.Unix(0, h.LeaseExpiresUnixNS)
		}
		r.cfg.Apply.ApplyReplicatedHolderAdd(p.Key, p.Limit, h.Token, h.ConnID, expires)
	}
	for _, e := range p.Enqueued {
		r.cfg.Apply.ApplyReplicatedEnqueuedAdd(p.Key, e.ConnID, e.Token, time.Duration(e.LeaseTTLNS))
	}
}

// applyOp installs the mutation on the local lock manager via the
// Apply interface. Idempotent — applying the same op twice has no
// observable effect on the second application.
func (r *Replicator) applyOp(op *Op) {
	if r.cfg.Apply == nil {
		return
	}
	leaseExpires := time.Time{}
	if op.LeaseExpiresUnixNS != 0 {
		leaseExpires = time.Unix(0, op.LeaseExpiresUnixNS)
	}
	switch op.Kind {
	case OpHolderAdd:
		r.cfg.Apply.ApplyReplicatedHolderAdd(op.Key, op.Limit, op.Token, op.ConnID, leaseExpires)
	case OpHolderRemove:
		r.cfg.Apply.ApplyReplicatedHolderRemove(op.Key, op.Token)
	case OpHolderRenew:
		r.cfg.Apply.ApplyReplicatedHolderRenew(op.Key, op.Token, leaseExpires)
	case OpEnqueuedAdd:
		r.cfg.Apply.ApplyReplicatedEnqueuedAdd(op.Key, op.ConnID, op.Token, time.Duration(op.LeaseTTLNS))
	case OpEnqueuedRemove:
		r.cfg.Apply.ApplyReplicatedEnqueuedRemove(op.Key, op.ConnID)
	}
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

func (r *Replicator) heartbeatLoop(ctx context.Context, pc *peerConn) {
	t := time.NewTicker(HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-pc.Done():
			return
		case <-t.C:
			err := pc.WriteFrame(&Frame{Type: FrameHeartbeat, Heartbeat: &Heartbeat{
				Epoch: r.Epoch(),
				Now:   time.Now().UnixNano(),
			}})
			if err != nil {
				pc.Close(err)
				return
			}
		}
	}
}

func (r *Replicator) setState(s State) {
	r.mu.Lock()
	old := r.state
	r.state = s
	r.mu.Unlock()
	if old != s {
		r.log.Info("replication: state change", "from", old, "to", s)
	}
}

func (r *Replicator) attachPeer(pc *peerConn) {
	r.mu.Lock()
	r.peer = pc
	r.mu.Unlock()
}

func (r *Replicator) detachPeer(pc *peerConn) {
	r.mu.Lock()
	if r.peer == pc {
		r.peer = nil
	}
	r.mu.Unlock()
}

// ---------------------------------------------------------------------------
// External role check
// ---------------------------------------------------------------------------

// IsPrimary returns true when the replicator's role is Primary. The
// server uses this to decide whether to accept client mutations.
func (r *Replicator) IsPrimary() bool {
	if r == nil {
		return true // standalone server with no replicator: behave as primary
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cfg.Role == RolePrimary
}

// ShouldRefuseMutations returns true when the server should refuse
// client traffic (the secondary role, or a primary that has lost peer
// contact and not yet self-promoted). Read-only ops can still be served.
func (r *Replicator) ShouldRefuseMutations() bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cfg.Role == RoleSecondary {
		// Secondaries always refuse — even when actively replicating.
		// The only path to serving traffic on a former secondary is
		// operator-driven promotion (out of scope for v1).
		return true
	}
	// Primary refuses only while paused (peer lost but not yet promoted).
	return r.state == StatePaused
}
