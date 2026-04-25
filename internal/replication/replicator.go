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
// *lock.LockManager (via the ApplyReplicated* methods); declared as
// an interface here so the replication package does not import lock
// (avoiding an import cycle).
type Apply interface {
	ApplyReplicatedHolderAdd(key string, limit int, token string, connID uint64, leaseExpires time.Time)
	ApplyReplicatedHolderRemove(key string, token string)
	ApplyReplicatedHolderRenew(key string, token string, leaseExpires time.Time)
	ApplyReplicatedEnqueuedAdd(key string, connID uint64, token string, leaseTTL time.Duration)
	ApplyReplicatedEnqueuedRemove(key string, connID uint64)
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
	Apply       Apply
	Log         *slog.Logger
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

	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}
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
		done:     make(chan struct{}),
	}
	return r
}

// Start spawns the replicator's worker goroutines. Returns nil if the
// replicator started cleanly. On the secondary side it begins
// listening on cfg.ListenAddr immediately; on the primary side it
// begins dialling cfg.PeerAddr in a loop.
func (r *Replicator) Start(ctx context.Context) error {
	switch r.cfg.Role {
	case RolePrimary:
		go r.runPrimary(ctx)
	case RoleSecondary:
		// Secondary owns its own listener; failure to bind is fatal.
		lis, err := net.Listen("tcp", r.cfg.ListenAddr)
		if err != nil {
			return fmt.Errorf("listen %s: %w", r.cfg.ListenAddr, err)
		}
		if r.cfg.TLSConfig != nil {
			lis = tls.NewListener(lis, r.cfg.TLSConfig)
		}
		r.mu.Lock()
		r.listener = lis
		r.mu.Unlock()
		go r.runSecondary(ctx, lis)
	default:
		return fmt.Errorf("unsupported role %q", r.cfg.Role)
	}
	return nil
}

// Stop terminates the replicator cleanly.
func (r *Replicator) Stop() {
	r.stopOnce.Do(func() { close(r.stop) })
	r.mu.Lock()
	if r.listener != nil {
		_ = r.listener.Close()
	}
	if r.peer != nil {
		r.peer.Close(nil)
	}
	r.mu.Unlock()
	<-r.done
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

// AwaitAcked blocks until the peer has acked at least seq, or until
// ctx is cancelled / the replicator stops / the role transitions to
// Solo (in which case the primary now has authority on its own and
// post-pause writes don't require peer ack).
//
// Returns nil on success. Returns ctx.Err() on context cancellation.
// Returns ErrSolo if the primary self-promoted past the pause window
// (the caller can interpret this as "your write is durable on me, but
// not on the peer — I'm authoritative now anyway").
func (r *Replicator) AwaitAcked(ctx context.Context, seq uint64) error {
	if seq == 0 {
		return nil
	}
	for {
		if r.ackedSeq.Load() >= seq {
			return nil
		}
		st := r.State()
		if st == StateSolo {
			return ErrSolo
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.stop:
			return ErrStopped
		case <-time.After(20 * time.Millisecond):
			// Re-check on wakeup. (A condvar would be tighter but
			// requires careful integration with ctx cancellation;
			// short polling is simpler and the latency cost is
			// bounded.)
		}
	}
}

// ErrSolo is returned by AwaitAcked when the primary self-promoted
// past the pause window. The mutation is in the primary's local
// state; further peer ack is not coming until the peer reconnects
// and catches up.
var ErrSolo = errors.New("replication: primary in solo mode")

// ErrStopped is returned by AwaitAcked when the replicator is shutting down.
var ErrStopped = errors.New("replication: stopped")

// ---------------------------------------------------------------------------
// Primary loop
// ---------------------------------------------------------------------------

// runPrimary owns the primary-side state machine: dial the peer, send
// hello, drain outbound ops, watch heartbeats, transition to paused
// → solo on peer loss.
func (r *Replicator) runPrimary(ctx context.Context) {
	defer close(r.done)
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
			// TODO(replication): emit a full snapshot. For v1 we just
			// log; the secondary will operate from incremental ops only.
			r.log.Warn("replication: snapshot req received (v1 stub)")
			end := &Frame{Type: FrameSnapshotEnd, SnapshotEnd: &SnapshotEnd{Epoch: r.Epoch(), LastSeq: r.ackedSeq.Load()}}
			return pc.WriteFrame(end)
		default:
			r.log.Debug("replication: unexpected frame on primary reader", "type", f.Type)
			return nil
		}
	})
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
	defer close(r.done)
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
	r.setState(StateActive)
	r.attachPeer(pc)
	defer func() {
		r.detachPeer(pc)
		// Peer disconnect on the secondary side → secondary can no
		// longer mirror, so it enters FAILED. It refuses client traffic.
		// Manual operator action (or witness, in phase 3) can promote
		// it to primary.
		r.setState(StateFailed)
	}()

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
			// TODO(replication): apply snapshot part.
			r.log.Warn("replication: snapshot part received (v1 stub)")
			return nil
		case FrameSnapshotEnd:
			return nil
		default:
			r.log.Debug("replication: unexpected frame on secondary reader", "type", f.Type)
			return nil
		}
	})
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
