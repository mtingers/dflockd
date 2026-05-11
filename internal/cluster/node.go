package cluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/mtingers/dflockd/internal/lock"
	"github.com/mtingers/dflockd/internal/raft"
)

// Config holds the cluster-time wiring. Embedding raft.Config lets the
// caller tune timers / thresholds; everything else is dflockd-specific.
type Config struct {
	Raft          raft.Config
	Members       map[raft.NodeID]Member // every member's addresses, keyed by node id
	AdvertiseAddr string                 // this node's client-facing host:port (returned to redirected clients)
	// SweepInterval is how often the leader proposes a lease-expiry sweep
	// (and, less often, an idle-resource GC). Zero → defaultSweepInterval.
	SweepInterval time.Duration
}

const (
	defaultSweepInterval = time.Second
	// gcEverySweeps: propose a KindGC every N sweep ticks (idle-resource
	// pruning is far less urgent than reclaiming expired leases).
	gcEverySweeps = 30
)

// Member is one cluster member's pair of addresses: the Raft transport
// address (peer-to-peer consensus traffic) and the client-facing
// address (returned by LeaderClientAddr so a redirected client knows
// where to retry).
type Member struct {
	RaftAddr   string
	ClientAddr string
}

// Validate runs the embedded Raft validation and a few cluster-level
// invariants.
func (c *Config) Validate() error {
	if err := c.Raft.Validate(); err != nil {
		return err
	}
	if len(c.Members) == 0 {
		return fmt.Errorf("cluster: Config.Members is required")
	}
	if _, ok := c.Members[c.Raft.ID]; !ok {
		return fmt.Errorf("cluster: this node's ID %q is not in Members", c.Raft.ID)
	}
	return nil
}

// Node is one cluster member: it owns the raft.Node, the FSM (which
// shells into a shared LockManager), and the public Propose surface.
// Storage and Transport are caller-owned (so tests can share a
// MemNetwork, and production can wire in a FileStorage + TCP transport).
type Node struct {
	cfg       Config
	raft      *raft.Node
	storage   raft.Storage
	transport raft.Transport
	lm        *lock.LockManager
	fsm       *fsm
	log       *slog.Logger

	membersMu sync.Mutex // guards cfg.Members (mutated by AddVoter/RemoveServer, read by LeaderClientAddr)

	sweepStop chan struct{}
	sweepWG   sync.WaitGroup
}

// NewNode wires the raft node + FSM. It does not start any work until
// Start is called.
func NewNode(cfg Config, lm *lock.LockManager, storage raft.Storage, transport raft.Transport, logger *slog.Logger) (*Node, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}
	f := newFSM(lm)
	rn, err := raft.NewNode(cfg.Raft, f, storage, transport, raftConfigFor(cfg), logger)
	if err != nil {
		return nil, fmt.Errorf("cluster: build raft node: %w", err)
	}
	return &Node{
		cfg: cfg, raft: rn, storage: storage, transport: transport, lm: lm, fsm: f,
		log: logger.With("node", cfg.Raft.ID), sweepStop: make(chan struct{}),
	}, nil
}

// raftConfigFor builds the raft.Configuration the cluster.Config maps to.
// raft.Configuration carries the per-member Raft transport address.
func raftConfigFor(cfg Config) raft.Configuration {
	voters := make(map[raft.NodeID]string, len(cfg.Members))
	for id, m := range cfg.Members {
		voters[id] = m.RaftAddr
	}
	return raft.Configuration{Voters: voters}
}

// Start launches the underlying raft node and the leader-driven sweep
// loop (lease expiry + idle GC).
func (n *Node) Start() {
	n.raft.Start()
	n.sweepWG.Add(1)
	go n.sweepLoop()
}

// Close stops the sweep loop (so no more proposes are in flight), then
// stops the raft node and waits for its goroutines to exit. Idempotent.
func (n *Node) Close() error {
	select {
	case <-n.sweepStop:
	default:
		close(n.sweepStop)
	}
	n.sweepWG.Wait()
	return n.raft.Close()
}

// IsLeader reports whether this node currently believes it is the
// cluster leader.
func (n *Node) IsLeader() bool { return n.raft.IsLeader() }

// LeaderID returns the cluster's current leader's node id, or "" if
// unknown.
func (n *Node) LeaderID() raft.NodeID { return n.raft.LeaderID() }

// Status exposes a snapshot of the raft node's state to callers
// (typically the HTTP admin endpoint).
func (n *Node) Status() raft.NodeStatus { return n.raft.Status() }

// LockManager returns the FSM-backing LockManager — useful so the
// server can register grant listeners on it directly.
func (n *Node) LockManager() *lock.LockManager { return n.lm }

// LeaderClientAddr returns the client-facing address of the current
// leader (suitable for an error_not_leader redirect). ok=false if
// there's no known leader, or if the leader's address isn't in Members.
func (n *Node) LeaderClientAddr() (string, bool) {
	id := n.LeaderID()
	if id == "" {
		return "", false
	}
	n.membersMu.Lock()
	m, ok := n.cfg.Members[id]
	n.membersMu.Unlock()
	if !ok {
		return "", false
	}
	return m.ClientAddr, true
}

// ---------------------------------------------------------------------------
// Propose helpers — one per Command kind.
// ---------------------------------------------------------------------------

// Propose submits cmd to the cluster, blocks until it commits + applies,
// and returns the resulting ApplyResult. ErrNotLeader if not leader at
// submission time; ErrLeadershipLost if we lost leadership mid-flight.
func (n *Node) Propose(ctx context.Context, cmd Command) (lock.ApplyResult, error) {
	cmd.NowNanos = time.Now().UnixNano()
	data, err := cmd.Encode()
	if err != nil {
		return lock.ApplyResult{}, err
	}
	fut, err := n.raft.Propose(ctx, data)
	if err != nil {
		return lock.ApplyResult{}, err
	}
	v, err := fut.Wait(ctx)
	if err != nil {
		return lock.ApplyResult{}, err
	}
	return unwrapApplyResult(v)
}

// ProposeAcquire is sugar over Propose for the most common command.
func (n *Node) ProposeAcquire(ctx context.Context, key string, limit int, ref string, connID uint64, leaseTTL time.Duration, salt [8]byte) (lock.ApplyResult, error) {
	return n.Propose(ctx, Command{
		Kind: KindAcquire, Key: key, Limit: limit, Ref: ref, ConnID: connID,
		LeaseTTLNanos: int64(leaseTTL), SaltB64: EncodeSalt(salt),
	})
}

// ProposeRelease frees a held slot.
func (n *Node) ProposeRelease(ctx context.Context, key, token string) (lock.ApplyResult, error) {
	return n.Propose(ctx, Command{Kind: KindRelease, Key: key, Token: token})
}

// ProposeRenew extends a held lease.
func (n *Node) ProposeRenew(ctx context.Context, key, token string, leaseTTL time.Duration) (lock.ApplyResult, error) {
	return n.Propose(ctx, Command{Kind: KindRenew, Key: key, Token: token, LeaseTTLNanos: int64(leaseTTL)})
}

// ProposeEnqueue is the two-phase phase-1 helper.
func (n *Node) ProposeEnqueue(ctx context.Context, key string, limit int, ref string, connID uint64, leaseTTL time.Duration, salt [8]byte) (lock.ApplyResult, error) {
	return n.Propose(ctx, Command{
		Kind: KindEnqueue, Key: key, Limit: limit, Ref: ref, ConnID: connID,
		LeaseTTLNanos: int64(leaseTTL), SaltB64: EncodeSalt(salt),
	})
}

// ProposeEvict is called by the leader's lease sweep.
func (n *Node) ProposeEvict(ctx context.Context, key, token string) (lock.ApplyResult, error) {
	return n.Propose(ctx, Command{Kind: KindEvict, Key: key, Token: token})
}

// ProposeCleanupConn releases everything a (ref, connID) holds.
func (n *Node) ProposeCleanupConn(ctx context.Context, ref string, connID uint64) (lock.ApplyResult, error) {
	return n.Propose(ctx, Command{Kind: KindCleanupConn, Ref: ref, ConnID: connID})
}

// ProposeGC asks the cluster to drop idle resources.
func (n *Node) ProposeGC(ctx context.Context) (lock.ApplyResult, error) {
	return n.Propose(ctx, Command{Kind: KindGC})
}

// ProposeEvictExpired asks the cluster to drop every holder past its
// lease deadline (and promote waiters into the freed slots).
func (n *Node) ProposeEvictExpired(ctx context.Context) (lock.ApplyResult, error) {
	return n.Propose(ctx, Command{Kind: KindEvictExpired})
}

// ---------------------------------------------------------------------------
// leader-driven background sweep
// ---------------------------------------------------------------------------

// sweepLoop runs on every node but only acts while this node is the
// leader: it proposes a lease-expiry sweep every tick and an idle GC
// every gcEverySweeps ticks. A non-leader tick is a no-op (the real
// leader's loop does the work); a leadership change between the IsLeader
// check and the propose just yields ErrNotLeader, which we ignore.
func (n *Node) sweepLoop() {
	defer n.sweepWG.Done()
	interval := n.cfg.SweepInterval
	if interval <= 0 {
		interval = defaultSweepInterval
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for tick := 0; ; tick++ {
		select {
		case <-n.sweepStop:
			return
		case <-t.C:
			if !n.IsLeader() {
				continue
			}
			n.runOneSweep(interval, tick%gcEverySweeps == 0)
		}
	}
}

func (n *Node) runOneSweep(budget time.Duration, alsoGC bool) {
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	if _, err := n.ProposeEvictExpired(ctx); err != nil {
		n.logSweepErr("evict-expired", err)
		return
	}
	if alsoGC {
		if _, err := n.ProposeGC(ctx); err != nil {
			n.logSweepErr("gc", err)
		}
	}
}

func (n *Node) logSweepErr(what string, err error) {
	switch {
	case errors.Is(err, raft.ErrNotLeader), errors.Is(err, raft.ErrLeadershipLost):
		// expected around leadership changes
	case errors.Is(err, raft.ErrStopped), errors.Is(err, context.Canceled):
		// shutting down
	case errors.Is(err, context.DeadlineExceeded):
		// a degraded cluster (no quorum) — the next tick retries; no spam
	default:
		n.log.Debug("cluster sweep proposal failed", "op", what, "err", err)
	}
}

// Barrier proposes a no-op and waits for it to apply — a cheap
// linearizable read barrier for the leader's own queries.
func (n *Node) Barrier(ctx context.Context) error {
	_, err := n.Propose(ctx, Command{Kind: KindBarrier})
	return err
}

// AddVoter proposes adding a new voting member with the given Raft
// transport address. The change is durable and takes effect on append
// (the new node starts being counted toward quorum immediately). It is
// the caller's responsibility to start the new node and to make sure
// the leader's transport can reach it.
func (n *Node) AddVoter(ctx context.Context, id raft.NodeID, raftAddr, clientAddr string) error {
	n.setMember(id, Member{RaftAddr: raftAddr, ClientAddr: clientAddr})
	fut, err := n.raft.AddVoter(ctx, id, raftAddr)
	if err != nil {
		return err
	}
	_, err = fut.Wait(ctx)
	return err
}

// RemoveServer proposes removing a voter from the cluster. A leader
// removing itself steps down once the entry commits.
func (n *Node) RemoveServer(ctx context.Context, id raft.NodeID) error {
	fut, err := n.raft.RemoveServer(ctx, id)
	if err != nil {
		return err
	}
	if _, err = fut.Wait(ctx); err != nil {
		return err
	}
	n.deleteMember(id)
	return nil
}

func (n *Node) setMember(id raft.NodeID, m Member) {
	n.membersMu.Lock()
	n.cfg.Members[id] = m
	n.membersMu.Unlock()
}

func (n *Node) deleteMember(id raft.NodeID) {
	n.membersMu.Lock()
	delete(n.cfg.Members, id)
	n.membersMu.Unlock()
}
