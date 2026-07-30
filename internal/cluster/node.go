package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
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
	// Now supplies proposal and maintenance wall-clock time. Production
	// leaves it nil (time.Now); fault-injection harnesses can provide an
	// offset clock without changing the host clock.
	Now func() time.Time
}

const (
	defaultSweepInterval = time.Second
	// gcEverySweeps: propose a KindGC every N sweep ticks (idle-resource
	// pruning is far less urgent than reclaiming expired leases).
	gcEverySweeps = 30
	// leadershipTransferTimeout bounds the TransferLeadership call made on
	// a graceful Close (it returns near-instantly in the happy path; the
	// timeout just keeps Close from hanging if the run loop is wedged).
	leadershipTransferTimeout = 3 * time.Second
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
	for id, member := range c.Members {
		if member.RaftAddr == "" {
			return fmt.Errorf("cluster: member %q RaftAddr is required", id)
		}
		if member.ClientAddr == "" {
			return fmt.Errorf("cluster: member %q ClientAddr is required", id)
		}
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

	sweepStop chan struct{}
	sweepWG   sync.WaitGroup

	// admin-op counters (monotonic; read by MetricsSnapshot)
	adminAdds         atomic.Uint64
	adminAddFailed    atomic.Uint64
	adminRemoves      atomic.Uint64
	adminRemoveFailed atomic.Uint64
}

// NewNode wires the raft node + FSM. It does not start any work until
// Start is called.
func NewNode(cfg Config, lm *lock.LockManager, storage raft.Storage, transport raft.Transport, logger *slog.Logger) (*Node, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
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
	clientAddrs := make(map[raft.NodeID]string, len(cfg.Members))
	for id, m := range cfg.Members {
		voters[id] = m.RaftAddr
		clientAddrs[id] = m.ClientAddr
	}
	return raft.Configuration{Voters: voters, ClientAddrs: clientAddrs}
}

// Start launches the underlying raft node and the leader-driven sweep
// loop (lease expiry + idle GC).
func (n *Node) Start() error {
	if err := n.raft.Start(); err != nil {
		return err
	}
	n.sweepWG.Add(1)
	go n.sweepLoop()
	return nil
}

// Close gracefully hands off leadership if this node is the leader (so a
// successor is elected within a round trip rather than after an election
// timeout), stops the sweep loop, then stops the raft node and waits for
// its goroutines to exit. Idempotent.
func (n *Node) Close() error {
	n.transferLeadershipBestEffort()
	select {
	case <-n.sweepStop:
	default:
		close(n.sweepStop)
	}
	n.sweepWG.Wait()
	return n.raft.Close()
}

func (n *Node) transferLeadershipBestEffort() {
	if !n.IsLeader() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), leadershipTransferTimeout)
	defer cancel()
	if err := n.raft.TransferLeadership(ctx); err != nil {
		n.log.Debug("graceful leadership transfer skipped", "err", err)
	}
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

// Done closes when the underlying consensus engine terminates.
func (n *Node) Done() <-chan struct{} { return n.raft.Done() }

// Err returns the fatal consensus-engine failure, if any.
func (n *Node) Err() error { return n.raft.Err() }

// Ready reports whether Raft is running and this node remains a voter.
// Leadership is not required because followers can redirect clients.
func (n *Node) Ready() bool { return n.raft.Ready() }

// clusterStatusView is the JSON-friendly shape returned by StatusJSON.
type clusterStatusView struct {
	NodeID        string   `json:"node_id"`
	Role          string   `json:"role"`
	Term          uint64   `json:"term"`
	LeaderID      string   `json:"leader_id"`
	LeaderAddr    string   `json:"leader_addr,omitempty"`
	CommitIndex   uint64   `json:"commit_index"`
	LastLogIndex  uint64   `json:"last_log_index"`
	SnapshotIndex uint64   `json:"snapshot_index"`
	Voters        []string `json:"voters"`
}

// StatusJSON returns this node's Raft status as a JSON object — surfaced
// in the server's `stats` response.
func (n *Node) StatusJSON() json.RawMessage {
	st := n.raft.Status()
	v := clusterStatusView{
		NodeID:        string(st.ID),
		Role:          st.Role,
		Term:          uint64(st.Term),
		LeaderID:      string(st.LeaderID),
		CommitIndex:   uint64(st.CommitIndex),
		LastLogIndex:  uint64(st.LastLogIndex),
		SnapshotIndex: uint64(st.LastSnapshotIndex),
		Voters:        nodeIDsToStrings(st.Voters),
	}
	if addr, ok := n.LeaderClientAddr(); ok {
		v.LeaderAddr = addr
	}
	b, err := json.Marshal(v)
	if err != nil {
		return json.RawMessage(`{"error":"marshal cluster status"}`)
	}
	return b
}

func nodeIDsToStrings(ids []raft.NodeID) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = string(id)
	}
	return out
}

// LockManager returns the FSM-backing LockManager — useful so the
// server can register grant listeners on it directly.
func (n *Node) LockManager() *lock.LockManager { return n.lm }

// LeaderClientAddr returns the client-facing address of the current
// leader (suitable for an error_not_leader redirect). ok=false if
// there's no known leader, or if no address is known for it.
//
// Every redirected request on a follower calls this, so it must not touch the
// Raft run loop: reading membership through Status() would make each redirect
// wait behind whatever the loop is doing — including a snapshot send, which
// reads the whole snapshot inline. Both paths here are lock-free. The
// replicated address is preferred; the static startup map is the fallback for
// configurations that predate replicated client metadata, and it is immutable
// after construction (membership changes publish through Raft, not into it).
func (n *Node) LeaderClientAddr() (string, bool) {
	if addr, ok := n.raft.LeaderClientAddr(); ok {
		return addr, true
	}
	id := n.LeaderID()
	if id == "" {
		return "", false
	}
	m, ok := n.cfg.Members[id]
	if !ok || m.ClientAddr == "" {
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
	cmd.NowNanos = n.cfg.Now().UnixNano()
	policy, _ := n.lm.ActiveFSMPolicy()
	cmd.Policy = &policy
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
	return n.ProposeRenewOwned(ctx, key, token, "", 0, leaseTTL)
}

// ProposeRenewOwned renews a held lease and rebinds its replicated owner to
// the connection that presented the token.
func (n *Node) ProposeRenewOwned(ctx context.Context, key, token, ref string, connID uint64, leaseTTL time.Duration) (lock.ApplyResult, error) {
	return n.Propose(ctx, Command{
		Kind: KindRenew, Key: key, Token: token, Ref: ref, ConnID: connID,
		LeaseTTLNanos: int64(leaseTTL),
	})
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

// ProposeCancel abandons one acquire/enqueue operation. matchSalt requires
// the exact connection identity; false permits stable-ref recovery when the
// original connection-local operation metadata is unavailable.
func (n *Node) ProposeCancel(ctx context.Context, key, ref string, connID uint64, salt [8]byte, matchSalt bool) (lock.ApplyResult, error) {
	cmd := Command{Kind: KindCancel, Key: key, Ref: ref, ConnID: connID}
	if matchSalt {
		cmd.SaltB64 = EncodeSalt(salt)
	}
	return n.Propose(ctx, cmd)
}

// ProposeAttach rebinds an existing two-phase waiter or raced promotion to a
// reconnected stable session before Wait blocks.
func (n *Node) ProposeAttach(ctx context.Context, key, ref string, connID uint64) (lock.ApplyResult, error) {
	return n.Propose(ctx, Command{Kind: KindAttach, Key: key, Ref: ref, ConnID: connID})
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

// sweepLoop runs on every node but only acts while this node is the leader.
// Each tick checks local FSM state before proposing maintenance, avoiding log
// churn when no expiry or GC work is due. The replicated commands remain the
// authority; a leadership change during submission simply yields ErrNotLeader.
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
	now := n.cfg.Now()
	if n.lm.EvictionDue(now) {
		if _, err := n.ProposeEvictExpired(ctx); err != nil {
			n.logSweepErr("evict-expired", err)
			return
		}
	}
	if alsoGC && n.lm.GCDue(now) {
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
// the leader's transport can reach it. The client-facing address is not
// published until the change commits.
func (n *Node) AddVoter(ctx context.Context, id raft.NodeID, raftAddr, clientAddr string) error {
	if !membershipIdentityBound(n.transport) {
		n.adminAddFailed.Add(1)
		return ErrMembershipIdentityRequired
	}
	fut, err := n.raft.AddVoterWithMetadata(ctx, id, raftAddr, clientAddr)
	if err != nil {
		n.adminAddFailed.Add(1)
		return err
	}
	if _, err = fut.Wait(ctx); err != nil {
		n.adminAddFailed.Add(1)
		return err
	}
	n.adminAdds.Add(1)
	return nil
}

// RemoveServer proposes removing a voter from the cluster. A leader
// removing itself steps down once the entry commits.
func (n *Node) RemoveServer(ctx context.Context, id raft.NodeID) error {
	if !membershipIdentityBound(n.transport) {
		n.adminRemoveFailed.Add(1)
		return ErrMembershipIdentityRequired
	}
	fut, err := n.raft.RemoveServer(ctx, id)
	if err != nil {
		n.adminRemoveFailed.Add(1)
		return err
	}
	if _, err = fut.Wait(ctx); err != nil {
		n.adminRemoveFailed.Add(1)
		return err
	}
	n.adminRemoves.Add(1)
	return nil
}

func membershipIdentityBound(transport raft.Transport) bool {
	bound, ok := transport.(raft.IdentityBoundTransport)
	return ok && bound.PeerIdentityBound()
}

// MetricsSnapshot returns a flat read of every monotonic cluster
// counter — including the raft-layer counters (proposals, applies,
// leader-change count) plus the cluster-layer admin-op counters.
func (n *Node) MetricsSnapshot() raft.ClusterMetrics {
	return raft.ClusterMetrics{
		Raft:              n.raft.Counters().Snapshot(),
		AdminAddVoter:     n.adminAdds.Load(),
		AdminAddVoterFail: n.adminAddFailed.Load(),
		AdminRemoveServer: n.adminRemoves.Load(),
		AdminRemoveFail:   n.adminRemoveFailed.Load(),
	}
}

// member resolves full metadata (Raft + client address) for id from the
// effective replicated configuration, falling back to the static startup map.
//
// This reads through Status(), which is a Raft run-loop round trip with no
// timeout. Keep it off per-request paths — LeaderClientAddr deliberately uses
// the lock-free published leadership state instead.
func (n *Node) member(id raft.NodeID) (Member, bool) {
	effective := n.raft.Status().Configuration
	if effective.ClientAddrs != nil {
		raftAddr, voter := effective.Voters[id]
		clientAddr, known := effective.ClientAddrs[id]
		if !voter || !known {
			return Member{}, false
		}
		return Member{RaftAddr: raftAddr, ClientAddr: clientAddr}, true
	}
	m, ok := n.cfg.Members[id]
	return m, ok
}
