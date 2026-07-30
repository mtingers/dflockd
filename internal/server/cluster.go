package server

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/mtingers/dflockd/internal/lock"
	"github.com/mtingers/dflockd/internal/protocol"
	"github.com/mtingers/dflockd/internal/raft"
)

// Cluster is the contract the cluster.Node fulfills for the server's
// cluster-mode handlers. server doesn't import internal/cluster
// directly (so cluster can import server without a cycle if it ever
// needs to); the top-level wiring (cmd/dflockd) connects the two.
//
// Implementations are concurrency-safe; the server may call any method
// from many handler goroutines at once.
type Cluster interface {
	IsLeader() bool
	Ready() bool
	LeaderClientAddr() (string, bool)
	// StatusJSON returns this node's Raft status as a JSON object
	// (role, term, leader, commit/last-log indices, voters …) — spliced
	// into the `stats` response so operators can see cluster health.
	StatusJSON() json.RawMessage
	ProposeAcquire(ctx context.Context, key string, limit int, ref string, connID uint64, leaseTTL time.Duration, salt [8]byte) (lock.ApplyResult, error)
	ProposeEnqueue(ctx context.Context, key string, limit int, ref string, connID uint64, leaseTTL time.Duration, salt [8]byte) (lock.ApplyResult, error)
	ProposeRelease(ctx context.Context, key, token string) (lock.ApplyResult, error)
	ProposeRenewOwned(ctx context.Context, key, token, ref string, connID uint64, leaseTTL time.Duration) (lock.ApplyResult, error)
	ProposeCancel(ctx context.Context, key, ref string, connID uint64, salt [8]byte, matchSalt bool) (lock.ApplyResult, error)
	ProposeAttach(ctx context.Context, key, ref string, connID uint64) (lock.ApplyResult, error)
	ProposeCleanupConn(ctx context.Context, ref string, connID uint64) (lock.ApplyResult, error)
	// Barrier proposes a no-op and waits for it to apply — the public
	// linearizable-read primitive. Returns ErrNotLeader on a follower.
	Barrier(ctx context.Context) error
	// AddVoter proposes a single-server addition. Returns
	// ErrConfigChangeInProgress / ErrNotLeader for the obvious cases.
	AddVoter(ctx context.Context, id raft.NodeID, raftAddr, clientAddr string) error
	// RemoveServer proposes a single-server removal. A leader removing
	// itself steps down once the entry commits.
	RemoveServer(ctx context.Context, id raft.NodeID) error
	// MetricsSnapshot returns a flat read of monotonic cluster counters.
	MetricsSnapshot() raft.ClusterMetrics
}

// SetCluster wires a cluster.Node into this server. After this call,
// all mutating handlers route through the cluster (returning
// error_not_leader off-leader, proposing on-leader). Pass nil to
// detach during shutdown; a server that has hosted replicated state never
// falls back to local mutation.
func (s *Server) SetCluster(c Cluster) {
	if c == nil {
		s.cluster.Store(nil)
		return
	}
	s.clusterConfigured.Store(true)
	s.cluster.Store(&c)
}

// clusterConnID returns the already randomized, full-width process connection
// ID. It remains a helper so every FSM identity path stays centralized.
func (s *Server) clusterConnID(raw uint64) uint64 {
	return raw
}

// clusterOrNil returns the installed Cluster, or nil if running in
// single-node mode.
func (s *Server) clusterOrNil() Cluster {
	if c := s.cluster.Load(); c != nil {
		return *c
	}
	return nil
}

// Ready reports whether this process can serve traffic. Single-node mode is
// ready whenever its servers are running; cluster mode additionally requires
// a live local Raft voter.
func (s *Server) Ready() bool {
	if c := s.clusterOrNil(); c != nil {
		return c.Ready()
	}
	return !s.clusterConfigured.Load()
}

func (s *Server) clusterUnavailableAck() *protocol.Ack {
	if s.clusterConfigured.Load() {
		return &protocol.Ack{Status: protocol.StatusError}
	}
	return nil
}

// notLeaderAck builds the error_not_leader response with the (possibly
// empty) hint of the current leader's client-facing address.
func notLeaderAck(c Cluster) *protocol.Ack {
	addr, _ := c.LeaderClientAddr()
	return &protocol.Ack{Status: protocol.StatusErrorNotLeader, Extra: addr}
}

// ---------------------------------------------------------------------------
// Cluster-mode handlers — one per command. Same dispatcher signature as
// the legacy ones; the table in conn.go switches on s.clusterOrNil().
// ---------------------------------------------------------------------------

// clusterRef stamps the requester id the FSM routes grants by. It is the
// decimal form of the full-width randomized connection ID, making reuse across
// process restarts negligibly likely and remaining stable within a connection.
func (s *Server) clusterRef(connID uint64) string {
	return fmt.Sprintf("%d", s.clusterConnID(connID))
}

// pendingGrant is a grant listener registered by a queued two-phase
// Enqueue, held until the matching Wait consumes it (or the conn dies).
type pendingGrant struct {
	ch        <-chan lock.Grant
	cancel    func()
	key       string
	ref       string
	connID    uint64
	salt      [8]byte
	matchSalt bool
}

type pendingGrantKey struct {
	connID uint64
	key    string
}

func (s *Server) stashPendingGrant(rawConnID uint64, ch <-chan lock.Grant, cancel func(), key, ref string, connID uint64, salt [8]byte) {
	mapKey := pendingGrantKey{connID: rawConnID, key: key}
	next := &pendingGrant{
		ch: ch, cancel: cancel, key: key, ref: ref, connID: connID,
		salt: salt, matchSalt: true,
	}
	if prior, loaded := s.pendingGrants.Swap(mapKey, next); loaded {
		prior.(*pendingGrant).cancel()
	}
}

func (s *Server) takePendingGrant(connID uint64, key string) (*pendingGrant, bool) {
	v, ok := s.pendingGrants.LoadAndDelete(pendingGrantKey{connID: connID, key: key})
	if !ok {
		return nil, false
	}
	return v.(*pendingGrant), true
}

// dropPendingGrant cancels and discards any pending grant listener for
// connID (used on disconnect).
func (s *Server) dropPendingGrant(connID uint64) {
	s.pendingGrants.Range(func(key, _ any) bool {
		mapKey, ok := key.(pendingGrantKey)
		if !ok || mapKey.connID != connID {
			return true
		}
		if value, loaded := s.pendingGrants.LoadAndDelete(mapKey); loaded {
			value.(*pendingGrant).cancel()
		}
		return true
	})
}

// clusterSalt generates a fresh 8-byte salt for one token.
func clusterSalt() ([8]byte, error) {
	var s [8]byte
	if _, err := rand.Read(s[:]); err != nil {
		return s, fmt.Errorf("salt: %w", err)
	}
	return s, nil
}

func (s *Server) clusterAcquire(ctx context.Context, c Cluster, req *protocol.Request, connID uint64) *protocol.Ack {
	if !c.IsLeader() {
		return notLeaderAck(c)
	}
	salt, err := clusterSalt()
	if err != nil {
		return &protocol.Ack{Status: protocol.StatusError}
	}
	cid := s.clusterConnID(connID)
	return s.clusterDoAcquire(ctx, c, req, s.effectiveRef(connID, cid), cid, salt)
}

func (s *Server) clusterDoAcquire(ctx context.Context, c Cluster, req *protocol.Request, ref string, cid uint64, salt [8]byte) *protocol.Ack {
	// Register the listener BEFORE proposing so a synchronous-grant
	// promotion (e.g. an Evict that frees the slot just before our
	// Acquire commits) doesn't slip past us.
	grants, cancel := s.lm.WatchGrantsFor(ref, requestKey(req))
	defer cancel()
	leaseTTL := req.LeaseTTL
	result, err := c.ProposeAcquire(ctx, requestKey(req), requestLimit(req), ref, cid, leaseTTL, salt)
	if err != nil {
		s.cancelClusterOperation(c, requestKey(req), ref, cid, salt, true)
		return clusterProposeErrAck(err, c)
	}
	if result.Status == lock.StatusOK {
		return &protocol.Ack{Status: protocol.StatusOK, Token: result.Token, LeaseTTL: result.LeaseSec}
	}
	if result.Status != lock.StatusQueued {
		return ackForLockStatus(result.Status)
	}
	ack := s.clusterWaitForGrant(ctx, grants, req.AcquireTimeout)
	if ack.Status != protocol.StatusOK {
		s.cancelClusterOperation(c, requestKey(req), ref, cid, salt, true)
	}
	return ack
}

// clusterWaitForGrant blocks on grants or the acquire timeout. A timeout is
// followed by a replicated cancellation in the caller, which atomically
// removes either the queued waiter or a raced promotion.
func (s *Server) clusterWaitForGrant(ctx context.Context, grants <-chan lock.Grant, timeout time.Duration) *protocol.Ack {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case g, ok := <-grants:
		if !ok {
			return &protocol.Ack{Status: protocol.StatusError}
		}
		return &protocol.Ack{Status: protocol.StatusOK, Token: g.Token, LeaseTTL: g.LeaseSec}
	case <-timer.C:
		return &protocol.Ack{Status: protocol.StatusTimeout}
	case <-ctx.Done():
		return &protocol.Ack{Status: protocol.StatusError}
	}
}

func (s *Server) clusterEnqueue(ctx context.Context, c Cluster, req *protocol.Request, connID uint64) *protocol.Ack {
	if !c.IsLeader() {
		return notLeaderAck(c)
	}
	salt, err := clusterSalt()
	if err != nil {
		return &protocol.Ack{Status: protocol.StatusError}
	}
	cid := s.clusterConnID(connID)
	ref := s.effectiveRef(connID, cid)
	// Register the grant listener before proposing and hold it on the
	// connection: a promotion can land between this Enqueue's commit and
	// the client's Wait, and without a live listener that grant is lost
	// (the holder then leaks until its lease expires).
	ch, cancel := s.lm.WatchGrantsFor(ref, requestKey(req))
	result, err := c.ProposeEnqueue(ctx, requestKey(req), requestLimit(req), ref, cid, req.LeaseTTL, salt)
	if err != nil {
		cancel()
		s.cancelClusterOperation(c, requestKey(req), ref, cid, salt, true)
		return clusterProposeErrAck(err, c)
	}
	if result.Status == lock.StatusQueued {
		s.stashPendingGrant(connID, ch, cancel, requestKey(req), ref, cid, salt)
	} else {
		cancel() // acquired fast-path, or an error status — nothing will Wait
	}
	return enqueueAckFromResult(result)
}

func enqueueAckFromResult(result lock.ApplyResult) *protocol.Ack {
	switch result.Status {
	case lock.StatusAcquired:
		return &protocol.Ack{Status: protocol.StatusAcquired, Token: result.Token, LeaseTTL: result.LeaseSec}
	case lock.StatusQueued:
		return &protocol.Ack{Status: protocol.StatusQueued}
	default:
		return ackForLockStatus(result.Status)
	}
}

func (s *Server) clusterWait(ctx context.Context, c Cluster, req *protocol.Request, connID uint64) *protocol.Ack {
	if !c.IsLeader() {
		return notLeaderAck(c)
	}
	// Prefer the listener the matching Enqueue stashed (so a promotion
	// between the Enqueue and this Wait was already captured); otherwise
	// register one now.
	key := requestKey(req)
	if pg, ok := s.takePendingGrant(connID, key); ok {
		defer pg.cancel()
		attached, err := c.ProposeAttach(ctx, pg.key, pg.ref, pg.connID)
		if err != nil {
			s.cancelClusterOperation(c, pg.key, pg.ref, pg.connID, pg.salt, pg.matchSalt)
			return clusterProposeErrAck(err, c)
		}
		if attached.Status == lock.StatusOK {
			return &protocol.Ack{Status: protocol.StatusOK, Token: attached.Token, LeaseTTL: attached.LeaseSec}
		}
		if attached.Status != lock.StatusQueued {
			return ackForLockStatus(attached.Status)
		}
		ack := s.clusterWaitForGrant(ctx, pg.ch, req.AcquireTimeout)
		if ack.Status != protocol.StatusOK {
			s.cancelClusterOperation(c, pg.key, pg.ref, pg.connID, pg.salt, pg.matchSalt)
		}
		return ack
	}
	// No stashed listener (a Wait without its Enqueue, or after a
	// reconnect): watch the ref this connection actually proposes under.
	cid := s.clusterConnID(connID)
	ref := s.effectiveRef(connID, cid)
	grants, cancel := s.lm.WatchGrantsFor(ref, requestKey(req))
	defer cancel()
	attached, err := c.ProposeAttach(ctx, requestKey(req), ref, cid)
	if err != nil {
		s.cancelClusterOperation(c, requestKey(req), ref, cid, [8]byte{}, false)
		return clusterProposeErrAck(err, c)
	}
	if attached.Status == lock.StatusOK {
		return &protocol.Ack{Status: protocol.StatusOK, Token: attached.Token, LeaseTTL: attached.LeaseSec}
	}
	if attached.Status != lock.StatusQueued {
		return ackForLockStatus(attached.Status)
	}
	ack := s.clusterWaitForGrant(ctx, grants, req.AcquireTimeout)
	if ack.Status != protocol.StatusOK {
		s.cancelClusterOperation(c, requestKey(req), ref, cid, [8]byte{}, false)
	}
	return ack
}

func (s *Server) clusterRelease(ctx context.Context, c Cluster, req *protocol.Request) *protocol.Ack {
	if !c.IsLeader() {
		return notLeaderAck(c)
	}
	result, err := c.ProposeRelease(ctx, requestKey(req), req.Token)
	if err != nil {
		return clusterProposeErrAck(err, c)
	}
	if result.Status == lock.StatusOK {
		return &protocol.Ack{Status: protocol.StatusOK}
	}
	return ackForLockStatus(result.Status)
}

func (s *Server) clusterRenew(ctx context.Context, c Cluster, req *protocol.Request, connID uint64) *protocol.Ack {
	if !c.IsLeader() {
		return notLeaderAck(c)
	}
	cid := s.clusterConnID(connID)
	result, err := c.ProposeRenewOwned(ctx, requestKey(req), req.Token, s.effectiveRef(connID, cid), cid, req.LeaseTTL)
	if err != nil {
		return clusterProposeErrAck(err, c)
	}
	if result.Status == lock.StatusOK {
		// Wire the renew "remaining" via Extra (matches the legacy path).
		return &protocol.Ack{Status: protocol.StatusOK, Extra: fmtSeconds(result.LeaseSec)}
	}
	return ackForLockStatus(result.Status)
}

func (s *Server) cancelClusterOperation(c Cluster, key, ref string, connID uint64, salt [8]byte, matchSalt bool) {
	if c == nil || !c.IsLeader() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.ReadTimeout)
	defer cancel()
	if _, err := c.ProposeCancel(ctx, key, ref, connID, salt, matchSalt); err != nil {
		s.log.Warn("cluster cancel propose failed", "key", key, "ref", ref, "conn_id", connID, "err", err)
	}
}

// clusterProposeErrAck classifies a Propose failure. The two big
// outcomes worth distinguishing are "we're not (or no longer) the
// leader" → error_not_leader, and "the cluster stopped" → generic error.
func clusterProposeErrAck(err error, c Cluster) *protocol.Ack {
	if err != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		return &protocol.Ack{Status: protocol.StatusError}
	}
	// Anything else (including ErrLeadershipLost / ErrNotLeader) we
	// surface as error_not_leader so the client retries elsewhere.
	return notLeaderAck(c)
}

// ackForLockStatus maps a lock.ApplyStatus to the matching protocol.Ack.
func ackForLockStatus(st lock.ApplyStatus) *protocol.Ack {
	for _, m := range lockStatusAcks {
		if m.st == st {
			return &protocol.Ack{Status: m.status}
		}
	}
	return &protocol.Ack{Status: protocol.StatusError}
}

type lockStatusAck struct {
	st     lock.ApplyStatus
	status string
}

var lockStatusAcks = []lockStatusAck{
	{lock.StatusErrMaxLocks, protocol.StatusErrorMaxLocks},
	{lock.StatusErrMaxWaiters, protocol.StatusErrorMaxWaiters},
	{lock.StatusErrLimitMismatch, protocol.StatusErrorLimitMismatch},
	{lock.StatusErrAlreadyEnqueued, protocol.StatusErrorAlreadyEnqueued},
	{lock.StatusErrNotEnqueued, protocol.StatusErrorNotEnqueued},
	{lock.StatusErrLeaseExpired, protocol.StatusErrorLeaseExpired},
	{lock.StatusNotHeld, protocol.StatusError},
	{lock.StatusOK, protocol.StatusOK},
}

func fmtSeconds(s int) string {
	if s < 0 {
		s = 0
	}
	return fmt.Sprintf("%d", s)
}
