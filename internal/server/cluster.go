package server

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/mtingers/dflockd/internal/lock"
	"github.com/mtingers/dflockd/internal/protocol"
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
	LeaderClientAddr() (string, bool)
	// StatusJSON returns this node's Raft status as a JSON object
	// (role, term, leader, commit/last-log indices, voters …) — spliced
	// into the `stats` response so operators can see cluster health.
	StatusJSON() json.RawMessage
	ProposeAcquire(ctx context.Context, key string, limit int, ref string, connID uint64, leaseTTL time.Duration, salt [8]byte) (lock.ApplyResult, error)
	ProposeEnqueue(ctx context.Context, key string, limit int, ref string, connID uint64, leaseTTL time.Duration, salt [8]byte) (lock.ApplyResult, error)
	ProposeRelease(ctx context.Context, key, token string) (lock.ApplyResult, error)
	ProposeRenew(ctx context.Context, key, token string, leaseTTL time.Duration) (lock.ApplyResult, error)
	ProposeCleanupConn(ctx context.Context, ref string, connID uint64) (lock.ApplyResult, error)
}

// SetCluster wires a cluster.Node into this server. After this call,
// all mutating handlers route through the cluster (returning
// error_not_leader off-leader, proposing on-leader). Pass nil to
// disable; the legacy single-node path resumes.
func (s *Server) SetCluster(c Cluster) {
	if c == nil {
		s.cluster.Store(nil)
		return
	}
	if s.connIDEpoch.Load() == 0 {
		s.connIDEpoch.Store(randomConnIDEpoch())
	}
	s.cluster.Store(&c)
}

// randomConnIDEpoch returns a non-zero value to OR into the high 32 bits
// of cluster-mode connection ids. Pure collision-avoidance across
// process restarts — crypto strength isn't required, but crypto/rand is
// already on hand and cheap.
func randomConnIDEpoch() uint64 {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return uint64(time.Now().UnixNano()&0xFFFFFFFF) << 32
	}
	e := uint64(binary.BigEndian.Uint32(b[:]))
	if e == 0 {
		e = 1
	}
	return e << 32
}

// clusterConnID maps a per-process connection counter value to the
// globally-unique id the FSM sees (epoch in the high bits, the 32-bit
// counter in the low bits).
func (s *Server) clusterConnID(raw uint64) uint64 {
	return s.connIDEpoch.Load() | uint64(uint32(raw))
}

// clusterOrNil returns the installed Cluster, or nil if running in
// single-node mode.
func (s *Server) clusterOrNil() Cluster {
	if c := s.cluster.Load(); c != nil {
		return *c
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

// clusterRef stamps the requester id the FSM routes grants by. It's the
// decimal form of clusterConnID, so it's globally unique (across process
// restarts) and stable within a connection.
func (s *Server) clusterRef(connID uint64) string {
	return fmt.Sprintf("%d", s.clusterConnID(connID))
}

// pendingGrant is a grant listener registered by a queued two-phase
// Enqueue, held until the matching Wait consumes it (or the conn dies).
type pendingGrant struct {
	ch     <-chan lock.Grant
	cancel func()
}

func (s *Server) stashPendingGrant(connID uint64, ch <-chan lock.Grant, cancel func()) {
	// Overwrite any prior entry without cancelling it: a fresh
	// WatchGrants on the same ref already replaced the registry slot, so
	// the old channel is simply unreferenced. (A connection with two
	// concurrent queued enqueues is already loosely-handled — one grant
	// fits in the buffer-1 channel — and this doesn't make it worse.)
	s.pendingGrants.Store(connID, &pendingGrant{ch: ch, cancel: cancel})
}

func (s *Server) takePendingGrant(connID uint64) (*pendingGrant, bool) {
	v, ok := s.pendingGrants.LoadAndDelete(connID)
	if !ok {
		return nil, false
	}
	return v.(*pendingGrant), true
}

// dropPendingGrant cancels and discards any pending grant listener for
// connID (used on disconnect).
func (s *Server) dropPendingGrant(connID uint64) {
	if pg, ok := s.takePendingGrant(connID); ok {
		pg.cancel()
	}
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
	return s.clusterDoAcquire(ctx, c, req, fmt.Sprintf("%d", cid), cid, salt)
}

func (s *Server) clusterDoAcquire(ctx context.Context, c Cluster, req *protocol.Request, ref string, cid uint64, salt [8]byte) *protocol.Ack {
	// Register the listener BEFORE proposing so a synchronous-grant
	// promotion (e.g. an Evict that frees the slot just before our
	// Acquire commits) doesn't slip past us.
	grants, cancel := s.lm.WatchGrants(ref)
	defer cancel()
	leaseTTL := req.LeaseTTL
	result, err := c.ProposeAcquire(ctx, requestKey(req), requestLimit(req), ref, cid, leaseTTL, salt)
	if err != nil {
		return clusterProposeErrAck(err, c)
	}
	if result.Status == lock.StatusOK {
		return &protocol.Ack{Status: protocol.StatusOK, Token: result.Token, LeaseTTL: result.LeaseSec}
	}
	if result.Status != lock.StatusQueued {
		return ackForLockStatus(result.Status)
	}
	return s.clusterWaitForGrant(ctx, grants, req.AcquireTimeout)
}

// clusterWaitForGrant blocks on grants or the acquire timeout. A
// timeout that hits before the grant arrives leaves the holder entry
// in the FSM until its lease expires; that's the documented cluster
// semantic (see PLAN.md §4.7).
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
	ref := fmt.Sprintf("%d", cid)
	// Register the grant listener before proposing and hold it on the
	// connection: a promotion can land between this Enqueue's commit and
	// the client's Wait, and without a live listener that grant is lost
	// (the holder then leaks until its lease expires).
	ch, cancel := s.lm.WatchGrants(ref)
	result, err := c.ProposeEnqueue(ctx, requestKey(req), requestLimit(req), ref, cid, req.LeaseTTL, salt)
	if err != nil {
		cancel()
		return clusterProposeErrAck(err, c)
	}
	if result.Status == lock.StatusQueued {
		s.stashPendingGrant(connID, ch, cancel)
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
	if pg, ok := s.takePendingGrant(connID); ok {
		defer pg.cancel()
		return s.clusterWaitForGrant(ctx, pg.ch, req.AcquireTimeout)
	}
	grants, cancel := s.lm.WatchGrants(s.clusterRef(connID))
	defer cancel()
	return s.clusterWaitForGrant(ctx, grants, req.AcquireTimeout)
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

func (s *Server) clusterRenew(ctx context.Context, c Cluster, req *protocol.Request) *protocol.Ack {
	if !c.IsLeader() {
		return notLeaderAck(c)
	}
	result, err := c.ProposeRenew(ctx, requestKey(req), req.Token, req.LeaseTTL)
	if err != nil {
		return clusterProposeErrAck(err, c)
	}
	if result.Status == lock.StatusOK {
		// Wire the renew "remaining" via Extra (matches the legacy path).
		return &protocol.Ack{Status: protocol.StatusOK, Extra: fmtSeconds(result.LeaseSec)}
	}
	return ackForLockStatus(result.Status)
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

// avoid an unused-import warning on internal/atomic when the build is
// stripped down for early phases.
var _ atomic.Pointer[Cluster]
