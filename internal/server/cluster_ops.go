package server

import (
	"context"
	"errors"
	"time"

	"github.com/mtingers/dflockd/internal/lock"
	"github.com/mtingers/dflockd/internal/raft"
)

// This file is the cluster-mode surface used by the HTTP API. The TCP
// line-protocol path has its own handlers (cluster.go); these mirror the
// same propose-then-wait flow but return primitive results the HTTP
// layer renders. Single-node mode never reaches this file.

// ErrNotClusterLeader is returned by the exported Cluster* operations
// when this node is in cluster mode but isn't currently the leader. The
// caller (the HTTP API) should reject/redirect the request — see
// ClusterLeaderAddr for where the leader is.
var ErrNotClusterLeader = errors.New("dflockd: not the cluster leader")

// errClosedGrantChan signals that a grant listener channel was closed
// (a connection-cleanup race) rather than producing a grant.
var errClosedGrantChan = errors.New("dflockd: grant listener closed")

// IsClusterMode reports whether this server is part of a Raft cluster.
func (s *Server) IsClusterMode() bool { return s.clusterConfigured.Load() }

// IsClusterLeader reports whether this node is the cluster leader.
// Always false in single-node mode; callers gate mutating ops on
// IsClusterMode() first.
func (s *Server) IsClusterLeader() bool {
	c := s.clusterOrNil()
	return c != nil && c.IsLeader()
}

// ClusterLeaderAddr returns the leader's client-facing address — the one
// a redirected client should retry against — or "" if not clustered or
// the leader is unknown.
func (s *Server) ClusterLeaderAddr() string {
	if c := s.clusterOrNil(); c != nil {
		if a, ok := c.LeaderClientAddr(); ok {
			return a
		}
	}
	return ""
}

// ClusterStatusJSON returns the cluster's Raft status as a JSON object,
// or nil in single-node mode.
func (s *Server) ClusterStatusJSON() []byte {
	if c := s.clusterOrNil(); c != nil {
		return c.StatusJSON()
	}
	return nil
}

// ---------------------------------------------------------------------------
// Exported cluster lock ops (used by the HTTP API). Each returns a
// lock.ApplyResult plus an error:
//   - err == ErrNotClusterLeader → this node isn't (or stopped being)
//     the leader; the HTTP layer renders a 503 redirect.
//   - err == context.Canceled/DeadlineExceeded → the caller's ctx fired.
//   - err != nil otherwise → an internal failure (HTTP 500).
//   - err == nil → result.Status is meaningful:
//       StatusOK       + Token + LeaseSec : granted / release-ok / renew-ok
//       StatusQueued                       : Acquire/Wait timed out (cancelled)
//       StatusAcquired + Token + LeaseSec  : Enqueue fast-path
//       StatusNotHeld                      : Release/Renew of an unknown token
//       StatusErrX / StatusErrLeaseExpired : domain error
// ---------------------------------------------------------------------------

// ClusterAcquire is the cluster-mode entrypoint for lock/semaphore
// acquire. Proposes a KindAcquire command through Raft; if the FSM
// answers StatusQueued, blocks on a grant listener for up to
// acquireTimeout. Returns ErrNotClusterLeader on a follower so the
// caller can redirect.
func (s *Server) ClusterAcquire(ctx context.Context, key string, limit int, connID uint64, leaseTTL, acquireTimeout time.Duration) (lock.ApplyResult, error) {
	c, err := s.clusterLeaderOrErr()
	if err != nil {
		return lock.ApplyResult{}, err
	}
	salt, err := clusterSalt()
	if err != nil {
		return lock.ApplyResult{}, err
	}
	cid := s.clusterConnID(connID)
	ref := s.effectiveRef(connID, cid)
	grants, cancel := s.lm.WatchGrantsFor(ref, key)
	defer cancel()
	result, err := c.ProposeAcquire(ctx, key, limit, ref, cid, leaseTTL, salt)
	if err != nil {
		s.cancelClusterOperation(c, key, ref, cid, salt, true)
		return lock.ApplyResult{}, classifyProposeErr(err)
	}
	if result.Status != lock.StatusQueued {
		return result, nil
	}
	result, err = s.waitGrantResult(ctx, grants, acquireTimeout)
	if err != nil || result.Status == lock.StatusQueued {
		s.cancelClusterOperation(c, key, ref, cid, salt, true)
	}
	return result, err
}

// ClusterEnqueue is phase 1 of the two-phase enqueue/wait flow in
// cluster mode. Proposes a KindEnqueue command; if it commits with
// StatusQueued, stashes the grant listener on the connection so the
// matching ClusterWait can claim it without a lost-wakeup window.
func (s *Server) ClusterEnqueue(ctx context.Context, key string, limit int, connID uint64, leaseTTL time.Duration) (lock.ApplyResult, error) {
	c, err := s.clusterLeaderOrErr()
	if err != nil {
		return lock.ApplyResult{}, err
	}
	salt, err := clusterSalt()
	if err != nil {
		return lock.ApplyResult{}, err
	}
	cid := s.clusterConnID(connID)
	ref := s.effectiveRef(connID, cid)
	ch, cancel := s.lm.WatchGrantsFor(ref, key)
	result, err := c.ProposeEnqueue(ctx, key, limit, ref, cid, leaseTTL, salt)
	if err != nil {
		cancel()
		s.cancelClusterOperation(c, key, ref, cid, salt, true)
		return lock.ApplyResult{}, classifyProposeErr(err)
	}
	if result.Status == lock.StatusQueued {
		s.stashPendingGrant(connID, ch, cancel, key, ref, cid, salt)
	} else {
		cancel()
	}
	return result, nil
}

// ClusterWait is phase 2 of the two-phase enqueue/wait flow in cluster
// mode. Consumes the listener stashed by ClusterEnqueue (or, as a
// fallback, opens a fresh one for this connection's ref and key) and
// blocks up to acquireTimeout for the grant to land.
func (s *Server) ClusterWait(ctx context.Context, key string, connID uint64, acquireTimeout time.Duration) (lock.ApplyResult, error) {
	c, err := s.clusterLeaderOrErr()
	if err != nil {
		return lock.ApplyResult{}, err
	}
	if pg, ok := s.takePendingGrant(connID, key); ok {
		defer pg.cancel()
		attached, err := c.ProposeAttach(ctx, key, pg.ref, pg.connID)
		if err != nil {
			s.cancelClusterOperation(c, pg.key, pg.ref, pg.connID, pg.salt, pg.matchSalt)
			return lock.ApplyResult{}, classifyProposeErr(err)
		}
		if attached.Status != lock.StatusQueued {
			return attached, nil
		}
		result, err := s.waitGrantResult(ctx, pg.ch, acquireTimeout)
		if err != nil || result.Status == lock.StatusQueued {
			s.cancelClusterOperation(c, pg.key, pg.ref, pg.connID, pg.salt, pg.matchSalt)
		}
		return result, err
	}
	cid := s.clusterConnID(connID)
	ref := s.effectiveRef(connID, cid)
	grants, cancel := s.lm.WatchGrantsFor(ref, key)
	defer cancel()
	attached, err := c.ProposeAttach(ctx, key, ref, cid)
	if err != nil {
		s.cancelClusterOperation(c, key, ref, cid, [8]byte{}, false)
		return lock.ApplyResult{}, classifyProposeErr(err)
	}
	if attached.Status != lock.StatusQueued {
		return attached, nil
	}
	result, err := s.waitGrantResult(ctx, grants, acquireTimeout)
	if err != nil || result.Status == lock.StatusQueued {
		s.cancelClusterOperation(c, key, ref, cid, [8]byte{}, false)
	}
	return result, err
}

// ClusterRelease proposes a KindRelease command and returns the FSM
// result. ErrNotClusterLeader on a follower.
func (s *Server) ClusterRelease(ctx context.Context, key, token string) (lock.ApplyResult, error) {
	c, err := s.clusterLeaderOrErr()
	if err != nil {
		return lock.ApplyResult{}, err
	}
	result, err := c.ProposeRelease(ctx, key, token)
	if err != nil {
		return lock.ApplyResult{}, classifyProposeErr(err)
	}
	return result, nil
}

// ClusterRenew proposes a KindRenew command and returns the FSM
// result. ErrNotClusterLeader on a follower.
func (s *Server) ClusterRenew(ctx context.Context, key, token string, connID uint64, leaseTTL time.Duration) (lock.ApplyResult, error) {
	c, err := s.clusterLeaderOrErr()
	if err != nil {
		return lock.ApplyResult{}, err
	}
	cid := s.clusterConnID(connID)
	result, err := c.ProposeRenewOwned(ctx, key, token, s.effectiveRef(connID, cid), cid, leaseTTL)
	if err != nil {
		return lock.ApplyResult{}, classifyProposeErr(err)
	}
	return result, nil
}

// ClusterCancel abandons the pending two-phase operation for connID and key.
// It is used by HTTP cleanup paths after the request context has gone away.
func (s *Server) ClusterCancel(ctx context.Context, key string, connID uint64) (lock.ApplyResult, error) {
	c, err := s.clusterLeaderOrErr()
	if err != nil {
		return lock.ApplyResult{}, err
	}
	if pg, ok := s.takePendingGrant(connID, key); ok {
		pg.cancel()
		result, err := c.ProposeCancel(ctx, pg.key, pg.ref, pg.connID, pg.salt, pg.matchSalt)
		if err != nil {
			return lock.ApplyResult{}, classifyProposeErr(err)
		}
		return result, nil
	}
	cid := s.clusterConnID(connID)
	result, err := c.ProposeCancel(ctx, key, s.effectiveRef(connID, cid), cid, [8]byte{}, false)
	if err != nil {
		return lock.ApplyResult{}, classifyProposeErr(err)
	}
	return result, nil
}

// ClusterBarrier proposes a no-op and waits for it to apply — the
// linearizable-read primitive. ErrNotClusterLeader on a follower so
// the caller (HTTP / TCP) can redirect.
func (s *Server) ClusterBarrier(ctx context.Context) error {
	c, err := s.clusterLeaderOrErr()
	if err != nil {
		return err
	}
	if err := c.Barrier(ctx); err != nil {
		return classifyProposeErr(err)
	}
	return nil
}

// ClusterAddVoter proposes a single-server addition. ErrNotClusterLeader
// on a follower; any other error is the underlying raft error.
func (s *Server) ClusterAddVoter(ctx context.Context, id, raftAddr, clientAddr string) error {
	c, err := s.clusterLeaderOrErr()
	if err != nil {
		return err
	}
	if err := c.AddVoter(ctx, raft.NodeID(id), raftAddr, clientAddr); err != nil {
		return classifyProposeErr(err)
	}
	return nil
}

// ClusterRemoveVoter proposes a single-server removal. ErrNotClusterLeader
// on a follower; any other error is the underlying raft error.
func (s *Server) ClusterRemoveVoter(ctx context.Context, id string) error {
	c, err := s.clusterLeaderOrErr()
	if err != nil {
		return err
	}
	if err := c.RemoveServer(ctx, raft.NodeID(id)); err != nil {
		return classifyProposeErr(err)
	}
	return nil
}

// ClusterMetricsSnapshot returns counters for /metrics rendering, or
// the zero value in single-node mode.
func (s *Server) ClusterMetricsSnapshot() raft.ClusterMetrics {
	if c := s.clusterOrNil(); c != nil {
		return c.MetricsSnapshot()
	}
	return raft.ClusterMetrics{}
}

// CleanupConnID proposes a cluster-wide CleanupConn (when clustered &
// leader) or runs the local LockManager cleanup. Used by the HTTP
// session store. On a follower the cluster cleanup is dropped — lease
// expiry is the backstop, matching the TCP path.
func (s *Server) CleanupConnID(connID uint64) error {
	c := s.clusterOrNil()
	if c == nil {
		if s.clusterConfigured.Load() {
			s.dropPendingGrant(connID)
			return errors.New("dflockd: cluster unavailable")
		}
		return s.lm.CleanupConnection(connID)
	}
	s.dropPendingGrant(connID)
	if !c.IsLeader() {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.ReadTimeout)
	defer cancel()
	cid := s.clusterConnID(connID)
	_, err := c.ProposeCleanupConn(ctx, s.effectiveRef(connID, cid), cid)
	return err
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func (s *Server) clusterLeaderOrErr() (Cluster, error) {
	c := s.clusterOrNil()
	if c == nil {
		return nil, errors.New("dflockd: not in cluster mode")
	}
	if !c.IsLeader() {
		return nil, ErrNotClusterLeader
	}
	return c, nil
}

// classifyProposeErr maps a Raft propose failure to the error the HTTP
// layer acts on: a leadership/shutdown problem → ErrNotClusterLeader;
// anything else passes through (rendered as a generic 5xx).
func classifyProposeErr(err error) error {
	switch {
	case errors.Is(err, raft.ErrNotLeader), errors.Is(err, raft.ErrLeadershipLost), errors.Is(err, raft.ErrStopped):
		return ErrNotClusterLeader
	default:
		return err
	}
}

// waitGrantResult blocks for a promotion grant, the acquire timeout, or
// ctx. Grant -> {StatusOK, Token, LeaseSec}; timeout -> {StatusQueued}.
// The caller follows a timeout with a replicated cancellation before returning.
// Context done / closed channel returns an error.
func (s *Server) waitGrantResult(ctx context.Context, grants <-chan lock.Grant, timeout time.Duration) (lock.ApplyResult, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case g, open := <-grants:
		if !open {
			return lock.ApplyResult{}, errClosedGrantChan
		}
		return lock.ApplyResult{Status: lock.StatusOK, Token: g.Token, LeaseSec: g.LeaseSec}, nil
	case <-timer.C:
		return lock.ApplyResult{Status: lock.StatusQueued}, nil
	case <-ctx.Done():
		return lock.ApplyResult{}, ctx.Err()
	}
}
