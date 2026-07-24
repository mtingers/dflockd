package httpapi

import (
	"errors"
	"net/http"

	"github.com/mtingers/dflockd/internal/lock"
	"github.com/mtingers/dflockd/internal/server"
)

// Cluster-mode versions of the lock handlers: they propose through the
// Raft cluster (via the server's exported Cluster* surface) instead of
// touching the LockManager directly, and on a follower they return a
// 503 with the leader's address so the client can retry there. The
// single-node code paths (the run*Local bodies in handlers.go) are
// untouched.

// writeNotLeader emits the cluster-mode "you reached a follower" response:
// 503 with the leader's client (TCP) address in the body and an
// X-Dflockd-Leader header. Note: that's the leader's *TCP* address — the
// HTTP API runs on a separate port that the cluster config doesn't
// carry, so an HTTP client retries against a node it already knows.
func (h *httpServer) writeNotLeader(w http.ResponseWriter, leaderAddr string) {
	if leaderAddr != "" {
		w.Header().Set("X-Dflockd-Leader", leaderAddr)
	}
	detail := "this node is not the cluster leader"
	if leaderAddr != "" {
		detail += "; leader (raft client addr) is " + leaderAddr
	}
	writeError(w, http.StatusServiceUnavailable, "not_leader", detail)
}

// applyStatusToLockErr maps a domain ApplyStatus to the LockManager
// sentinel it corresponds to, so writeLockErr renders the same HTTP
// status the single-node path would. Returns nil for non-error statuses.
var applyStatusToLockErr = map[lock.ApplyStatus]error{
	lock.StatusErrMaxLocks:        lock.ErrMaxLocks,
	lock.StatusErrMaxWaiters:      lock.ErrMaxWaiters,
	lock.StatusErrLimitMismatch:   lock.ErrLimitMismatch,
	lock.StatusErrAlreadyEnqueued: lock.ErrAlreadyEnqueued,
	lock.StatusErrNotEnqueued:     lock.ErrNotEnqueued,
	lock.StatusErrLeaseExpired:    lock.ErrLeaseExpired,
}

func errForApplyStatus(st lock.ApplyStatus) error {
	if err, ok := applyStatusToLockErr[st]; ok {
		return err
	}
	return errors.New("dflockd: unexpected cluster apply status")
}

// handleClusterNotLeader is the common front for the cluster handlers:
// if err is ErrNotClusterLeader it writes the redirect and returns true
// (the caller should stop).
func (h *httpServer) handleClusterNotLeader(w http.ResponseWriter, srv *server.Server, err error) bool {
	if errors.Is(err, server.ErrNotClusterLeader) {
		h.writeNotLeader(w, srv.ClusterLeaderAddr())
		return true
	}
	return false
}

// ---------------------------------------------------------------------------
// run*Cluster — invoked from handlers.go when the server is clustered.
// ---------------------------------------------------------------------------

func (h *httpServer) runAcquireCluster(w http.ResponseWriter, r *http.Request, s *Session, key, prefix string, limit, timeoutS, leaseS int) {
	srv := h.sessions.Server()
	leaseTTL := h.leaseDuration(leaseS)
	ctx, cancel := s.RequestContext(r.Context())
	defer cancel()
	res, err := srv.ClusterAcquire(ctx, prefix+key, limit, s.ConnID, leaseTTL, durSeconds(timeoutS))
	if h.handleClusterNotLeader(w, srv, err) {
		return
	}
	tok, herr := grantResultToHTTP(res, err)
	h.renderAcquireOutcome(w, r, key, prefix, tok, leaseTTL, herr)
}

func (h *httpServer) runEnqueueCluster(w http.ResponseWriter, r *http.Request, s *Session, key, prefix string, limit, leaseS int) {
	srv := h.sessions.Server()
	leaseTTL := h.leaseDuration(leaseS)
	ctx, cancel := s.RequestContext(r.Context())
	defer cancel()
	res, err := srv.ClusterEnqueue(ctx, prefix+key, limit, s.ConnID, leaseTTL)
	if h.handleClusterNotLeader(w, srv, err) {
		return
	}
	status, tok, leaseSec, herr := enqueueResultToHTTP(res, err)
	h.renderEnqueueOutcome(w, r, s, key, prefix, status, tok, leaseSec, herr)
}

func (h *httpServer) runWaitCluster(w http.ResponseWriter, r *http.Request, s *Session, prefixedKey string, timeoutS int) {
	srv := h.sessions.Server()
	ctx, cancel := s.RequestContext(r.Context())
	defer cancel()
	res, err := srv.ClusterWait(ctx, prefixedKey, s.ConnID, durSeconds(timeoutS))
	if h.handleClusterNotLeader(w, srv, err) {
		return
	}
	tok, herr := grantResultToHTTP(res, err)
	// The cluster Wait grant carries the lease in res.LeaseSec.
	h.renderWaitOutcome(w, r, prefixedKey, tok, res.LeaseSec, herr)
}

func (h *httpServer) runReleaseCluster(w http.ResponseWriter, r *http.Request, prefixedKey, token string) {
	srv := h.sessions.Server()
	res, err := srv.ClusterRelease(r.Context(), prefixedKey, token)
	if h.handleClusterNotLeader(w, srv, err) {
		return
	}
	if err != nil {
		writeLockErr(w, err)
		return
	}
	switch res.Status {
	case lock.StatusOK:
		w.WriteHeader(http.StatusNoContent)
	case lock.StatusNotHeld:
		writeError(w, http.StatusNotFound, "not_held", "")
	default:
		writeLockErr(w, errForApplyStatus(res.Status))
	}
}

func (h *httpServer) runRenewCluster(w http.ResponseWriter, r *http.Request, prefixedKey, token string, leaseS int) {
	srv := h.sessions.Server()
	leaseTTL := h.leaseDuration(leaseS)
	res, err := srv.ClusterRenew(r.Context(), prefixedKey, token, leaseTTL)
	if h.handleClusterNotLeader(w, srv, err) {
		return
	}
	if err != nil {
		writeLockErr(w, err)
		return
	}
	switch res.Status {
	case lock.StatusOK:
		writeJSON(w, http.StatusOK, renewResponse{RemainingS: res.LeaseSec})
	case lock.StatusNotHeld:
		writeError(w, http.StatusNotFound, "not_held", "")
	default:
		writeLockErr(w, errForApplyStatus(res.Status))
	}
}

// grantResultToHTTP maps an Acquire/Wait cluster result to the
// (token, err) shape the render*Outcome functions expect: a grant →
// (token, nil); a timeout → ("", nil); a context error / internal
// failure → ("", err); a domain error status → ("", lock.ErrX).
func grantResultToHTTP(res lock.ApplyResult, err error) (string, error) {
	if err != nil {
		return "", err
	}
	switch res.Status {
	case lock.StatusOK:
		return res.Token, nil
	case lock.StatusQueued:
		return "", nil // timed out — render*Outcome treats "" as timeout
	default:
		return "", errForApplyStatus(res.Status)
	}
}

func enqueueResultToHTTP(res lock.ApplyResult, err error) (status, tok string, leaseSec int, herr error) {
	if err != nil {
		return "", "", 0, err
	}
	switch res.Status {
	case lock.StatusAcquired:
		return "acquired", res.Token, res.LeaseSec, nil
	case lock.StatusQueued:
		return "queued", "", 0, nil
	default:
		return "", "", 0, errForApplyStatus(res.Status)
	}
}
