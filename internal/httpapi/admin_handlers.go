package httpapi

import (
	"crypto/subtle"
	"net/http"
	"strings"
	"time"

	"github.com/mtingers/dflockd/internal/server"
)

// Admin endpoints: cluster reconfiguration over HTTP. Distinct from the
// per-session auth — the admin token is a *separate* secret in the
// dflockd config so client credentials (read/write workloads) don't
// carry reconfiguration authority. Default-deny: no admin token →
// every admin call returns 503 admin_disabled.

const adminAuthHeader = "X-Dflockd-Admin"

// adminAddVoterReq is the JSON body of POST /v1/admin/voters.
type adminAddVoterReq struct {
	NodeID     string `json:"node_id"`
	RaftAddr   string `json:"raft_addr"`
	ClientAddr string `json:"client_addr"`
}

// adminVoterResp acknowledges a successful reconfig.
type adminVoterResp struct {
	Status string `json:"status"`
	NodeID string `json:"node_id"`
}

// handleReadIndex proposes a no-op barrier through Raft and returns 200
// once it applies. The semantics: every preceding write that committed
// against this leader is reflected in state observable on this leader
// after the call returns. Follower → 503 not_leader.
func (h *httpServer) handleReadIndex(w http.ResponseWriter, r *http.Request) {
	srv := h.sessions.Server()
	if !srv.IsClusterMode() {
		writeError(w, http.StatusNotFound, "cluster_disabled", "this server is not in cluster mode")
		return
	}
	if err := srv.ClusterBarrier(r.Context()); err != nil {
		if h.handleClusterNotLeader(w, srv, err) {
			return
		}
		writeError(w, http.StatusInternalServerError, "internal", "")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleAdminAddVoter adds a voting member to the cluster. Body:
// {"node_id","raft_addr","client_addr"}. Requires X-Dflockd-Admin to
// match the configured admin token. Returns 503 not_leader on a
// follower, 503 admin_disabled when no admin token is configured.
func (h *httpServer) handleAdminAddVoter(w http.ResponseWriter, r *http.Request) {
	if !h.checkAdminAuth(w, r) {
		return
	}
	srv := h.sessions.Server()
	if !srv.IsClusterMode() {
		writeError(w, http.StatusNotFound, "cluster_disabled", "this server is not in cluster mode")
		return
	}
	req, ok := decodeAddVoterReq(w, r)
	if !ok {
		return
	}
	if err := srv.ClusterAddVoter(r.Context(), req.NodeID, req.RaftAddr, req.ClientAddr); err != nil {
		if h.handleClusterNotLeader(w, srv, err) {
			return
		}
		writeError(w, http.StatusBadRequest, "add_voter_failed", err.Error())
		return
	}
	h.log.Info("admin: add voter", "node_id", req.NodeID, "raft_addr", req.RaftAddr, "remote", r.RemoteAddr)
	writeJSON(w, http.StatusOK, adminVoterResp{Status: "ok", NodeID: req.NodeID})
}

// handleAdminRemoveVoter removes a voting member from the cluster.
// Path: /v1/admin/voters/{id}. Auth and follower semantics match
// handleAdminAddVoter.
func (h *httpServer) handleAdminRemoveVoter(w http.ResponseWriter, r *http.Request) {
	if !h.checkAdminAuth(w, r) {
		return
	}
	srv := h.sessions.Server()
	if !srv.IsClusterMode() {
		writeError(w, http.StatusNotFound, "cluster_disabled", "this server is not in cluster mode")
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "missing node id in path")
		return
	}
	if err := srv.ClusterRemoveVoter(r.Context(), id); err != nil {
		if h.handleClusterNotLeader(w, srv, err) {
			return
		}
		writeError(w, http.StatusBadRequest, "remove_voter_failed", err.Error())
		return
	}
	h.log.Info("admin: remove voter", "node_id", id, "remote", r.RemoteAddr)
	writeJSON(w, http.StatusOK, adminVoterResp{Status: "ok", NodeID: id})
}

// checkAdminAuth enforces admin authority. Writes the response and
// returns false on failure.
//
//   - Empty configured token → 503 admin_disabled (default-deny).
//   - Missing/mismatched header → 401 unauthorized (timed at 100ms
//     to match the regular auth-failure slowdown).
//   - OK → returns true.
func (h *httpServer) checkAdminAuth(w http.ResponseWriter, r *http.Request) bool {
	want := h.cfg.AdminToken
	if want == "" {
		writeError(w, http.StatusServiceUnavailable, "admin_disabled", "admin endpoints require --admin-token / DFLOCKD_ADMIN_TOKEN")
		return false
	}
	got := r.Header.Get(adminAuthHeader)
	if got == "" || subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
		rejectAdminAuth(w)
		return false
	}
	return true
}

// rejectAdminAuth writes the 401 with a slowdown matching the regular
// auth failure path so the timing channel is the same as a normal
// per-session auth miss.
func rejectAdminAuth(w http.ResponseWriter) {
	time.Sleep(authFailureDelay)
	writeError(w, http.StatusUnauthorized, "admin_unauthorized", "")
}

// decodeAddVoterReq validates the AddVoter request body. Returns ok=false
// if the request shape is invalid (and writes the 400 response).
func decodeAddVoterReq(w http.ResponseWriter, r *http.Request) (adminAddVoterReq, bool) {
	var req adminAddVoterReq
	if !decodeJSON(w, r, &req) {
		return adminAddVoterReq{}, false
	}
	if req.NodeID == "" || req.RaftAddr == "" || req.ClientAddr == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "node_id, raft_addr, and client_addr are all required")
		return adminAddVoterReq{}, false
	}
	if !isPlainAddr(req.RaftAddr) || !isPlainAddr(req.ClientAddr) {
		writeError(w, http.StatusBadRequest, "bad_request", "raft_addr / client_addr must be host:port")
		return adminAddVoterReq{}, false
	}
	return req, true
}

// isPlainAddr is a deliberately loose host:port check — it only rejects
// shapes that are obviously wrong (empty halves, no colon). The transport
// will reject anything net.Dial-incompatible.
func isPlainAddr(s string) bool {
	colon := strings.LastIndexByte(s, ':')
	if colon <= 0 || colon == len(s)-1 {
		return false
	}
	for _, c := range s[colon+1:] {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// Compile-time check: this file uses the canonical not-leader sentinel
// (via handleClusterNotLeader). Catches the symbol getting renamed.
var _ = server.ErrNotClusterLeader
