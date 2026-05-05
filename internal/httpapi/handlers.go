package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/mtingers/dflockd/internal/lock"
	"github.com/mtingers/dflockd/internal/protocol"
)

// maxRequestBody bounds JSON bodies. 1MB is generous; valid requests
// are < 1KB. The cap protects against junk uploads.
const maxRequestBody = 1 << 20

// maxProtocolSeconds matches the protocol's seconds cap so HTTP can
// reject overflow values before they reach the protocol layer.
var maxProtocolSeconds = int64(math.MaxInt64) / int64(time.Second)

// ---------------------------------------------------------------------------
// Health, ready, stats
// ---------------------------------------------------------------------------

type statusResponse struct {
	Status string `json:"status"`
}

// GET /health — unauthenticated liveness probe.
func (h *httpServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, statusResponse{Status: "ok"})
}

// GET /ready — unauthenticated readiness probe. Reports "draining"
// during graceful shutdown.
func (h *httpServer) handleReady(w http.ResponseWriter, r *http.Request) {
	if h.draining.Load() {
		writeJSON(w, http.StatusServiceUnavailable, statusResponse{Status: "draining"})
		return
	}
	writeJSON(w, http.StatusOK, statusResponse{Status: "ok"})
}

// GET /v1/stats — direct LockManager snapshot. Includes both TCP and
// HTTP connection counts in the Connections field.
func (h *httpServer) handleStats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, h.currentStats())
}

func (h *httpServer) currentStats() *lock.Stats {
	stats := h.sessions.LockManager().Stats(h.sessions.ConnCount() + int64(h.sessions.Count()))
	for i := range stats.Locks {
		stats.Locks[i].Key = lock.StripKeyPrefix(stats.Locks[i].Key)
	}
	for i := range stats.Semaphores {
		stats.Semaphores[i].Key = lock.StripKeyPrefix(stats.Semaphores[i].Key)
	}
	for i := range stats.IdleLocks {
		stats.IdleLocks[i].Key = lock.StripKeyPrefix(stats.IdleLocks[i].Key)
	}
	for i := range stats.IdleSemaphores {
		stats.IdleSemaphores[i].Key = lock.StripKeyPrefix(stats.IdleSemaphores[i].Key)
	}
	return stats
}

// ---------------------------------------------------------------------------
// Sessions
// ---------------------------------------------------------------------------

type createSessionResponse struct {
	SessionID    string `json:"session_id"`
	IdleTimeoutS int    `json:"idle_timeout_s"`
}

// POST /v1/sessions
func (h *httpServer) handleCreateSession(w http.ResponseWriter, r *http.Request) {
	s, err := h.sessions.Create(remoteIPFromAddr(r.RemoteAddr))
	if err != nil {
		switch {
		case errors.Is(err, ErrMaxSessions):
			writeError(w, http.StatusServiceUnavailable, "max_sessions", "")
		case errors.Is(err, ErrMaxSessionsPerIP):
			writeError(w, http.StatusServiceUnavailable, "max_sessions_per_ip", "")
		case errors.Is(err, ErrShuttingDown):
			writeError(w, http.StatusServiceUnavailable, "draining", "")
		default:
			writeError(w, http.StatusInternalServerError, "session_create_failed", err.Error())
		}
		return
	}
	writeJSON(w, http.StatusOK, createSessionResponse{
		SessionID:    s.ID,
		IdleTimeoutS: int(h.sessions.IdleTimeout().Seconds()),
	})
}

// DELETE /v1/sessions/{id}
func (h *httpServer) handleDeleteSession(w http.ResponseWriter, r *http.Request, id string) {
	if err := h.sessions.Delete(id); err != nil {
		writeError(w, http.StatusGone, "session_gone", "")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// POST /v1/sessions/{id}/ping
func (h *httpServer) handlePingSession(w http.ResponseWriter, r *http.Request, id string) {
	if _, err := h.sessions.Lookup(id); err != nil {
		writeError(w, http.StatusGone, "session_gone", "")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ---------------------------------------------------------------------------
// Lock and semaphore handlers
// ---------------------------------------------------------------------------

type acquireRequest struct {
	AcquireTimeoutS int `json:"acquire_timeout_s"`
	LeaseTTLS       int `json:"lease_ttl_s,omitempty"`
}

type semAcquireRequest struct {
	AcquireTimeoutS int `json:"acquire_timeout_s"`
	Limit           int `json:"limit"`
	LeaseTTLS       int `json:"lease_ttl_s,omitempty"`
}

type releaseRequest struct {
	Token string `json:"token"`
}

type renewRequest struct {
	Token     string `json:"token"`
	LeaseTTLS int    `json:"lease_ttl_s,omitempty"`
}

type renewResponse struct {
	RemainingS int `json:"remaining_s"`
}

type enqueueRequest struct {
	LeaseTTLS int `json:"lease_ttl_s,omitempty"`
}

type semEnqueueRequest struct {
	Limit     int `json:"limit"`
	LeaseTTLS int `json:"lease_ttl_s,omitempty"`
}

type waitRequest struct {
	TimeoutS int `json:"timeout_s"`
}

// opResponse is the unified shape for acquire/enqueue/wait responses.
//
// Status: "ok" (granted), "timeout", "acquired" (two-phase fast path),
// or "queued". Token + LeaseTTLS are populated only when the caller
// now holds the slot.
type opResponse struct {
	Status    string `json:"status"`
	Token     string `json:"token,omitempty"`
	LeaseTTLS int    `json:"lease_ttl_s,omitempty"`
}

// POST /v1/locks/{key}
func (h *httpServer) handleAcquireLock(w http.ResponseWriter, r *http.Request, key string) {
	s, ok := h.sessionOrGone(w, r)
	if !ok {
		return
	}
	defer s.BeginRequest()()
	var req acquireRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if !validateSecondsField(w, "acquire_timeout_s", req.AcquireTimeoutS) {
		return
	}
	if !validateOptionalLeaseField(w, req.LeaseTTLS) {
		return
	}
	leaseTTL := h.leaseDuration(req.LeaseTTLS)
	tok, err := h.sessions.LockManager().Acquire(
		r.Context(), lock.LockPrefix+key, durSeconds(req.AcquireTimeoutS),
		leaseTTL, s.ConnID, 1)
	h.renderAcquireOutcome(w, r, s, key, lock.LockPrefix, tok, leaseTTL, err)
}

// POST /v1/locks/{key}/release
func (h *httpServer) handleReleaseLock(w http.ResponseWriter, r *http.Request, key string) {
	h.doRelease(w, r, lock.LockPrefix+key)
}

// POST /v1/locks/{key}/renew
func (h *httpServer) handleRenewLock(w http.ResponseWriter, r *http.Request, key string) {
	h.doRenew(w, r, lock.LockPrefix+key)
}

// POST /v1/locks/{key}/enqueue
func (h *httpServer) handleEnqueueLock(w http.ResponseWriter, r *http.Request, key string) {
	s, ok := h.sessionOrGone(w, r)
	if !ok {
		return
	}
	defer s.BeginRequest()()
	var req enqueueRequest
	if !decodeOptionalJSON(w, r, &req) {
		return
	}
	if !validateOptionalLeaseField(w, req.LeaseTTLS) {
		return
	}
	leaseTTL := h.leaseDuration(req.LeaseTTLS)
	status, tok, leaseSec, err := h.sessions.LockManager().Enqueue(
		lock.LockPrefix+key, leaseTTL, s.ConnID, 1)
	h.renderEnqueueOutcome(w, r, s, key, lock.LockPrefix, status, tok, leaseSec, err)
}

// POST /v1/locks/{key}/wait
func (h *httpServer) handleWaitLock(w http.ResponseWriter, r *http.Request, key string) {
	h.doWait(w, r, lock.LockPrefix+key)
}

// POST /v1/semaphores/{key}
func (h *httpServer) handleAcquireSem(w http.ResponseWriter, r *http.Request, key string) {
	s, ok := h.sessionOrGone(w, r)
	if !ok {
		return
	}
	defer s.BeginRequest()()
	var req semAcquireRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if !validateSecondsField(w, "acquire_timeout_s", req.AcquireTimeoutS) {
		return
	}
	if req.Limit <= 0 {
		writeError(w, http.StatusBadRequest, "bad_request", "limit must be > 0")
		return
	}
	if !validateOptionalLeaseField(w, req.LeaseTTLS) {
		return
	}
	leaseTTL := h.leaseDuration(req.LeaseTTLS)
	tok, err := h.sessions.LockManager().Acquire(
		r.Context(), lock.SemPrefix+key, durSeconds(req.AcquireTimeoutS),
		leaseTTL, s.ConnID, req.Limit)
	h.renderAcquireOutcome(w, r, s, key, lock.SemPrefix, tok, leaseTTL, err)
}

// POST /v1/semaphores/{key}/release
func (h *httpServer) handleReleaseSem(w http.ResponseWriter, r *http.Request, key string) {
	h.doRelease(w, r, lock.SemPrefix+key)
}

// POST /v1/semaphores/{key}/renew
func (h *httpServer) handleRenewSem(w http.ResponseWriter, r *http.Request, key string) {
	h.doRenew(w, r, lock.SemPrefix+key)
}

// POST /v1/semaphores/{key}/enqueue
func (h *httpServer) handleEnqueueSem(w http.ResponseWriter, r *http.Request, key string) {
	s, ok := h.sessionOrGone(w, r)
	if !ok {
		return
	}
	defer s.BeginRequest()()
	var req semEnqueueRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Limit <= 0 {
		writeError(w, http.StatusBadRequest, "bad_request", "limit must be > 0")
		return
	}
	if !validateOptionalLeaseField(w, req.LeaseTTLS) {
		return
	}
	leaseTTL := h.leaseDuration(req.LeaseTTLS)
	status, tok, leaseSec, err := h.sessions.LockManager().Enqueue(
		lock.SemPrefix+key, leaseTTL, s.ConnID, req.Limit)
	h.renderEnqueueOutcome(w, r, s, key, lock.SemPrefix, status, tok, leaseSec, err)
}

// POST /v1/semaphores/{key}/wait
func (h *httpServer) handleWaitSem(w http.ResponseWriter, r *http.Request, key string) {
	h.doWait(w, r, lock.SemPrefix+key)
}

// ---------------------------------------------------------------------------
// Shared handler helpers
// ---------------------------------------------------------------------------

// sessionOrGone resolves the X-Dflockd-Session header to a Session,
// writing a 410 if missing/unknown. Returns (session, ok).
func (h *httpServer) sessionOrGone(w http.ResponseWriter, r *http.Request) (*Session, bool) {
	id := r.Header.Get("X-Dflockd-Session")
	if id == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "X-Dflockd-Session header required")
		return nil, false
	}
	s, err := h.sessions.Lookup(id)
	if err != nil {
		writeError(w, http.StatusGone, "session_gone", "")
		return nil, false
	}
	return s, true
}

// leaseDuration picks the request's lease TTL or falls back to the
// configured default.
func (h *httpServer) leaseDuration(reqLease int) time.Duration {
	if reqLease > 0 {
		return time.Duration(reqLease) * time.Second
	}
	return h.cfg.DefaultLeaseTTL
}

// renderAcquireOutcome writes the response for a single-phase acquire,
// handling the disconnect-cleanup race where lm.Acquire returns a token
// but the client is already gone.
func (h *httpServer) renderAcquireOutcome(w http.ResponseWriter, r *http.Request, s *Session, key, prefix, tok string, leaseTTL time.Duration, err error) {
	if err != nil {
		// Context cancellation = client disconnected. Don't bother
		// writing a body the client can't read.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		writeLockErr(w, err)
		return
	}
	if tok == "" {
		// Acquire timeout fired (no error, no token).
		writeJSON(w, http.StatusOK, opResponse{Status: "timeout"})
		return
	}
	if r.Context().Err() != nil {
		// Race: granted but client already disconnected. Release so
		// the slot doesn't sit until lease expiry.
		_ = h.sessions.LockManager().Release(prefix+key, tok)
		return
	}
	writeJSON(w, http.StatusOK, opResponse{
		Status:    "ok",
		Token:     tok,
		LeaseTTLS: int(leaseTTL.Seconds()),
	})
}

// renderEnqueueOutcome writes the response for a two-phase enqueue.
func (h *httpServer) renderEnqueueOutcome(w http.ResponseWriter, r *http.Request, s *Session, key, prefix, status, tok string, leaseSec int, err error) {
	if err != nil {
		writeLockErr(w, err)
		return
	}
	if r.Context().Err() != nil {
		// Client gone before we could respond. Clean up whichever
		// state Enqueue produced.
		switch status {
		case "acquired":
			_ = h.sessions.LockManager().Release(prefix+key, tok)
		case "queued":
			// Issue a zero-timeout Wait to dequeue the waiter. Wait
			// can still return a token if the waiter was promoted to
			// holder between Enqueue and now (fast path or grant
			// drain on timeout) — capture and release it instead of
			// stranding the slot until lease expiry.
			cleanupTok, _, _ := h.sessions.LockManager().Wait(context.Background(), prefix+key, 0, s.ConnID)
			if cleanupTok != "" {
				_ = h.sessions.LockManager().Release(prefix+key, cleanupTok)
			}
		}
		return
	}
	writeJSON(w, http.StatusOK, opResponse{
		Status:    status,
		Token:     tok,
		LeaseTTLS: leaseSec,
	})
}

// doRelease handles both lock and semaphore release.
func (h *httpServer) doRelease(w http.ResponseWriter, r *http.Request, prefixedKey string) {
	s, ok := h.sessionOrGone(w, r)
	if !ok {
		return
	}
	defer s.BeginRequest()()
	var req releaseRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Token == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "token required")
		return
	}
	if err := validateProtocolField("token", req.Token); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if h.sessions.LockManager().Release(prefixedKey, req.Token) {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeError(w, http.StatusNotFound, "not_held", "")
}

// doRenew handles both lock and semaphore renew.
func (h *httpServer) doRenew(w http.ResponseWriter, r *http.Request, prefixedKey string) {
	s, ok := h.sessionOrGone(w, r)
	if !ok {
		return
	}
	defer s.BeginRequest()()
	var req renewRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Token == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "token required")
		return
	}
	if err := validateProtocolField("token", req.Token); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if !validateOptionalLeaseField(w, req.LeaseTTLS) {
		return
	}
	leaseTTL := h.leaseDuration(req.LeaseTTLS)
	remaining, ok := h.sessions.LockManager().Renew(prefixedKey, req.Token, leaseTTL)
	if !ok {
		writeError(w, http.StatusNotFound, "not_held", "")
		return
	}
	writeJSON(w, http.StatusOK, renewResponse{RemainingS: remaining})
}

// doWait handles both lock and semaphore wait.
func (h *httpServer) doWait(w http.ResponseWriter, r *http.Request, prefixedKey string) {
	s, ok := h.sessionOrGone(w, r)
	if !ok {
		return
	}
	defer s.BeginRequest()()
	var req waitRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if !validateSecondsField(w, "timeout_s", req.TimeoutS) {
		return
	}
	tok, leaseSec, err := h.sessions.LockManager().Wait(
		r.Context(), prefixedKey, durSeconds(req.TimeoutS), s.ConnID)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		writeLockErr(w, err)
		return
	}
	if tok == "" {
		writeJSON(w, http.StatusOK, opResponse{Status: "timeout"})
		return
	}
	if r.Context().Err() != nil {
		_ = h.sessions.LockManager().Release(prefixedKey, tok)
		return
	}
	writeJSON(w, http.StatusOK, opResponse{
		Status:    "ok",
		Token:     tok,
		LeaseTTLS: leaseSec,
	})
}

// ---------------------------------------------------------------------------
// Decoding and validation
// ---------------------------------------------------------------------------

func decodeJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	return decodeJSONBody(w, r, v, false)
}

func decodeOptionalJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	return decodeJSONBody(w, r, v, true)
}

// decodeJSONBody reads exactly one JSON value from r.Body. allowEmpty
// permits a missing/empty body. Unknown fields are rejected to catch
// caller typos at request time rather than via silent drops.
func decodeJSONBody(w http.ResponseWriter, r *http.Request, v any, allowEmpty bool) bool {
	if r.Body == nil {
		if allowEmpty {
			return true
		}
		writeError(w, http.StatusBadRequest, "bad_request", "missing body")
		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		if allowEmpty && errors.Is(err, io.EOF) {
			return true
		}
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return false
	}
	var extra any
	if err := dec.Decode(&extra); err != nil {
		if errors.Is(err, io.EOF) {
			return true
		}
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return false
	}
	writeError(w, http.StatusBadRequest, "bad_request", "body must contain a single JSON value")
	return false
}

// validateSecondsField rejects negative or overflow seconds values
// before we hand them to the LockManager.
func validateSecondsField(w http.ResponseWriter, name string, value int) bool {
	if value < 0 {
		writeError(w, http.StatusBadRequest, "bad_request", fmt.Sprintf("%s must be >= 0", name))
		return false
	}
	if int64(value) > maxProtocolSeconds {
		writeError(w, http.StatusBadRequest, "bad_request", fmt.Sprintf("%s too large", name))
		return false
	}
	return true
}

// validateOptionalLeaseField allows a missing (zero) lease, but if
// provided it must be non-negative and within range. The 0-means-default
// rule is documented in the request structs.
func validateOptionalLeaseField(w http.ResponseWriter, value int) bool {
	return validateSecondsField(w, "lease_ttl_s", value)
}

// validateRESTKey is the HTTP equivalent of protocol.validateKey. We
// re-implement it here so a malformed key can short-circuit before any
// LockManager call.
func validateRESTKey(k string) error {
	if k == "" {
		return fmt.Errorf("empty key")
	}
	if len(k) > protocol.MaxLineBytes {
		return fmt.Errorf("key too long (max %d bytes)", protocol.MaxLineBytes)
	}
	if strings.ContainsAny(k, " \t\n\r") {
		return fmt.Errorf("key contains whitespace")
	}
	return nil
}

// validateProtocolField is the same validation applied to non-key
// argument fields (tokens etc.).
func validateProtocolField(name, value string) error {
	if len(value) > protocol.MaxLineBytes {
		return fmt.Errorf("%s too long (max %d bytes)", name, protocol.MaxLineBytes)
	}
	if strings.ContainsAny(value, " \t\n\r") {
		return fmt.Errorf("%s contains whitespace", name)
	}
	return nil
}

func durSeconds(s int) time.Duration {
	return time.Duration(s) * time.Second
}
