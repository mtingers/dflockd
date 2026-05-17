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

// maxRequestBody bounds JSON bodies; valid requests are < 1KB.
const maxRequestBody = 1 << 20

// maxProtocolSeconds matches the protocol's seconds cap.
var maxProtocolSeconds = int64(math.MaxInt64) / int64(time.Second)

// ---------------------------------------------------------------------------
// Health, ready, stats
// ---------------------------------------------------------------------------

type statusResponse struct {
	Status string `json:"status"`
}

func (h *httpServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, statusResponse{Status: "ok"})
}

func (h *httpServer) handleReady(w http.ResponseWriter, r *http.Request) {
	if h.draining.Load() {
		writeJSON(w, http.StatusServiceUnavailable, statusResponse{Status: "draining"})
		return
	}
	writeJSON(w, http.StatusOK, statusResponse{Status: "ok"})
}

func (h *httpServer) handleStats(w http.ResponseWriter, r *http.Request) {
	stats := h.currentStats()
	if cj := h.sessions.Server().ClusterStatusJSON(); cj != nil {
		// Splice a "cluster" object alongside the lock stats (single-node
		// output is unchanged), matching the TCP `stats` command.
		writeJSON(w, http.StatusOK, struct {
			*lock.Stats
			Cluster json.RawMessage `json:"cluster"`
		}{stats, cj})
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

// handleOpenAPI serves the embedded OpenAPI 3.1 contract. Unauthenticated
// (the spec describes auth, so requiring auth to read it would be circular).
func (h *httpServer) handleOpenAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "public, max-age=60")
	_, _ = w.Write(openAPISpec)
}

func (h *httpServer) currentStats() *lock.Stats {
	// Use the server's unified counter so /v1/stats and the TCP
	// "stats" command always report the same connections number.
	stats := h.sessions.LockManager().Stats(h.sessions.TotalConnCount())
	stripStatsPrefixes(stats)
	return stats
}

// stripStatsPrefixes turns internal lock:/sem: keys back into user keys.
func stripStatsPrefixes(s *lock.Stats) {
	stripLocks(s.Locks)
	stripSems(s.Semaphores)
	stripIdle(s.IdleLocks)
	stripIdle(s.IdleSemaphores)
}

func stripLocks(xs []lock.LockInfo) {
	for i := range xs {
		xs[i].Key = lock.StripKeyPrefix(xs[i].Key)
	}
}

func stripSems(xs []lock.SemInfo) {
	for i := range xs {
		xs[i].Key = lock.StripKeyPrefix(xs[i].Key)
	}
}

func stripIdle(xs []lock.IdleInfo) {
	for i := range xs {
		xs[i].Key = lock.StripKeyPrefix(xs[i].Key)
	}
}

// ---------------------------------------------------------------------------
// Sessions
// ---------------------------------------------------------------------------

type createSessionResponse struct {
	SessionID    string `json:"session_id"`
	IdleTimeoutS int    `json:"idle_timeout_s"`
}

func (h *httpServer) handleCreateSession(w http.ResponseWriter, r *http.Request) {
	s, err := h.sessions.Create(remoteIPFromAddr(r.RemoteAddr))
	if err != nil {
		h.writeCreateSessionErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, createSessionResponse{
		SessionID: s.ID, IdleTimeoutS: int(h.sessions.IdleTimeout().Seconds()),
	})
}

// writeCreateSessionErr maps Create errors to HTTP responses. Known
// sentinel errors get a stable user-facing code with no detail; an
// unexpected error is logged server-side and surfaces as a generic 500
// so the client can't probe internal state via error-message diffs.
func (h *httpServer) writeCreateSessionErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrMaxSessions):
		writeError(w, http.StatusServiceUnavailable, "max_sessions", "")
	case errors.Is(err, ErrMaxSessionsPerIP):
		writeError(w, http.StatusServiceUnavailable, "max_sessions_per_ip", "")
	case errors.Is(err, ErrShuttingDown):
		writeError(w, http.StatusServiceUnavailable, "draining", "")
	default:
		h.log.Error("session create failed", "err", err)
		writeError(w, http.StatusInternalServerError, "session_create_failed", "")
	}
}

func (h *httpServer) handleDeleteSession(w http.ResponseWriter, r *http.Request, id string) {
	if err := h.sessions.Delete(id); err != nil {
		if errors.Is(err, ErrSessionGone) {
			writeError(w, http.StatusGone, "session_gone", "")
			return
		}
		writeLockErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *httpServer) handlePingSession(w http.ResponseWriter, r *http.Request, id string) {
	if _, err := h.sessions.Lookup(id); err != nil {
		writeError(w, http.StatusGone, "session_gone", "")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ---------------------------------------------------------------------------
// Lock and semaphore handlers — thin wrappers over shared serve* funcs
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
type opResponse struct {
	Status    string `json:"status"`
	Token     string `json:"token,omitempty"`
	LeaseTTLS int    `json:"lease_ttl_s,omitempty"`
}

func (h *httpServer) handleAcquireLock(w http.ResponseWriter, r *http.Request, key string) {
	h.serveAcquireLock(w, r, key)
}

func (h *httpServer) handleReleaseLock(w http.ResponseWriter, r *http.Request, key string) {
	h.serveRelease(w, r, lock.LockPrefix+key)
}

func (h *httpServer) handleRenewLock(w http.ResponseWriter, r *http.Request, key string) {
	h.serveRenew(w, r, lock.LockPrefix+key)
}

func (h *httpServer) handleEnqueueLock(w http.ResponseWriter, r *http.Request, key string) {
	h.serveEnqueueLock(w, r, key)
}

func (h *httpServer) handleWaitLock(w http.ResponseWriter, r *http.Request, key string) {
	h.serveWait(w, r, lock.LockPrefix+key)
}

func (h *httpServer) handleAcquireSem(w http.ResponseWriter, r *http.Request, key string) {
	h.serveAcquireSem(w, r, key)
}

func (h *httpServer) handleReleaseSem(w http.ResponseWriter, r *http.Request, key string) {
	h.serveRelease(w, r, lock.SemPrefix+key)
}

func (h *httpServer) handleRenewSem(w http.ResponseWriter, r *http.Request, key string) {
	h.serveRenew(w, r, lock.SemPrefix+key)
}

func (h *httpServer) handleEnqueueSem(w http.ResponseWriter, r *http.Request, key string) {
	h.serveEnqueueSem(w, r, key)
}

func (h *httpServer) handleWaitSem(w http.ResponseWriter, r *http.Request, key string) {
	h.serveWait(w, r, lock.SemPrefix+key)
}

// ---------------------------------------------------------------------------
// serve* — orchestrate per-endpoint flow
// ---------------------------------------------------------------------------

func (h *httpServer) serveAcquireLock(w http.ResponseWriter, r *http.Request, key string) {
	s, req, done, ok := preludeJSON(h, w, r, validateAcquire)
	if !ok {
		return
	}
	defer done()
	h.runAcquire(w, r, s, key, lock.LockPrefix, 1, req.AcquireTimeoutS, req.LeaseTTLS)
}

func (h *httpServer) serveAcquireSem(w http.ResponseWriter, r *http.Request, key string) {
	s, req, done, ok := preludeJSON(h, w, r, validateSemAcquire)
	if !ok {
		return
	}
	defer done()
	h.runAcquire(w, r, s, key, lock.SemPrefix, req.Limit, req.AcquireTimeoutS, req.LeaseTTLS)
}

func (h *httpServer) serveEnqueueLock(w http.ResponseWriter, r *http.Request, key string) {
	s, req, done, ok := preludeOptionalJSON(h, w, r, validateEnqueue)
	if !ok {
		return
	}
	defer done()
	h.runEnqueue(w, r, s, key, lock.LockPrefix, 1, req.LeaseTTLS)
}

func (h *httpServer) serveEnqueueSem(w http.ResponseWriter, r *http.Request, key string) {
	s, req, done, ok := preludeJSON(h, w, r, validateSemEnqueue)
	if !ok {
		return
	}
	defer done()
	h.runEnqueue(w, r, s, key, lock.SemPrefix, req.Limit, req.LeaseTTLS)
}

func (h *httpServer) serveWait(w http.ResponseWriter, r *http.Request, prefixedKey string) {
	s, req, done, ok := preludeJSON(h, w, r, validateWait)
	if !ok {
		return
	}
	defer done()
	h.runWait(w, r, s, prefixedKey, req.TimeoutS)
}

func (h *httpServer) serveRelease(w http.ResponseWriter, r *http.Request, prefixedKey string) {
	_, req, done, ok := preludeJSON(h, w, r, validateRelease)
	if !ok {
		return
	}
	defer done()
	h.runRelease(w, r, prefixedKey, req.Token)
}

func (h *httpServer) serveRenew(w http.ResponseWriter, r *http.Request, prefixedKey string) {
	_, req, done, ok := preludeJSON(h, w, r, validateRenew)
	if !ok {
		return
	}
	defer done()
	h.runRenew(w, r, prefixedKey, req.Token, req.LeaseTTLS)
}

// ---------------------------------------------------------------------------
// run* — call LockManager and render result
// ---------------------------------------------------------------------------

func (h *httpServer) runAcquire(w http.ResponseWriter, r *http.Request, s *Session, key, prefix string, limit, timeoutS, leaseS int) {
	if h.sessions.Server().IsClusterMode() {
		h.runAcquireCluster(w, r, s, key, prefix, limit, timeoutS, leaseS)
		return
	}
	leaseTTL := h.leaseDuration(leaseS)
	ctx, cancel := s.RequestContext(r.Context())
	defer cancel()
	tok, err := h.sessions.LockManager().Acquire(
		ctx, prefix+key, durSeconds(timeoutS), leaseTTL, s.ConnID, limit)
	h.renderAcquireOutcome(w, r, key, prefix, tok, leaseTTL, err)
}

func (h *httpServer) runEnqueue(w http.ResponseWriter, r *http.Request, s *Session, key, prefix string, limit, leaseS int) {
	if h.sessions.Server().IsClusterMode() {
		h.runEnqueueCluster(w, r, s, key, prefix, limit, leaseS)
		return
	}
	leaseTTL := h.leaseDuration(leaseS)
	status, tok, leaseSec, err := h.sessions.LockManager().Enqueue(prefix+key, leaseTTL, s.ConnID, limit)
	h.renderEnqueueOutcome(w, r, s, key, prefix, status, tok, leaseSec, err)
}

func (h *httpServer) runWait(w http.ResponseWriter, r *http.Request, s *Session, prefixedKey string, timeoutS int) {
	if h.sessions.Server().IsClusterMode() {
		h.runWaitCluster(w, r, s, prefixedKey, timeoutS)
		return
	}
	ctx, cancel := s.RequestContext(r.Context())
	defer cancel()
	tok, leaseSec, err := h.sessions.LockManager().Wait(
		ctx, prefixedKey, durSeconds(timeoutS), s.ConnID)
	h.renderWaitOutcome(w, r, prefixedKey, tok, leaseSec, err)
}

func (h *httpServer) runRelease(w http.ResponseWriter, r *http.Request, prefixedKey, token string) {
	if h.sessions.Server().IsClusterMode() {
		h.runReleaseCluster(w, r, prefixedKey, token)
		return
	}
	ok, err := h.sessions.LockManager().Release(prefixedKey, token)
	if err != nil {
		writeLockErr(w, err)
		return
	}
	if ok {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeError(w, http.StatusNotFound, "not_held", "")
}

func (h *httpServer) runRenew(w http.ResponseWriter, r *http.Request, prefixedKey, token string, leaseS int) {
	if h.sessions.Server().IsClusterMode() {
		h.runRenewCluster(w, r, prefixedKey, token, leaseS)
		return
	}
	leaseTTL := h.leaseDuration(leaseS)
	remaining, ok, err := h.sessions.LockManager().Renew(prefixedKey, token, leaseTTL)
	if err != nil {
		writeLockErr(w, err)
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, "not_held", "")
		return
	}
	writeJSON(w, http.StatusOK, renewResponse{RemainingS: remaining})
}

// ---------------------------------------------------------------------------
// render* — split outcome paths
// ---------------------------------------------------------------------------

func (h *httpServer) renderAcquireOutcome(w http.ResponseWriter, r *http.Request, key, prefix, tok string, leaseTTL time.Duration, err error) {
	if err != nil {
		renderLockErr(w, r, err)
		return
	}
	if tok == "" {
		renderTimeout(w)
		return
	}
	if r.Context().Err() != nil {
		h.bestEffortRelease(prefix+key, tok)
		return
	}
	renderToken(w, "ok", tok, int(leaseTTL.Seconds()))
}

func (h *httpServer) renderEnqueueOutcome(w http.ResponseWriter, r *http.Request, s *Session, key, prefix, status, tok string, leaseSec int, err error) {
	if err != nil {
		writeLockErr(w, err)
		return
	}
	if r.Context().Err() != nil {
		h.cleanupCanceledEnqueue(s, prefix, key, status, tok)
		return
	}
	renderToken(w, status, tok, leaseSec)
}

func (h *httpServer) renderWaitOutcome(w http.ResponseWriter, r *http.Request, prefixedKey, tok string, leaseSec int, err error) {
	if err != nil {
		renderLockErr(w, r, err)
		return
	}
	if tok == "" {
		renderTimeout(w)
		return
	}
	if r.Context().Err() != nil {
		h.bestEffortRelease(prefixedKey, tok)
		return
	}
	renderToken(w, "ok", tok, leaseSec)
}

// renderLockErr writes the appropriate response for a LockManager error.
// If the HTTP client is already gone there is no useful response to
// write; if only the session context was cancelled, surface the
// documented session_gone contract.
func renderLockErr(w http.ResponseWriter, r *http.Request, err error) {
	if !isContextErr(err) {
		writeLockErr(w, err)
		return
	}
	if r.Context().Err() != nil {
		return
	}
	writeError(w, http.StatusGone, "session_gone", "")
}

func isContextErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func renderTimeout(w http.ResponseWriter) {
	writeJSON(w, http.StatusOK, opResponse{Status: "timeout"})
}

func renderToken(w http.ResponseWriter, status, tok string, leaseSec int) {
	writeJSON(w, http.StatusOK, opResponse{Status: status, Token: tok, LeaseTTLS: leaseSec})
}

// cleanupCanceledEnqueue handles both the "acquired fast path" and the
// "queued + then promoted" cleanup variants.
func (h *httpServer) cleanupCanceledEnqueue(s *Session, prefix, key, status, tok string) {
	switch status {
	case "acquired":
		h.bestEffortRelease(prefix+key, tok)
	case "queued":
		h.dequeueAfterPromote(s, prefix, key)
	}
}

// dequeueAfterPromote issues a 0-timeout Wait. If the waiter was
// promoted between Enqueue and now, Wait returns the granted token;
// release it instead of stranding the slot.
func (h *httpServer) dequeueAfterPromote(s *Session, prefix, key string) {
	cleanupTok, _, err := h.sessions.LockManager().Wait(context.Background(), prefix+key, 0, s.ConnID)
	if err != nil {
		h.log.Warn("dequeue-after-promote wait failed", "key", prefix+key, "err", err)
		return
	}
	if cleanupTok != "" {
		h.bestEffortRelease(prefix+key, cleanupTok)
	}
}

// bestEffortRelease releases a token on a cleanup path that has no
// response left to write. A failure here (e.g. fence persistence) is
// logged so operators see it; the lease-expiry sweep reclaims the slot.
func (h *httpServer) bestEffortRelease(prefixedKey, tok string) {
	if _, err := h.sessions.LockManager().Release(prefixedKey, tok); err != nil {
		h.log.Warn("best-effort release failed", "key", prefixedKey, "err", err)
	}
}

// ---------------------------------------------------------------------------
// Prelude helpers — shared session/decode/validate/claim flow
// ---------------------------------------------------------------------------

// preludeJSON reads + validates the JSON body, then claims the session.
// On any failure it writes the response itself and returns ok=false.
func preludeJSON[T any](h *httpServer, w http.ResponseWriter, r *http.Request, validate func(http.ResponseWriter, *T) bool) (*Session, T, func(), bool) {
	return preludeBody(h, w, r, validate, false)
}

// preludeOptionalJSON is preludeJSON but treats a missing body as {} —
// used by enqueue, where the body is optional.
func preludeOptionalJSON[T any](h *httpServer, w http.ResponseWriter, r *http.Request, validate func(http.ResponseWriter, *T) bool) (*Session, T, func(), bool) {
	return preludeBody(h, w, r, validate, true)
}

// preludeBody is the shared implementation. allowEmpty toggles whether
// a missing body is OK. The flow is: session → body → validate → claim.
func preludeBody[T any](h *httpServer, w http.ResponseWriter, r *http.Request, validate func(http.ResponseWriter, *T) bool, allowEmpty bool) (*Session, T, func(), bool) {
	s, req, ok := readSessionAndBody[T](h, w, r, allowEmpty, validate)
	if !ok {
		var zero T
		return nil, zero, nil, false
	}
	return finalizeClaim(w, s, req)
}

// readSessionAndBody resolves the session header, decodes the body,
// and runs the validator. Returns ok=false if any step writes a
// response.
func readSessionAndBody[T any](h *httpServer, w http.ResponseWriter, r *http.Request, allowEmpty bool, validate func(http.ResponseWriter, *T) bool) (*Session, T, bool) {
	var req T
	s, ok := h.sessionOrGone(w, r)
	if !ok {
		return nil, req, false
	}
	if !decodeJSONBody(w, r, &req, allowEmpty) {
		return nil, req, false
	}
	if validate != nil && !validate(w, &req) {
		return nil, req, false
	}
	return s, req, true
}

// finalizeClaim is the BeginRequest step common to every prelude.
func finalizeClaim[T any](w http.ResponseWriter, s *Session, req T) (*Session, T, func(), bool) {
	done, ok := s.BeginRequest()
	if !ok {
		var zero T
		writeError(w, http.StatusGone, "session_gone", "")
		return nil, zero, nil, false
	}
	return s, req, done, true
}

// sessionOrGone resolves the X-Dflockd-Session header.
func (h *httpServer) sessionOrGone(w http.ResponseWriter, r *http.Request) (*Session, bool) {
	id := r.Header.Get("X-Dflockd-Session")
	if id == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "X-Dflockd-Session header required")
		return nil, false
	}
	return lookupOrGone(w, h.sessions, id)
}

func lookupOrGone(w http.ResponseWriter, ss *SessionStore, id string) (*Session, bool) {
	s, err := ss.Lookup(id)
	if err != nil {
		writeError(w, http.StatusGone, "session_gone", "")
		return nil, false
	}
	return s, true
}

func (h *httpServer) leaseDuration(reqLease int) time.Duration {
	if reqLease > 0 {
		return time.Duration(reqLease) * time.Second
	}
	return h.cfg.DefaultLeaseTTL
}

// ---------------------------------------------------------------------------
// Per-request validators
// ---------------------------------------------------------------------------

func validateAcquire(w http.ResponseWriter, req *acquireRequest) bool {
	return validateSecondsField(w, "acquire_timeout_s", req.AcquireTimeoutS) &&
		validateOptionalLeaseField(w, req.LeaseTTLS)
}

func validateSemAcquire(w http.ResponseWriter, req *semAcquireRequest) bool {
	if !validateSecondsField(w, "acquire_timeout_s", req.AcquireTimeoutS) {
		return false
	}
	if !validatePositiveLimit(w, req.Limit) {
		return false
	}
	return validateOptionalLeaseField(w, req.LeaseTTLS)
}

func validateEnqueue(w http.ResponseWriter, req *enqueueRequest) bool {
	return validateOptionalLeaseField(w, req.LeaseTTLS)
}

func validateSemEnqueue(w http.ResponseWriter, req *semEnqueueRequest) bool {
	if !validatePositiveLimit(w, req.Limit) {
		return false
	}
	return validateOptionalLeaseField(w, req.LeaseTTLS)
}

func validateWait(w http.ResponseWriter, req *waitRequest) bool {
	return validateSecondsField(w, "timeout_s", req.TimeoutS)
}

func validateRelease(w http.ResponseWriter, req *releaseRequest) bool {
	return validateNonEmptyToken(w, req.Token)
}

func validateRenew(w http.ResponseWriter, req *renewRequest) bool {
	if !validateNonEmptyToken(w, req.Token) {
		return false
	}
	return validateOptionalLeaseField(w, req.LeaseTTLS)
}

func validatePositiveLimit(w http.ResponseWriter, limit int) bool {
	if limit <= 0 {
		writeError(w, http.StatusBadRequest, "bad_request", "limit must be > 0")
		return false
	}
	return true
}

func validateNonEmptyToken(w http.ResponseWriter, token string) bool {
	if token == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "token required")
		return false
	}
	if err := validateProtocolField("token", token); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return false
	}
	return true
}

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

func validateOptionalLeaseField(w http.ResponseWriter, value int) bool {
	return validateSecondsField(w, "lease_ttl_s", value)
}

// ---------------------------------------------------------------------------
// JSON body decoding
// ---------------------------------------------------------------------------

func decodeJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	return decodeJSONBody(w, r, v, false)
}

func decodeJSONBody(w http.ResponseWriter, r *http.Request, v any, allowEmpty bool) bool {
	if r.Body == nil {
		return handleNoBody(w, allowEmpty)
	}
	return decodeAndCheckTrailing(w, r, v, allowEmpty)
}

func handleNoBody(w http.ResponseWriter, allowEmpty bool) bool {
	if allowEmpty {
		return true
	}
	writeError(w, http.StatusBadRequest, "bad_request", "missing body")
	return false
}

func decodeAndCheckTrailing(w http.ResponseWriter, r *http.Request, v any, allowEmpty bool) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if !decodeOnce(w, dec, v, allowEmpty) {
		return false
	}
	return checkSingleValue(w, dec)
}

// decodeOnce reads the first JSON value. allowEmpty silences EOF.
func decodeOnce(w http.ResponseWriter, dec *json.Decoder, v any, allowEmpty bool) bool {
	if err := dec.Decode(v); err != nil {
		if allowEmpty && errors.Is(err, io.EOF) {
			return true
		}
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return false
	}
	return true
}

// checkSingleValue rejects bodies that contain a second JSON value.
func checkSingleValue(w http.ResponseWriter, dec *json.Decoder) bool {
	var extra any
	err := dec.Decode(&extra)
	if errors.Is(err, io.EOF) {
		return true
	}
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return false
	}
	writeError(w, http.StatusBadRequest, "bad_request", "body must contain a single JSON value")
	return false
}

// ---------------------------------------------------------------------------
// REST-key-and-token validators
// ---------------------------------------------------------------------------

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
