package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/mtingers/dflockd/internal/lock"
	"github.com/mtingers/dflockd/internal/protocol"
)

const maxProtocolSeconds = int64(math.MaxInt64) / int64(time.Second)

// ---------------------------------------------------------------------------
// Session endpoints
// ---------------------------------------------------------------------------

type createSessionResponse struct {
	SessionID    string `json:"session_id"`
	IdleTimeoutS int    `json:"idle_timeout_s"`
}

// POST /v1/sessions
func (h *httpServer) handleCreateSession(w http.ResponseWriter, r *http.Request) {
	id, err := h.bridge.CreateSession()
	if err != nil {
		if errors.Is(err, ErrMaxSessions) {
			writeError(w, http.StatusServiceUnavailable, "max_sessions", "")
			return
		}
		// A bridge-auth failure here means our own configured --auth-token
		// didn't work — i.e. something is misconfigured at the server.
		writeError(w, http.StatusInternalServerError, "session_create_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, createSessionResponse{
		SessionID:    id,
		IdleTimeoutS: int(h.bridge.IdleTimeout().Seconds()),
	})
}

// DELETE /v1/sessions/{id}
func (h *httpServer) handleDeleteSession(w http.ResponseWriter, r *http.Request, id string) {
	if err := h.bridge.DeleteSession(id); err != nil {
		writeError(w, http.StatusGone, "session_gone", "")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// POST /v1/sessions/{id}/ping
func (h *httpServer) handlePingSession(w http.ResponseWriter, r *http.Request, id string) {
	s, err := h.bridge.LookupSession(id)
	if err != nil {
		writeError(w, http.StatusGone, "session_gone", "")
		return
	}
	// LookupSession already refreshed the bridge-level lastSeen. The
	// remaining job for the protocol-level ping is to keep the underlying
	// virtual conn's read deadline from firing — but if there's another
	// command in flight, ServeConn isn't blocked in ReadRequest anyway,
	// so the protocol ping is redundant. Skip it to avoid serializing
	// behind reqMu for the duration of a long-poll Acquire/Wait, which
	// would otherwise stall callers using pings as a liveness probe.
	if s.inFlight.Load() > 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	resp, err := s.command("ping", "_", "")
	if err != nil {
		writeError(w, http.StatusGone, "session_gone", err.Error())
		return
	}
	if resp != "ok" {
		writeError(w, http.StatusInternalServerError, "unexpected_protocol_response", resp)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

// GET /v1/stats — sessionless. We don't need a virtual conn for this; we
// ask LockManager directly for stats (same as the "stats" protocol cmd does
// internally via server.handleRequest).
func (h *httpServer) handleStats(w http.ResponseWriter, r *http.Request) {
	stats := h.bridge.LockManager().Stats(h.bridge.ConnCount() + int64(h.bridge.SessionCount()))
	// Mirror signal channels into the stats struct. Both fields are now
	// the same type (signal.ChannelInfo aliased through lock), so no
	// conversion is needed.
	stats.SignalChannels = append(stats.SignalChannels, h.bridge.Signals().Stats()...)
	// Strip key prefixes so the HTTP caller sees the logical key.
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
	writeJSON(w, http.StatusOK, stats)
}

// ---------------------------------------------------------------------------
// Lock endpoints (single-phase)
// ---------------------------------------------------------------------------

type acquireRequest struct {
	AcquireTimeoutS int `json:"acquire_timeout_s"`
	LeaseTTLS       int `json:"lease_ttl_s,omitempty"`
}

// opResponse is the common shape for acquire/enqueue/wait responses.
// Status values: "ok" (acquired or waited successfully), "timeout",
// "acquired" (two-phase fast path), "queued" (two-phase waiter). Token
// and LeaseTTLS are populated only when the caller now holds the lock.
type opResponse struct {
	Status    string `json:"status"`
	Token     string `json:"token,omitempty"`
	LeaseTTLS int    `json:"lease_ttl_s,omitempty"`
}

// Type aliases for backward-compatible naming in tests and handlers.
// All three point to the same underlying struct since the response
// schemas are identical on the wire.
type (
	acquireResponse = opResponse
	enqueueResponse = opResponse
	waitResponse    = opResponse
)

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

// POST /v1/locks/{key}
func (h *httpServer) handleAcquireLock(w http.ResponseWriter, r *http.Request, key string) {
	s, ok := h.sessionOr410(w, r)
	if !ok {
		return
	}
	var req acquireRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if err := validateRESTSeconds("acquire_timeout_s", req.AcquireTimeoutS); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if err := validateRESTSeconds("lease_ttl_s", req.LeaseTTLS); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	arg := strconv.Itoa(req.AcquireTimeoutS)
	if req.LeaseTTLS > 0 {
		arg += " " + strconv.Itoa(req.LeaseTTLS)
	}
	resp, err := s.command("l", key, arg)
	if err != nil {
		writeError(w, http.StatusGone, "session_gone", err.Error())
		return
	}
	if maybeCleanupOnDisconnect(r, s, resp, key, "r") {
		return
	}
	renderOpResponse(w, resp, "acquire")
}

// POST /v1/locks/{key}/release
func (h *httpServer) handleReleaseLock(w http.ResponseWriter, r *http.Request, key string) {
	h.doRelease(w, r, "r", key)
}

// POST /v1/locks/{key}/renew
func (h *httpServer) handleRenewLock(w http.ResponseWriter, r *http.Request, key string) {
	h.doRenew(w, r, "n", key)
}

// ---------------------------------------------------------------------------
// Semaphore endpoints (single-phase)
// ---------------------------------------------------------------------------

type semAcquireRequest struct {
	AcquireTimeoutS int `json:"acquire_timeout_s"`
	Limit           int `json:"limit"`
	LeaseTTLS       int `json:"lease_ttl_s,omitempty"`
}

// POST /v1/semaphores/{key}
func (h *httpServer) handleAcquireSem(w http.ResponseWriter, r *http.Request, key string) {
	s, ok := h.sessionOr410(w, r)
	if !ok {
		return
	}
	var req semAcquireRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if err := validateRESTSeconds("acquire_timeout_s", req.AcquireTimeoutS); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if req.Limit <= 0 {
		writeError(w, http.StatusBadRequest, "bad_request", "limit must be > 0")
		return
	}
	if err := validateRESTSeconds("lease_ttl_s", req.LeaseTTLS); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	arg := strconv.Itoa(req.AcquireTimeoutS) + " " + strconv.Itoa(req.Limit)
	if req.LeaseTTLS > 0 {
		arg += " " + strconv.Itoa(req.LeaseTTLS)
	}
	resp, err := s.command("sl", key, arg)
	if err != nil {
		writeError(w, http.StatusGone, "session_gone", err.Error())
		return
	}
	if maybeCleanupOnDisconnect(r, s, resp, key, "sr") {
		return
	}
	renderOpResponse(w, resp, "sem_acquire")
}

// POST /v1/semaphores/{key}/release
func (h *httpServer) handleReleaseSem(w http.ResponseWriter, r *http.Request, key string) {
	h.doRelease(w, r, "sr", key)
}

// POST /v1/semaphores/{key}/renew
func (h *httpServer) handleRenewSem(w http.ResponseWriter, r *http.Request, key string) {
	h.doRenew(w, r, "sn", key)
}

// ---------------------------------------------------------------------------
// Two-phase endpoints (Phase 2)
// ---------------------------------------------------------------------------

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

// POST /v1/locks/{key}/enqueue
func (h *httpServer) handleEnqueueLock(w http.ResponseWriter, r *http.Request, key string) {
	s, ok := h.sessionOr410(w, r)
	if !ok {
		return
	}
	var req enqueueRequest
	if !decodeOptionalJSON(w, r, &req) {
		return
	}
	if err := validateRESTSeconds("lease_ttl_s", req.LeaseTTLS); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	arg := ""
	if req.LeaseTTLS > 0 {
		arg = strconv.Itoa(req.LeaseTTLS)
	}
	resp, err := s.command("e", key, arg)
	if err != nil {
		writeError(w, http.StatusGone, "session_gone", err.Error())
		return
	}
	if maybeCleanupOnDisconnect(r, s, resp, key, "r") {
		return
	}
	renderOpResponse(w, resp, "enqueue")
}

// POST /v1/locks/{key}/wait
func (h *httpServer) handleWaitLock(w http.ResponseWriter, r *http.Request, key string) {
	h.doWait(w, r, "w", key)
}

// POST /v1/semaphores/{key}/enqueue
func (h *httpServer) handleEnqueueSem(w http.ResponseWriter, r *http.Request, key string) {
	s, ok := h.sessionOr410(w, r)
	if !ok {
		return
	}
	var req semEnqueueRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Limit <= 0 {
		writeError(w, http.StatusBadRequest, "bad_request", "limit must be > 0")
		return
	}
	if err := validateRESTSeconds("lease_ttl_s", req.LeaseTTLS); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	arg := strconv.Itoa(req.Limit)
	if req.LeaseTTLS > 0 {
		arg += " " + strconv.Itoa(req.LeaseTTLS)
	}
	resp, err := s.command("se", key, arg)
	if err != nil {
		writeError(w, http.StatusGone, "session_gone", err.Error())
		return
	}
	if maybeCleanupOnDisconnect(r, s, resp, key, "sr") {
		return
	}
	renderOpResponse(w, resp, "enqueue")
}

// POST /v1/semaphores/{key}/wait
func (h *httpServer) handleWaitSem(w http.ResponseWriter, r *http.Request, key string) {
	h.doWait(w, r, "sw", key)
}

// ---------------------------------------------------------------------------
// Signal publish
// ---------------------------------------------------------------------------

type signalRequest struct {
	Payload string `json:"payload"`
}

type signalResponse struct {
	Delivered int `json:"delivered"`
}

// POST /v1/signals/{channel}
//
// Signal publish is sessionless — callers (webhooks, CI steps, ops
// scripts) shouldn't need a session just to fire one signal. We call
// the signal Manager directly rather than routing through a transient
// virtual connection, avoiding the overhead of net.Pipe + ServeConn +
// multiplex goroutines per publish.
func (h *httpServer) handlePublishSignal(w http.ResponseWriter, r *http.Request, channel string) {
	var req signalRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Payload == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "payload must not be empty")
		return
	}
	if strings.TrimSpace(req.Payload) == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "payload must not be empty")
		return
	}
	if strings.ContainsAny(channel, "*>") {
		writeError(w, http.StatusBadRequest, "bad_request", "signal channel must not contain wildcards")
		return
	}
	if strings.ContainsAny(req.Payload, "\n\r") {
		writeError(w, http.StatusBadRequest, "bad_request", "payload must not contain newline characters")
		return
	}
	if maxPayload := protocol.MaxSignalPayloadBytes(channel); maxPayload < 0 || len(req.Payload) > maxPayload {
		if maxPayload < 0 {
			maxPayload = 0
		}
		writeError(w, http.StatusBadRequest, "bad_request", fmt.Sprintf("payload too large (max %d bytes)", maxPayload))
		return
	}
	n := h.bridge.Signals().Signal(channel, req.Payload)
	writeJSON(w, http.StatusOK, signalResponse{Delivered: n})
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

// sessionOr410 pulls the session from the X-Dflockd-Session header, or
// writes a 410 and returns (nil, false).
func (h *httpServer) sessionOr410(w http.ResponseWriter, r *http.Request) (*session, bool) {
	id := r.Header.Get("X-Dflockd-Session")
	if id == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "X-Dflockd-Session header required")
		return nil, false
	}
	s, err := h.bridge.LookupSession(id)
	if err != nil {
		writeError(w, http.StatusGone, "session_gone", "")
		return nil, false
	}
	return s, true
}

// decodeJSON reads and decodes the JSON body. Writes 400 on parse error
// and returns false so the caller can early-return.
const maxRequestBody = 1 << 20 // 1MB

func decodeJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	return decodeJSONBody(w, r, v, false)
}

func decodeOptionalJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	return decodeJSONBody(w, r, v, true)
}

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
	// Reject unknown fields to catch caller typos early.
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

// doRelease is the shared implementation for lock and semaphore release.
// cmd is "r" (lock) or "sr" (sem).
func (h *httpServer) doRelease(w http.ResponseWriter, r *http.Request, cmd, key string) {
	s, ok := h.sessionOr410(w, r)
	if !ok {
		return
	}
	var req releaseRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Token == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "token required")
		return
	}
	if err := validateRESTToken(req.Token); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	resp, err := s.command(cmd, key, req.Token)
	if err != nil {
		writeError(w, http.StatusGone, "session_gone", err.Error())
		return
	}
	if resp == "ok" {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeProtocolError(w, resp, "release")
}

// doRenew is the shared implementation for lock and semaphore renew.
// cmd is "n" (lock) or "sn" (sem).
func (h *httpServer) doRenew(w http.ResponseWriter, r *http.Request, cmd, key string) {
	s, ok := h.sessionOr410(w, r)
	if !ok {
		return
	}
	var req renewRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Token == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "token required")
		return
	}
	if err := validateRESTToken(req.Token); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if err := validateRESTSeconds("lease_ttl_s", req.LeaseTTLS); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	arg := req.Token
	if req.LeaseTTLS > 0 {
		arg += " " + strconv.Itoa(req.LeaseTTLS)
	}
	resp, err := s.command(cmd, key, arg)
	if err != nil {
		writeError(w, http.StatusGone, "session_gone", err.Error())
		return
	}
	// Expect "ok <remaining>"
	parts := strings.Fields(resp)
	if len(parts) == 2 && parts[0] == "ok" {
		n, err := strconv.Atoi(parts[1])
		if err != nil {
			writeError(w, http.StatusInternalServerError, "unexpected_protocol_response", resp)
			return
		}
		writeJSON(w, http.StatusOK, renewResponse{RemainingS: n})
		return
	}
	writeProtocolError(w, resp, "renew")
}

// doWait is the shared implementation for lock and semaphore wait.
// cmd is "w" (lock) or "sw" (sem).
func (h *httpServer) doWait(w http.ResponseWriter, r *http.Request, cmd, key string) {
	s, ok := h.sessionOr410(w, r)
	if !ok {
		return
	}
	var req waitRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if err := validateRESTSeconds("timeout_s", req.TimeoutS); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	resp, err := s.command(cmd, key, strconv.Itoa(req.TimeoutS))
	if err != nil {
		writeError(w, http.StatusGone, "session_gone", err.Error())
		return
	}
	releaseCmd := "r"
	if cmd == "sw" {
		releaseCmd = "sr"
	}
	if maybeCleanupOnDisconnect(r, s, resp, key, releaseCmd) {
		return
	}
	renderOpResponse(w, resp, "wait")
}

// renderOpResponse maps every acquire/enqueue/wait-shaped protocol
// response to a JSON opResponse. Understands all three status words
// ("ok", "timeout", "acquired", "queued") and the "<word> <token>
// <ttl>" payload form used by acquire-on-free and wait-grant. op is
// used only to annotate any fall-through unexpected response.
func renderOpResponse(w http.ResponseWriter, resp, op string) {
	switch resp {
	case "timeout":
		writeJSON(w, http.StatusOK, opResponse{Status: "timeout"})
		return
	case "queued":
		writeJSON(w, http.StatusOK, opResponse{Status: "queued"})
		return
	}
	parts := strings.Fields(resp)
	if len(parts) == 3 && (parts[0] == "ok" || parts[0] == "acquired") {
		ttl, err := strconv.Atoi(parts[2])
		if err != nil {
			writeError(w, http.StatusInternalServerError, "unexpected_protocol_response", resp)
			return
		}
		writeJSON(w, http.StatusOK, opResponse{Status: parts[0], Token: parts[1], LeaseTTLS: ttl})
		return
	}
	writeProtocolError(w, resp, op)
}

// parseOkTokenLease returns (token, ttl, ok) for lines like "ok <token> <ttl>".
func parseOkTokenLease(resp string) (string, int, bool) {
	parts := strings.Fields(resp)
	if len(parts) != 3 || parts[0] != "ok" {
		return "", 0, false
	}
	ttl, err := strconv.Atoi(parts[2])
	if err != nil {
		return "", 0, false
	}
	return parts[1], ttl, true
}

// extractGrantToken returns the token from a grant response ("ok <token>
// <ttl>" or "acquired <token> <ttl>") or "" if resp isn't a grant. Used
// by the release-on-disconnect path to reach the token from the same
// response shape renderOpResponse consumes.
func extractGrantToken(resp string) string {
	parts := strings.Fields(resp)
	if len(parts) == 3 && (parts[0] == "ok" || parts[0] == "acquired") {
		return parts[1]
	}
	return ""
}

// maybeCleanupOnDisconnect checks whether the HTTP caller went away while
// a lock/sem command was in flight. If the server granted us a token, we
// release it immediately; if a two-phase enqueue only reached "queued",
// we issue a zero-timeout wait to remove that waiter. Returns true when
// the handler should skip writing a response (client is gone either way).
//
// Covers the window where the client opens POST /locks/.../wait, the
// server grants the lock, and the client disconnected before the JSON
// response was sent — without this, the session holds the grant for the
// remainder of the lease TTL with no way for the caller to find it.
func maybeCleanupOnDisconnect(r *http.Request, s *session, resp, key, releaseCmd string) bool {
	if r.Context().Err() == nil {
		return false
	}
	if token := extractGrantToken(resp); token != "" {
		_, _ = s.command(releaseCmd, key, token)
		return true
	}
	if resp == "queued" {
		waitCmd := "w"
		if releaseCmd == "sr" {
			waitCmd = "sw"
		}
		waitResp, err := s.command(waitCmd, key, "0")
		if err == nil {
			if token := extractGrantToken(waitResp); token != "" {
				_, _ = s.command(releaseCmd, key, token)
			}
		}
	}
	return true
}

// validateRESTKey rejects keys with whitespace or protocol-breaking
// characters. The line-based TCP protocol has similar checks; we surface
// them at the HTTP layer so the client gets a clean 400 rather than a
// session-breaking 500.
func validateRESTKey(k string) error {
	if k == "" {
		return fmt.Errorf("empty key")
	}
	if strings.ContainsAny(k, " \t\n\r") {
		return fmt.Errorf("key contains whitespace")
	}
	return nil
}

func validateRESTToken(token string) error {
	if strings.ContainsAny(token, " \t\n\r") {
		return fmt.Errorf("token contains whitespace")
	}
	return nil
}

func validateRESTLineField(name, value string) error {
	if strings.ContainsAny(value, "\n\r") {
		return fmt.Errorf("%s must not contain newline characters", name)
	}
	return nil
}

func validateRESTSeconds(name string, value int) error {
	if value < 0 {
		return fmt.Errorf("%s must be >= 0", name)
	}
	if int64(value) > maxProtocolSeconds {
		return fmt.Errorf("%s too large (max %d)", name, maxProtocolSeconds)
	}
	return nil
}
