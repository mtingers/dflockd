package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/mtingers/dflockd/internal/signal"
)

// handleSSE is the real SSE implementation. Opens a dedicated session for
// the stream (so the internal pinger doesn't contend with user commands),
// registers the listen subscription, and pumps sig frames to the client
// as text/event-stream events until the client disconnects.
//
// Implementation notes on the dedicated-session choice (per proposal):
//   - The session's reqMu is held briefly for the initial "listen" and
//     each 15s "ping", so the SSE goroutine doesn't contend with any
//     other request.
//   - Closing the HTTP request (client disconnect or server shutdown)
//     cancels r.Context(), which tears down the session and triggers
//     UnlistenAll via the normal protocol cleanup path.
func (h *httpServer) handleSSE(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	pattern := q.Get("pattern")
	if pattern == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "pattern query param required")
		return
	}
	if err := validateRESTKey(pattern); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if err := signal.ValidatePattern(pattern); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	group := q.Get("group")
	if err := validateRESTLineField("group", group); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}

	// Confirm streaming is available before we try to write.
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming_unsupported", "")
		return
	}

	// Open dedicated session.
	id, err := h.bridge.CreateSession(remoteIPFromAddr(r.RemoteAddr))
	if err != nil {
		if errors.Is(err, ErrMaxSessions) {
			writeError(w, http.StatusServiceUnavailable, "max_sessions", "")
			return
		}
		if errors.Is(err, ErrMaxSessionsPerIP) {
			writeError(w, http.StatusServiceUnavailable, "max_sessions_per_ip", "")
			return
		}
		if errors.Is(err, ErrBridgeShutdown) {
			writeError(w, http.StatusServiceUnavailable, "draining", "")
			return
		}
		writeError(w, http.StatusInternalServerError, "session_create_failed", err.Error())
		return
	}
	defer h.bridge.DeleteSession(id)

	s, err := h.bridge.LookupSession(id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "session_lookup_failed", err.Error())
		return
	}

	// Register the listen subscription.
	resp, err := s.command("listen", pattern, group)
	if err != nil {
		writeError(w, http.StatusGone, "session_gone", err.Error())
		return
	}
	if resp != "ok" {
		writeProtocolError(w, resp, "listen")
		return
	}

	// Start the SSE response.
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // disable nginx buffering if proxied
	w.WriteHeader(http.StatusOK)

	// Per-write deadline bounds how long a slow/stuck client can block this
	// goroutine inside w.Write. WriteTimeout is 0 on the server (intentional
	// for long-poll semantics), so without this, a client that stops reading
	// parks the goroutine until TCP keepalive eventually fires — minutes or
	// longer. ResponseController is a no-op fallback if the conn doesn't
	// support deadlines.
	rc := http.NewResponseController(w)
	const sseWriteTimeout = 30 * time.Second
	setSSEDeadline := func() {
		_ = rc.SetWriteDeadline(time.Now().Add(sseWriteTimeout))
	}

	// Emit an initial comment so proxies forward headers promptly.
	setSSEDeadline()
	if _, err := fmt.Fprint(w, ":ok\n\n"); err != nil {
		return
	}
	flusher.Flush()

	// Internal pinger so the virtual conn's ReadTimeout doesn't fire
	// during an idle SSE stream. Protocol-level ping resets the server-
	// side read deadline.
	pingInterval := h.cfg.HTTPSSEPingInterval
	if pingInterval <= 0 {
		pingInterval = 15 * time.Second
	}
	pingTicker := time.NewTicker(pingInterval)
	defer pingTicker.Stop()

	ctx := r.Context()

	for {
		select {
		case <-ctx.Done():
			return
		case <-pingTicker.C:
			// Use commandContext so a hung/slow server can't block this
			// handler past the HTTP client's disconnect. If ctx fires
			// mid-ping, commandContext aborts (closing the session), the
			// ping returns an error, and we return immediately — the
			// deferred DeleteSession then completes teardown.
			if _, err := s.commandContext(ctx, "ping", "_", ""); err != nil {
				return
			}
		case line, ok := <-s.signals():
			if !ok {
				return
			}
			// line is "sig <channel> <payload>"
			chPayload := strings.TrimPrefix(line, "sig ")
			idx := strings.Index(chPayload, " ")
			if idx < 0 {
				continue
			}
			channel := chPayload[:idx]
			payload := chPayload[idx+1:]
			// Marshal with json.Marshal rather than fmt %q: the latter
			// produces Go-syntax escapes (\xNN, \a, \v, ...) that are
			// not valid JSON, so any payload containing control chars
			// would yield a frame no JSON parser could consume.
			frame, err := marshalSigFrame(channel, payload)
			if err != nil {
				continue // skip malformed frame; stream continues
			}
			setSSEDeadline()
			if _, err := w.Write(frame); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// marshalSigFrame builds one SSE "event: sig" frame containing a JSON
// payload {"channel": ..., "payload": ...}. Uses json.Marshal on each
// field so the output is always valid JSON — including for payloads
// with control characters that Go's %q would escape as \xNN.
func marshalSigFrame(channel, payload string) ([]byte, error) {
	ch, err := json.Marshal(channel)
	if err != nil {
		return nil, err
	}
	pl, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 0, 32+len(ch)+len(pl))
	buf = append(buf, "event: sig\ndata: {\"channel\":"...)
	buf = append(buf, ch...)
	buf = append(buf, ",\"payload\":"...)
	buf = append(buf, pl...)
	buf = append(buf, "}\n\n"...)
	return buf, nil
}
