package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/mtingers/dflockd/internal/lock"
)

// errorBody is the JSON shape for every non-2xx response.
//
//	{"error": "<code>", "detail": "<optional human-readable>"}
//
// The `error` field is machine-readable and stable; `detail` is
// informational and may change.
type errorBody struct {
	Error  string `json:"error"`
	Detail string `json:"detail,omitempty"`
}

// writeJSON encodes body with status. nil body writes only a status
// line. Marshal failures fall through to a plain 500.
func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if body == nil {
		w.WriteHeader(status)
		return
	}
	data, err := json.Marshal(body)
	if err != nil {
		http.Error(w, `{"error":"internal","detail":"marshal failed"}`, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(status)
	w.Write(data)
	w.Write([]byte("\n"))
}

// writeError is the canonical way to emit a non-2xx JSON response.
func writeError(w http.ResponseWriter, status int, code, detail string) {
	writeJSON(w, status, errorBody{Error: code, Detail: detail})
}

// httpStatusForLockErr maps a LockManager error to (http.Status, code).
// Unrecognised errors map to 500 with code "internal".
func httpStatusForLockErr(err error) (int, string) {
	switch {
	case errors.Is(err, lock.ErrMaxLocks):
		return http.StatusServiceUnavailable, "max_locks"
	case errors.Is(err, lock.ErrMaxWaiters):
		return http.StatusServiceUnavailable, "max_waiters"
	case errors.Is(err, lock.ErrLimitMismatch):
		return http.StatusConflict, "limit_mismatch"
	case errors.Is(err, lock.ErrAlreadyEnqueued):
		return http.StatusConflict, "already_enqueued"
	case errors.Is(err, lock.ErrNotEnqueued):
		return http.StatusConflict, "not_enqueued"
	case errors.Is(err, lock.ErrLeaseExpired):
		return http.StatusConflict, "lease_expired"
	case errors.Is(err, lock.ErrWaiterClosed):
		return http.StatusGone, "session_gone"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return http.StatusRequestTimeout, "client_canceled"
	}
	return http.StatusInternalServerError, "internal"
}

// writeLockErr writes the appropriate response for a LockManager error.
func writeLockErr(w http.ResponseWriter, err error) {
	status, code := httpStatusForLockErr(err)
	writeError(w, status, code, err.Error())
}
