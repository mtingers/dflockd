package httpapi

import (
	"encoding/json"
	"net/http"
)

// errorBody is the common JSON shape for error responses.
//
//	{"error": "<code>", "detail": "<optional human-readable>"}
//
// The `error` field is machine-readable (stable); `detail` is informational.
type errorBody struct {
	Error  string `json:"error"`
	Detail string `json:"detail,omitempty"`
}

// writeJSON writes a JSON response with the given status code and body.
// Marshal failures degrade to a plain 500 — this should never happen for
// our own structs.
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

// writeError writes a JSON error response. Use for all non-2xx responses.
func writeError(w http.ResponseWriter, status int, code, detail string) {
	writeJSON(w, status, errorBody{Error: code, Detail: detail})
}

// mapProtocolError translates a protocol-layer response line that starts
// with "error..." into an HTTP status + JSON body. Returns (status, body)
// for callers that need to branch further; most callers use writeProtocolError.
//
// This is the single source of truth for the "protocol → HTTP" mapping
// documented in docs/proposals/http-api.md.
func mapProtocolError(resp, opContext string) (int, errorBody) {
	switch resp {
	case "error_auth":
		return http.StatusUnauthorized, errorBody{Error: "unauthorized"}
	case "error_max_locks":
		return http.StatusServiceUnavailable, errorBody{Error: "max_locks"}
	case "error_max_waiters":
		return http.StatusServiceUnavailable, errorBody{Error: "max_waiters"}
	case "error_limit_mismatch":
		return http.StatusConflict, errorBody{Error: "limit_mismatch"}
	case "error_already_enqueued":
		return http.StatusConflict, errorBody{Error: "already_enqueued"}
	case "error_not_enqueued":
		return http.StatusConflict, errorBody{Error: "not_enqueued"}
	case "error_lease_expired":
		return http.StatusConflict, errorBody{Error: "lease_expired"}
	case "error":
		// Generic protocol error on a state-mutating op usually means
		// "token/key combination not held." 404 is friendlier than 400
		// because it gives the caller a clear "this resource isn't
		// yours (anymore)" signal.
		return http.StatusNotFound, errorBody{Error: "not_held", Detail: opContext}
	default:
		return http.StatusInternalServerError, errorBody{
			Error:  "unexpected_protocol_response",
			Detail: resp,
		}
	}
}

// writeProtocolError maps a protocol response to an HTTP response.
func writeProtocolError(w http.ResponseWriter, resp, opContext string) {
	status, body := mapProtocolError(resp, opContext)
	writeJSON(w, status, body)
}
