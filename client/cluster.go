package client

import (
	"errors"
	"strings"
)

// NotLeaderError surfaces a server response of "error_not_leader [<addr>]".
// In cluster mode the server returns this for any mutating command when
// it is not the current Raft leader; the trailing address (when present)
// is the leader's client-facing host:port. Callers should redial there
// (or round-robin the configured members if Leader is empty) and retry.
//
// Detect with errors.As:
//
//	var nle *NotLeaderError
//	if errors.As(err, &nle) { ... reconnect to nle.Leader ... }
type NotLeaderError struct {
	Leader string // empty when the server has no known leader yet
}

// Error implements the error interface.
func (e *NotLeaderError) Error() string {
	if e.Leader == "" {
		return "dflockd: not leader; no leader currently known"
	}
	return "dflockd: not leader; leader at " + e.Leader
}

// notLeaderRespPrefix is the wire status (with optional space-separated
// trailing address). We accept both the bare status and the address-suffixed
// form so a client written against an older server doesn't misparse.
const notLeaderRespPrefix = "error_not_leader"

// notLeaderFromResp returns the typed error if resp is exactly the
// not_leader status or starts with it + a leader hint; nil otherwise.
func notLeaderFromResp(resp string) error {
	if resp == notLeaderRespPrefix {
		return &NotLeaderError{}
	}
	if rest, ok := strings.CutPrefix(resp, notLeaderRespPrefix+" "); ok {
		return &NotLeaderError{Leader: strings.TrimSpace(rest)}
	}
	return nil
}

// IsNotLeader reports whether err is (or wraps) a *NotLeaderError. The
// matching *NotLeaderError is written to out if non-nil.
func IsNotLeader(err error, out **NotLeaderError) bool {
	var nle *NotLeaderError
	if errors.As(err, &nle) {
		if out != nil {
			*out = nle
		}
		return true
	}
	return false
}
