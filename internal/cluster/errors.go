package cluster

import (
	"errors"

	"github.com/mtingers/dflockd/internal/lock"
)

// Sentinel errors.
var (
	ErrUnknownPeer = errors.New("cluster: unknown peer")
	ErrNoLeader    = errors.New("cluster: no leader known")

	errUnknownKind = errors.New("cluster: unknown command kind")
)

// applyErrResult wraps an apply-time error into an ApplyResult-typed
// failure return. The raft Future receives this object; the caller can
// recover the underlying error via fmtErrTypedResult below.
type applyErrTyped struct {
	Result lock.ApplyResult
	Err    error
}

func applyErrResult(err error) applyErrTyped {
	return applyErrTyped{Err: err}
}

// resultOr packages a (result, err) pair from ApplyX into the future
// return value: on err it surfaces an applyErrTyped, otherwise the bare
// ApplyResult.
func resultOr(result lock.ApplyResult, err error) any {
	if err != nil {
		return applyErrTyped{Result: result, Err: err}
	}
	return result
}

// unwrapApplyResult converts the FSM Apply return value into the
// (ApplyResult, error) pair callers expect.
func unwrapApplyResult(v any) (lock.ApplyResult, error) {
	if v == nil {
		return lock.ApplyResult{Status: lock.StatusOK}, nil
	}
	if t, ok := v.(applyErrTyped); ok {
		return t.Result, t.Err
	}
	if r, ok := v.(lock.ApplyResult); ok {
		return r, nil
	}
	return lock.ApplyResult{}, errUnknownKind
}
