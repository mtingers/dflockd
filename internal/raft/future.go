package raft

import (
	"context"
	"sync"
)

// Future is what Propose / ProposeConfChange / Barrier return: a handle
// the caller blocks on with Wait. Exactly one of (result, err) is set
// once the entry applies (or is rejected by a leadership change). Future
// is safe for one waiter; concurrent Wait calls are not supported.
type Future struct {
	mu     sync.Mutex
	done   chan struct{}
	result any
	err    error
	closed bool
}

func newFuture() *Future {
	return &Future{done: make(chan struct{})}
}

// Wait blocks until the future is resolved or ctx is cancelled.
func (f *Future) Wait(ctx context.Context) (any, error) {
	select {
	case <-f.done:
		return f.result, f.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// resolve fulfils the future. Multiple calls after the first are silently
// ignored (the first wins) so a panic in one delivery path can't be
// followed by a contradictory second delivery.
func (f *Future) resolve(result any, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return
	}
	f.result, f.err, f.closed = result, err, true
	close(f.done)
}
