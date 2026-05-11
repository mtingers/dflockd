package lock

import "sync"

// Grant is one promotion produced by an ApplyX call: a waiter became a
// holder, with the given token and lease. The Ref names the requester so
// the caller (server handler / cluster apply goroutine) can route the
// grant to the right blocked caller.
type Grant struct {
	Key      string // includes lock:/sem: prefix
	Ref      string
	Token    string
	LeaseSec int
	ConnID   uint64 // optional, single-node convenience
}

// listenerRegistry maps requester refs to grant channels. It is the
// runtime routing table for grants produced by ApplyX; the FSM state
// itself never references channels (channels aren't replicable).
type listenerRegistry struct {
	mu sync.Mutex
	ch map[string]chan Grant
}

func (r *listenerRegistry) register(ref string) <-chan Grant {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ch == nil {
		r.ch = make(map[string]chan Grant)
	}
	c := make(chan Grant, 1)
	r.ch[ref] = c
	return c
}

func (r *listenerRegistry) unregister(ref string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.ch, ref)
}

// deliver attempts to send g to ref's listener. Returns true on send,
// false if no listener or the channel is full / closed. A non-delivery
// is benign: the FSM has the holder entry; lease expiry will reclaim it
// if the would-be holder never observes the grant.
func (r *listenerRegistry) deliver(g Grant) bool {
	r.mu.Lock()
	c, ok := r.ch[g.Ref]
	r.mu.Unlock()
	if !ok {
		return false
	}
	return trySendOnce(c, g)
}

func trySendOnce(c chan Grant, g Grant) (sent bool) {
	defer func() {
		// recover from a panic-on-closed-channel; treat as not-delivered.
		_ = recover()
	}()
	select {
	case c <- g:
		return true
	default:
		return false
	}
}

// RouteGrants delivers each grant to its registered listener (if any).
// Grants without a listener are dropped — the holder entry stays in the
// FSM and is reclaimed when its lease expires.
func (lm *LockManager) RouteGrants(grants []Grant) {
	for _, g := range grants {
		lm.listeners.deliver(g)
	}
}

// WatchGrants registers a one-shot listener for ref. The caller must
// invoke the returned cancel once it is done (success or otherwise) so
// the registry doesn't leak. Re-registering the same ref overwrites the
// prior channel.
func (lm *LockManager) WatchGrants(ref string) (<-chan Grant, func()) {
	ch := lm.listeners.register(ref)
	return ch, func() { lm.listeners.unregister(ref) }
}
