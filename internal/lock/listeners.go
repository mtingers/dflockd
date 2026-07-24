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

// listener is one registered interest in grants for a ref. key scopes
// it to a single resource; "" means "any key for this ref" (the HTTP
// wait path, which has no key in hand).
type listener struct {
	key string
	ch  chan Grant
}

// wants reports whether this listener should receive a grant for key.
func (l *listener) wants(key string) bool { return l.key == "" || l.key == key }

// listenerRegistry maps requester refs to grant listeners. It is the
// runtime routing table for grants produced by ApplyX; the FSM state
// itself never references channels (channels aren't replicable).
//
// A ref can have several listeners at once — a queued two-phase
// Enqueue holds one for the whole gap until its Wait, while any later
// command on the same connection registers its own — so registrations
// are tracked individually and each cancel removes only its own.
type listenerRegistry struct {
	mu   sync.Mutex
	refs map[string][]*listener
}

func (r *listenerRegistry) register(ref, key string) (<-chan Grant, *listener) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.refs == nil {
		r.refs = make(map[string][]*listener)
	}
	l := &listener{key: key, ch: make(chan Grant, 1)}
	r.refs[ref] = append(r.refs[ref], l)
	return l.ch, l
}

// unregister drops exactly the listener target — never a sibling that
// registered later for the same ref. Idempotent.
func (r *listenerRegistry) unregister(ref string, target *listener) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ls := r.refs[ref]
	for i, l := range ls {
		if l != target {
			continue
		}
		r.refs[ref] = append(ls[:i:i], ls[i+1:]...)
		if len(r.refs[ref]) == 0 {
			delete(r.refs, ref)
		}
		return
	}
}

// deliver attempts to send g to a listener registered for its ref and
// interested in its key, trying each in registration order until one
// accepts. Returns true on send, false if none matched or every
// candidate's channel was full / closed. A non-delivery is benign: the
// FSM has the holder entry; lease expiry will reclaim it if the
// would-be holder never observes the grant.
func (r *listenerRegistry) deliver(g Grant) bool {
	for _, l := range r.candidates(g) {
		if trySendOnce(l.ch, g) {
			return true
		}
	}
	return false
}

// candidates snapshots the listeners eligible for g, so delivery
// happens off the registry lock.
func (r *listenerRegistry) candidates(g Grant) []*listener {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*listener
	for _, l := range r.refs[g.Ref] {
		if l.wants(g.Key) {
			out = append(out, l)
		}
	}
	return out
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

// WatchGrants registers a one-shot listener for any grant addressed to
// ref. The caller must invoke the returned cancel once it is done
// (success or otherwise) so the registry doesn't leak; cancel affects
// only this registration. Prefer WatchGrantsFor when the key is known.
func (lm *LockManager) WatchGrants(ref string) (<-chan Grant, func()) {
	return lm.WatchGrantsFor(ref, "")
}

// WatchGrantsFor registers a one-shot listener for grants addressed to
// ref for one key, so two operations outstanding on the same ref can't
// consume each other's grant. An empty key matches any.
func (lm *LockManager) WatchGrantsFor(ref, key string) (<-chan Grant, func()) {
	ch, l := lm.listeners.register(ref, key)
	return ch, func() { lm.listeners.unregister(ref, l) }
}
