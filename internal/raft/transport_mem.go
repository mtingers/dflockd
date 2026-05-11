package raft

import (
	"context"
	"errors"
	"sync"
	"time"
)

// MemNetwork is an in-process Raft "network" for tests: it routes RPCs
// between MemTransports by node id, with knobs to partition links, drop a
// node, and add latency. It is the substrate that lets a multi-node Raft
// cluster — including elections, catch-up, partitions, and recovery — run
// deterministically inside one test process.
type MemNetwork struct {
	mu       sync.Mutex
	handlers map[NodeID]func(from NodeID, req Message) Message
	blocked  map[linkKey]bool // unordered pair -> link is down
	down     map[NodeID]bool  // node treated as crashed (no traffic in or out)
	delay    time.Duration
}

type linkKey struct{ a, b NodeID }

func mkLink(a, b NodeID) linkKey {
	if a > b {
		a, b = b, a
	}
	return linkKey{a, b}
}

// errUnreachable is returned by Send when the peer is down, partitioned,
// or has no handler registered. Callers treat it like any RPC failure.
var errUnreachable = errors.New("raft: peer unreachable")

// NewMemNetwork returns an empty network.
func NewMemNetwork() *MemNetwork {
	return &MemNetwork{handlers: map[NodeID]func(NodeID, Message) Message{}, blocked: map[linkKey]bool{}, down: map[NodeID]bool{}}
}

// Transport returns a Transport bound to id, registering it on the network.
func (n *MemNetwork) Transport(id NodeID) *MemTransport {
	return &MemTransport{net: n, id: id}
}

// Partition makes the link between a and b drop traffic until Heal.
func (n *MemNetwork) Partition(a, b NodeID) { n.setLink(mkLink(a, b), true) }

// Reconnect restores the link between a and b.
func (n *MemNetwork) Reconnect(a, b NodeID) { n.setLink(mkLink(a, b), false) }

// Isolate partitions a from every other registered node.
func (n *MemNetwork) Isolate(a NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for id := range n.handlers {
		if id != a {
			n.blocked[mkLink(a, id)] = true
		}
	}
}

// Heal clears all partitions (but not crashed-node state).
func (n *MemNetwork) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.blocked = map[linkKey]bool{}
}

// Crash marks id as down: no traffic flows to or from it. Use to model a
// node failure without losing its (separately-held) Storage.
func (n *MemNetwork) Crash(id NodeID, down bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.down[id] = down
}

// SetDelay adds a fixed latency to every Send.
func (n *MemNetwork) SetDelay(d time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.delay = d
}

func (n *MemNetwork) setLink(k linkKey, blocked bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.blocked[k] = blocked
}

func (n *MemNetwork) register(id NodeID, h func(NodeID, Message) Message) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[id] = h
}

func (n *MemNetwork) unregister(id NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.handlers, id)
}

// route returns the target's handler if the link is up and neither end is
// crashed; nil otherwise. It also returns the configured delay.
func (n *MemNetwork) route(from, to NodeID) (func(NodeID, Message) Message, time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.down[from] || n.down[to] || n.blocked[mkLink(from, to)] {
		return nil, 0
	}
	return n.handlers[to], n.delay
}

// MemTransport is a Transport backed by a MemNetwork.
type MemTransport struct {
	net     *MemNetwork
	id      NodeID
	handler func(from NodeID, req Message) Message
	wg      sync.WaitGroup // joins every in-flight invoke goroutine
}

var _ Transport = (*MemTransport)(nil)

func (t *MemTransport) LocalID() NodeID { return t.id }

func (t *MemTransport) SetHandler(h func(from NodeID, req Message) Message) {
	t.handler = h
	t.net.register(t.id, h)
}

func (t *MemTransport) AddPeer(NodeID, string) {} // membership is implicit on a MemNetwork
func (t *MemTransport) RemovePeer(NodeID)      {}

func (t *MemTransport) Close() error {
	t.net.unregister(t.id)
	t.wg.Wait() // join every invoke handler goroutine before returning
	return nil
}

// Send routes req to `to`, honouring partitions, crashes, and delay. It
// runs the target's handler on a fresh goroutine so a slow or stuck peer
// can't block the caller past ctx.
func (t *MemTransport) Send(ctx context.Context, to NodeID, req Message) (Message, error) {
	h, delay := t.net.route(t.id, to)
	if h == nil {
		return t.failAfterDelay(ctx, delay)
	}
	return t.invoke(ctx, h, to, req, delay)
}

func (t *MemTransport) failAfterDelay(ctx context.Context, delay time.Duration) (Message, error) {
	if err := sleepCtx(ctx, delay); err != nil {
		return nil, err
	}
	return nil, errUnreachable
}

func (t *MemTransport) invoke(ctx context.Context, h func(NodeID, Message) Message, to NodeID, req Message, delay time.Duration) (Message, error) {
	if err := sleepCtx(ctx, delay); err != nil {
		return nil, err
	}
	ch := make(chan Message, 1)
	t.wg.Add(1)
	go t.dispatchHandler(h, req, ch)
	select {
	case resp := <-ch:
		return resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (t *MemTransport) dispatchHandler(h func(NodeID, Message) Message, req Message, ch chan<- Message) {
	defer t.wg.Done()
	if resp := h(t.id, req); resp != nil {
		ch <- resp
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
