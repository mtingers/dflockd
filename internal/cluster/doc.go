// Package cluster glues the generic raft package to dflockd's
// LockManager: it defines the Command codec and the raft.FSM
// implementation that decodes committed entries into ApplyX calls on
// the LockManager, owns the cluster Node's lifecycle (raft node +
// storage + transport + FSM + leader-only loops), and exposes a typed
// Propose surface the rest of dflockd talks to.
//
// The cluster.Node is constructed once per process; its public methods
// are safe for concurrent use. Internally it follows the same
// single-goroutine ownership discipline as raft.Node — the FSM is only
// mutated by the raft apply goroutine (which calls into LockManager's
// deterministic ApplyX). Leader-only loops (lease sweep, idle GC) start
// when this node becomes leader and stop when it loses leadership;
// they emit Evict / GC commands as ordinary proposals.
package cluster
