// Package raft is a from-scratch implementation of the Raft consensus
// algorithm (Ongaro & Ousterhout, "In Search of an Understandable
// Consensus Algorithm"), used by dflockd to replicate lock state across
// a cluster of nodes. It deliberately carries no dflockd-specific code:
// the application plugs in via the FSM interface and the Storage /
// Transport interfaces, so the package can be exercised — including
// multi-node election, log catch-up, leader failure, partition heal,
// snapshot install, and membership change — entirely in one test process
// over an in-memory transport.
//
// Scope: leader election (with PreVote), log replication (heartbeats, the
// consistency check, conflict-hint back-off, commit-only-over-current-term),
// durable HardState + log, FSM apply with per-entry result futures,
// snapshotting + log compaction + InstallSnapshot, single-server
// membership changes (Raft §4.3), a leadership-confirmed linearizable read
// barrier (ReadIndex), and leadership transfer (TimeoutNow). Out of scope:
// joint-consensus membership, witness-only members, multi-raft.
//
// Concurrency model: a single goroutine (the run loop in node.go) owns all
// consensus state — term, vote, role, the in-memory log window, commit
// index, and per-peer progress. Everything outside talks to it over
// channels (proposals, inbound RPCs, ticks, conf-changes, transfer
// requests, stop). A second goroutine (the apply loop) is the only writer
// of FSM state, feeding it committed entries in index order. This makes
// the package race-free by construction rather than by lock discipline.
package raft
