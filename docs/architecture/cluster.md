# Cluster mode (Raft HA)

dflockd has an opt-in **high-availability cluster mode**: N nodes (odd; 3
or 5) replicate lock state via Raft, persist the log + FSM snapshots to
disk, and survive the loss of a minority of members without losing
state. Clients connect to any node; mutating operations are linearizable
(they go through the leader's Raft log); a non-leader tells the client
where the leader is via `error_not_leader <host:port>`.

With no cluster flags set the server is byte-for-byte the standalone
v2.1.x single-node binary and shares its zero-runtime-dependency
profile. Cluster mode is layered additively under `internal/raft/`,
`internal/cluster/`, plus thin hooks in `internal/lock/` and
`internal/server/`.

## Topology

```
                 ┌── clients (TCP / HTTP) ──┐
                 │  follow error_not_leader │
                 └──────────┬────────────┘
                            ▼
   ┌── node A (leader) ──┐   ┌── node B ──┐   ┌── node C ──┐
   │ server               │   │ server     │   │ server     │
   │  ↓ mutating op       │   │ (followers │   │ (redirect  │
   │ cluster.Node         │◄─►│  redirect) │◄─►│  clients)  │
   │ ↓ Propose            │   │ raft.Node  │   │ raft.Node  │
   │ raft.Node            │   │ ↓ apply    │   │ ↓ apply    │
   │ ↓ apply              │   │ LockMgr    │   │ LockMgr    │
   │ LockMgr              │   └────────────┘   └────────────┘
   │ Storage: WAL,        │
   │  HardState, snaps    │
   └──────────────────────┘
```

Each member owns a **stable** Raft node ID. A member contributes one
**Raft transport address** (peer-to-peer consensus traffic, the
`--raft-addr` bind) and one **client-facing address** (what
`error_not_leader` redirects clients to, the `--advertise-addr` or
derived `--host:--port`).

## What's replicated

- **Lock state**: every `Acquire` / `Enqueue` / `Release` / `Renew` /
  `Evict` / `CleanupConn` / `GC` / `EvictExpired` is a Raft log entry.
  A leader-only sweep loop (ticking at `--lease-sweep-interval`, default
  1 s) proposes an `EvictExpired` command — which drops every holder
  past its lease deadline and promotes waiters into the freed slots —
  and a `GC` command every 30 ticks. Followers don't run that loop; a
  leadership change between the tick and the propose just yields
  `ErrNotLeader` and is retried on the next tick.
- **Fencing tokens** carry through replication. The token's lex-sortable
  prefix is a `FenceCounter` kept in FSM state (snapshotted), bumped on
  every grant during apply; tokens are deterministic on every node.
- **Configuration changes** (`AddVoter` / `RemoveServer`) are themselves
  Raft log entries; they take effect on append (Raft §4.3). Each
  configuration also carries the client-facing address used for status
  and redirects, so metadata survives failover, snapshot, and restart.
- **FSM policy** is versioned and carried by commands, then persisted in
  snapshots: `MaxLocks`, `MaxWaiters`, `OrphanTTL`, `GCMaxIdleTime`, and
  `AutoReleaseOnDisconnect`. The first policy-bearing command establishes
  cluster behavior; later commands must match it.

## What's persisted

`--raft-dir` is a directory holding:

| File / subdir | Content |
|---|---|
| `wal` | Append-only WAL of length-prefixed CRC'd records (the Raft log). |
| `hardstate` | Two-slot CRC journal of `(currentTerm, votedFor, commitIndex)`. |
| `snapshots/snap-<idx>-<term>` | Atomic-rename FSM snapshot files. |
| `.lock` | `flock(2)`-held by the running process; refuses two opens. |

The directory is exclusive-locked for the storage's lifetime; cluster
mode requires Unix-style advisory file locking (refused on Windows).
fsyncs are issued before any RPC reply that depends on the just-written
state.

## Transport security

Every Raft connection requires the same high-entropy shared secret
(`--raft-auth-token-file`, direct flag, or environment). A fresh-nonce
HMAC-SHA256 challenge-response authenticates the peer and derives
directional AES-GCM keys; sequence numbers reject replayed secure
frames. Mutual TLS additionally verifies the issuing CA and requires
each certificate Common Name to exactly equal its Raft node ID.

Shared-secret-only transport supports static bootstrap. Runtime
`AddVoter` and `RemoveServer` require mutual TLS because a common secret
does not provide revocable per-node identity.

The authenticated-encryption handshake uses the `raft.v3` protocol
marker. Upgrading from an older plaintext Raft build requires an
all-node restart; mixed protocol versions refuse the connection.

## Determinism

The FSM apply path takes the **leader's wall-clock time** from each
command (so every replica sees the same `now`), generates lock tokens
from `FenceCounter` + a leader-provided per-token salt, and never calls
`time.Now`, `crypto/rand`, or anything else that would diverge. The
`Snapshot()` / `Restore()` round-trip is byte-deterministic (sorted
iteration over resources, holders, and the enqueued index), so two
nodes with identical state produce identical snapshot bytes.

## Failure modes (and what dflockd does)

| Failure | Behaviour |
|---|---|
| One follower lost | Leader keeps committing (still has a quorum); follower catches up on rejoin via AppendEntries or InstallSnapshot. |
| Leader lost | A new election begins. A follower wins (Raft §5.4 election restriction guarantees its log is up-to-date) and serves writes. Clients see one or more `error_not_leader` redirects and retry. |
| Full restart | Each node recovers its HardState + WAL + latest snapshot from `--raft-dir`. Once a quorum is alive an election runs. State is intact. |
| Partition into a minority | Minority cannot commit (no quorum). Its local `stats` view may be stale; mutating clients see `error_not_leader` and retry against the majority. |
| Client connection drops mid-`acquire` | `client.Cluster` uses a generated stable ref and re-attaches after failover. Low-level TCP and HTTP callers need their own stable ref. `OrphanTTL` controls retention after graceful disconnect; hard-failover reattachment does not require it. |
| Node disk fills | A failure on any durable HardState, WAL, or snapshot mutation fail-stops Raft, removes readiness, cancels listeners, and causes a nonzero process exit before the node can acknowledge non-durable state. |

## Clock-skew posture

Lease deadlines are stored as **absolute Unix nanos** in the FSM (set
from the leader's `now` at propose time). The leader's sweep evicts
holders past those deadlines against its own clock. dflockd assumes
cluster members are NTP-synced within a few seconds. A small skew
(seconds) merely shifts lease expiry slightly relative to the wall
clock; it does not affect safety. Catastrophic skew (minutes) is the
operator's problem to solve — same as on a single-node server.

## Pointers

- `PLAN.md` — full 15-phase design and the §7 production-readiness
  checklist.
- `internal/raft/doc.go` — Raft scope and concurrency model.
- `internal/cluster/doc.go` — glue layer.
- See [Operations / Cluster](../operations/cluster.md) to bring one up.
