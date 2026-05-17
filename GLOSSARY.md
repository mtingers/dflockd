# Glossary

Shared vocabulary for dflockd. Terms appearing in code, tests, docs,
PLAN.md, PRODUCTION_READINESS.md, the README, and CHANGELOG should
all use these spellings and meanings.

Conventions in this file: `code identifiers` in backticks, *italics*
for terms-as-terms, related entries linked via [[wiki-style]] cross-
references and quoted in the "See also" line where helpful.

## Core domain — locks, semaphores, FIFO

- **Lock** — A mutually-exclusive resource keyed by a UTF-8 *key*. The
  base case of a *semaphore* with `Limit = 1`. Internally a [[Resource]].
- **Semaphore** — A resource that allows up to `Limit > 1` simultaneous
  holders. Same FIFO queue, snapshot format, and lease lifecycle as a
  lock. CLI/wire commands prefixed `s` (e.g. `sl` = sem-lock).
- **Resource** — The unified `internal/lock.ResourceState`: `{Limit,
  Holders, Waiters, LastActivity}`. Single struct that covers both
  locks (Limit=1) and semaphores (Limit>1). One resource exists per
  contended key.
- **Holder** — A connection currently in the grant set of a resource:
  `{connID, leaseExpires, token}`. Multiple holders allowed only for
  Limit>1 semaphores.
- **Waiter** — A connection queued for a resource it can't yet hold.
  Waiters are served strict FIFO. Each carries a `salt [8]byte` so the
  token it will receive on promotion is deterministic at FSM apply
  time. See [[Fencing token]], [[Salt]].
- **Lease** — The grant has an absolute wall-clock expiry equal to
  *grant time + lease TTL*. The server's lease-expiry sweep evicts
  expired holders. The Go client renews automatically at half the TTL.
- **Lease TTL** — `lease_ttl_s` (seconds). Defaults to
  `--default-lease-ttl` (33s) when 0 is requested. See
  [[Lease sweep]], [[Auto-release on disconnect]].
- **Lease sweep** — Background loop that calls `evictExpired` across
  shards. Single-node: runs in `LeaseExpiryLoop`. Cluster: runs
  leader-only as a proposed `EvictExpired` command. See
  [[Cluster mode]].
- **Auto-release on disconnect** — Default-on (`--auto-release-on-
  disconnect=true`). On TCP close the server runs `CleanupConn` (or
  proposes it in cluster mode) — every key the connection held is
  released and every waiter it had is removed.
- **Two-phase enqueue/wait** — The TCP protocol's `e`/`se` (enqueue)
  followed by `w`/`sw` (wait). Enqueue returns immediately with a
  preliminary token; Wait blocks until the grant lands. Used by
  middleware patterns that need to know they're in the queue before
  parking the request. See `internal/lock/state.go:enqueuedState`.
- **Connection ID (`connID`)** — A per-`server.Server`-instance uint64
  identifying a TCP connection. In cluster mode it's globally unique
  across a failover (high bits = random per-process epoch, low bits =
  counter), so a leader-survivor's fresh IDs never collide with a
  dead leader's orphans.
- **Fencing token** — A 32-lowercase-hex string returned on every
  acquire. Layout: first 16 hex = a server- (or in cluster mode,
  cluster-) monotonic uint64 *fence number*, big-endian; last 16 hex
  = unguessable random *salt*. The fence prefix strictly increases
  with each grant on a given node so a token is also a fencing token
  in the [Martin Kleppmann sense](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html).
- **Salt** — The 8 random bytes (16 hex chars) at the tail of every
  token. Anti-guessing material. Per-waiter salts are also carried
  in `Enqueue` so the future `Wait` grant is deterministic. See
  [[FSM determinism]].
- **Fence number** — The monotonic uint64 prefix of a token. With
  `--fence-state-file` set, monotonicity holds strictly across
  restarts via an fsynced two-slot journal; default-off seeds from
  `time.Now().UnixNano()` (best-effort, breaks if wall clock regresses).

## Wire protocols

- **TCP line protocol** — Three newline-terminated UTF-8 lines:
  `command\nkey\narg\n`. Canonical surface; everything else in dflockd
  is layered on it. Documented in `docs/architecture/protocol.md`.
- **HTTP REST API** — Optional, opt-in (`--http-port`). RESTful
  resources `/v1/sessions/`, `/v1/locks/`, `/v1/semaphores/`. Backed
  by the same `LockManager`, so an HTTP caller and a TCP caller on
  the same key share a queue. See `internal/httpapi/openapi.json`.
- **Session** (HTTP) — A server-side aggregate that owns a single
  `connID` for the HTTP API; persists across multiple HTTP requests
  bearing the `X-Dflockd-Session` header. `DELETE` on the session
  releases everything it holds — the HTTP analogue of a TCP
  disconnect.
- **`error_not_leader` (TCP) / `503 not_leader` (HTTP)** — Returned
  by a non-leader cluster node for a mutating command, carrying the
  current leader's client address. Clients are expected to retry at
  that address. The Go client surfaces it as
  `*client.NotLeaderError{Leader string}` via `client.IsNotLeader`.

## Cluster + Raft

- **Single-node mode** — Default. No `--raft-dir`. `LockManager` is
  the sole source of truth; in-memory state only (except optional
  fence-state file). Byte-identical behaviour to v2.1.x.
- **Cluster mode** — Opt-in: `--raft-dir`, `--node-id`, `--raft-addr`,
  `--cluster-peers`. The `LockManager` becomes the *FSM* of a Raft
  group; every mutating command is proposed through Raft and applied
  on every node in the same order.
- **Raft** — The consensus algorithm (Ongaro & Ousterhout). In this
  repo, hand-rolled in `internal/raft` with no external deps. See its
  `doc.go` for the implementation's scope and concurrency model.
- **`raft.Node`** — One Raft consensus member's worth of state +
  goroutines (run loop, apply goroutine, RPC handlers). Application-
  agnostic. See `internal/raft/node.go`.
- **`cluster.Node`** — The dflockd-side glue. Owns a `raft.Node` +
  file storage + TCP transport + the FSM adapter, plus leader-only
  loops (lease sweep, GC). The thing `cmd/dflockd` wires up. See
  `internal/cluster/node.go`. Not the same as [[`raft.Node`]].
- **Term** — Raft logical clock; a monotonically-increasing uint64.
  Every leader election picks a new term.
- **Leader / Follower / Candidate** — The three Raft roles. dflockd
  also runs a **PreVote** phase before a real campaign to prevent
  term inflation from a partitioned-then-rejoined node.
- **Voter / Member** — A node that is part of the current Raft
  configuration and counts toward the majority. Non-voting "learner"
  members are not implemented (out of scope per PLAN.md §1).
- **Log entry / Log index** — One replicated operation in the Raft
  log, indexed from 1. Entries are persisted (fsync'd) in the WAL
  before any RPC that depends on them is answered.
- **`commitIndex`** — The highest log index known to be replicated to
  a majority of voters. The apply goroutine feeds entries `≤
  commitIndex` to the FSM in order.
- **WAL** — Write-ahead log on disk: `<raft-dir>/raft-log/`. Append-
  only; size-capped per file; torn-tail-truncated on open; flushed
  with `fsync` before RPC replies. See `internal/raft/wal.go`.
- **`HardState`** — `{currentTerm, votedFor, commitIndex}`; persisted
  (with fsync) to `<raft-dir>/raft-state` before any RPC that relies
  on it is sent. See `internal/raft/hardstate.go`.
- **Snapshot (FSM)** — A serialized full copy of the
  `LockManager` state plus its `fsmFenceCounter`, written atomically
  to `<raft-dir>/snapshots/` and `flock(2)`-protected. Used for log
  compaction and for catching up far-behind followers.
- **InstallSnapshot** — Raft RPC that ships a snapshot to a follower
  whose log start is behind the leader's snapshot index. Application
  side: `FSM.Restore`, serialised through the apply goroutine.
- **FSM** — Finite State Machine. dflockd's FSM is `*lock.LockManager`
  driven through the `ApplyAcquire` / `ApplyEnqueue` / `ApplyRelease`
  / `ApplyRenew` / `ApplyEvict` / `ApplyCleanupConn` / `ApplyGC`
  methods (pure functions of state + command + a leader-supplied
  `NowNanos` and per-token `Salt`).
- **FSM determinism** — Every replica's FSM must reach byte-identical
  state after applying the same log. Achieved by: leader-supplied
  `NowNanos`, per-command salts, sorted-key iteration in
  `ApplyCleanupConn`, and bounded-fixed-width fields. Verified by
  `internal/lock/apply_test.go:TestApplyDeterministicReplay`.
- **Apply path** — The single-goroutine pipeline that takes
  committed entries off `raft.Node`'s apply channel and calls into
  the FSM. The only writer of FSM state. See [[FSM determinism]].
- **Membership change** — Single-server `AddVoter` / `RemoveServer`
  (Raft §4.3 — joint consensus is out of scope). A `ConfigEntry`
  takes effect *as soon as appended* (not at commit), per the paper.
- **Leadership transfer / `TimeoutNow`** — The leader hands leadership
  to its most-caught-up follower by sending `TimeoutNow`. Triggered
  by `raft.Node.TransferLeadership` (and `cluster.Node.Close` on the
  leader during a graceful rolling restart).
- **`ReadIndex` / `Barrier`** — The internal mechanism for a
  linearizable read: the leader confirms its leadership with a
  heartbeat round, then waits for `commitIndex` to apply before
  reading. Exposed today as `cluster.Node.Barrier(ctx)`; a public
  `ReadIndex` API is a documented follow-on.
- **Mutual TLS (mTLS) on Raft transport** — `--raft-tls-cert/-key/-ca`
  (all-or-none). When set, every inter-node TCP connection is TLS 1.3
  with `RequireAndVerifyClientCert`. Default off; the startup log
  warns when running plaintext.
- **PreVote** — A pre-flight vote round before bumping `currentTerm`.
  A partitioned node that rejoins doesn't disturb the term.

## Ops + observability

- **`--raft-dir`** — The cluster-mode persistence root. Holds the
  WAL, HardState, snapshots, and a `flock(2)` exclusivity lock that
  refuses a second concurrent process. Refused on non-Unix platforms.
- **`--fence-state-file`** — Optional path to a 64-byte journal that
  pre-allocates fence-counter ranges with `fsync` (one fsync per ~1M
  grants). Set it for strict cross-restart fence monotonicity.
- **Sweep loop / GC loop** — In cluster mode, leader-only:
  `cluster.Node.sweepLoop` proposes `EvictExpired` every
  `--lease-sweep-interval` (default 1s) and `GC` every 30 ticks. In
  single-node mode the same logic runs in-process without proposals.
- **`/metrics`** — Prometheus exposition. Single-node: HTTP
  request/lock counters. Cluster: adds `dflockd_raft_*` gauges
  (role, term, commit index, last log index, snapshot index, voters,
  is_leader).
- **`stats`** — TCP/HTTP read-only command. Returns counts of
  resources, holders, waiters, and (cluster mode) a `cluster` JSON
  block matching the `/metrics` gauges.
- **CRC32 sharding (client side)** — The Go/Python/TS clients hash
  the key with CRC-32 (IEEE) to pick a server from `Servers` so the
  same key reaches the same node in single-node, multi-instance
  deployments. Bypassed in cluster-aware client code paths.

## Known to drift if not pinned

- **"Node"** alone is ambiguous: prefer [[`raft.Node`]] or
  [[`cluster.Node`]].
- **"Lock state"** vs [[Resource]]: pre-v2 callers said "lock state",
  but the unified type is `ResourceState`.
- **"Fence" / "fence token" / "fencing token"**: all three appear in
  docs/comments; the official spelling is [[Fencing token]] (token
  whose prefix is a [[Fence number]]).
- **"Cluster member" / "voter" / "peer"**: in v1 they're synonyms.
  Once non-voting learners exist, "member" will become the umbrella
  term and "voter" the narrower one. See [[Voter / Member]].
