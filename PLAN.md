# PLAN — dflockd HA cluster (Raft + persistent storage + replication)

Status: living document. Phase checklist at the bottom is the source of truth for
progress. Implementation lands on branch `raft-replication`.

---

## 1. Goal

Run dflockd as an **N-node (odd; 3 or 5) highly-available cluster**:

- Lock state is replicated via **Raft consensus** (implemented in-repo, no new
  runtime dependencies — `go.sum` stays empty).
- State is **durable**: the Raft log and periodic FSM snapshots are persisted to
  disk, so the cluster recovers its lock state after a full restart, not just a
  single-node crash.
- Clients connect to any member; mutating operations are linearizable (they go
  through the Raft log on the leader); a non-leader tells the client where the
  leader is (`error_not_leader <host:port>`) and the client library follows it.
- The cluster tolerates a **minority** of nodes being down/partitioned (1 of 3,
  2 of 5) with no loss of safety and (modulo an election) no loss of liveness.

### Non-goals (v1)

- **Multi-Raft / sharded clusters.** One Raft group owns all keys. Horizontal
  scale-out via independent shards remains a "run more clusters" story.
- **Strict FIFO preservation across a leadership change.** A client blocked in
  `acquire`/`wait` when the leader fails loses its queue position; on reconnect
  to the new leader it re-enqueues at the back. (A holder that *was* granted
  keeps its token and lease seamlessly — see §4.7.) A later phase adds
  client-stable acquisition IDs to close this gap; v1 documents it.
- **Cross-WAN / geo deployments.** Assumes a low-latency LAN between members and
  roughly NTP-synced clocks (lease deadlines are absolute wall-clock times — §4.6).
- **Witness/arbiter-only nodes.** Every member is a full voting Raft member.
  (Non-voting "learner" members are an optional later phase.)
- **Online downgrade to single-node.** Cluster mode is chosen at startup.

### Backward compatibility

When neither `--raft-dir` nor `--cluster-peers` is set, the server behaves
**exactly as today** — single process, fully in-memory, no Raft, the existing
wire protocol and HTTP API unchanged, `--fence-state-file` still honoured. All
cluster code is dormant. The Go/Python/TS clients work unchanged against a
single node; cluster awareness is opt-in (`ClusterMode` / following
`error_not_leader`).

---

## 2. Architecture

```
                 ┌──────── clients (TCP line proto / HTTP) ────────┐
                 │  route mutating ops to the leader; follow        │
                 │  error_not_leader redirects; retry on failover   │
                 └────────────────────────┬─────────────────────────┘
                                          ▼
   ┌─────────── node A (leader) ───────────┐   ┌─── node B ───┐  ┌─── node C ───┐
   │  server (TCP/HTTP)                     │   │  server      │  │  server      │
   │     │ mutating op → cluster.Propose    │   │  (followers  │  │   redirect   │
   │     ▼                                  │   │  redirect)   │  │   clients)   │
   │  cluster.Node ── raft.Node ── transport│◄─►│  raft.Node   │◄►│  raft.Node   │
   │     │ commit                           │   │     │ apply  │  │     │ apply  │
   │     ▼ apply (single goroutine)         │   │     ▼        │  │     ▼        │
   │  FSM = lock.LockManager (deterministic)│   │  LockManager │  │  LockManager │
   │     ▲                                  │   │              │  │              │
   │  storage: raft-state, raft-log/, snaps/│   │  storage…    │  │  storage…    │
   └────────────────────────────────────────┘   └──────────────┘  └──────────────┘
```

### Package layout

| Package | Role |
|---|---|
| `internal/raft` | Generic Raft. No dflockd specifics. Node (role FSM, election, replication), in-memory log, `Storage` interface + a file-backed implementation, `Transport` interface + an in-memory impl (tests) and a framed-TCP impl, `FSM` interface, snapshot/`InstallSnapshot`, config. |
| `internal/cluster` | Glue. `cluster.Node` owns a `raft.Node` + the file storage + the TCP transport + the FSM adapter. Defines the dflockd `Command` types (acquire/release/renew/enqueue/wait-claim/evict/gc/cleanup-conn/membership/no-op) and their (de)serialization. Implements `raft.FSM` over `*lock.LockManager`. Exposes `Propose(ctx, Command) (result, error)`, `IsLeader()`, `LeaderAddr()`, `Barrier()`, `TransferLeadership()`, `AddVoter`/`RemoveServer`, `Stats()`. |
| `internal/lock` | Gains: `Snapshot() *Snapshot` / `Restore(*Snapshot)` (FSM serialization, includes `FenceCounter` + `LastIndex`/`LastTerm`); a set of `Apply*` methods that perform the *committed* mutation deterministically and return the result + grant notifications (the apply path); `FenceCounter uint64` carried in state and bumped at apply; `waiter.salt [8]byte` and `waiter.ref` to support deterministic token minting on promotion. The existing `Acquire/Release/Renew/Enqueue/Wait` direct methods stay for non-cluster mode. |
| `internal/server` | Gains: a `Cluster` interface (so `server` doesn't import `cluster` directly — mirrors the existing `LockManager` access pattern); when a `Cluster` is wired, mutating commands are turned into `cluster.Command`s, proposed, and the apply result is awaited; non-leader → `error_not_leader <addr>`; the lease-sweep / GC loops run **only on the leader** and emit `Evict`/`GC` commands rather than mutating directly; connection-close cleanup proposes a `CleanupConn` command. |
| `internal/config` | New flags / env vars (§9). |
| `internal/protocol` | One new status: `error_not_leader` (carries the leader's `host:port` as the trailing token). Nothing else on the client↔server wire changes. |
| `internal/httpapi` | `error_not_leader` → `503` (or `421 Misdirected Request`) with `{"status":"not_leader","leader":"host:port"}`; new `GET /v1/admin/cluster` (membership + leader + log/commit indices) and `POST /v1/admin/transfer-leadership`. |
| `cmd/dflockd` | Wires `cluster.Node` when configured; `SIGUSR1` → leadership transfer (operator hook); fatal on storage-init failure. |
| `cmd/cluster-soak` | (new) in-process N-node cluster correctness-under-load harness (like the old `cmd/bench-rep`), plus a fault-injection mode (kill/restart leader, partition a follower). |
| `client` | `Lock`/`Semaphore` gain `ClusterMode bool`. In cluster mode the client treats `Servers` as cluster members, routes every key to the discovered leader, caches the leader, re-resolves on `error_not_leader` / dial failure with bounded backoff, and retries the in-flight op. CRC sharding is bypassed. |

### Relationship to the existing `feat/replication` branch

That branch's hand-rolled primary/secondary coordinator + witness (`internal/replication/replicator.go`, `witness.go`) is **not used** — Raft replaces it wholesale. Its *data-plane* ideas are adopted in spirit: `lock.LockManager` already has the `evictExpired` / `grantNext` / `getOrCreate` helpers factored out (good — the FSM apply path reuses them); the `Snapshot()` / `[]SnapshotEntry` shape is a good starting point for FSM serialization; lock tokens being opaque and carried verbatim is exactly right for a replicated FSM. We diverge on *who decides* (Raft, not a primary that self-promotes after a timeout) and on *what's persisted* (a real on-disk log, not "a synchronously-replicated peer in RAM").

---

## 3. Raft scope (what we implement)

Following the Raft paper (Ongaro & Ousterhout) — the core, done carefully:

- **Leader election** with randomized election timeouts; terms; `RequestVote`
  (incl. the up-to-date-log restriction §5.4.1); persistent `currentTerm` /
  `votedFor`.
- **Log replication** via `AppendEntries` (incl. heartbeats, the consistency
  check, conflict resolution / fast back-off using the follower's conflict
  hint, `commitIndex` advancement only over an entry of the current term §5.4.2).
- **Persistence**: `currentTerm`, `votedFor`, and the log are flushed (`fsync`)
  to disk *before* the RPC that depends on them is answered.
- **State machine apply**: a single apply goroutine feeds committed entries to
  the `FSM` in index order; `lastApplied` tracked; apply results routed back to
  the proposing caller via a per-entry future.
- **Snapshotting & log compaction**: when the log exceeds a threshold (entries
  or bytes), snapshot the FSM, persist `{lastIncludedIndex, lastIncludedTerm,
  membership, fsmBytes}`, then truncate the log prefix. `InstallSnapshot` RPC
  for followers that have fallen behind the leader's log start.
- **Membership changes**: single-server add/remove (Raft §4.3 — simpler and
  sufficient; joint consensus is out of scope). A `ConfigEntry` in the log; the
  new configuration takes effect *as soon as it is appended* (not when committed).
  `AddVoter` waits for the new member to catch up before adding it (avoid an
  availability dip). A leader removing *itself* steps down after the entry commits.
- **Read path**: dflockd's "reads" are `stats` (and `ping`). `stats` is served
  from the leader's applied state after a no-op barrier (cheap linearizable read
  via `ReadIndex`-lite: leader confirms leadership with a heartbeat round, waits
  for `commitIndex` ≥ readIndex to be applied, then reads). `ping` answered by
  any node. We do **not** implement follower reads or lease reads in v1.
- **Liveness niceties**: PreVote (avoid term inflation from a flapping node) —
  implement (small, high value). Leadership transfer (`TimeoutNow`) — implement
  (needed for graceful operator-driven handoff). Learner/non-voting members —
  *optional* later phase.

**Deliberately out of scope:** joint-consensus membership, witness-only nodes,
multi-raft, cross-DC tuning, log-entry compression, snapshot streaming
chunk-resumption (snapshots are sent as one framed message bounded by a max
size — fine for dflockd's small state).

---

## 4. Key design decisions (and why)

### 4.1 Why hand-rolled Raft

The project's identity is "single Go binary, zero runtime dependencies"
(`go.sum` is empty). `hashicorp/raft` / `etcd/raft` would change that. The FSM
is small and the workload is modest, so a careful from-scratch implementation —
heavily tested, with a deterministic in-memory transport for multi-node tests in
one process — is the right trade. Scope is bounded (§3).

### 4.2 The FSM is the LockManager

All lock state lives in one Raft group. A client mutating op becomes a
`cluster.Command`, is proposed on the leader, and is applied to *every* node's
`LockManager` from the committed log. `FSM.Apply(index, term, cmdBytes) ApplyResult`
must be a **pure function of (current state, command)** — no `time.Now()`, no
`crypto/rand`, no map-iteration-order-dependent output. To make that true:

- **Time**: every command carries `NowUnixNano` set by the *leader* at propose
  time. `Apply` uses that value wherever the single-node code used `time.Now()`
  (lease deadlines, `LastActivity`, expiry checks within the entry). Followers
  apply the same value → deterministic. (Clock skew between leader and follower
  affects only *future* leader behaviour, not FSM consistency — see §4.6.)
- **Randomness**: lock tokens are `<16-hex fence prefix><16-hex salt>`. The
  leader generates the 8 random `salt` bytes and puts them in the command (and,
  for a queued waiter, the salt rides along in the waiter entry so promotion is
  deterministic too). The **fence prefix** is `state.FenceCounter`, a `uint64`
  in FSM state, incremented on every grant during `Apply`. Token =
  `encodeToken(state.FenceCounter, cmd.Salt)`, computed identically on all nodes.
- **Map order**: `Apply` never produces output whose value depends on Go map
  iteration order. (Where the single-node code iterates `st.Holders` to e.g. pick
  "the" lock owner for stats, that's a *read* path, not apply — fine. The apply
  path mutates by explicit token.)
- **Idempotent-ish & order-tolerant** apply (defence in depth): re-applying a
  holder-add for an existing token replaces it; a holder-remove for an unknown
  token is a no-op; resources auto-create on first reference. (Raft guarantees
  exactly-once in-order apply, so this is belt-and-suspenders, but it makes
  snapshot-then-tail-replay and the occasional duplicate-command-on-retry safe.)

`ApplyResult` carries both the **direct response** for the proposing caller
(e.g. `{granted, token, leaseSec}` / `{queued, position}` / `{ok}` / an error
sentinel) and a slice of **grant notifications** `{ref, token, leaseSec}` for
waiters that this command promoted to holders. The leader's apply loop resolves
the proposer's future with the direct response and routes each notification to
the matching locally-blocked handler (by `ref`, see §4.7); a notification with
no local handler is dropped (the holder entry still exists in the FSM and will
either be claimed when the client reconnects — §4.7 — or expire — §4.6).

### 4.3 Command set

Serialized compactly (a small hand-rolled binary encoding or `encoding/json` —
JSON to start for debuggability, switchable later; the format is internal). Each
command struct embeds `Base{ NowUnixNano int64 }`.

| Command | Emitted by | FSM effect |
|---|---|---|
| `NoOp` | leader on election (commit-barrier; also the linearizable-read marker) | none |
| `Acquire{key, limit, ref, leaseTTLNano, salt}` | leader handling `l`/`sl` (single-phase) or the grant side of a queued waiter is handled by the *waiter entry*, not a command | free slot → add holder (fence++/token); else → append waiter `{ref, leaseTTL, salt}` (unless `ref` already holds/queues `key` → idempotent return) |
| `Enqueue{key, limit, ref, leaseTTLNano, salt}` | leader handling `e`/`se` phase 1 | same as `Acquire` but also records the two-phase enqueued bookkeeping; returns `acquired`+token or `queued` |
| `WaitClaim{key, ref}` | leader handling `w`/`sw` phase 2, **and** a client re-attaching after failover | if `ref` was promoted (token present) → finalize, return token; if still queued → return `queued` (handler then blocks); if unknown → `error_not_enqueued` |
| `Release{key, token}` | leader handling `r`/`sr` | remove holder; `grantNext` → promote head waiter(s) (each: fence++/token, emit notification) |
| `Renew{key, token, leaseTTLNano}` | leader handling `n`/`sn` | extend lease (or reject if already expired-per-`NowUnixNano`, evicting + promoting) |
| `Evict{key, token}` | **leader** lease-sweep loop, one per expired holder | remove holder; `grantNext` (notifications). Idempotent if already gone. |
| `GC{keys[]}` | **leader** GC loop | drop idle resources (no holders/waiters/recent activity per `NowUnixNano`) |
| `CleanupConn{ref}` | leader on a client connection close (auto-release path) | release that ref's holders + drop its waiters/enqueued state; `grantNext` |
| `AddVoter{id, addr}` / `RemoveServer{id}` | admin / membership ops | change cluster configuration (a `ConfigEntry`, special-cased by `raft.Node`, *not* the FSM — but the FSM ignores it cleanly) |

`ref` (a "client reference") is `"<nodeID>:<connID>"` in v1 — globally unique,
identifies the connection that issued the op so `CleanupConn` and grant routing
work. (Future: an optional client-supplied stable ID for failover-resilient FIFO.)

### 4.4 Who can mutate; client routing

- Only the **leader** proposes commands. A follower (or candidate) that receives
  a mutating client command replies `error_not_leader <leaderHost:leaderPort>`
  (empty addr if the leader is currently unknown — client backs off and retries
  another member). The advertised address is the member's *client* address, not
  its Raft address — config carries both (`--advertise-addr`, defaults to
  `--host:--port`).
- `ping` is answered by anyone. `stats` requires leadership (a follower returns
  `error_not_leader`) — keeps `/v1/stats` linearizable and avoids confusing
  "this node's partial view" output.
- The **Go client** in `ClusterMode`: maintains `leader atomic.Pointer[conn]`;
  on a fresh op, use the cached leader; on `error_not_leader <addr>` reconnect to
  `<addr>` (or, if empty/unreachable, round-robin the configured members) and
  retry; on dial/IO error likewise; cap total retry time at `AcquireTimeout`
  (or a separate `ClusterDialTimeout`). CRC sharding is skipped — `Servers` is a
  membership list, not a shard map. Two-phase enqueue/wait pin to the
  leader-conn that served the `enqueue`; on failover the client re-issues
  `enqueue` (losing its slot — documented) unless it can re-attach via
  `WaitClaim` (only possible with a stable ref — future).

### 4.5 Persistence layout

`--raft-dir=<dir>` (required for cluster mode). Contents:

```
<dir>/
  node-id                 # the stable node id, created on first start (random if --node-id unset)
  raft-state              # CRC'd, fsync'd: {currentTerm, votedFor, lastConfigIndex}. Two-slot journal like internal/lock/fence.go.
  raft-log/
    000000000001.seg      # append-only segments of length-prefixed CRC'd entries; rotated by size
    000000000257.seg
  snapshots/
    snap-000000000256-000000000003.tmp   # written then atomically renamed
    snap-000000000256-000000000003       # {meta: lastIncludedIndex,lastIncludedTerm,config,fsmFormat,fsmLen} + fsm bytes; CRC'd
```

- Reuse the patterns already in `internal/lock/fence.go`: `flock(2)` the dir
  (refuse to start if another dflockd holds it; refuse on non-Unix), CRC-32 per
  record, `fsync` the file *and* the directory entry after a rename.
- On startup: acquire the dir lock → load newest valid snapshot → `FSM.Restore`
  it → open log segments, discard anything ≤ snapshot index, validate CRCs,
  replay entries into the FSM up to the last durable entry → read `raft-state`
  → ready. A torn tail record (partial write, bad CRC) truncates the log there
  (it was never acknowledged, so it's safe to drop).
- Compaction: after a snapshot at index *i*, segments wholly ≤ *i* are deleted;
  the segment containing *i* is left (cheap; cleaned on the next rotation).

### 4.6 Leases in the cluster

- Lease deadlines are stored as absolute Unix-nanos in the FSM (set from the
  command's `NowUnixNano + leaseTTLNano`) → deterministic across nodes.
- **Only the leader** runs `LeaseExpiryLoop` and `GCLoop` (a follower would race
  the leader and propose nothing — so it just doesn't run them). The leader's
  sweep finds holders whose absolute deadline < its local `time.Now()`, and
  proposes one `Evict` per holder (batched: a single `Evict{key, tokens[]}` per
  key, or a small slice of `Evict` — implementation detail). The `Evict` then
  applies on all nodes deterministically.
- **Clock-skew posture** (documented): if leader L's clock is *behind* real
  time, leases live a bit too long (lock held slightly longer than the TTL — a
  liveness nit, not a safety bug; identical to the single-node server trusting
  its own clock). If L's clock is *ahead*, leases are evicted a bit early — a
  holder might lose its lock fractionally before the TTL it was promised; the
  client's background renewer (at TTL/2) makes this a non-issue in practice. The
  cluster assumes members are within a few seconds of each other (NTP). We do
  **not** attempt a logical-clock lease scheme in v1.
- On a leadership change, the new leader simply starts its sweep loop; the
  absolute deadlines in the FSM are already correct, so nothing special is
  needed. (A holder granted by the old leader at `T+60s` is evicted by the new
  leader once *its* clock passes `T+60s`.)

### 4.7 Waiters, grants, and failover

- The full `ResourceState` (holders + ordered waiter queue + two-phase enqueued
  map) is in the FSM, replicated, snapshotted. A waiter entry is
  `{ref, leaseTTLNano, salt}` — note: **no channel** (channels aren't FSM data).
- On the **leader**, a client blocked in `acquire`/`wait` registers a
  leader-local `chan grantNotice` in a `map[ref]chan grantNotice` (the
  "wait registry") *before/around* proposing, and selects on it + the request
  timeout + connection-close. When `Apply` (on the leader) promotes that ref to
  holder, the apply loop sends the notice (token + leaseSec) on that channel.
  Followers have no wait registry — their `Apply` produces the same notification
  slice but there's nothing to route it to, which is correct.
- On **leadership change**: the old leader's wait registry evaporates with the
  process / role change. Blocked clients see their connection drop (or, if the
  old leader merely stepped down, get `error_not_leader`). In `ClusterMode` the
  client reconnects to the new leader and **re-issues** the op:
  - It was a *holder* already (had a token): `renew`/`release` work as-is (no
    connID check — §2 note) → seamless.
  - It was a *waiter* (no token yet): re-`acquire`/`enqueue` → the FSM, keyed by
    the *new* `ref` (new conn), enqueues it fresh at the back → **FIFO position
    lost**. Documented limitation. (With a stable client ref it would re-attach
    via `WaitClaim` — future phase.)
  - It was a promoted-but-unobserved waiter (token minted, notice lost in the
    failover): the holder entry exists in the FSM with a ticking lease the
    client doesn't know about. The client's re-`acquire` enqueues fresh; once
    the ghost holder's lease expires (≤ TTL), the slot frees. Self-healing.
    (Stable client ref → `WaitClaim` would return the existing token. Future.)
- `trySendGrant` semantics carry over: a notice that can't be delivered
  (full/closed/absent channel) is dropped; the holder entry persists; lease
  expiry is the backstop. This is already how the single-node code behaves on a
  torn-down waiter.

### 4.8 connID namespace & cleanup

In cluster mode all *stateful* client connections live on the leader (followers
redirect them away), so connection-close cleanup happens on the leader, which
proposes `CleanupConn{ref}`. A node crash skips cleanup → those holders persist
until lease expiry (correct: a crash/partition must not auto-release). `ref`
embeds the node id so a `CleanupConn` is unambiguous even right after a failover
(the new leader inherits old-leader-ref'd holders; their `ref`s never "close" on
the new leader → they ride lease expiry, as intended).

### 4.9 Security & limits

- Raft RPC transport: a static **cluster shared secret**
  (`--raft-auth-token-file`) drives challenge-response authentication and
  directional AES-GCM sessions. Optional mutual TLS
  (`--raft-tls-cert/-key/-ca`) additionally binds certificate CN to NodeID.
  (Independent of `--auth-token`, which is the client-facing secret.)
- `MaxLocks` / `MaxWaiters` are enforced at `Apply` time (deterministically — a
  command that would exceed the cap returns the error sentinel from `Apply`, and
  the leader relays it to the client). `--max-connections*` stay per-node, on the
  accept path, as today.
- Raft RPC frames are length-prefixed with a hard cap (a few MiB; snapshots get
  a larger but still bounded cap). A peer that sends garbage / overlong frames is
  dropped.

---

## 5. Testing strategy

- **Unit**, per package, tiny functions → easy to hit branches. The
  deterministic in-memory `Transport` (a goroutine-safe message bus with
  controllable delivery/partition/drop) lets `internal/raft` run a real 3- and
  5-node cluster *inside one test process* with no sockets — election, log
  catch-up, leader failure, partition heal, snapshot install, membership change
  — all as fast deterministic table tests.
- **Storage** tests: write/reopen/verify; torn-tail truncation; CRC corruption
  rejected; snapshot round-trip; compaction; concurrent-open refused (`flock`).
- **FSM** tests: every `Command` against `LockManager` state — including
  determinism checks (apply the same log on two fresh managers → identical
  `Snapshot()`), idempotent re-apply, snapshot-then-tail-replay equivalence.
- **`internal/server`** integration: a 3-node in-process cluster wired over the
  in-memory transport, real `Server`s on real loopback TCP ports; assert: client
  on a follower gets `error_not_leader`; client on the leader acquires/releases;
  kill the leader → a follower wins → the client (cluster-mode) retries and
  continues; bring the old leader back → it rejoins as follower and catches up;
  restart the *whole* cluster → state recovered from disk.
- **Race**: `go test -race ./...` is the gate (the project already runs it).
- **`cmd/cluster-soak`**: N workers doing acquire/release loops via the public
  `client` against the cluster, with a fault thread (periodically kill+restart
  the leader, partition a follower); at the end every surviving node's
  `LockManager` must converge (zero holders once all clients release). Also a
  `--linearizability-check` mode recording an op history and checking it against
  a sequential lock model (a Porcupine-style check, hand-rolled — small).
- **Complexity gate**: `make complexity` stays green; new functions target the
  house style (short, low cyclomatic complexity, table-driven dispatch over
  switch ladders). Run `go run ./tools/complexity -prod -top 30` before each
  phase wraps.

---

## 6. Phases

Each phase ends with: code compiles, `go test ./...` (and `-race` for anything
concurrent) green, `go vet` clean, `make complexity` not regressed, a CHANGELOG
note staged. Phases are ordered so each builds on tested foundations.

### Phase 0 — scaffolding & shared types
- `internal/raft/doc.go`, `internal/raft/types.go`: `Term`, `Index`, `NodeID`,
  `Entry{Index, Term, Type (EntryNormal|EntryConfig|EntryNoOp), Data []byte}`,
  `ServerID`/`ServerAddr`, `Configuration{Voters map[NodeID]Addr}`.
- `internal/raft/config.go`: `Config{HeartbeatInterval, ElectionTimeoutMin/Max,
  MaxAppendEntries, SnapshotThresholdEntries, SnapshotThresholdBytes,
  MaxInflightSnapshotBytes, ApplyChanDepth, ...}` with `DefaultConfig()` +
  `Validate()`.
- Done: package builds; `Config.Validate` tested.

### Phase 1 — persistent storage (`internal/raft/storage*.go`)
- `Storage` interface: `LoadHardState() (HardState, error)` / `SaveHardState`;
  `LoadConfiguration` / `SaveConfiguration`; `Entries(lo, hi) ([]Entry, error)`;
  `Append([]Entry) error`; `Truncate(prefixThru Index)`/`TruncateSuffix(from Index)`;
  `FirstIndex()/LastIndex()`; `SaveSnapshot(meta, fsmReader)` / `LoadLatestSnapshot()`;
  `ListSnapshots`; `Close()`. Plus `DirLock`.
- File implementation: segmented append-only log (record = `len|crc|payload`),
  segment rotation by size, suffix-truncate (rewrite the tail segment), torn-tail
  detection; two-slot CRC journal for `HardState` (mirrors `internal/lock/fence.go`);
  snapshot files written-then-renamed with dir fsync; `flock` the dir.
- Also: `MemStorage` (in-memory `Storage` for fast raft tests).
- Tests: round-trip; reopen; torn tail truncated; bad CRC rejected; rotation;
  prefix/suffix truncate; snapshot save/load/list/GC; `flock` double-open refused;
  `MemStorage` parity (same test suite via an interface conformance helper).
- Done: storage solid in isolation.

### Phase 2 — Raft log (in-memory) + node core
- `internal/raft/log.go`: `raftLog` over `Storage` — `lastIndex/lastTerm`,
  `term(i)`, `entries(lo,hi)`, `append`, `matchLog(prevIdx,prevTerm)`,
  `findConflict`, `commitTo`, `appliedTo`, `compactTo(snapMeta)`,
  `maybeCompact(cfg)`. Caches a tail window in memory; reads older from `Storage`.
- `internal/raft/node.go`: `Node` — role (`Follower|Candidate|Leader`), `term`,
  `votedFor`, `leaderID`, election/heartbeat timers (randomized), `tick()`,
  the run loop (single goroutine owns all state; everything else talks to it via
  channels: `proposec`, `recvc` for inbound RPCs, `readyc`/applier handoff,
  `confchangec`, `transferc`, `tickc`, `stopc`). PreVote + Vote. Becoming
  leader: append a `NoOp` of the new term immediately; init `nextIndex/matchIndex`.
- Internally factored so the run loop is a thin dispatcher over small handlers
  (`stepFollower/stepCandidate/stepLeader` each a table of message-type → handler).
- Tests (in-memory transport + `MemStorage`): single node elects itself; 3-node
  elects exactly one leader; higher term steps a leader down; split vote re-elects;
  PreVote doesn't bump terms on a partitioned flapper; election restriction
  (stale-log candidate can't win).
- Done: elections correct under the in-memory transport.

### Phase 3 — replication RPCs + Transport
- `internal/raft/transport.go`: `Transport` interface — `Send(to NodeID, msg)`,
  `SetHandler(func(from NodeID, msg) msg)` (RPC-style request/response), plus
  `AddPeer/RemovePeer`. Messages: `RequestVote{,Reply}`, `AppendEntries{,Reply}`
  (reply carries `ConflictIndex/ConflictTerm` hint), `InstallSnapshot{,Reply}`,
  `TimeoutNow`.
- `MemTransport`: in-process bus with per-link controls — `Partition(a,b)`,
  `Heal`, `Drop(p)`, `Delay(d)`, `Reorder` — for deterministic fault tests.
- Node: leader replication (`AppendEntries` to each follower, `nextIndex`
  back-off on conflict using the hint, advance `matchIndex` → `commitIndex` by
  majority over a current-term entry); follower `AppendEntries` handling
  (term check, log-match, conflict truncate+append, `commitIndex` update,
  reset election timer); `RequestVote` handling (term, election restriction,
  granted-vote persistence *before* reply).
- Tests: log converges across 3 nodes; follower with a divergent tail is fixed;
  a lagging follower catches up; commit only advances over a current-term entry
  (the classic Figure-8 scenario); partition a follower then heal → it catches
  up; leader loses majority → stops committing → regains → resumes.
- Done: replication + safety under faults, all on `MemTransport`.

### Phase 4 — FSM, apply pipeline, snapshots & compaction
- `internal/raft/fsm.go`: `FSM` interface — `Apply(e Entry) any` (returns the
  result object for the proposer's future), `Snapshot() (FSMSnapshot, error)`,
  `Restore(io.Reader) error`; `FSMSnapshot{ Persist(w io.Writer) error; Release() }`.
- Apply loop: a dedicated goroutine drains committed-but-unapplied entries in
  index order, calls `FSM.Apply`, fulfils the matching `proposal` future
  (`map[Index]*proposal`, created at propose time on the leader; on a
  follower/term-change the map is cleared with `ErrNotLeader`/`ErrLeadershipLost`).
  `ConfigEntry` and `NoOp` are handled by the node (FSM sees them as no-ops or
  not at all).
- Snapshotting: when `raftLog.maybeCompact` says so (or on `Node.Snapshot()`
  request), capture `FSM.Snapshot()` *without holding the run loop* (the FSM
  provides a point-in-time view), persist via `Storage.SaveSnapshot`, then tell
  the run loop to `compactTo(meta)`. `InstallSnapshot`: leader streams a snapshot
  (one bounded framed message) to a follower whose `nextIndex` < log start;
  follower persists it, `FSM.Restore`s it, resets its log to start at
  `lastIncludedIndex`, replies; leader sets that follower's `nextIndex`.
- `Node.Propose(ctx, data) (future)`, `Node.ProposeConfChange`, `Node.ReadIndex(ctx)`
  (leadership-confirmed linearizable read barrier), `Node.Barrier(ctx)` (propose a
  `NoOp`, wait for it to apply), `Node.TransferLeadership(ctx, target)`.
- Tests: propose → commit → apply → future resolves with the FSM result;
  proposer on a node that loses leadership gets `ErrLeadershipLost`; snapshot
  taken, log truncated, a restarted node restores from it; a far-behind follower
  gets `InstallSnapshot` and converges; `ReadIndex` reflects all
  acked-before-it writes; leadership transfer hands off cleanly and fast.
- Done: a generic, persistent, snapshotting Raft with a pluggable FSM — fully
  tested without touching dflockd.

### Phase 5 — `lock.LockManager` as a deterministic FSM
- `internal/lock/state.go` / `lock.go`: add `FenceCounter uint64` to the
  manager's per-cluster state; `waiter` gains `salt [8]byte` + `ref string`;
  add `Snapshot() *ClusterSnapshot` (all shards: per-key `{limit, holders[],
  waiters[], enqueued[]}` + `FenceCounter`) and `Restore(*ClusterSnapshot)`
  (wipe + rebuild; rebuilds `connOwned`/`connEnqueued` indices from the ref data).
- `internal/lock/apply.go` (new): the committed-mutation API, each method pure
  over (state, args) using a passed-in `now time.Time`:
  - `ApplyAcquire(now, key, limit, ref, leaseTTL, salt) AcquireResult`
  - `ApplyEnqueue(...)`, `ApplyWaitClaim(now, key, ref) WaitResult`
  - `ApplyRelease(now, key, token) (ok bool, grants []Grant)`
  - `ApplyRenew(now, key, token, leaseTTL) (remaining int, ok bool, grants []Grant)`
  - `ApplyEvict(now, key, token) (ok bool, grants []Grant)`
  - `ApplyGC(now, keys) (removed int)` and `IdleKeys(now)` (the leader sweep
    calls `IdleKeys` to build the `GC` command list)
  - `ApplyCleanupConn(now, ref) (released int, grants []Grant)`
  These reuse the existing `getOrCreate`/`grantNext`/`evictExpired` internals,
  refactored to (a) take `now` rather than call `time.Now()`, (b) mint tokens
  from `FenceCounter` + the relevant `salt`, (c) accumulate `[]Grant` instead of
  pushing on channels. `Grant{ref, token, leaseSec}`.
  Note: the *non-cluster* `Acquire`/`Release`/etc. are reimplemented as thin
  wrappers — capture `now := time.Now()`, generate a salt, call the `Apply*`
  core, then push grants onto the local waiter channels — so there is **one**
  implementation of the lock logic, exercised by both paths.
- `internal/lock/fence.go`: in cluster mode the `fenceAllocator` is unused
  (`NewLockManagerForCluster` skips it); `--fence-state-file` + cluster mode is a
  config error (Raft persistence supersedes it).
- Tests: each `Apply*` against hand-built states; the determinism property
  (replay a command list on two fresh managers → equal `Snapshot()`); snapshot
  round-trip; the non-cluster wrappers still pass the *entire existing*
  `lock_test.go` suite (this is the regression guard for the refactor).
- Done: `LockManager` is a deterministic, serializable state machine; single-node
  behaviour byte-for-byte unchanged.

### Phase 6 — `internal/cluster` (assemble Raft + FSM + storage + transport)
- `cluster/command.go`: the `Command` structs (§4.3), `Encode`/`Decode`
  (versioned envelope: `{ver, kind, payload}`), and `cluster.fsm` implementing
  `raft.FSM` — `Apply(e raft.Entry) any` decodes the command, switches to the
  matching `lm.Apply*`, packages `ApplyResult{Direct any; Grants []lock.Grant}`;
  `Snapshot`/`Restore` delegate to `lm.Snapshot`/`Restore` (gob/json the
  `ClusterSnapshot`, with a format tag).
- `cluster/node.go`: `Node` — owns `*raft.Node`, the file `Storage`, the TCP
  `Transport` (Phase 7), the `fsm`, the **wait registry** (`map[ref]chan Grant`),
  and the leader-only loop manager. API: `Propose(ctx, Command) (any, error)`
  (proposes, awaits the future, returns `ApplyResult.Direct`); `IsLeader()`;
  `LeaderClientAddr() (string, bool)`; `RegisterWaiter(ref) (<-chan Grant, cancel)`;
  the apply-side hook that routes `ApplyResult.Grants` to registered waiters;
  `Stats()` (membership, role, term, commit/apply indices, leader); `Barrier(ctx)`;
  `TransferLeadership(ctx)`; `AddVoter/RemoveServer`; `Start(ctx)`/`Close()`.
- `cluster/loops.go`: when this node becomes leader, start `leaseSweepLoop` and
  `gcLoop`; on losing leadership, stop them. `leaseSweepLoop`: every interval,
  ask the FSM (via a read-only snapshot/iterator) for `{key, token}` pairs past
  deadline, `Propose(Evict{...})` for each (best-effort: log+continue on
  not-leader). `gcLoop`: `Propose(GC{IdleKeys(now)})`.
- Tests: an in-process `cluster.Node` ×3 over `MemTransport` + `MemStorage`:
  `Propose(Acquire)` → all FSMs hold the lock; leader's `Propose` returns the
  token; kill the leader → new leader → `Propose` still works; the lease loop on
  the new leader evicts an expired holder; snapshot+restart recovers state.
- Done: a working dflockd Raft cluster, transport-agnostic, tested in-process.

### Phase 7 — framed TCP transport for Raft RPCs (`internal/raft/nettransport.go` or `cluster/transport_tcp.go`)
- A length-prefixed framed
  codec over `net.Conn`: `{len uint32}{frame bytes}`; frame = `{type, reqID,
  payload}`; request/response correlated by `reqID`. One persistent conn per
  peer pair (dial lazily, redial with backoff on failure), a single reader
  goroutine demuxing replies to waiting callers, write-mutex-serialized sends.
  Handshake: proto version + fresh-nonce HMAC proof + optional mutual TLS;
  derive directional AEAD keys and reject secret/protocol/certificate-ID
  mismatches. Listener accepts inbound peer conns,
  dispatches frames to the registered `raft.Node` handler, writes replies back.
  Reuse the TLS config helpers already in `internal/server`/`internal/httpapi`.
- Bounded frame sizes (normal vs. snapshot); idle/read/write deadlines;
  graceful close.
- Tests: two `raft.Node`s over a real TCP `Transport` on loopback elect a leader
  and replicate; kill+reconnect a peer; oversized/garbage frame → conn dropped,
  node unaffected; (optional) TLS variant via the existing test cert helper.
- Done: real-network Raft.

### Phase 8 — server integration (`internal/server`)
- `server.Cluster` interface (so `server` doesn't import `cluster`): `IsLeader()`,
  `LeaderClientAddr() (string, bool)`, `Propose(ctx, kind, args...) (any, error)`
  *(actually: a small typed surface — `ProposeAcquire`, `ProposeRelease`, …, or a
  single `Propose(ctx, cluster.Command)` with `server` importing only the command
  types from a leaf package — decide during impl to avoid an import cycle; likely
  the command types move to `internal/cluster/clustercmd` or stay in `cluster`
  and `server` imports `cluster` which is fine since `cluster` doesn't import
  `server`)*, `RegisterWaiter(ref)`, `Stats()`.
- `Server.SetCluster(c)`. When set:
  - `handleAcquire/handleEnqueue/handleWait/handleRelease/handleRenew` and the
    semaphore variants: if `!c.IsLeader()` → `Ack{Status: error_not_leader, Extra: addr}`;
    else build the `Command` (filling `ref = nodeID:connID`, `now`, a fresh
    `salt`, `leaseTTL`), `c.Propose`, and for the blocking ones also
    `c.RegisterWaiter(ref)` and select notice/timeout/conn-close; map
    `ApplyResult.Direct` (or the propose error) to the `Ack`.
  - `handleStats`: leader-only (`error_not_leader` otherwise); the data comes
    from the FSM's applied state (a `Barrier` first for strict linearizability —
    optional, behind a flag).
  - `teardownConn`: instead of `lm.CleanupConnection(connID)` → if clustered &
    leader, `c.Propose(CleanupConn{ref})`; if clustered & not leader, nothing
    (no stateful conns here) — though belt-and-suspenders: a follower that *did*
    somehow grant something can't clean it up; lease expiry covers it.
  - Background loops: in `serve()`, when clustered, do **not** start
    `lm.LeaseExpiryLoop`/`GCLoop` here — the `cluster.Node`'s leader-only loops
    own them.
- `internal/protocol`: `StatusErrorNotLeader = "error_not_leader"`; `FormatResponse`
  emits `error_not_leader <addr>\n` (addr may be empty); a tiny `respErrorNotLeader`
  fast path when addr is empty.
- Tests: unit — a fake `Cluster` (leader/follower toggle, canned propose
  results) drives every handler; integration deferred to Phase 13.
- Done: the TCP server speaks cluster.

### Phase 9 — config + `cmd/dflockd`
- New flags / env (all default off → standalone):
  `--raft-dir` (`DFLOCKD_RAFT_DIR`), `--node-id` (`DFLOCKD_NODE_ID`, random if
  unset & persisted to `<raft-dir>/node-id`), `--cluster-peers`
  (`DFLOCKD_CLUSTER_PEERS`, comma list of `id=raftHost:raftPort`; this node may
  omit itself or include itself), `--raft-addr` (this node's Raft bind, default
  derived), `--advertise-addr` (this node's *client* `host:port` as told to
  redirected clients, default `--host:--port`), `--cluster-bootstrap` (only the
  very first node of a brand-new cluster sets this — it self-elects a 1-node
  config and others join via membership change; or alternatively
  `--cluster-peers` lists the full initial set on every node and bootstrap is
  implicit if `raft-dir` is empty — pick one, document it), `--cluster-secret-file`,
  `--cluster-tls-cert/-key/-ca`, plus tunables: `--raft-heartbeat-ms`,
  `--raft-election-timeout-min/max-ms`, `--raft-snapshot-threshold`,
  `--raft-max-append-entries`. `Validate()`: cluster requires `--raft-dir`;
  `--fence-state-file` + cluster mode = error; peers parse; timeouts sane
  (`min < max`, `heartbeat << min`); ids unique.
- `cmd/dflockd/main.go`: if cluster configured → `cluster.NewNode(cfg, lm, log)`
  → `node.Start(ctx)` (fatal on error) → `srv.SetCluster(node)` → `defer
  node.Close()`; `SIGUSR1` → `node.TransferLeadership` (logged best-effort).
  Otherwise unchanged.
- Tests: config parse/validate matrix; `cmd/dflockd` smoke (build + `--version`
  unaffected; a 1-node `--cluster-bootstrap` comes up and serves).
- Done: operable from the CLI.

### Phase 10 — Go client cluster mode (`client/`)
- `Lock`/`Semaphore`: add `ClusterMode bool` and (optional) `LeaderHint string`.
  Internals: a `clusterRouter` wrapping the member list — `currentLeaderConn()`,
  `onNotLeader(addr)`, `onIOError()`, with bounded exponential backoff and a
  total deadline; every op runs through `router.do(ctx, func(conn) (resp, err))`
  which retries on `error_not_leader` / dial / IO errors against the hinted then
  round-robined members. CRC sharding is bypassed when `ClusterMode`. Two-phase
  helpers pin to the conn that served `enqueue`; on failover they surface a typed
  error so callers know the slot was lost (and `Lock.Acquire`'s convenience
  wrapper just retries from scratch).
- `FenceFromToken` etc. unchanged (tokens are still opaque 32-hex).
- Tests: a mock cluster (one "leader" socket + two "follower" sockets that reply
  `error_not_leader leaderAddr`) — client finds the leader, survives a
  leader-address change mid-stream, gives up after the deadline; race test.
- Done: clients survive failover transparently for the simple acquire/release case.

### Phase 11 — HTTP API cluster awareness + admin (`internal/httpapi`)
- `error_not_leader` from the lock-manager-equivalent path → `503` with
  `{"status":"not_leader","leader":"host:port"}` (and a `Location`-ish header? —
  no, keep it JSON; `503` + `Retry-After: 0`), or `421 Misdirected Request` if
  we want clients to redirect — pick `503` + body (simpler, matches the TCP
  semantics). Document it in OpenAPI.
- New: `GET /v1/admin/cluster` → `{node_id, role, term, leader_id, leader_addr,
  commit_index, applied_index, log_first_index, last_snapshot_index, members:[{id,addr,voter,match_index}]}`
  (auth-required). `POST /v1/admin/transfer-leadership` (optional body
  `{"target":"node-id"}`) → 200 / 409 `not_leader` / 412 `no_cluster` / 503.
  Both behind the existing bearer auth; added to `registeredRoutes`,
  `registerRoutes`, and the embedded OpenAPI (`make openapi-sync`).
- Tests: handler tests with a fake cluster; OpenAPI drift test stays green;
  route-method parity test updated.
- Done: HTTP clients & operators are cluster-aware.

### Phase 12 — dynamic membership changes
- `cluster.Node.AddVoter(id, raftAddr, advertiseAddr)` — appends a `ConfigEntry`
  (after streaming a snapshot/catch-up to the joiner so it doesn't tank
  availability); `RemoveServer(id)` — appends the removal; a leader removing
  itself steps down post-commit. Joiner side: a node started with `--raft-dir`
  empty and `--cluster-join <existing-member-addr>` reaches out, gets added,
  receives a snapshot, becomes a follower.
- Wire it to the admin HTTP API: `POST /v1/admin/members {id, raft_addr, advertise_addr}` /
  `DELETE /v1/admin/members/{id}`.
- Tests: 3→4→5 grow; 5→4→3 shrink; leader removes a follower; leader removes
  itself; a brand-new node joins an existing cluster and catches up via snapshot;
  membership entry takes effect on append (a removed node stops voting
  immediately).
- Done: clusters can be grown/shrunk online.

### Phase 13 — integration & soak
- `internal/cluster/integration_test.go` (and/or `internal/server/cluster_test.go`):
  3 real `Server`s + `cluster.Node`s over the TCP transport on loopback, real
  clients: elect; acquire/release; follower redirect; kill leader → reelect →
  client retries → continues; restart whole cluster from disk → state intact;
  partition the leader into a minority → it stops serving, majority elects a new
  leader → heal → old leader rejoins; semaphore (`limit>1`) replicates; two-phase
  enqueue/wait across nodes; lease expiry replicated; GC replicated.
- `cmd/cluster-soak`: workers + fault thread; convergence assertion; optional
  hand-rolled linearizability check over a recorded history vs. a sequential lock
  model. Document a target run (e.g. `--nodes 3 --workers 50 --rounds 1000
  --kill-leader-every 2s`) and its expected zero-failure outcome.
- Run `go test -race ./...` and `make complexity` as the bar.
- Done: the cluster behaves under load + faults; soak harness committed.

### Phase 14 — docs + changelog
- `docs/architecture/cluster.md`: the model, the diagram, the failure modes, the
  clock-skew posture, the FIFO-across-failover caveat, the persistence layout.
- `docs/operations/cluster.md`: bootstrap a 3-node cluster; add/remove a node;
  transfer leadership; back up `--raft-dir`; disaster recovery (lost a node /
  lost a majority); monitoring (`/v1/admin/cluster`, the new metrics).
- `docs/server.md`: the new flags table.
- `docs/architecture/protocol.md`: `error_not_leader <addr>`.
- README: a short "High availability" section pointing to the docs.
- `internal/httpapi/openapi.json` + `make openapi-sync`: the admin endpoints.
- New Prometheus metrics on `/metrics`: `dflockd_raft_role`, `_term`,
  `_commit_index`, `_applied_index`, `_last_snapshot_index`, `_leader_changes_total`,
  `_proposals_total{result}`, `_apply_duration_seconds`, `_replication_lag_entries{peer}`,
  `_peer_up{peer}`.
- `CHANGELOG.md`: a thorough `[Unreleased]` → next-minor entry.
- Done: a reader can stand up and operate a cluster from the docs.

### Phase 15 — production-readiness review
A self-review pass (see §7) producing a checklist with each item ticked or a
follow-up filed. Fix what's cheap; file issues for what isn't. The bar: no known
safety bug; graceful behaviour under the failure modes in §7; complexity gate
green; race-clean; docs complete.

---

## 7. Production-readiness review checklist (Phase 15)

**Raft safety**
- [ ] `currentTerm`/`votedFor`/log entries are `fsync`'d before any RPC reply
      that relies on them; verified by a crash-injection storage test.
- [ ] Election restriction (§5.4.1) enforced — stale-log candidates can't win.
- [ ] `commitIndex` only advances over an entry of the **current** term (§5.4.2);
      Figure-8 scenario covered by a test.
- [ ] A leader that can't reach a majority stops committing (and `ReadIndex`
      blocks); proven by a partition test.
- [ ] Snapshot install resets the follower's log start correctly; no entry is
      ever applied twice or skipped (apply index strictly +1).
- [ ] Membership change takes effect on **append**; a leader removing itself
      steps down after commit; no two-leader window.
- [ ] PreVote prevents term inflation from a partitioned/flapping node.

**FSM determinism**
- [ ] `Apply` uses only `(state, command)` — no `time.Now`, no `crypto/rand`, no
      map-order-dependent output. Static check (grep) + a property test
      (two fresh managers, same log → equal snapshot).
- [ ] Token = `encodeToken(FenceCounter, salt)`; `FenceCounter` is in the
      snapshot; monotonic across leader changes and full restarts.
- [ ] Snapshot↔restore is lossless; snapshot-then-tail-replay ≡ full-replay.

**Liveness & operations**
- [ ] Bounded backoff everywhere a peer can be down (transport redial, propose
      retry on the client, leader-loop propose-on-not-leader).
- [ ] Election timeouts randomized; `heartbeat << electionMin`; sane defaults.
- [ ] Leadership transfer is fast (`TimeoutNow`) and used by graceful shutdown /
      `SIGUSR1`; a shutting-down leader hands off before exiting if it can.
- [ ] Disk-full / IO-error on the log path → the node steps down / refuses to
      ack rather than lying about durability; surfaced in logs + a metric.
- [ ] `--raft-dir` is `flock`'d; second process refused; non-Unix refused
      (matches `--fence-state-file`).
- [ ] Recovery from: one node lost (rejoin, catch up); whole cluster bounced
      (recover from disk); a node's disk wiped (rejoin as fresh, get a snapshot);
      a majority lost (documented manual procedure — unsafe, operator-forced).

**Security & resource bounds**
- [x] Raft transport: mandatory shared-secret HMAC handshake; directional
      AES-GCM RPC sessions; optional mTLS binds certificate CN to NodeID;
      protocol mismatch refused.
- [ ] All Raft frames length-bounded (normal vs. snapshot caps); a peer sending
      garbage is dropped without affecting the node.
- [ ] `MaxLocks`/`MaxWaiters` enforced at `Apply` (deterministically); per-node
      conn caps unchanged.
- [ ] No client-controllable unbounded allocation on the propose path (command
      size bounded; key/arg validation as today, before propose).
- [ ] A panic in `Apply` is contained: log + the node steps down (it can't
      safely continue with a half-applied entry) rather than crashing silently.

**Correctness under concurrency**
- [ ] `go test -race ./...` green, including the in-process cluster tests and
      the soak harness.
- [ ] The Raft `Node` run loop owns all consensus state; everything else is
      channels — no shared-mutable raciness. The apply goroutine is the only
      writer of FSM state. The wait registry is mutex-guarded and never held
      across a propose.
- [ ] No goroutine leaks: `Close()` joins every spawned goroutine (transport
      readers, apply loop, leader loops, snapshot worker).

**Backward compatibility**
- [ ] With no cluster flags: byte-for-byte the v2.1.x behaviour — existing tests
      all pass unchanged; the protocol, HTTP API, and `--fence-state-file` work
      as before.
- [ ] The Go client without `ClusterMode` works against a single node exactly as
      today.

**Code quality**
- [ ] `make complexity` not regressed; new functions follow the house style
      (short, low cyclomatic complexity, table-driven dispatch).
- [ ] `go vet ./...` clean; `gofmt` clean; no `TODO`/`FIXME` left for safety-
      critical paths (only for documented out-of-scope follow-ups).
- [ ] Every exported symbol in `internal/raft` / `internal/cluster` has a doc
      comment; package docs explain the model.
- [ ] Fuzz targets for the Raft frame codec and the command codec (mirrors the
      existing protocol fuzzers).

---

## 8. Risks & mitigations

| Risk | Mitigation |
|---|---|
| Raft is famously easy to get subtly wrong. | Tight scope (§3); the deterministic `MemTransport` lets us run adversarial multi-node scenarios as fast unit tests; the soak harness with fault injection; the §7 checklist; lean on the paper's exact invariants and cite them in comments. |
| FSM non-determinism (the silent killer — divergence). | `now`-in-command + `salt`-in-command + `FenceCounter`-in-state; a determinism property test; a grep-gate against `time.Now`/`rand` in the apply package. |
| Big refactor of `lock.LockManager` regresses single-node behaviour. | The non-cluster methods become thin wrappers over the new `Apply*` core, so the **entire existing `lock_test.go` suite is the regression guard** — it must pass unchanged before the phase closes. |
| Lease semantics + clock skew. | Absolute deadlines in the FSM; leader-only sweep via `Evict` commands; documented NTP assumption; client renews at TTL/2 so early-eviction is invisible in practice. |
| FIFO across failover (v1 limitation). | Explicitly documented; holders (with tokens) survive seamlessly; a future phase adds stable client refs + `WaitClaim` re-attach. |
| Scope creep / one giant unmergeable change. | Phased; each phase is independently green and reviewable; cluster code is wholly behind opt-in flags so it can land incrementally without affecting the shipped single-node server. |
| Import cycles (`lock` ↔ `cluster`, `server` ↔ `cluster`). | `internal/raft` depends on nothing dflockd-specific; `cluster` imports `lock` + `raft`; `server` imports `cluster` (one-way — `cluster` never imports `server`); if a command-type leaf package is needed to keep `server` light, factor `cluster/clustercmd`. Settled in Phase 6/8. |

## 9. Open questions to confirm during implementation

- **Bootstrap UX**: "full peer list on every node + implicit bootstrap when
  `raft-dir` empty" vs. "one node with `--cluster-bootstrap`, others
  `--cluster-join`". Leaning toward the latter (clearer, fewer footguns,
  standard). Decide at Phase 9; document whichever.
  - **Resolved**: shipped *both*. `--cluster-peers` is the static-list path
    (every member started with the full membership) — the path
    `internal/cluster` E2E + `tools/cluster-smoke` exercise.
    `--cluster-bootstrap` is the one-node-then-grow path: a single member
    comes up alone and additional members are added via `AddVoter` later.
    Static is the recommended bootstrap for a fresh 3- or 5-node cluster;
    bootstrap+`AddVoter` is the recommended path when growing from one.
- **Command codec**: JSON (debuggable, slower) vs. a hand-rolled binary form.
  Start JSON; the format is internal so switching later is a one-package change.
  Revisit if soak shows it matters.
  - **Resolved**: JSON (`encoding/json`, base64 for byte fields like salts).
    Stable on the wire only insofar as the `Kind` discriminator is — see
    `internal/cluster/command.go`'s "never renumber existing values" comment.
    No measured need to switch.
- **Leader forwarding vs. redirect**: v1 = redirect (`error_not_leader`). If
  operators want a single VIP with no client changes, a follower-forwards-to-leader
  proxy mode could be a later phase — but it complicates two-phase op stickiness
  and connID semantics, so not now.
  - **Resolved**: redirect. TCP: `error_not_leader <addr>`. HTTP: `503
    {"error":"not_leader"}` + `X-Dflockd-Leader: <addr>`. No proxy mode
    planned.
- **`stats` linearizability**: applied-state read vs. `Barrier`-then-read. Start
  with applied-state (cheap); add a `?linearizable=1` knob if asked.
  - **Resolved**: applied-state (best-effort) for `stats`. `Barrier(ctx)` is
    on `cluster.Node` and can be invoked by a future linearizable-read
    public API. No `?linearizable=1` knob has been requested.

---

## 10. Phase checklist

*Updated by the one-shot audit on 2026-05-16. `✅` = shipped, `🟡` =
partial (sub-deliverable deferred — see note), `❌` = not shipped.*

- [x] Phase 0 — scaffolding & shared types ✅
- [x] Phase 1 — persistent storage ✅
- [x] Phase 2 — Raft log + node core (election) ✅
- [x] Phase 3 — replication RPCs + Transport (+ MemTransport faults) ✅
- [x] Phase 4 — FSM, apply pipeline, snapshots & compaction ✅
- [x] Phase 5 — LockManager as a deterministic FSM ✅ (`ApplyAcquire`/
      `Enqueue`/`Release`/`Renew`/`Evict`/`CleanupConn`/`GC` in
      `internal/lock/apply.go`)
- [x] Phase 6 — internal/cluster (assemble it) ✅ (`cluster.Node` in
      `internal/cluster/node.go`)
- [x] Phase 7 — framed TCP transport for Raft RPCs ✅
      (`internal/raft/tcptransport.go` + `tcpframe.go`)
- [x] Phase 8 — server integration (propose / redirect / leader-only loops) ✅
- [x] Phase 9 — config + cmd/dflockd wiring ✅ (`--raft-dir`, `--node-id`,
      `--raft-addr`, `--cluster-peers`, `--cluster-bootstrap`,
      `--raft-auth-token-file`, plus mTLS flags and `--lease-sweep-interval`)
- [ ] Phase 10 — Go client cluster mode 🟡 (`*NotLeaderError` shipped;
      `ClusterMode`/`clusterRouter`/leader cache/auto-retry **deferred**
      — see PRODUCTION_READINESS.md "Recommended next work" item 4)
- [ ] Phase 11 — HTTP API cluster awareness + admin endpoints 🟡
      (HTTP-in-cluster shipped with `503 {"error":"not_leader"}` +
      `X-Dflockd-Leader` header; **admin endpoints** —
      `GET /v1/admin/cluster`, `POST /v1/admin/transfer-leadership`,
      `POST /v1/admin/members`, `DELETE /v1/admin/members/{id}` —
      **deferred**, see PRODUCTION_READINESS.md item 1)
- [ ] Phase 12 — dynamic membership changes 🟡 (`AddVoter` / `RemoveServer`
      exist on `cluster.Node` and `internal/raft/membership_test.go`
      proves them; no admin surface yet to call them from outside a Go
      program; full `--cluster-join` snapshot-transfer joiner flow
      **deferred** — see PRODUCTION_READINESS.md item 5)
- [ ] Phase 13 — integration & soak tests 🟡 (`internal/cluster/e2e3_test.go`
      + `tools/cluster-smoke` shipped; **`cmd/cluster-soak`** with
      fault-injection knobs **deferred** — see PRODUCTION_READINESS.md
      item 3)
- [ ] Phase 14 — docs + changelog + metrics 🟡 (CHANGELOG, docs site
      ([cluster.md], [server.md], [protocol.md]), README HA section,
      OpenAPI sync all shipped; **counter-style metrics**
      (`_leader_changes_total`, `_proposals_total{result}`,
      `_apply_duration_seconds`, `_replication_lag_entries{peer}`,
      `_peer_up{peer}`) **deferred** — gauges shipped; see
      PRODUCTION_READINESS.md item 2)
- [x] Phase 15 — production-readiness review ✅
      ([PRODUCTION_READINESS.md](PRODUCTION_READINESS.md) +
      post-review hardening pass + mTLS / graceful transfer / cluster
      observability / HTTP-in-cluster follow-ons)
