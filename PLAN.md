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
single node; cluster awareness is opt-in (`client.Cluster` or explicit
`error_not_leader` handling).

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
   │  storage: hardstate, wal, snapshots/   │   │  storage…    │  │  storage…    │
   └────────────────────────────────────────┘   └──────────────┘  └──────────────┘
```

### Package layout

| Package | Role |
|---|---|
| `internal/raft` | Generic Raft. No dflockd specifics. Node (role FSM, election, replication), in-memory log, `Storage` interface + a file-backed implementation, `Transport` interface + an in-memory impl (tests) and a framed-TCP impl, `FSM` interface, snapshot/`InstallSnapshot`, config. |
| `internal/cluster` | Glue. `cluster.Node` owns a `raft.Node` + the file storage + the TCP transport + the FSM adapter. Defines the dflockd `Command` types (acquire/release/renew/enqueue/wait-claim/evict/gc/cleanup-conn/membership/no-op) and their (de)serialization. Implements `raft.FSM` over `*lock.LockManager`. Exposes `Propose(ctx, Command) (result, error)`, `IsLeader()`, `LeaderAddr()`, `Barrier()`, `TransferLeadership()`, `AddVoter`/`RemoveServer`, `Stats()`. |
| `internal/lock` | Adds streaming `Snapshot(io.Writer)` / `Restore(io.Reader)` FSM serialization, deterministic `Apply*` methods that return results + grant notifications, a snapshotted FSM fence counter, stable refs, salts, and indexed ref ownership. Existing direct methods remain the standalone path. |
| `internal/server` | Gains: a `Cluster` interface (so `server` doesn't import `cluster` directly — mirrors the existing `LockManager` access pattern); when a `Cluster` is wired, mutating commands are turned into `cluster.Command`s, proposed, and the apply result is awaited; non-leader → `error_not_leader <addr>`; the lease-sweep / GC loops run **only on the leader** and emit `Evict`/`GC` commands rather than mutating directly; connection-close cleanup proposes a `CleanupConn` command. |
| `internal/config` | New flags / env vars (§9). |
| `internal/protocol` | Adds `error_not_leader` plus the TCP `barrier` and `stable-ref` commands; existing commands retain their standalone behavior. |
| `internal/httpapi` | Maps follower mutations to `503 {"error":"not_leader"}` + `X-Dflockd-Leader`; exposes `GET /v1/readindex`, `POST /v1/admin/voters`, `DELETE /v1/admin/voters/{id}`, cluster status, and metrics. |
| `cmd/dflockd` | Wires `cluster.Node` when configured; graceful SIGINT/SIGTERM shutdown calls `cluster.Node.Close`, which attempts leadership transfer; fatal on storage-init failure. |
| `cmd/cluster-soak` | In-process N-node harness with periodic leader kills plus an external real-cluster mode with pluggable partition, restart, and process-clock-skew hooks. |
| `client` | Adds the standalone, failover-aware `client.Cluster` wrapper. It caches a leader address, follows member-clamped redirects, rotates after dial failures, bounds attempts, and optionally sets auth + stable refs on each fresh connection. Existing `Lock`/`Semaphore` APIs remain unchanged. |

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
- **Snapshotting & log compaction**: when the applied log exceeds the configured
  entry threshold, snapshot the FSM, persist `{lastIncludedIndex, lastIncludedTerm,
  membership, fsmBytes}`, then truncate the log prefix. `InstallSnapshot` RPC
  for followers that have fallen behind the leader's log start.
- **Membership changes**: single-server add/remove (Raft §4.3 — simpler and
  sufficient; joint consensus is out of scope). A `ConfigEntry` in the log; the
  new configuration takes effect *as soon as it is appended* (not when committed).
  A leader removing *itself* steps down after the entry commits.
- **Read path**: `stats` and `ping` are served from any node; a follower's
  `stats` is its node-local applied view and may lag. Callers that need a
  linearization point use the leader-only TCP `barrier` command or
  `GET /v1/readindex`, both implemented as a committed no-op.
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
Each `Apply*` call returns an `ApplyResult` for the proposing caller plus any
grant notifications `{ref, key, token, leaseSec}` produced while promoting
waiters. The cluster FSM routes notifications through the lock manager's
runtime-only listener registry, then returns only `ApplyResult` through the
Raft proposal future. A notification with no local listener is dropped; the
holder remains authoritative in FSM state until re-attachment or expiry.

### 4.3 Command set

Commands use a bounded, validated internal JSON codec with a stable numeric
`Kind`. The flat command includes `NowNanos` plus only the fields its kind needs.

| Command | Emitted by | FSM effect |
|---|---|---|
| `Barrier` | TCP `barrier` / HTTP readindex | committed no-op; returns after apply |
| `Acquire{key, limit, ref, connID, leaseTTLNano, salt}` | leader handling `l`/`sl` | free slot → add holder (fence++/token); else append waiter; matching stable ref may re-adopt an abandoned or prior-process slot |
| `Enqueue{key, limit, ref, connID, leaseTTLNano, salt}` | leader handling `e`/`se` phase 1 | same state transition as `Acquire`, plus two-phase enqueue bookkeeping |
| `Release{key, token}` | leader handling `r`/`sr` | remove holder; `grantNext` → promote head waiter(s) (each: fence++/token, emit notification) |
| `Renew{key, token, leaseTTLNano}` | leader handling `n`/`sn` | extend lease (or reject if already expired-per-`NowUnixNano`, evicting + promoting) |
| `Evict{key, token}` | explicit cluster eviction path | remove holder and promote waiters; idempotent if absent |
| `EvictExpired` | leader maintenance loop when expiry is due | scan and evict every expired holder deterministically |
| `GC` | periodic leader maintenance when GC is due | drop idle resources deterministically |
| `CleanupConn{ref, connID}` | leader on client disconnect | release or orphan this connection's slots, then promote as needed |

Raft `NoOp` and `ConfigEntry` entries are handled by `raft.Node`, outside this
application command codec. The `w` / `sw` phase does not append another log
entry; it waits on the listener registered by `Enqueue`.

The default client ref is the decimal form of a connection ID with a random
per-process epoch in its high bits. TCP callers may instead send
`stable-ref <opaque-id>` before their first operation; HTTP callers may supply
`stable_ref` when creating a session. With `--orphan-ttl > 0`, reconnecting or
creating a replacement node-local session with that ref can reclaim the same
waiter/holder after failover.

### 4.4 Who can mutate; client routing

- Only the **leader** proposes commands. A follower (or candidate) that receives
  a mutating client command replies `error_not_leader <leaderHost:leaderPort>`
  (empty addr if the leader is currently unknown — client backs off and retries
  another member). The advertised address is the member's *client* address, not
  its Raft address — config carries both (`--advertise-addr`, defaults to
  `--host:--port`).
- `ping` is answered by anyone. `stats` reports the local applied-state view on
  any member; callers needing a linearization point use `barrier` or
  `GET /v1/readindex` on the leader first.
- The **Go `client.Cluster` wrapper** keeps an atomic leader-address hint. Each
  operation dials a fresh connection, follows a known-member
  `error_not_leader <addr>` immediately, rotates through configured members
  after dial/unknown-hint failures, and stops at a fixed attempt budget. The
  exhaustion error matches `ErrTooManyRedirects` and wraps the terminal cause.
  `WithClusterStableRef` re-sends the caller's ref on every connection so
  acquire/enqueue/wait can re-attach after failover.

### 4.5 Persistence layout

`--raft-dir=<dir>` (required for cluster mode). Contents:

```
<dir>/
  .lock                   # flock(2)-held for the process lifetime
  hardstate               # two-slot CRC journal: {currentTerm, votedFor, commitIndex}
  wal                     # length-prefixed CRC'd entries; append + fsync
  snapshots/
    snap-00000000000000000256-00000000000000000003
                          # {index, term, config, fsm bytes}; CRC'd, atomic rename
```

- Reuse the patterns already in `internal/lock/fence.go`: `flock(2)` the dir
  (refuse to start if another dflockd holds it; refuse on non-Unix), CRC records,
  and `fsync` the file *and* directory entry after an atomic rename.
- On startup: acquire the dir lock → load the newest valid snapshot → validate
  the single WAL and truncate a torn tail → load `hardstate` → `FSM.Restore`
  the snapshot → replay committed WAL entries through `commitIndex` → ready.
- Compaction rewrites the WAL atomically with only entries after the snapshot.
  Local snapshot serialization and the initial compacted-WAL write happen off
  the Raft run loop; the run loop validates and commits the prepared files.

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
  old leader merely stepped down, get `error_not_leader`). `client.Cluster`
  reconnects to the new leader and re-issues the op:
  - It was a *holder* already (had a token): `renew`/`release` work as-is (no
    connID check — §2 note) → seamless.
  - With the default connection-derived ref, a waiter re-enqueues at the back
    and a promoted-but-unobserved holder expires by lease.
  - With `--orphan-ttl > 0` and a stable ref, `ApplyAcquire` /
    `ApplyEnqueue` re-adopt the existing `(key, ref)` slot when the previous
    owner is provably gone (abandoned stamp or a different server-process
    connID epoch). FIFO position, salt, and any minted token are preserved.
    TCP uses `client.WithClusterStableRef`; HTTP creates a replacement
    node-local session with the same optional `stable_ref`.
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
- **`internal/server`** integration: a 3-node cluster with real loopback TCP;
  assert follower redirect, leader acquire/release, hard leader failure,
  `client.Cluster` retry, re-election, and stable-ref re-attachment.
- **Race**: `go test -race ./...` is the gate (the project already runs it).
- **`cmd/cluster-soak`**: N workers doing acquire/release loops against either
  an in-process cluster or real client endpoints, with token/fence invariants.
  In-process mode kills leaders; external mode drives pluggable partition,
  restart, and process-clock-skew hooks. External workers contend on a bounded
  key pool; an exact per-key checker validates recorded acquire/release
  histories, including ambiguous release replies and skew-adjusted lease
  expiry.
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
  `HardState`, `Configuration{Voters map[NodeID]string}`, and `SnapshotMeta`.
- `internal/raft/config.go`: `Config{HeartbeatInterval, ElectionTimeoutMin/Max,
  MaxAppendEntries, SnapshotThresholdEntries, MaxSnapshotBytes,
  ApplyChanDepth, PreVote}` with `DefaultConfig()` + `Validate()`.
- Done: package builds; `Config.Validate` tested.

### Phase 1 — persistent storage (`internal/raft/storage*.go`)
- `Storage` persists `HardState`, indexed log access/appends/suffix truncation,
  and one current snapshot via `SnapshotMeta` / `OpenSnapshot` /
  `SaveSnapshot`; `Close` releases storage resources.
- `FileStorage` uses one append-only CRC-record WAL, a two-slot CRC journal for
  `HardState`, atomic snapshot/WAL generation replacement, and a process-lifetime
  directory `flock`. Torn WAL tails truncate on open; bounded corrupt inputs fail.
- Also: `MemStorage` (in-memory `Storage` for fast raft tests).
- Tests cover round-trip/reopen, torn tails, corruption, suffix truncation,
  snapshot save/load/compaction, async generation publication, and double-open
  refusal.
- Done: storage solid in isolation.

### Phase 2 — Raft log (in-memory) + node core
- `internal/raft/raftlog.go`: `raftLog` delegates indexed access to `Storage`
  and implements term matching, candidate freshness, conflict hints,
  follower suffix repair, commit clamping, and snapshot installation.
- `internal/raft/node.go`: `Node` — role, term, vote, leader ID,
  election/heartbeat timers, and a single-owner run loop fed through proposal,
  RPC, apply, configuration, control, and stop channels. PreVote + Vote. Becoming
  leader: append a `NoOp` of the new term immediately; init `nextIndex/matchIndex`.
- The run loop remains a thin dispatcher over small election, replication,
  RPC, proposal, and snapshot handlers.
- Tests (in-memory transport + `MemStorage`): single node elects itself; 3-node
  elects exactly one leader; higher term steps a leader down; split vote re-elects;
  PreVote doesn't bump terms on a partitioned flapper; election restriction
  (stale-log candidate can't win).
- Done: elections correct under the in-memory transport.

### Phase 3 — replication RPCs + Transport
- `internal/raft/transport.go`: `Transport` interface — context-bounded
  `Send(to, request)`, inbound handler registration, peer updates, local ID,
  and close. RPCs cover RequestVote/PreVote, AppendEntries with conflict hints,
  InstallSnapshot, and TimeoutNow.
- `MemNetwork` / `MemTransport`: in-process bus with link partition/reconnect,
  node isolation/crash, full heal, and fixed-delay controls.
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
- Snapshotting: the apply goroutine captures `FSM.Snapshot()` after the entry
  threshold. `FileStorage` prepares the snapshot and compacted WAL off the run
  loop; the run loop validates and atomically publishes the generation.
  `InstallSnapshot` persists and restores one bounded snapshot frame.
- Public node operations are `Propose`, `Barrier`, `AddVoter`,
  `RemoveServer`, `TransferLeadership`, `Status`, and lifecycle methods.
- Tests: propose → commit → apply → future resolves with the FSM result;
  proposer on a node that loses leadership gets `ErrLeadershipLost`; snapshot
  taken, log truncated, a restarted node restores from it; a far-behind follower
  gets `InstallSnapshot` and converges; `Barrier` reflects all
  acked-before-it writes; leadership transfer hands off cleanly and fast.
- Done: a generic, persistent, snapshotting Raft with a pluggable FSM — fully
  tested without touching dflockd.

### Phase 5 — `lock.LockManager` as a deterministic FSM
- `internal/lock` adds `Snapshot(io.Writer)` / `Restore(io.Reader)`, a
  snapshotted `fsmFenceCounter`, ref/salt/orphan state, and ref ownership
  indexes rebuilt on restore.
- The deterministic committed-mutation surface is `ApplyAcquire`,
  `ApplyEnqueue`, `ApplyRelease`, `ApplyRenew`, `ApplyEvict`,
  `ApplyCleanupConn`, `ApplyEvictExpired`, and `ApplyGC`. Each takes
  leader-supplied time (and salt where token minting is possible) and returns
  an `ApplyResult` plus any `[]Grant`.
- `WatchGrantsFor` / `RouteGrants` keep runtime notification channels outside
  serialized FSM state. The existing direct methods remain an unchanged
  standalone code path over the shared resource structures.
- `internal/lock/fence.go`: in cluster mode the `fenceAllocator` is unused
  (`NewLockManagerForCluster` skips it); `--fence-state-file` + cluster mode is a
  config error (Raft persistence supersedes it).
- Tests: each `Apply*` against hand-built states; the determinism property
  (replay a command list on two fresh managers → equal snapshot bytes); snapshot
  round-trip; the direct non-cluster path still passes the existing
  `lock_test.go` suite.
- Done: `LockManager` is a deterministic, serializable state machine; single-node
  behaviour byte-for-byte unchanged.

### Phase 6 — `internal/cluster` (assemble Raft + FSM + storage + transport)
- `cluster/command.go` supplies a bounded, validated JSON command codec;
  `cluster.fsm` decodes committed commands, dispatches to `Apply*`, routes
  grants, and delegates snapshot/restore to the lock manager.
- `cluster.Node` owns the Raft node, FSM, storage, transport, member-address
  map, maintenance loop, admin counters, typed proposal methods, status,
  barrier, membership, and lifecycle surfaces.
- One sweep loop runs on every node but acts only on the leader. It checks
  `EvictionDue` / `GCDue` before proposing replicated `EvictExpired` / `GC`
  commands, avoiding idle log churn.
- Tests: an in-process `cluster.Node` ×3 over `MemTransport` + `MemStorage`:
  `Propose(Acquire)` → all FSMs hold the lock; leader's `Propose` returns the
  token; kill the leader → new leader → `Propose` still works; the lease loop on
  the new leader evicts an expired holder; snapshot+restart recovers state.
- Done: a working dflockd Raft cluster, transport-agnostic, tested in-process.

### Phase 7 — framed TCP transport for Raft RPCs (`internal/raft/tcptransport.go`)
- A length-prefixed binary codec over `net.Conn` carries message tag, request
  ID, and payload; request/response pairs correlate by request ID. One outbound
  connection per peer is dialed lazily and reused, with a reader goroutine
  demultiplexing replies and serialized frame writes.
  Handshake: proto version + fresh-nonce HMAC proof + optional mutual TLS;
  derive directional AEAD keys and reject secret/protocol/certificate-ID
  mismatches. Listener accepts inbound peer conns,
  dispatches frames to the registered `raft.Node` handler, writes replies back.
  The Raft package owns its TLS identity helper.
- Bounded normal/snapshot frames, context-derived read/write deadlines,
  TCP keepalive, replay-checked secure sequence numbers, and graceful close.
- Tests: two `raft.Node`s over a real TCP `Transport` on loopback elect a leader
  and replicate; kill+reconnect a peer; oversized/garbage frame → conn dropped,
  node unaffected; auth, encryption, replay, protocol, and mTLS identity paths
  are covered.
- Done: real-network Raft.

### Phase 8 — server integration (`internal/server`)
- `server.Cluster` keeps `server` decoupled from the concrete cluster package.
  It exposes leadership/address/status, typed acquire/enqueue/release/renew/
  cleanup proposals, barrier, membership changes, and metrics.
- `Server.SetCluster(c)`. When set:
  - `handleAcquire/handleEnqueue/handleWait/handleRelease/handleRenew` and the
    semaphore variants: if `!c.IsLeader()` → `Ack{Status: error_not_leader, Extra: addr}`;
    else construct a connection-epoch ref (or caller stable ref), register
    `WatchGrantsFor`, call the typed cluster proposal, and map its
    `ApplyResult` or error to the response.
  - `handleStats`: served from the local applied FSM on every node and includes
    local Raft status. Callers use the separate barrier/readindex API when they
    need a leader-confirmed linearization point.
  - `teardownConn`: instead of `lm.CleanupConnection(connID)` → if clustered &
    leader, call `ProposeCleanupConn(ref, connID)`; if clustered & not leader, nothing
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
  `--raft-dir`, `--node-id`, `--cluster-peers`, `--raft-addr`,
  `--advertise-addr`, `--cluster-bootstrap`, mandatory
  `--raft-auth-token[-file]`, optional `--raft-tls-cert/-key/-ca`, and
  `--orphan-ttl`. Static bootstrap lists the full initial membership on every
  node; single-node bootstrap grows through the admin voter endpoint.
- `cmd/dflockd/main.go`: `startCluster` opens storage, builds mTLS + authenticated
  TCP transport, registers peers, constructs and starts `cluster.Node`, and
  wires it with `srv.SetCluster`. Reverse-order shutdown unwires the server,
  attempts leadership transfer through `Node.Close`, then closes transport and
  storage. Otherwise startup is unchanged.
- Tests: config parse/validate matrix; `cmd/dflockd` smoke (build + `--version`
  unaffected; a 1-node `--cluster-bootstrap` comes up and serves).
- Done: operable from the CLI.

### Phase 10 — Go client cluster mode (`client/`)
- Shipped as a separate `client.Cluster` wrapper, leaving the existing
  `Lock`/`Semaphore` and CRC-sharding APIs unchanged.
- The wrapper caches a known-member leader hint, dials a fresh connection per
  operation, follows redirects and dial fallbacks within a fixed attempt budget,
  preserves the final retry cause, and supports per-connection auth + stable ref.
- Tests cover direct redirects, cached routing, unknown-hint clamping, exact
  budget exhaustion, terminal diagnostics, and concurrent use under `-race`.
- Done: cluster operations retry transparently; TCP stable refs preserve
  acquire/enqueue/wait state across failover.

### Phase 11 — HTTP API cluster awareness + admin (`internal/httpapi`)
- Follower mutations return `503 {"error":"not_leader"}` with
  `X-Dflockd-Leader`; cluster status is included in `/v1/stats`.
- `GET /v1/readindex` supplies the linearizable-read barrier.
- `POST /v1/admin/voters` and `DELETE /v1/admin/voters/{id}` expose
  add/remove membership behind the separate default-deny admin token.
- Handler, auth, route-parity, and OpenAPI synchronization tests shipped.
- Done: HTTP session IDs remain node-local, while optional `stable_ref`
  preserves the replicated holder/waiter identity across replacement sessions.

### Phase 12 — dynamic membership changes
- `cluster.Node.AddVoter(id, raftAddr, advertiseAddr)` and
  `RemoveServer(id)` append one-at-a-time configuration entries; the published
  client-member map changes only after commit.
- The admin voter endpoints expose both operations. Operators start a new node
  with empty storage and the full `--cluster-peers` view; a compacted leader
  catches it up through `InstallSnapshot`. There is no `--cluster-join` client.
- Tests cover add/remove, self-removal, failed-change rollback, and cold-node
  snapshot catch-up.
- Done: clusters can be grown/shrunk online.

### Phase 13 — integration & soak
- Real-TCP integration covers election, replication, redirect, failover,
  persistence, snapshot catch-up, and stable-ref hard-crash re-attachment.
- `cmd/cluster-soak` drives concurrent writes with periodic in-process leader
  kills and asserts token uniqueness + per-key fence monotonicity.
- External mode drives real cluster endpoints while a strict executable hook
  injects Raft-only partitions, service restarts, and process-local clock skew;
  `tools/cluster-soak/ssh-linux.sh` supplies a Linux/systemd implementation.
- The first bounded invocation prefix per contended key is checked exactly for
  linearizability at shutdown; full-run token uniqueness and fencing
  monotonicity remain online invariants.
- Run `go test -race ./...` and `make complexity` as the bar.
- Done: CI-sized and long-horizon multi-host harnesses are shipped. A recorded
  multi-hour campaign remains deployment evidence, not an implementation gap.

### Phase 14 — docs + changelog
- `docs/architecture/cluster.md`: the model, the diagram, the failure modes, the
  clock-skew posture, the FIFO-across-failover caveat, the persistence layout.
- `docs/operations/cluster.md`: bootstrap a 3-node cluster; add/remove a node;
  security, recovery, TCP/HTTP stable refs, failover behavior, and monitoring.
- `docs/server.md`: the new flags table.
- `docs/architecture/protocol.md`: `error_not_leader <addr>`.
- README: a short "High availability" section pointing to the docs.
- `internal/httpapi/openapi.json` + `make openapi-sync`: the admin endpoints.
- Prometheus metrics on `/metrics`: role/term/index/voter gauges plus
  leader-change, proposal, apply, apply-nanos, and admin-change counters.
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
- [x] `currentTerm`/`votedFor`/log entries are `fsync`'d before any RPC reply
      that relies on them; verified by a crash-injection storage test.
- [x] Election restriction (§5.4.1) enforced — stale-log candidates can't win.
- [x] `commitIndex` only advances over an entry of the **current** term (§5.4.2);
      Figure-8 scenario covered by a test.
- [x] A leader that can't reach a majority stops committing (and `Barrier`
      blocks); proven by a partition test.
- [x] Snapshot install resets the follower's log start correctly; no entry is
      ever applied twice or skipped (apply index strictly +1).
- [x] Membership change takes effect on **append**; a leader removing itself
      steps down after commit; no two-leader window.
- [x] PreVote prevents term inflation from a partitioned/flapping node.

**FSM determinism**
- [x] `Apply` uses only `(state, command)` — no `time.Now`, no `crypto/rand`, no
      map-order-dependent output. Static check (grep) + a property test
      (two fresh managers, same log → equal snapshot).
- [x] Token = `encodeToken(FenceCounter, salt)`; `FenceCounter` is in the
      snapshot; monotonic across leader changes and full restarts.
- [x] Snapshot↔restore is lossless; snapshot-then-tail-replay ≡ full-replay.

**Liveness & operations**
- [x] Bounded backoff everywhere a peer can be down (transport redial, propose
      retry on the client, leader-loop propose-on-not-leader).
- [x] Election timeouts randomized; `heartbeat << electionMin`; sane defaults.
- [x] Leadership transfer is fast (`TimeoutNow`) and used by graceful shutdown;
      a shutting-down leader hands off before exiting if it can.
- [x] Disk-full / IO-error on every durable mutation path stops the node before
      it can acknowledge state that may disappear after restart; surfaced in logs.
- [x] `--raft-dir` is `flock`'d; second process refused; non-Unix refused
      (matches `--fence-state-file`).
- [x] Recovery from: one node lost (rejoin, catch up); whole cluster bounced
      (recover from disk); a node's disk wiped (rejoin as fresh, get a snapshot);
      majority loss is explicitly non-recoverable until an original quorum is
      restored (no force-reconfigure tooling ships).

**Security & resource bounds**
- [x] Raft transport: mandatory shared-secret HMAC handshake; directional
      AES-GCM RPC sessions; optional mTLS binds certificate CN to NodeID;
      protocol mismatch refused.
- [x] All Raft frames length-bounded (normal vs. snapshot caps); a peer sending
      garbage is dropped without affecting the node.
- [x] `MaxLocks`/`MaxWaiters` enforced at `Apply` (deterministically); per-node
      conn caps unchanged.
- [x] No client-controllable unbounded allocation on the propose path (command
      size bounded; key/arg validation as today, before propose).
- [x] An unexpected panic in `Apply` is logged with a stack, returned to the
      proposer, and fail-stops the node before any later committed entry touches
      the FSM. Queued proposal futures resolve `ErrStopped`; an indeterminate
      `Restore` failure follows the same fail-stop path.

**Correctness under concurrency**
- [x] `go test -race ./...` green, including the in-process cluster tests and
      the soak harness.
- [x] The Raft `Node` run loop owns all consensus state; everything else is
      channels — no shared-mutable raciness. The apply goroutine is the only
      writer of FSM state. The wait registry is mutex-guarded and never held
      across a propose.
- [x] No goroutine leaks: `Close()` joins every spawned goroutine (transport
      readers, apply loop, leader loops, snapshot worker).

**Backward compatibility**
- [x] With no cluster flags: byte-for-byte the v2.1.x behaviour — existing tests
      all pass unchanged; the protocol, HTTP API, and `--fence-state-file` work
      as before.
- [x] Existing Go client APIs work against a single node exactly as today;
      `client.Cluster` is additive.

**Code quality**
- [x] `make complexity` not regressed; new functions follow the house style
      (short, low cyclomatic complexity, table-driven dispatch).
- [x] `go vet ./...` clean; `gofmt` clean; no `TODO`/`FIXME` left for safety-
      critical paths (only for documented out-of-scope follow-ups).
- [x] Every exported symbol in `internal/raft` / `internal/cluster` has a doc
      comment; package docs explain the model.
- [x] Fuzz targets for the Raft frame codec and the command codec (mirrors the
      existing protocol fuzzers).

---

## 8. Risks & mitigations

| Risk | Mitigation |
|---|---|
| Raft is famously easy to get subtly wrong. | Tight scope (§3); the deterministic `MemTransport` lets us run adversarial multi-node scenarios as fast unit tests; the soak harness with fault injection; the §7 checklist; lean on the paper's exact invariants and cite them in comments. |
| FSM non-determinism (the silent killer — divergence). | `now`-in-command + `salt`-in-command + `FenceCounter`-in-state; a determinism property test; a grep-gate against `time.Now`/`rand` in the apply package. |
| Cluster FSM work regresses single-node behaviour. | The direct methods remain the standalone path over shared resource structures; the existing `lock_test.go` suite is the regression guard. |
| Lease semantics + clock skew. | Absolute deadlines in the FSM; leader-only sweep via `EvictExpired`; documented NTP assumption; renew-at-TTL/2 leaves margin for small skew. |
| FIFO across failover. | TCP and HTTP stable refs + `--orphan-ttl` preserve waiter/holder identity across reconnect; hard-failover regression tests assert the same token is returned. |
| Scope creep / one giant unmergeable change. | Phased; each phase is independently green and reviewable; cluster code is wholly behind opt-in flags so it can land incrementally without affecting the shipped single-node server. |
| Import cycles (`lock` ↔ `cluster`, `server` ↔ `cluster`). | `internal/raft` is application-agnostic; `cluster` imports `lock` + `raft`; `server` depends on its own `Cluster` interface; `cmd/dflockd` wires the concrete node. |

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
    exposed through the TCP `barrier` command, `client.Barrier`, and
    `GET /v1/readindex`. No `?linearizable=1` knob has been requested.

---

## 10. Phase checklist

*Reconciled with the shipped tree on 2026-07-24. `✅` = shipped, `🟡` =
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
- [x] Phase 10 — failover-aware Go client ✅ (`client.Cluster`, bounded
      retries, member-clamped leader cache, terminal diagnostics, stable refs)
- [x] Phase 11 — HTTP cluster awareness + admin endpoints ✅
      (`503 not_leader` + leader header, `/v1/readindex`, voter add/remove;
      optional session `stable_ref` preserves replicated identity across
      replacement node-local sessions)
- [x] Phase 12 — dynamic membership changes ✅ (`AddVoter` /
      `RemoveServer`, admin surface, post-commit member publication,
      cold-node snapshot catch-up)
- [x] Phase 13 — integration & soak tests ✅ (`internal/cluster/e2e3_test.go`
      + real-TCP failover tests + `tools/cluster-smoke` +
      `cmd/cluster-soak` in-process and external fault modes +
      exact recorded-history checking +
      `tools/cluster-soak/ssh-linux.sh`)
- [x] Phase 14 — docs + changelog + metrics ✅ (cluster/operator/server/
      protocol docs, OpenAPI, gauges, and counter metrics shipped)
- [x] Phase 15 — production-readiness review ✅
      ([PRODUCTION_READINESS.md](PRODUCTION_READINESS.md) +
      post-review P1-P3 hardening, authenticated Raft transport, and external
      multi-host fault-soak tooling)
