# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **PR-5: failover re-attach actually closes — hard crash + the primary Acquire path + the `--orphan-ttl` flag.** PR-4 shipped the stable-ref mechanism but left three gaps that meant the headline promise didn't hold for the case that matters. PR-5 closes them. (1) **Hard crash.** The re-adopt finders required `abandonedAtNanos != 0` — a stamp set *only* by the graceful `ApplyCleanupConn` path. On a hard leader crash (`kill -9`, partition) no `CleanupConn` is replicated, so the slot the new leader inherits still has `abandonedAtNanos == 0` and the old finders skipped it — i.e. the actual failover scenario didn't re-attach. Re-adopt now matches by `(key, ref)` alone (`findHolderByRef` / `findWaiterByRef`), so a reconnect reclaims its slot whether the previous connection closed gracefully or vanished. (2) **The primary API.** Re-adopt was wired only into the two-phase `ApplyEnqueue`; the single-phase `ApplyAcquire` (what `client.Acquire` / `cl.Acquire` use — the common blocking-lock path) had no re-adopt at all, so a reconnecting holder queued behind its own orphan and timed out. Re-adopt now runs in both paths via a shared `reAttachByRef` helper. (3) **Dead-conn cleanup.** On re-adopt the previous connection's stale `connOwned` / `connEnqueued` index entries are now evicted (`evictDeadConn`), so a hard-crashed conn id can't later corrupt the re-adopted slot or leak. (4) **The flag.** `--orphan-ttl` (and `DFLOCKD_ORPHAN_TTL_S`) is now actually wired — PR-4 added the `Config.OrphanTTL` field but no flag/env parser, so the feature could never be enabled in a real deployment. The loader rejects `--orphan-ttl > 0` outside cluster mode (it's a no-op there). New regression guard `internal/cluster.TestE2EStableRefReAttachAcrossFailover` proves end-to-end over real TCP that a held lock survives a hard leader crash and returns the same token on reconnect (it times out without the `ApplyAcquire` fix). Default `OrphanTTL = 0` still preserves byte-identical legacy behavior.
- **PR-4: FIFO across leader failover via stable client refs.** Introduces the stable-ref mechanism (completed in PR-5 — see above). New mechanism: when `--orphan-ttl > 0` is set on every node, the FSM's `ApplyCleanupConn` marks ref-tagged waiters and holders "orphaned" (stamps `AbandonedAtNanos`) instead of removing them on TCP disconnect. A reconnect with matching `(key, ref)` re-adopts the existing FSM slot via `ApplyEnqueue`, preserving the original salt + queue position + minted token. The `EvictExpired` sweep retires orphans past `OrphanTTL`. Wire protocol: new `stable-ref <ref>` TCP command (`protocol.CmdStableRef`) locks the caller-supplied opaque identifier onto the connection; subsequent acquire/enqueue/wait use it as the FSM ref instead of the connID-derived default. Go client: `client.SetStableRef(conn, ref)` and `client.WithClusterStableRef(ref)` opt callers into the failover-safe path. Snapshot codec bumped to v2 (the new `abandonedAtNanos` field on holder + waiter); v1 snapshots are still readable (the field defaults to 0). Default `OrphanTTL = 0` preserves byte-identical pre-PR-4 behavior — operators opt in by setting it. Security: refs are caller-supplied opaque identifiers, NOT authenticated credentials; treat them like session tokens (generate randomly, don't log, don't share). The `--auth-token` mechanism, when set, auth-gates the connection before the ref is accepted.
- **PR-3: dynamic-join with InstallSnapshot validated.** A new integration test (`internal/cluster.TestDynamicJoinColdNodeCatchesUpViaSnapshot`) brings up a 3-node cluster with a low snapshot threshold, drives enough commits to force a snapshot, calls `AddVoter("n4")`, then starts n4 with empty `MemStorage`. n4 catches up via the leader's `sendAppendEntries → sendInstallSnapshot` fallback; the test asserts `LastSnapshotIndex > 0` on n4 to prove the InstallSnapshot path was traversed (not just AppendEntries). Closes PR-2 deferred item 3 (Gap 2). No production code change required — the mechanism was already wired; PR-3 supplies the test.
- **Cluster mode: beta envelope widens (PR-2).** Three more of the v1 deferred items closed: a failover-aware Go client (`client.Cluster`) with retry budget + member-clamped leader hint; an in-process soak harness (`cmd/cluster-soak`); fuzz targets for the Raft frame codec (`FuzzRaftFrameDecode`) and the cluster `Command` codec (`FuzzClusterCommandDecode`).
- **`client.Cluster` — failover-aware Go client.** Wraps the one-shot `*Conn` API with a process-local leader cache and transparent `*NotLeaderError` redirect following. Operation methods mirror the one-shot API: `Acquire/Release/Renew/Enqueue/Wait/SemAcquire/SemRelease/SemRenew/SemEnqueue/SemWait/Barrier`, each taking `ctx context.Context`. Constructed via `NewCluster(members []string, opts...)`. Options: `WithClusterRedirectBudget(n)` (default 3), `WithClusterAuthToken(t)` (session auth on every dialed conn). The redirect budget bounds attacker-controlled redirect loops and long partitions; an exhausted budget surfaces `ErrTooManyRedirects` while also wrapping the final dial or redirect cause. Known leader redirects are followed immediately, and fallback rotation starts after the failed target without retrying a cached leader consecutively. **Security:** the leader cache only accepts hints naming an address already in the operator-supplied members list — a hostile server returning `error_not_leader evil.example.com:6388` does not cause the client to dial that host. The constructor rejects an empty members list with `ErrNoMembers`.
- **`cmd/cluster-soak` — CI-friendly multi-node soak harness.** Spins up an in-process N-node Raft cluster on a `MemTransport`, drives sustained `Acquire`/`Release` from K writer goroutines, and (when `--kill-interval > 0`) periodically calls `Close()` on the current leader to force re-election. Two invariants asserted: no duplicate token across the run, and per-key fence values are monotonic (a regression would mean FSM divergence or a duplicate grant on a still-held key). Exits 0 on a clean run, non-zero on the first violation. Flags: `--nodes` (3), `--workers` (4), `--duration` (30s), `--kill-interval` (5s), `--seed` (1).
- **Fuzz targets for the cluster wire formats.** `internal/raft.FuzzRaftFrameDecode` decodes arbitrary bytes through `decodeRPC` and asserts (a) no panic and (b) re-encode + re-decode round-trips `(kind, reqID)` byte-identically. `internal/cluster.FuzzClusterCommandDecode` does the same for `cluster.Command.Encode`/`Decode`. Both ship a seed corpus covering every RPC type / `Kind` enum value. Sustained 10-second sessions execute ~95 k inputs/sec with no crashes.
- **Cluster mode graduates from alpha → beta (GA-eligible for static-bootstrap workloads).** A production-hardening pass closed two of the v1 known limitations: cluster reconfiguration is now exposed via HTTP admin endpoints, and counter-style metrics for proposals / applies / leader-changes / admin-changes are emitted on `/metrics`. See `PRODUCTION_READINESS.md` for the full posture.
- **Admin HTTP endpoints for cluster reconfiguration.** `POST /v1/admin/voters` and `DELETE /v1/admin/voters/{id}` propose `AddVoter` / `RemoveServer` through Raft. **Default-deny:** both return `503 admin_disabled` until the operator sets `--admin-token` (or `DFLOCKD_ADMIN_TOKEN`); requests must carry a matching `X-Dflockd-Admin` header (compared in constant time, with a 100 ms slowdown on a miss). Each success is audit-logged with the node id, raft addr, and remote addr. On a follower the endpoints return `503 not_leader` with `X-Dflockd-Leader` set. Closes prior limitation item 1.
- **`GET /v1/readindex` (HTTP)** and **`barrier` (TCP)** linearizable-read primitives. Both propose a no-op through Raft and return only after it applies, so a subsequent read on the same leader reflects every preceding committed write. Single-node returns immediately; a follower returns `503 not_leader` (HTTP) / `error_not_leader <addr>` (TCP). The Go client exposes `client.Barrier(conn)`.
- **Counter-style cluster metrics on `/metrics`.** New monotonic counters: `dflockd_raft_proposals_total`, `dflockd_raft_proposals_failed_total`, `dflockd_raft_apply_total`, `dflockd_raft_apply_failed_total`, `dflockd_raft_apply_nanos_total` (cumulative apply latency, divide-by-`apply_total` for a mean), `dflockd_raft_leader_changes_total`, and `dflockd_raft_admin_changes_total{op="add_voter"|"add_voter_failed"|"remove_server"|"remove_server_failed"}`. Closes prior limitation item 2. Tracked via `sync/atomic.Uint64`, zero allocations on the hot path.
- **`internal/raft.ClusterMetrics`** / **`Counters`** / **`CountersSnapshot`** types — the snapshot bundle the HTTP metrics handler reads. `raft.Node.Counters()` exposes the counter struct so the cluster layer can bump it from the propose / apply paths.
- **`--admin-token` flag** (`config.Config.AdminToken`, sourced from CLI then `DFLOCKD_ADMIN_TOKEN`). Separate from `--auth-token` — admin auth gates cluster reconfig endpoints; session auth gates lock operations.
- **`make test-race` target** — runs `go test -race -p=2 -count=1 -timeout=240s ./...`. The capped parallelism stops the raft-timer goroutines from starving each other under whole-tree load (a fix for the rare flake on Apple Silicon).
- **High-availability cluster mode (alpha → beta).** dflockd can now run as an N-node Raft-replicated cluster with persistent Raft log + FSM snapshots on disk. Opt-in: set `--raft-dir`, `--node-id`, `--raft-addr`, `--cluster-peers`, and `--raft-auth-token-file`; clients connect to any member and follow the `error_not_leader <host:port>` redirect (the Go client surfaces `*client.NotLeaderError` for that). With no cluster flags the server is byte-for-byte the v2.1.x single-node binary and `go.sum` remains empty — Raft is hand-rolled in `internal/raft/` (~3,500 lines + ~1,500 lines of tests). Scope: leader election with PreVote, log replication with the same-term commit rule, durable HardState + WAL + atomic-rename snapshot files (each `flock(2)`-protected), the FSM apply pipeline, snapshot install on far-behind followers, single-server membership changes (Raft §4.3 — `AddVoter` / `RemoveServer`). Both the TCP and HTTP APIs work in cluster mode. See `PLAN.md` for the design + the §7 production-readiness checklist.
- **`internal/lock` FSM apply path.** New `ApplyAcquire` / `ApplyEnqueue` / `ApplyRelease` / `ApplyRenew` / `ApplyEvict` / `ApplyCleanupConn` / `ApplyGC` methods are pure functions of (current state, args) — they take an explicit `now time.Time` and a per-token salt, and bump a `fsmFenceCounter` recorded in the snapshot. The existing direct methods are unchanged; the new methods drive every node from the replicated log. `Snapshot(io.Writer)` / `Restore(io.Reader)` round-trip is byte-deterministic (sorted iteration), and `WatchGrants(ref)` / `RouteGrants(grants)` route promotion grants to local blocked handlers.
- **`internal/cluster`.** Glue package that assembles `internal/raft` + `internal/lock` + storage + transport. `cluster.Node` exposes typed `ProposeAcquire/Enqueue/Release/Renew/Evict/CleanupConn/GC` + `Barrier` and integration helpers (`IsLeader`, `LeaderClientAddr`, `AddVoter`, `RemoveServer`, `Status`).
- **`error_not_leader` wire status** with an optional trailing leader address; the Go client converts it to `*NotLeaderError{Leader string}` via `client.IsNotLeader(err, &nle)`.
- **Authenticated encryption on every Raft connection.** A mandatory shared secret (`--raft-auth-token-file`, direct flag, or environment) drives a fresh-nonce HMAC-SHA256 challenge-response and directional AES-GCM sessions with replay-checked sequence numbers.
- **Mutual TLS on the Raft transport** — `--raft-tls-cert` / `--raft-tls-key` / `--raft-tls-ca` (all-or-none). When set, every inter-node connection also uses TLS 1.3 with `RequireAndVerifyClientCert`; the verified leaf certificate Common Name must exactly equal the peer's NodeID.
- **Graceful leadership transfer.** `raft.Node.TransferLeadership(ctx)` (and `cluster.Node.Close()` on the leader) hands leadership to the most-caught-up follower via `TimeoutNow`, so a rolling restart of the leader re-elects within a round trip instead of waiting out an election timeout.
- **Cluster status in `stats`.** In cluster mode the `stats` TCP command's JSON gains a `"cluster"` object (node id, role, term, leader id + client address, commit / last-log / snapshot indices, voters). Single-node output is unchanged.
- **The HTTP API works in cluster mode.** `--http-port` is no longer rejected with `--raft-dir`. Mutating endpoints propose through Raft; on a follower they return `503 {"error":"not_leader"}` with an `X-Dflockd-Leader` header (the leader's raft client address). `GET /v1/stats` carries the same `"cluster"` block, and `GET /metrics` exposes `dflockd_raft_*` gauges (`_state`, `_is_leader`, `_term`, `_commit_index`, `_last_log_index`, `_snapshot_index`, `_voters`). Single-node behaviour is unchanged. New exported `internal/server` surface: `IsClusterMode` / `IsClusterLeader` / `ClusterLeaderAddr` / `ClusterStatusJSON` / `ClusterAcquire` / `ClusterEnqueue` / `ClusterWait` / `ClusterRelease` / `ClusterRenew` / `CleanupConnID`, plus the `ErrNotClusterLeader` sentinel.

### Changed

- **HA roadmap and readiness docs now match the shipped tree.** `PLAN.md`
  records the actual `client.Cluster`, voter admin endpoints, cold-node
  snapshot join, stable-ref behavior, authenticated Raft v3 transport,
  in-process soak scope, and shipped metric set. The Phase 15 checklist
  now carries current verdicts instead of pre-implementation boxes.
  `PRODUCTION_READINESS.md` records the completed P1-P3 hardening audit
  and retains the three real follow-ons: HTTP stable refs, multi-host
  fault-injection soak, and fail-stop handling for an unexpected FSM
  `Apply` panic.
- `lock.NewLockManager` and `internal/server.Server` gain non-breaking additions: `SetCluster(c)` enables the cluster-mode handler path; mutating commands on a follower return `error_not_leader <addr>`; the per-conn cleanup proposes `CleanupConn` through the cluster; the lock manager's lease-expiry / GC loops are suppressed in cluster mode (a leader-driven `EvictExpired` / `GC` sweep through Raft replaces them).
- Semaphore `limit` is now capped at 1,048,576 (`MaxSemaphoreLimit`) at parse time — far above any real use; the bound just keeps the value within the cluster snapshot's fixed-width encoding.

### Fixed (cluster mode)

- **Raft TCP connections no longer self-recycle ~5 s after they're established.** The handshake's read deadline was never cleared, so every connection died and was redialed every ~5 s — harmless on loopback (so tests/smoke missed it) but on a real network it caused constant churn, spurious RPC failures, and, with aggressive election timers, spurious leader elections. The steady-state read loops now use a 60 s idle deadline (a dead/partitioned peer is reaped, an idle-by-design conn is recycled), writes have a 10 s deadline, every conn enables TCP keepalive, and a 250 ms per-peer dial backoff stops continuous heartbeats from hammering a downed peer.
- **Lease expiry and idle GC now actually run in cluster mode.** A leader-bound sweep loop proposes `EvictExpired` every `--lease-sweep-interval` (default 1 s) and `GC` every 30 ticks; previously neither ran, so a holder whose client crashed held its lock forever and idle resources accumulated unbounded.
- **`CleanupConn` is now byte-deterministic across replicas** — a connection holding multiple contended keys had its waiters promoted (and tokens minted) in Go map-iteration order, which could differ per replica and diverge the FSM. The cleanup now processes owned keys in sorted order.
- **Cluster connection ids are now globally unique** (a per-process random epoch in the high bits + the counter in the low bits), so after a leader failover a survivor's fresh low ids can't collide with the dead leader's orphaned holders/waiters — which would have made `CleanupConn` release the wrong client's locks.
- **Two-phase enqueue → wait no longer has a lost-wakeup window** — the grant listener is registered when the `Enqueue` commits (not only when the `Wait` arrives) and held on the connection, so a promotion in between is captured rather than dropped.
- **Persistence durability hardening:** the WAL and HardState directory entries are fsync'd after creation (so the first `SaveHardState` survives a crash on a brand-new `--raft-dir`); a corrupt snapshot file now fails the open loudly instead of being silently treated as "no snapshot" (which, after a log compaction, would reset the node to empty state at term 0); WAL/snapshot reads are size-capped (≤ 64 MiB) so a corrupt length can't OOM the process; a partial WAL write is rolled back; `walFile.rewrite` no longer leaves the handle nil on a mid-rewrite error; `handleInstallSnapshot` ignores a snapshot at-or-below the committed index; a decoded `cluster.Command` is validated (key/ref length, limit range, TTL sign) before it touches the FSM.
- **Data race fixed:** `cluster.Node`'s members map (read by `LeaderClientAddr` on every redirect, written by `AddVoter`/`RemoveServer`) is now mutex-guarded; `TCPTransport`'s handler field is an `atomic.Pointer`; `Send`/`dialFresh` check the closed flag so a `Send` racing `Close` can't misuse the WaitGroup. Also: `node.shipApplyBatch` drains `snapSavec` while blocked on a full `applyc`, removing a (rare) deadlock under sustained writes with a tiny snapshot threshold.

### Security

- **HTTP `POST /v1/sessions` no longer leaks raw Go error messages on a 500.** `writeCreateSessionErr`'s default branch was passing `err.Error()` straight into the response body. Known sentinel errors (`ErrMaxSessions`, `ErrMaxSessionsPerIP`, `ErrShuttingDown`) already used empty-detail responses; the unknown-error path now matches them — log server-side, return `session_create_failed` with no detail.
- **Lock-token comparison is now constant-time.** Three sites in `internal/lock/lock.go` and one in `internal/lock/apply.go` compared the holder's stored token to the caller-supplied token with `==` — a byte-by-byte short-circuit that exposed a timing oracle (an attacker enumerating the first byte of a token sees a measurable difference between a first-byte mismatch and a later mismatch). All four now go through `constantTimeTokenEqual`, which wraps `crypto/subtle.ConstantTimeCompare`. The token format (`[0-9a-f]{32}`) is unchanged.
- **Admin endpoints (`POST /v1/admin/voters`, `DELETE /v1/admin/voters/{id}`) are default-deny.** Without an explicit `--admin-token`, both return `503 admin_disabled` — there is no fall-through to "anyone on the network can reconfigure the cluster". The header comparison is constant-time, and an auth miss sleeps 100 ms before responding to bound an online-brute-force rate to ~10/s/IP (which the rate-limit middleware further bounds).

### Documentation

- **Cluster docs are now in the mkdocs nav.** `docs/architecture/cluster.md` and `docs/operations/cluster.md` shipped weeks ago but were never added to `mkdocs.yml`, so a reader landing on the published docs site couldn't find them from the left rail. Added under new `Architecture > Cluster (Raft HA)` and `Operations > Cluster` entries.
- **`docs/server.md` has a Cluster mode section.** The page documented every non-cluster flag but never mentioned the cluster-mode flags or pointed at the cluster docs; a short section now cross-links architecture + operations.
- **`PLAN.md` §§9–10 reconciled with reality.** The phase checklist (declared the project's source of truth for progress) showed Phases 5–15 all unchecked when in fact Phases 5–9 and 14–15 had fully shipped. Open Questions left four design decisions "to confirm during implementation" — all four are answered by the code (both bootstrap shapes shipped; JSON command codec; redirect not forwarding; applied-state stats reads). Each phase now carries ✅ / 🟡 / ❌ with a pointer to the matching PRODUCTION_READINESS.md follow-on for partial phases; each Open Question carries a **Resolved** bullet.
- **`GLOSSARY.md` added** (~30 domain terms) — pins resource/holder/waiter/lease/fencing-token semantics, the raft.Node vs cluster.Node distinction, the FSM determinism contract, and ops-facing terms (`--raft-dir`, `--fence-state-file`, sweep loops, `/metrics`).
- **64 missing doc comments backfilled** across `client/`, `internal/raft/`, `internal/cluster/`, `internal/server/`, `internal/protocol/` — including the 8 public `Lock`/`Semaphore` methods in `client/lock.go`, the 8 Raft RPC request/response types in `internal/raft/transport.go`, every `Storage` interface implementation on `FileStorage` and `MemStorage`, and the 5 `Cluster*` methods on `internal/server.Server`.

### Changed

- **`cmd/bench`'s `worker`, `httpWorker`, `main` refactored to fit the cyclo≤10 / funlen≤40 bar.** Same behaviour; helpers extracted (`dialBenchConns` / `warmupLoop` / `measuredLoop` / `acquireReleaseOnce` for the TCP worker; equivalent `buildBenchHTTPClient` / `httpWarmupLoop` / `httpMeasuredLoop` for HTTP; `parseBenchFlags` / `runBenchWorkers` / `printBenchStats` for `main`).
- **Coverage** for `internal/cluster` 78.2% → 84.3% (added direct tests of the FSM raft adapter's `Snapshot`/`Restore`/`Persist`/`Release` round-trip + the membership-state accessors). `internal/httpapi` and `client` gained semaphore-path tests covering 4 + 3 previously-uncovered exported entry points.

### Known limitations (cluster mode, beta)

Resolved in this release:

- ~~`AddVoter` / `RemoveServer` need a small Go program~~ → exposed via `POST /v1/admin/voters` / `DELETE /v1/admin/voters/{id}` with default-deny `--admin-token`.
- ~~Counter-style cluster metrics not on `/metrics`~~ → `_proposals_total`, `_proposals_failed_total`, `_apply_total`, `_apply_failed_total`, `_apply_nanos_total`, `_leader_changes_total`, `_admin_changes_total{op}` all shipped.

Still open (tracked in `PRODUCTION_READINESS.md`):

- A client blocked in `acquire` / `wait` when the leader fails loses its queue position (the holder entry it never observed expires via lease; FIFO is preserved for already-granted tokens). Stable client refs that re-attach across failover are documented in `PLAN.md` §4.7 as a follow-on.
- Dynamic-join with snapshot transfer to a node started without prior state is not yet implemented; the supported flows are static bootstrap (all members listed in `--cluster-peers`) and `AddVoter` against a node that already has its members map in place. Tracked as PRODUCTION_READINESS.md item 5.
- The cluster-aware Go client (transparent leader cache + auto-redirect/retry on `*NotLeaderError`) is not yet shipped — callers handle `*NotLeaderError` themselves. Tracked as item 4.
- A multi-node soak harness (sustained writes + injected partitions + leader kills) is not yet in CI. The race detector + per-package unit tests pass; longer-horizon validation is a follow-on.

## [v2.1.1] - 2026-05-11

### Security

- **Build toolchain bumped to go1.26.3** for the standard-library fixes in [GO-2026-4971](https://pkg.go.dev/vuln/GO-2026-4971) (Windows-only panic in `net.Dial`/`net.Listen` on an address containing a NUL byte) and [GO-2026-4918](https://pkg.go.dev/vuln/GO-2026-4918) (HTTP/2 transport infinite loop on a malformed `SETTINGS_MAX_FRAME_SIZE`). Neither is a meaningful exposure for the dflockd server — the affected call paths are `client.Dial`, the HTTP listener bind, and `cmd/bench`'s HTTP client — but it clears the `govulncheck` gate.

[v2.1.1]: https://github.com/mtingers/dflockd/releases/tag/v2.1.1

## [v2.1.0] - 2026-05-10

### Added

- **`--fence-state-file` for strict cross-restart fencing.** When set, dflockd pre-allocates fence ranges to a checksummed two-slot journal via fsync — one fsync per ~1M grants. After a crash or clean restart, the new instance always seeds above the highest fence the prior instance could have issued, regardless of wall-clock regression. The file is exclusive-locked (`flock(2)`) while dflockd is running so two instances cannot share one state path; the flag is **refused at startup on platforms without exclusive file locking** (Windows and other non-Unix targets) rather than running without that guarantee. Default off preserves the "single binary, zero deps" promise. Measured overhead vs. the in-memory path on Apple M1 (`BenchmarkNewToken_*`, three 5 s runs): ~41 → ~44 ns/op single-threaded, ~137 → ~146 ns/op parallel — a constant ~3 ns/op overhead, dominated by the CAS loop the in-memory path skips (the CAS is necessary: `atomic.Add` on a uint64 wraps silently and would issue 0). Up to ~1M fence values are skipped per restart but monotonicity holds unconditionally. Acquire / Enqueue / Wait fence failures surface as `503 fence_persistence` over HTTP and the generic `error` status over TCP.
- **`LockManager.Close()`** to release the fence state file (no-op when persistence is disabled). `cmd/dflockd` calls it on graceful shutdown.
- **`ErrFencePersistence`** sentinel for fence persistence failures.
- **Fuzz tests** for the parsing/validation surface: `FuzzParseRequest` / `FuzzReadRequest` (TCP wire parsing), `FuzzFenceFromToken` / `FuzzParseServerResponse` (Go client), `FuzzRESTValidators` / `FuzzDecodeJSONBody` (HTTP API), `FuzzDecodeFenceRecord` and a stateful `FuzzLockManagerSequentialOps` (lock manager). Run with `go test -fuzz=FuzzX ./...`; the inline seed corpus also runs as part of plain `go test`.

### Changed

- **Lock tokens are now usable as fencing tokens.** The 32-char hex format is unchanged, but the layout is: first 16 chars are a server-monotonic uint64 (big-endian), last 16 chars are random salt. The prefix strictly increases on every grant. Without `--fence-state-file`, the counter seeds from `time.Now().UnixNano()` at startup (cross-restart monotonicity is best-effort, dependent on wall-clock not regressing). With it, monotonicity is strict (see Added). Existing clients are unaffected (tokens were already opaque). New helper: `client.FenceFromToken(token) (uint64, error)` — it validates the full 32-char hex shape, not just the fence prefix. OpenAPI tightened the lock-token pattern from `^\S+$` to `^[0-9a-f]{32}$` on `ReleaseRequest`, `RenewRequest`, and `OpResponse.token`. See README "Fencing tokens".
- **`lock.NewLockManager` now returns `(*LockManager, error)`.** Persistent fence state opens a file at startup; failure to open or perform the initial fsync is a startup error rather than a silent fallback.
- **HTTP requests now carry a full read deadline.** The HTTP API server sets `ReadTimeout` to the configured `--read-timeout` (default 23 s) in addition to the existing `ReadHeaderTimeout` (10 s), so a client that sends headers then dribbles the request body no longer holds a connection open until the idle timeout. Long-poll handlers are unaffected — the body is consumed before the lock-manager call returns.
- **`HTTP method` is normalised before becoming a `dflockd_http_requests_total` label.** Unknown verbs (which `net/http` accepts on the request line and pass straight through on an unmatched route) bucket as `OTHER`, so a client spraying distinct method tokens can no longer grow the per-route metrics map without bound.
- **Startup warns when DoS-relevant limits are left unbounded** — `--max-connections`, `--max-connections-per-ip`, `--max-waiters`, and (when the HTTP API is enabled) `--http-max-sessions` / `--http-max-sessions-per-ip` / `--http-max-connections-per-ip` / `--http-rate-limit-per-ip` — unless the server is bound only to loopback.
- **`LockManager.Release` / `Renew` no longer surface a downstream grant error.** A failure promoting the next waiter after a release or expired-lease eviction (only reachable with `--fence-state-file` set and the disk failing) is now logged rather than returned, since the release/eviction itself succeeded and the slot is reclaimed by the lease sweep. Both methods' `error` return is now always `nil`.

### Fixed

- **A panic inside a TCP command handler no longer crashes the process.** The TCP dispatch path recovers per request (matching what `net/http` already does for the REST API), logs the offending command and stack, and returns a generic `error` to the client; other connections keep serving.

[v2.1.0]: https://github.com/mtingers/dflockd/releases/tag/v2.1.0

## [v2.0.1] - 2026-05-05

### Fixed

- **HTTP `/wait` returns `410 session_gone` when the session is deleted mid-flight.** v2.0.0's `renderLockErr` swallowed every `context.Canceled` / `context.DeadlineExceeded` silently — including the case where `DELETE /v1/sessions/{id}` cancelled the session-lifetime context but the HTTP client was still connected. The handler now distinguishes the two: HTTP-client-gone is still silent (no useful response to write), but session-cancelled-while-client-alive surfaces the documented `410 session_gone` contract. Regression test in `TestHTTP_DeleteAbortsLongPollWait`.

### Changed

- **OpenAPI request schemas now mirror the Go decoder exactly.** All seven request schemas (`AcquireRequest`, `SemAcquireRequest`, `ReleaseRequest`, `RenewRequest`, `EnqueueRequest`, `SemEnqueueRequest`, `WaitRequest`) declare `additionalProperties: false` to match `json.Decoder.DisallowUnknownFields()`. Integer second-fields (`acquire_timeout_s`, `lease_ttl_s`, `timeout_s`) gained `maximum: 9223372036` to mirror the runtime `maxProtocolSeconds = MaxInt64 / time.Second` ceiling. Token strings and the `{key}` path parameter gained `minLength: 1`, `maxLength: 256`, and `pattern: "^\\S+$"` to mirror `validateRESTKey` / `validateProtocolField`. `SemAcquireRequest` was flattened from `allOf` to inline (`additionalProperties: false` does not compose under `allOf`). Bumped `info.version` to `2.0.1`. New tests: `TestOpenAPI_RouteMethodsMatchSpec` enforces method-level parity with `Routes()`; `TestOpenAPI_RequestSchemasRejectUnknownFields` keeps future schemas honest.

[v2.0.1]: https://github.com/mtingers/dflockd/releases/tag/v2.0.1

## [v2.0.0] - 2026-05-05

Major reset: dflockd is now exclusively a distributed FIFO lock server. The pub/sub layer that v1.11+ shipped has been removed. The pre-refactor source tree is preserved under `old/` (gitignored, build-tagged out) for reference.

### Removed

- **Pub/sub signal layer.** All wire commands (`listen`, `unlisten`, `signal`), the `internal/signal` package, the per-connection push-writer goroutine in the TCP server, the `--max-subscriptions` flag, and the corresponding env var.
- **HTTP signal endpoints.** `POST /v1/signals/{channel}` and `GET /v1/signals` (SSE), along with the SSE keepalive ping (`--http-sse-ping-interval`) and the `signal_channels` field on `/v1/stats`.
- **HTTP "bridge" architecture.** The previous design ran the line protocol over a `net.Pipe` per HTTP session, with a multiplexer goroutine splitting command responses from signal frames. The bridge has been removed entirely; HTTP handlers now call `LockManager` methods directly using the session's `connID`. One goroutine per HTTP request, no per-session multiplexer or push writer.
- **Go client signal types.** `SignalConn`, `Signal`, `Listen`, `Unlisten`, `Emit`, `WithGroup`, `WithHeartbeatInterval`, `DroppedSignals`.

### Changed

- **HTTP session lifecycle is explicit.** Sessions are now plain metadata (`{ID, ConnID, OwnerIP, lastSeen, inFlight, closed, ctx, cancel}`). `BeginRequest` claims a per-session mutex and bumps an in-flight counter so the idle sweeper can't reap a session mid-handler. JSON body parsing happens *before* `BeginRequest`, so a slow body can't pin the session indefinitely.
- **Session DELETE now drains correctly.** `sealAndDrain` cancels a session-lifetime context (waking any in-flight `lm.Acquire`/`lm.Wait` immediately), then waits for the handler's mutex before running `CleanupConnection`. A handler that already passed `Lookup` can no longer mint a token whose `connID` is being wiped.
- **Codebase split for testability.** Production code rewritten with a per-function 5-line / cyclomatic-3 target. Function counts grew (many small helpers); the C≥10 outlier count went from 28 to 5 (the remaining five are in `cmd/bench` / `tools/complexity` / `testutil`, deferred deliberately). A `tools/complexity` AST reporter ships with the repo (`make complexity`).
- **Lock manager: small-helper split.** `Acquire`, `Enqueue`, `Wait`, `Renew`, `Release`, `CleanupConnection`, and the background sweepers are now expressed as orchestration over named transition helpers (`tryFastAcquire`, `consumePreGrantedToken`, `evictHolder`, `cleanupShard`, etc.).
- **Client: state-machine deduplication.** `Lock` and `Semaphore` share a `runAcquire` / `runEnqueue` / `runWait` flow parameterised by a `resourceOps` struct that captures the per-resource protocol functions. The two types' methods are now thin wrappers over shared helpers.
- **Protocol package: table-driven.** Status formatting and per-command argument parsing are now table-driven; per-command parsers are 5 lines or fewer.
- **Server lifecycle loops split.** `serve`, `ServeConn`, `watchPeerClose`, and the HTTP `Run` are decomposed into pure helpers plus thin orchestration.
- **Config: validation and resolution split.** `Config.Validate` is now a slice of named single-purpose validators; `Load` is split into `defineFlags`, `resolveAll`, `applyDerivedDefaults`. Deprecated env-var aliases (`DFLOCKD_GC_LOOP_SLEEP`, `DFLOCKD_GC_MAX_UNUSED_TIME`) were dropped.
- **Tests: scenario helpers extracted.** HTTP scenario builders, TCP server runtime, lock test fixtures, and config env helpers are reusable. Tests grew from 155 to 229 functions, but the >10-line count fell from 95 to 63.

### Added

- **`GET /v1/openapi.json` served by the running server.** OpenAPI 3.1 spec embedded via `go:embed`, exempt from bearer auth so codegen tools can fetch the schema without credentials. A drift test (`TestOpenAPI_DocsCopyMatchesEmbedded`) fails CI if `docs/openapi.json` falls out of sync; `make openapi-sync` mirrors the canonical copy. A second test (`TestOpenAPI_RoutesMatchSpec`) enforces 1:1 correspondence between registered routes and documented paths.
- **HTTP bench mode.** `cmd/bench --http --servers http://host:port [--auth-token …]` drives the REST API (sessions + acquire + release). HTTP measures roughly 2× TCP latency at the same concurrency; baseline TCP perf is preserved.
- **Per-function complexity tooling.** `tools/complexity` walks every active `.go` file and reports non-blank line count + cyclomatic complexity per function. `make complexity` and `make complexity-strict` are the entry points.

### Fixed

Fixes for issues identified during the refactor (all previously-shipped behaviour was preserved at the wire level):

- **HTTP DELETE no longer blocks behind a long-poll `/wait`.** The session now exposes a session-lifetime context; `sealAndDrain` cancels it before waiting on the per-session mutex, so an in-flight `lm.Wait` returns `ctx.Canceled` within milliseconds instead of running to its `timeout_s`. Regression test in `TestHTTP_DeleteAbortsLongPollWait`.
- **Stranded grant on cancelled enqueue.** The "queued" cleanup path issued `lm.Wait(timeout=0)` and discarded its return values. `lm.Wait` can still return a token when the waiter is promoted between `Enqueue` and the cleanup call; that token now goes through `lm.Release` instead of being silently dropped.
- **Active long-poll requests are no longer reaped as idle.** The session sweeper now skips sessions with `inFlight > 0`. A `/wait` whose `timeout_s > 2 × HTTPSessionIdleTimeout` (default >40s) used to surface `session_gone` mid-flight.
- **Per-session command serialisation restored.** Two concurrent `/wait` calls on the same `(session, key)` could both park on the same waiter channel; only one received the grant, the other returned timeout. The new per-session mutex (`BeginRequest`) gives the HTTP API the same single-virtual-connection contract the old bridge had via `reqMu`.
- **Slow-body DoS closed.** `BeginRequest` is now claimed *after* JSON body parsing. A slowloris-style trickle on the body can no longer pin `inFlight=1` indefinitely; the sweeper reaps the stalled session after 2× idle timeout.
- **Protocol enqueue arg validation.** `e\nk\n30 junk\n` used to silently parse `30` as the lease and drop the rest; `parseEnqueue` now matches `se`'s arg-count check.
- **Bearer auth path simplification.** `/health`, `/ready`, and `/v1/openapi.json` are explicitly exempt; everything else requires `Authorization: Bearer …`.
- **`connections` count is consistent across transports.** TCP `stats` previously reported only TCP connections while HTTP `/v1/stats` reported TCP plus HTTP sessions. The HTTP server now registers a contributor on the TCP server (`SetExtraConnCounter`); both endpoints route through `Server.TotalConnCount()` so they always agree, and the Prometheus `dflockd_connections` gauge stays correct.
- **Bench: TCP worker no longer leaks the just-dialled conn on auth failure.** `cmd/bench` dialed before authenticating, but the deferred close iterated the `conns` slice — and the conn wasn't appended until after auth succeeded. An `Authenticate` failure therefore returned with the socket still open. The auth-failure branch now closes `c` explicitly.

### Security

- **HTTP auth failure now matches the TCP brute-force slowdown.** TCP's `rejectAuth` slept 100 ms before closing; the HTTP equivalent returned 401 instantly, leaving HTTP a faster credential-stuffing surface than TCP. The HTTP middleware now sleeps for the same 100 ms before writing the 401. Per-IP HTTP connection caps (`--http-max-connections-per-ip`) and rate limits (`--http-rate-limit-per-ip`) remain the recommended defenses for production exposure; both default to "unlimited" and should be set explicitly when the HTTP API is reachable from untrusted networks.

### Migration

For most callers, the v1 → v2 migration is "drop pub/sub and update version". Specifically:

- **Lock and semaphore APIs are unchanged.** TCP wire commands `l`/`r`/`n`/`e`/`w`/`sl`/`sr`/`sn`/`se`/`sw`/`ping`/`stats`/`auth` behave identically. `Lock` and `Semaphore` Go-client types keep the same fields, methods, and semantics. Same response codes.
- **HTTP lock and semaphore endpoints are unchanged.** `POST /v1/locks/{key}`, `/v1/locks/{key}/release`, `/renew`, `/enqueue`, `/wait`, the semaphore equivalents, `/v1/sessions{,/{id},/{id}/ping}`, `/v1/stats`, `/health`, `/ready`, `/metrics` all keep the same shape.
- **Pub/sub is gone.** Migrate to a dedicated message broker (NATS, Redis pub/sub) for the signal use case. The `signal_channels` array no longer appears in `/v1/stats`.
- **Config: `--max-subscriptions` and `--http-sse-ping-interval` are removed.** Drop them from your config.
- **Go client: `SignalConn` is gone.** Replace with the broker of your choice.

[v2.0.0]: https://github.com/mtingers/dflockd/releases/tag/v2.0.0

## [v1.16.1] - 2026-04-27

### Fixed

- **Lock: `Acquire`/`Wait` success branch no longer hands a token to a cancelled caller.** The timeout branches already released a just-granted token when the parent ctx was cancelled simultaneously with the grant; the success branches did not. With `select` non-deterministic when both `w.ch` and `timeoutCtx.Done()` were ready, ~half of those races returned the token to a caller that had abandoned, leaking the lock until lease expiry. Both branches now check `ctx.Err()` and re-grant to the next waiter when cancelled.
- **HTTP bridge: a grant that arrived just as the request was cancelled is no longer silently dropped.** `commandContext`'s `<-ctx.Done()` branch closed the session unconditionally, discarding any response already in `respCh`. A grant that landed between command-completion and HTTP cancellation was lost — `maybeCleanupOnDisconnect` never ran because the handler observed `ctx.Err()` instead of the grant. The branch now drains `respCh` first; if a response is queued the protocol command actually completed, so it is returned (the handler then releases the grant) and the session stays alive.
- **HTTP bridge: `lastSeen` is refreshed on command exit, not entry.** `defer s.lastSeen.Store(time.Now().UnixNano())` evaluated `time.Now()` at registration (Go's deferred-arg semantics), so a long-poll `Wait` left `lastSeen` pointing at the entry time. With short `--http-session-idle-timeout` values, the bridge sweeper's 2× cutoff could reap a session moments after a successful long-poll response. The defer is now wrapped in a closure so the timestamp reflects the actual exit time.
- **Lock: `Release`/`Renew` on a non-matching token no longer keep an empty resource alive past `--gc-max-idle`.** Both updated `LastActivity` before validating `Holders[token]`, so a caller spamming bogus tokens kept idle resources around indefinitely (defeating GC up to `--max-locks`). The `LastActivity` update now runs only after the token is confirmed to belong to a current holder.
- **HTTP SSE: internal ping no longer blocks past client disconnect.** `handleSSE`'s ping used `s.command` (background context), so a hung or slow server pinned the SSE goroutine, virtual conn, and `ServeConn` worker even after the HTTP client had disconnected (the bridge sweeper's `inFlight` check kept the session alive throughout). The ping now uses `s.commandContext(r.Context(), …)` so client disconnect aborts the in-flight ping and tears the session down promptly.
- **Bench: `cmd/bench` no longer panics when a worker's `Dial` partially fails.** The `conns` slice was pre-sized to `numConns` and assigned by index, so a later `Dial` failure left earlier nil slots; the deferred cleanup then dereferenced nil. The slice is now `make(..., 0, n)` plus `append`, so cleanup only sees successfully-dialed conns.

## [v1.16.0] - 2026-04-25

### Added

- **Operations: health, readiness, and Prometheus metrics.** The HTTP API now exposes unauthenticated `GET /health` and `GET /ready`, plus authenticated `GET /metrics` when bearer auth is enabled. Metrics include HTTP request counters/duration counters and runtime gauges for readiness, uptime, connections, HTTP sessions, locks, semaphores, waiters, and signal listeners.
- **Operations: per-IP HTTP protections and CORS.** Added `--http-rate-limit-per-ip`, `--http-rate-limit-burst`, `--http-max-connections-per-ip`, `--http-max-sessions-per-ip`, and `--http-cors-allowed-origins` with matching `DFLOCKD_HTTP_*` env vars.
- **Server: per-IP TCP connection cap.** Added `--max-connections-per-ip` / `DFLOCKD_MAX_CONNECTIONS_PER_IP` to prevent a single source from consuming every TCP connection slot.
- **CI: production gate workflow.** GitHub Actions now runs Go 1.26.2 tests, race tests, `go vet`, `govulncheck`, and strict docs builds.

### Changed

- **Client: background renewals use early-only jitter.** High-level `Lock` and `Semaphore` renewal loops now default to 10% jitter before the configured renewal ratio, reducing synchronized renewal bursts without risking late renewals.
- **Server: graceful shutdown reports draining.** New commands on existing TCP connections receive `error_draining` during shutdown, and HTTP readiness returns `503 {"status":"draining"}` while draining.
- **Lock: disconnect cleanup is indexed by connection.** Two-phase enqueue cleanup now uses a per-connection index so disconnect cost scales with that connection's enqueues instead of every enqueued waiter in a shard.
- **Cmd: server error fan-in now uses an error channel.** The daemon no longer relies on shared `tcpErr`/`httpErr` variables synchronized indirectly by `WaitGroup`.
- **Tests: package test configs now call `Config.Validate()`.** Test helpers validate constructed configs so future default/constraint drift fails in tests instead of bypassing validation.

### Security

- **Build: prefer Go 1.26.2 toolchain.** This clears reachable standard-library TLS/x509 findings reported by `govulncheck` against Go 1.26.1.

### Fixed

- **Config: auth token source precedence now matches documented CLI-first behavior.** Explicit values resolve as `--auth-token` > `--auth-token-file` > `DFLOCKD_AUTH_TOKEN` > `DFLOCKD_AUTH_TOKEN_FILE`; direct token values and token files are trimmed of surrounding whitespace, and empty tokens are rejected.
- **Client: high-level cancellation no longer leaves abandoned grants behind.** If context cancellation races with a successful lock or semaphore grant, the `Lock` and `Semaphore` APIs release the token before returning the context error.
- **HTTP/OpenAPI: validation limits now match the native protocol.** Keys, tokens, SSE groups, signal channels, renew args, and signal payloads are documented and validated against the same line/payload caps used by TCP clients.
- **Server: duplicate signal listens no longer count against `--max-subscriptions`.** Re-listening the same pattern/group at the cap is idempotent instead of being rejected.
- **HTTP: per-IP rate-limit buckets are now evicted when idle.** The token-bucket map previously retained an entry for every distinct client IP it had ever seen. A sweeper now drops buckets whose last activity is older than 10 minutes (sweep every 5 minutes), bounding memory at the working set of recently-active IPs.

## [v1.15.0] - 2026-04-25

### Added

- **HTTP REST + SSE API** (opt-in via `--http-port`) — a translation layer over the native TCP protocol that maps every operation to a REST endpoint. Every HTTP session owns an in-process virtual connection (`net.Pipe`) that feeds into the unchanged `ServeConn` handler, so FIFO ordering, lease expiry, two-phase `enqueue`/`wait`, signal pub/sub, and auto-release on disconnect all carry over without duplicated logic. A TCP client and an HTTP session contending on the same key share one FIFO queue.
  - **Session model:** `POST /v1/sessions` mints an opaque server-generated ID; every state-touching request carries `X-Dflockd-Session`. `DELETE /v1/sessions/{id}` triggers synchronous cleanup (same path as a TCP disconnect).
  - **Endpoints:** acquire/release/renew for locks (`/v1/locks/{key}/...`) and semaphores (`/v1/semaphores/{key}/...`), two-phase `/enqueue` + `/wait`, publish via `POST /v1/signals/{channel}` (sessionless), subscribe via SSE on `GET /v1/signals?pattern=...`, introspection at `GET /v1/stats`, and `POST /v1/sessions/{id}/ping` for keepalive.
  - **OpenAPI 3.1 contract** served at `GET /v1/openapi.json` (hand-authored, embedded via `go:embed`, mirrored at `docs/openapi.json`). A drift test enforces 1:1 correspondence between registered handlers and documented paths.
  - **Auth and TLS** reuse `--auth-token` and `--tls-cert`/`--tls-key` — same credentials served on both listeners. Bearer-token authentication on every request except the OpenAPI spec endpoint.
  - **New flags:** `--http-port` (default 0/disabled), `--http-host`, `--http-session-idle-timeout`, `--http-max-sessions`, `--http-sse-ping-interval`, plus matching `DFLOCKD_HTTP_*` env vars.
  - **Zero new dependencies** — pure stdlib (`net/http`, `net.Pipe`, `embed`).

### Changed

- **Server internals:** extracted `(*Server).ServeConn`, `NextConnID()`, `LockManager()`, `Signals()`, `Config()` as exported methods so alternate transports can reuse the protocol handler. TCP accept-loop connection accounting (`s.conns`, `s.connCount`) moved to the goroutine wrapping `ServeConn`; no behavior change for TCP clients.
- **Disconnect cleanup:** pending waiters and two-phase enqueue state are now always cleaned up when a connection closes, regardless of `--auto-release-on-disconnect`. A disconnected waiter cannot observe a later grant, and leaving enqueued state behind wedges both the waiter queue and the `error_already_enqueued` guard. The flag now only gates release of **held** locks and semaphore slots.
- **Protocol: signal payload and auth token lines accept up to 64 KiB** (was 256 bytes). Command/key lines and all other args keep the 256-byte cap; the relaxed limit lets realistic JSON events and long tokens pass through the wire layer.
- **Config: canonical env var names match flag names.** Introduced `DFLOCKD_GC_INTERVAL_S` and `DFLOCKD_GC_MAX_IDLE_S`; the legacy `DFLOCKD_GC_LOOP_SLEEP` and `DFLOCKD_GC_MAX_UNUSED_TIME` remain accepted as deprecated aliases with a one-shot stderr warning.

### Fixed

- **Server: blocking acquire didn't observe peer disconnect.** `lm.Acquire` could block for up to the full `acquire_timeout_s` while its client had already closed the connection, wasting its FIFO position. Added `watchPeerClose` which peeks the reader (without consuming bytes) while a blocking command is in flight and cancels the per-conn context on EOF. Handles the buffer-full and pipelined-data cases via a growing peek window.
- **Server: auth token env var overrode CLI flag.** `DFLOCKD_AUTH_TOKEN` silently won over `--auth-token`, contradicting the documented "CLI > env" precedence. `loadAuthToken` now honors the flag when explicitly set.
- **Server: Accept loop busy-spun on persistent errors.** A non-cancellation error from `listener.Accept` (e.g. FD exhaustion) looped immediately with no backoff. Now uses exponential backoff (5ms → 1s) capped, reset on every successful accept.
- **Server: `handleRequest` had a dead `ack == nil` branch.** Removed — every switch arm returns a non-nil `*protocol.Ack`.
- **Lock: grant-then-cancel leaked a just-granted token.** If `ctx` was cancelled at the moment a grant arrived via the waiter channel, the `Acquire`/`Wait` timeout branch returned the token anyway. The caller, having abandoned, didn't know they owned a lock — it leaked until lease expiry. Now the grant is released to the next FIFO waiter and the caller observes `ctx.Err()`.
- **Lock: `MaxLocks` was a soft cap under concurrency.** `resourceCount >= MaxLocks` followed by `resourceTotal.Add(1)` was check-then-add; two shards could both pass the check and overshoot. Replaced with a CAS loop.
- **Lock: asymmetric `connID == 0` guard.** `connRemoveOwned` no-op'd on `connID == 0`, but `connAddOwned` happily added — silent leak under `connOwned[0]` if any transport ever passed zero. Added matching guard and documented the sentinel's semantic as "skip per-connection bookkeeping."
- **Signal: `CancelConn` called under the read lock.** `Signal` held `m.mu.RLock` while calling `CancelConn()` on slow consumers, whose `conn.Close` syscall stalled every concurrent `Listen`/`Unlisten`. Doomed listeners are now collected into a local slice and cancelled after `RUnlock`.
- **Signal: `ReadLine` timeout misclassified when errors were wrapped.** Direct `err.(net.Error)` assertion couldn't see through `fmt.Errorf "%w"` chains or TLS layering; a wrapped timeout was reported as a disconnect (code 11) instead of a read timeout (code 10). Switched to `errors.As`.
- **Client: `stopRenew` hung indefinitely on an unresponsive server.** The renewal goroutine could be blocked mid-Renew in network I/O, and ctx cancellation doesn't interrupt that. `Release()` and `Close()` inherited the hang. Added a 2s grace window then force-close of the underlying conn so the Renew I/O errors out and the goroutine exits.
- **Client: silent signal drop had no observability.** `SignalConn.readLoop` dropped signals when `sigCh` (buffer 64) was full. The drop was entirely invisible to callers. Added `DroppedSignals() uint64` exposing a monotonic counter.
- **Client: cancel watcher could close a conn after the operation succeeded.** `close(done)` didn't wait for the cancel-watch goroutine to exit; if `ctx.Done()` fired concurrently with the stop signal, the watcher could close a conn whose `Acquire`/`Wait` had already returned a valid token. `closeConnOnContextDone` now returns a stop that blocks until the watcher exits.
- **HTTP: SSE data field used Go `%q` escapes, producing invalid JSON.** Go's `%q` verb uses Go-syntax escapes (`\xNN`, `\a`, `\v`) that are not valid JSON. Payloads with control bytes yielded frames no JSON parser could consume. Switched to `json.Marshal` on each field.
- **HTTP: dead sessions counted toward `--http-max-sessions`.** A session whose virtual conn had failed still occupied a map slot until the idle sweeper caught up. `CreateSession` now prunes dead entries under the cap-check lock; `LookupSession` eagerly evicts dead entries on access.
- **HTTP: bridge session could be swept between lookup and command.** The `lastSeen` refresh happened after releasing `b.mu`, so a sweeper tick between map read and atomic store could reap a session the caller had just resolved. `lastSeen` is now updated inside the lock.
- **HTTP: multiplexer left `sigCh` open when exiting via `<-s.closed`.** If `session.close()` raced with the inner select, the multiplexer returned without closing `sigCh`, so SSE handlers didn't notice the session died until their next ping tick (up to 15s). `close(sigCh)` is now deferred and fires on every exit path.
- **HTTP: no protocol-injection defense on tokens/group fields.** `POST /v1/locks/{key}/release` accepted `{"token": "abc\nping\n_\n"}` and wrote the token verbatim into the protocol stream, injecting a bonus command. `validateRESTToken` now rejects whitespace; SSE's `group` param goes through `validateRESTLineField`.
- **HTTP: handlers didn't reject negative `lease_ttl_s`.** Now return 400 `bad_request` rather than forwarding the bad integer to the protocol.
- **Bridge: virtual conn wasn't cancelled on session close.** `session.close()` closed the pipe but didn't signal the in-flight `lm.Acquire` via context. Added a per-session `context.CancelFunc` invoked by `close()` so blocking operations unblock via `ctx.Err()` instead of waiting for the pipe-break to propagate up through `ReadRequest`.
- **Server: full-buffer pipelined disconnect couldn't be observed.** When a peer pipelined enough bytes to fill the bufio reader behind a blocking command, `watchPeerClose` couldn't peek past the buffered data to see EOF, and the waiter stayed queued until grant or timeout. Abusive full-buffer pipelines now cancel the connection.
- **HTTP: `/v1/stats` reported `connections: 0`.** The HTTP layer didn't see the TCP `connCount`. The field now sums active TCP connections and HTTP sessions.
- **HTTP: no request body size cap.** Any endpoint could be fed an arbitrary-sized JSON body and consume unbounded memory on decode. Capped at 1 MiB via `http.MaxBytesReader`.
- **HTTP: `POST /v1/signals/{channel}` spun up a full virtual connection per publish.** Each publish created a `net.Pipe`, two goroutines, and a bridge-level auth round-trip. Now calls `signal.Manager.Signal` directly — same semantics, no per-request pipeline overhead.
- **HTTP: `POST /v1/locks/{key}/enqueue` rejected an empty body.** The only field (`lease_ttl_s`) is optional; an empty body now falls back to the default lease TTL via a new `decodeOptionalJSON` helper.
- **Protocol: timeout seconds arg overflowed to a negative duration.** Values beyond `MaxInt64 / time.Second` (~9.22e9) wrapped when multiplied by `time.Second`, silently converting long waits into immediate timeouts. `parseSecondsArg` now rejects out-of-range values before multiplication.
- **HTTP: SSE write could park on a stuck client indefinitely.** `WriteTimeout` is intentionally 0 so long-poll acquires aren't cut off, which left `w.Write` unbounded inside the SSE loop. Added a per-write 30s deadline via `http.NewResponseController`.
- **HTTP: release-on-disconnect for acquire/enqueue/wait.** If the HTTP caller went away while a grant command was in flight, the session held a phantom grant until lease-TTL expiry. Handlers now release the token (or wait-cancel a queued entry) synchronously on `r.Context().Err() != nil`.
- **Client: `WithLeaseTTL` accepted invalid values silently.** A negative lease TTL was silently treated as "use server default"; lease TTL beyond `maxProtocolSeconds` produced a duration overflow server-side. Both now return an explicit error. `Lock.RenewRatio` and `Semaphore.RenewRatio` also validated to `[0, 1)` — values `>= 1` scheduled the first renewal at-or-past lease expiry.
- **Client: timeout and option-TTL could be sent oversized.** Client-side mirror of the server overflow check: `timeoutArg` and `parseOptions` reject values above `maxProtocolSeconds` so a mis-sized duration fails locally rather than producing a server-side protocol error.
- **HTTP: signal payload size asymmetry with TCP clients.** HTTP accepted payloads up to ~1 MiB, but TCP subscribers' `readLine` caps at 64 KiB — a large HTTP publish disconnected every TCP listener on the matching pattern. Both transports now enforce `protocol.MaxSignalPayloadBytes(channel)` (64 KiB minus the pushed-frame framing overhead). TCP publish also rejects oversized payloads the same way.
- **Lock: two-phase enqueue bookkeeping leak when a queued waiter is granted but never calls `Wait`.** If the waiter's channel received its grant and the caller never drained it before lease expiry, the `connEnqueued[eqKey]` entry still held the stale `{waiter}` instead of a token, so expiry cleanup couldn't match and the same `(connID, key)` pair kept returning `ErrAlreadyEnqueued`. `grantNextWaiterLocked` now promotes the entry to `{token}` when the grant fires.
- **Lock: spurious `ErrMaxLocks` during GC.** `GCLoop` deleted entries from the shard map before decrementing `resourceTotal`, briefly over-counting the global total; concurrent `Acquire` calls at the cap could fail the CAS check on that stale read. Decrement now runs before the delete loop, so the counter is only ever permissively low in that window.
- **HTTP: SSE kept shutdown waiting for the full `ShutdownTimeout`.** `http.Server.Shutdown` waits for active handlers to return, but SSE handlers can only exit once their session's pipes are closed — and `bridge.Shutdown()` ran only after `Shutdown` returned. Now wired via `http.Server.RegisterOnShutdown` so bridge cleanup runs concurrently with `Shutdown`; SSE streams observe `sigCh` close within milliseconds.
- **HTTP: signal payload parity with TCP.** HTTP rejected only literally-empty payloads; TCP rejected whitespace-only via `TrimSpace`. HTTP now enforces the same rule.
- **SSE: invalid wildcard patterns opened a session before failing.** `GET /v1/signals?pattern=a.>.b` (misplaced `>`) minted a session and then returned an error on `listen`, leaving the server doing extra work for malformed clients. Now pattern validation runs before `CreateSession`, returning 400 immediately. `signal.ValidatePattern` was exported for this.
- **Server: TCP auth comparison short-circuited on read/parse failures.** `subtle.ConstantTimeCompare` was never invoked when `ReadRequest` failed or `req.Cmd != "auth"`, leaking "connected but sent nothing / wrong command" vs "wrong token" via timing. The comparison now always runs against `""` when the earlier checks fail.
- **Server/HTTP: IPv6 bind addresses were mangled.** `fmt.Sprintf("%s:%d", "::1", 6388)` produced `::1:6388` (unparseable). Both listeners now use `net.JoinHostPort`, which wraps IPv6 literals correctly.
- **Config: duration flag/env values could overflow when multiplied by `time.Second`.** A seconds value near `MaxInt64` wrapped to a negative duration. All duration-flag resolution now goes through `durationFromSeconds`, which rejects out-of-range values with a descriptive error before multiplication.
- **Client: `Lock.Token()` / `Semaphore.Token()` could lie after a failed re-acquire.** If `connect()` replaced the connection and the acquire/enqueue/wait that followed failed or was cancelled, the stale token from the previous successful acquire stayed in the struct. Introduced `clearConnIfCurrent` helper and cleared token/lease in `connect()` too.
- **Bench: flag inputs were unchecked.** `--workers 0`, `--rounds 0`, negative `--timeout`/`--lease`/`--connections`/`--warmup`, and empty entries in `--servers` would panic or produce nonsense runs. `cmd/bench` now validates these up front and exits with an error.
- **Bench: every worker hammered the same literal `--key`, which measured single-key contention instead of the scaling workload the docs describe.** Restored the documented behavior (each worker gets a `<key>_<id>` suffix) as the default and added a `--shared-key` flag for the opt-in contended workload. Defaults now reproduce the scaling numbers in `docs/index.md` (~90k ops/s at 100-500 workers on an M1 MacBook Air).
- **Server: `watchPeerClose` stall on blocking-command response added up to 50 ms latency per op and deadlocked under contention.** `stopPeerWatch` waited on the watcher goroutine to exit, but the watcher was sitting in `reader.Peek(…)` with a 50 ms read deadline and couldn't observe `close(stop)` until that timer fired. Each blocking-command response therefore paid up to 50 ms of extra latency, and once per-round wait on a contended key crossed the watcher's 10 ms initial delay the overhead was self-reinforcing (longer responses → longer queue waits → more handlers entering the peek loop). In practice a 200-worker `cmd/bench` run on a single key collapsed to ~20 ops/s and took hours instead of seconds. `stopPeerWatch` now forces the in-flight Peek to return immediately via `conn.SetReadDeadline(aLongTimeAgo)` before waiting on `<-done`; the deadline is reset to zero once the watcher has exited. Regression test in `TestContendedAcquireDoesNotDeadlockOnPeerWatcher`.

[Unreleased]: https://github.com/mtingers/dflockd/compare/v2.0.0...HEAD
[v1.16.0]: https://github.com/mtingers/dflockd/releases/tag/v1.16.0
[v1.15.0]: https://github.com/mtingers/dflockd/releases/tag/v1.15.0

## [v1.14.0] - 2026-03-13

### Added

- **Protocol: `ping` command** — no-op command that returns `ok`, used by clients to send heartbeats that keep idle connections alive past the server's read timeout
- **Client: `SignalConn` heartbeat** — `NewSignalConn` now sends periodic `ping` commands (default: every 15s) to prevent the server from disconnecting idle signal listener connections; accepts variadic `SignalConnOption` args with `WithHeartbeatInterval(d)` to customize or disable (`0`)

### Fixed

- **Server: lock and semaphore key collision** — lock keys (`l`, `r`, `n`, `e`, `w`) and semaphore keys (`sl`, `sr`, `sn`, `se`, `sw`) are now stored in separate namespaces, so using the same key string for both a lock and a semaphore no longer returns `error_limit_mismatch`; stats output continues to show the original key without internal prefixes

[v1.14.0]: https://github.com/mtingers/dflockd/releases/tag/v1.14.0

## [v1.13.3] - 2026-03-08

### Fixed

- **Bench: panic on zero workers or rounds** — the benchmark tool panicked with an index-out-of-range error when `--workers 0` or `--rounds 0` was passed, because `all[0]` was accessed on an empty slice; now exits cleanly with an error message

[v1.13.3]: https://github.com/mtingers/dflockd/releases/tag/v1.13.3

## [v1.13.2] - 2026-03-07

### Fixed

- **Config: `--gc-max-idle 0` rejected despite being documented as valid** — validation used `<= 0` instead of `< 0`, preventing the "prune immediately" setting documented in the v1.8.1 changelog from being configured via CLI or environment variable

[v1.13.2]: https://github.com/mtingers/dflockd/releases/tag/v1.13.2

## [v1.13.1] - 2026-03-07

### Fixed

- **Bench: deadlock when worker fails during warmup** — early error returns in the benchmark worker now call `warmupWg.Done()` before returning, preventing the main goroutine from hanging at the warmup barrier
- **Tests: wrong shard locked in semaphore cleanup assertions** — three semaphore tests (`TestSemRelease_CleansConnSemEnqueued`, `TestSemRenew_ExpiredCleansConnSemEnqueued`, `TestSemLeaseExpiry_CleansConnSemEnqueued`) locked the shard for key "k1" instead of "s1" when inspecting `connEnqueued` state, masking potential races

[v1.13.1]: https://github.com/mtingers/dflockd/releases/tag/v1.13.1

## [v1.13.0] - 2026-03-07

### Performance

- **Eliminate global `connMu` bottleneck** — moved per-connection tracking (`connOwned`, `connEnqueued`) from a single global mutex into each of the 64 shards, protected by the existing shard locks. All hot-path operations (`Acquire`, `Release`, `Renew`, `Enqueue`, `Wait`) now only acquire one shard lock instead of two mutexes. Throughput at 500 workers improved +7% (87K → 93K ops/s) and p50 latency at 200 workers improved -10%.
- **Benchmark warmup** — added `--warmup` flag (default 10 rounds) with a barrier so all workers finish warmup before measurement begins, eliminating cold-start noise from results

### Added

- `GOGC` / `GOMEMLIMIT` tuning guidance in server docs for latency-sensitive deployments

[v1.13.0]: https://github.com/mtingers/dflockd/releases/tag/v1.13.0

## [v1.12.0] - 2026-03-07

### Performance

- Cache `time.Now()` once per operation in `Acquire`, `Enqueue`, `Wait`, and `Release` instead of calling it multiple times per request
- Replace `time.NewTimer` allocation in `Acquire` and `Wait` slow paths with `context.WithTimeout`, avoiding a heap allocation per blocked acquire

### Removed

- Benchmark charts (PNG+SVG) from docs and README

[v1.12.0]: https://github.com/mtingers/dflockd/releases/tag/v1.12.0

## [v1.11.9] - 2026-03-07

### Documentation

- Performance benchmarks: added throughput/latency/memory charts (PNG+SVG), added Server RSS column, standardized to 1,000 rounds per worker

[v1.11.9]: https://github.com/mtingers/dflockd/releases/tag/v1.11.9

## [v1.11.6] - 2026-03-07

### Documentation

- Added performance benchmarks table to README and docs landing page showing throughput and latency from 1 to 500 concurrent workers

[v1.11.6]: https://github.com/mtingers/dflockd/releases/tag/v1.11.6

## [v1.11.5] - 2026-03-07

### Fixed

- **Docs: incorrect `nc` examples** — `printf | nc` examples for lock acquire+release were misleading because the connection closes on pipe exit, triggering auto-release before the release command could run. All multi-step examples now use interactive `nc` sessions with persistent connections. Fire-and-forget commands (stats, signal publish) still use `printf | nc` correctly.

[v1.11.5]: https://github.com/mtingers/dflockd/releases/tag/v1.11.5

## [v1.11.4] - 2026-03-07

### Documentation

- **Simplified and deduplicated docs** — removed ~880 lines of redundant content across README, examples, protocol spec, and server config with no information loss
- **README**: trimmed from 468 to 111 lines; removed duplicated protocol reference, TLS/auth/benchmark sections, and extra Go quick starts; kept config table, one Go example, and client library links
- **Examples**: replaced duplicated Go client examples with a cross-link to the client reference; kept concise TCP protocol examples for each command type; removed benchmarking section
- **Protocol spec**: removed trailing example sessions and interoperability section (duplicated from examples page and landing page)
- **Server config**: merged separate CLI flags and environment variables tables into a single combined table; trimmed auto-release section

[v1.11.4]: https://github.com/mtingers/dflockd/releases/tag/v1.11.4

## [v1.11.3] - 2026-03-07

### Documentation

- **README**: fixed CLI/env var precedence description (CLI flags take precedence, not env vars), removed nonexistent `--no-auto-release-on-disconnect` flag, added doc links for Python and TypeScript client libraries
- **Landing page**: added Client libraries section with doc links for Go, Python, and TypeScript; reorganized Reference section
- **Architecture overview**: fixed hash function name (`fnv32a` not `fnv32`), updated Lock state → Resource state with correct `ResourceState` struct fields (limit, holders, waiters, last_activity)
- **Protocol spec**: fixed key constraint (must not contain whitespace), updated error code 5 description, added missing empty line in stats request example
- **Examples**: reordered TCP examples logically (basic → custom lease → renew → FIFO → two-phase → stats → signals), moved auth example next to TLS, moved benchmarking to end

[v1.11.3]: https://github.com/mtingers/dflockd/releases/tag/v1.11.3

## [v1.11.2] - 2026-03-07

### Fixed

- **Client: protocol injection via unvalidated tokens** — `Release`, `Renew`, `SemRelease`, and `SemRenew` now validate token parameters, preventing embedded newlines from injecting arbitrary protocol commands
- **Client: nil pointer panic on concurrent operations** — error paths in `Lock.Acquire`, `Lock.Enqueue`, `Lock.Wait`, `Semaphore.Acquire`, `Semaphore.Enqueue`, and `Semaphore.Wait` now use a captured connection reference with an identity guard, preventing a nil pointer dereference when concurrent callers replace the connection
- **Client: whitespace-only payload validation** — `validateValue` now rejects whitespace-only strings, matching the server-side validation behavior
- **Server: closed-channel panic in waiter grant** — `grantNextWaiterLocked` now defends against a closed-channel panic when notifying waiters whose connections have already been torn down
- **Server: null JSON field in stats** — `SignalChannels` in the stats response is now initialized to an empty slice, preventing `null` in JSON output when no signal subscriptions exist

### Tests

- Added edge-case tests for token validation, whitespace payloads, and protocol parsing in `client/client_test.go`, `internal/protocol/protocol_test.go`, and `internal/signal/signal_test.go`

[v1.11.2]: https://github.com/mtingers/dflockd/releases/tag/v1.11.2

## [v1.11.1] - 2026-03-06

### Fixed

- Slow consumer eviction now closes the TCP connection, ensuring blocked consumers are promptly disconnected
- Signal dedup map no longer marks a connection as delivered when its write buffer is full, preventing missed wildcard delivery
- Reject whitespace-only signal payloads that previously bypassed validation
- Validate wildcard patterns on `unlisten` to match `listen` behavior

[v1.11.1]: https://github.com/mtingers/dflockd/releases/tag/v1.11.1

## [v1.11.0] - 2026-03-06

### Added

- **Pub/sub signal system** — publish messages to named channels with asynchronous push delivery to subscribed connections
- NATS-style wildcard pattern matching: `*` (single token) and `>` (one or more trailing tokens)
- Optional queue groups for load-balanced signal delivery (round-robin across group members)
- Slow consumer eviction: connections with full write buffers are disconnected to prevent back-pressure
- Protocol commands: `listen` (subscribe with optional group), `unlisten` (unsubscribe), `signal` (publish to literal channel)
- Asynchronous push messages (`sig <channel> <payload>\n`) delivered outside the request-response flow
- `--max-subscriptions` flag and `DFLOCKD_MAX_SUBSCRIPTIONS` env var to cap signal subscriptions per connection (default: 0 = unlimited)
- `signal_channels` field in `stats` JSON response showing active subscriptions with pattern, group, and listener count
- Go client `SignalConn` type with `Listen`, `Unlisten`, `Emit`, `Signals()`, and `Close` methods
- `WithGroup` listen option for queue group membership
- Package-level `Emit` function for publishing signals without a `SignalConn`
- Signal subscriptions automatically cleaned up on client disconnect
- 38 unit tests for signal manager (pattern matching, queue groups, dedup, cleanup, concurrency)
- 12 server integration tests (protocol parsing, push delivery, disconnect cleanup, stats, max subscriptions, wildcard rejection, dedup)
- 9 client integration tests (SignalConn lifecycle, Listen/Unlisten/Emit, queue groups, input validation)
- Documentation across README, protocol spec, server config, client API, architecture overview, examples, and quickstart

[v1.11.0]: https://github.com/mtingers/dflockd/releases/tag/v1.11.0

## [v1.10.1] - 2026-03-06

### Fixed

- **Server: reject stale tokens** — tokens whose leases expired before the waiter consumed them are now correctly rejected instead of silently accepted
- **Server: TOCTOU race in max connections** — closed a time-of-check/time-of-use race in max connections enforcement where concurrent accepts could exceed the limit
- **Server: reject auth tokens with newlines** — auth tokens containing newline characters are now rejected at config validation to prevent silent protocol failures
- **Server: data race in resource counting** — fixed a data race in lock/semaphore resource counting and added missing client error handling for `ErrWaiterClosed`
- **Config: TLS pair validation** — `validate()` now rejects configurations where only one of `--tls-cert` / `--tls-key` is provided, catching the error before server startup
- **Config: empty auth token file** — `loadAuthToken` now returns an error when `--auth-token-file` points to an empty file instead of silently proceeding with no auth

### Documentation

- **Protocol spec**: corrected undocumented error responses — `error_already_enqueued` for enqueue commands, `error_not_enqueued` and `error_lease_expired` for wait commands (previously documented as generic `error`)
- **Protocol spec**: added "Semantic error responses" table listing all 7 named error statuses
- **Server docs**: fixed precedence claim (CLI flags take precedence over env vars, not the reverse), removed nonexistent `--no-auto-release-on-disconnect` flag variant, added missing `--version` flag
- **Client docs**: added `OnRenewError` field to Lock and Semaphore tables, added `ErrAlreadyQueued` and `ErrLeaseExpired` to error sentinel table
- **Installation docs**: added pre-built binaries section with GitHub Releases link, fixed example log output to match Go slog format and correct default address
- **Quickstart docs**: fixed default address from `0.0.0.0:6388` to `127.0.0.1:6388`

[v1.10.1]: https://github.com/mtingers/dflockd/releases/tag/v1.10.1

## [v1.10.0] - 2026-02-24

### Changed

- **Server: 64-shard lock manager** — replaced single `sync.Mutex` with 64 `fnv32`-keyed shards and a separate `connMu` for connection tracking, reducing contention under concurrent load
- **Server: buffered CSPRNG token generation** — token generation now buffers 4096 bytes (256 tokens) per `crypto/rand` syscall, amortising syscall overhead by 256×
- **Server: pre-formatted protocol responses** — common response status lines (`ok`, `timeout`, `error_*`, `queued`) are pre-computed `[]byte` constants; dynamic responses use `strconv.AppendInt` instead of `fmt.Sprintf`
- **Server: O(1) waiter queue grant** — replaced O(n) `copy` on every waiter grant with a `WaiterHead` index; the slice is compacted when more than half is consumed
- **Server: deferred waiter allocation** — `Acquire` no longer allocates a waiter struct on the fast path when capacity is immediately available
- **Benchmark: persistent connections** — `cmd/bench` now uses `client.Dial` once per worker goroutine and calls the low-level `Acquire`/`Release` API, measuring lock latency instead of TCP connection overhead
- **Benchmark: `--connections` flag** — optional flag to control the number of persistent connections per worker (default: 1)

[v1.10.0]: https://github.com/mtingers/dflockd/releases/tag/v1.10.0

## [v1.9.0] - 2026-02-24

### Added

- `cmd/bench` — Go benchmark tool for measuring lock acquire/release latency and throughput under concurrent load
  - Configurable workers, rounds, key prefix, acquire timeout, lease TTL, and server addresses
  - Reports total ops, wall time, throughput (ops/s), mean, min, max, p50, p99, and stdev latencies

[v1.9.0]: https://github.com/mtingers/dflockd/releases/tag/v1.9.0

## [v1.8.1] - 2026-02-24

### Fixed

- **Client: data races in `Lock` and `Semaphore` types** — multiple fields (`conn`, `token`, `lease`, `cancelRenew`) were accessed without holding the mutex in `Acquire`, `Enqueue`, `Wait`, `Release`, and `Close` methods
- **Client: `Lock.Wait` and `Semaphore.Wait` connection leak on timeout** — the connection was left open in an ambiguous state after a Wait timeout (enqueue state consumed, no lock held); now properly closed
- **Client: `Lock.Wait` and `Semaphore.Wait` connection leak on non-timeout errors** — connections were not closed on server errors, only on context cancellation
- **Client: `Enqueue` and `SemEnqueue` missing `error_max_waiters` handling** — the `error_max_waiters` server response was not parsed, causing a generic `ErrServer` instead of `ErrMaxWaiters`
- **Client: `ErrMaxWaiters` sentinel error added** — new sentinel for the `error_max_waiters` protocol status, consistent with `ErrMaxLocks` and `ErrLimitMismatch`
- **Client: `startRenew` goroutine leak** — old renewal goroutines were cancelled but not waited on before starting new ones, risking concurrent renewals; now calls `stopRenew()` which waits for the old goroutine to exit
- **Client: `readLine` off-by-one buffer overflow** — buffer was `[maxResponseBytes + 1]byte` but the overflow check used `>` instead of `>=`, allowing one extra byte to be written past the intended limit
- **Server: protocol error recovery disconnect** — read-level errors (timeout, line too long) now disconnect the client since the protocol stream may be desynchronized; parse-level errors continue safely
- **Server: `handleConn` auth error could ignore `ReadRequest` error** — when the auth read returned an error, the nil `req` was still accessed for `req.Cmd`; now returns `error_auth` on any read failure during auth
- **Server: concurrent map access in `connOwned` tracking** — lock ownership tracking was not consistently protected by the mutex in all code paths
- **Config: `GCMaxIdleTime` validation** — negative values for `--gc-max-idle` were silently accepted; now `0` is explicitly allowed (meaning "prune immediately") while the env var override is correctly applied
- **Config: `WriteTimeout` of 0 handling** — a zero write timeout no longer sets an immediate deadline on writes; it correctly disables write deadlines

### Added

- `--auth-token-file` flag and `DFLOCKD_AUTH_TOKEN_FILE` env var for loading the auth token from a file instead of passing it on the command line (avoids leaking the secret in the process list)

### Documentation

- Added `ErrMaxWaiters` to the client error table
- Added `--auth-token-file` and `DFLOCKD_AUTH_TOKEN_FILE` to README, server docs, and configuration tables
- Documented auth token resolution priority order

[v1.8.1]: https://github.com/mtingers/dflockd/releases/tag/v1.8.1

## [v1.8.0] - 2026-02-24

### Added

- Graceful shutdown drain with configurable timeout: active connections are given time to finish before being force-closed
- `--shutdown-timeout` flag and `DFLOCKD_SHUTDOWN_TIMEOUT_S` env var (default: 30 seconds, 0 = wait forever)
- Connection tracking via `sync.Map` to enable force-close on shutdown deadline
- Integration tests for graceful shutdown drain and force-close scenarios

### Fixed

- Indentation bug in protocol error write block (`server.go:202-204`)

### Changed

- Extracted shared accept-loop and shutdown logic into private `serve()` method, eliminating duplication between `Run` and `RunOnListener`

[v1.8.0]: https://github.com/mtingers/dflockd/releases/tag/v1.8.0

## [v1.7.0] - 2026-02-24

### Added

- `--max-connections` flag and `DFLOCKD_MAX_CONNECTIONS` env var to limit concurrent connections (default: 0 = unlimited)
- `--max-waiters` flag and `DFLOCKD_MAX_WAITERS` env var to limit waiter queue depth per lock/semaphore key (default: 0 = unlimited)
- `ErrMaxWaiters` sentinel error and `error_max_waiters` protocol status for rejected waiters
- `--write-timeout` flag and `DFLOCKD_WRITE_TIMEOUT_S` env var to set write deadlines on responses (default: 5 seconds)
- `writeResponse` server helper that sets and clears `SetWriteDeadline` on every write
- Unit tests for max waiters across all four enqueue paths (FIFOAcquire, FIFOEnqueue, SemAcquire, SemEnqueue)
- Integration tests for max connections, max waiters, and write timeout

### Changed

- Default bind address changed from `0.0.0.0` to `127.0.0.1` for safer defaults

[v1.7.0]: https://github.com/mtingers/dflockd/releases/tag/v1.7.0

## [v1.6.0] - 2026-02-18

### Added

- Optional token-based authentication for client connections
- Server flag `--auth-token` and env var `DFLOCKD_AUTH_TOKEN` to set a shared secret
- `auth` protocol command for clients to authenticate before issuing other commands
- `Authenticate` client function for low-level auth on a `*Conn`
- `AuthToken` field on `Lock` and `Semaphore` high-level types for automatic auth on connect
- `ErrAuth` sentinel error returned when authentication fails
- Constant-time token comparison using `crypto/subtle` to prevent timing attacks
- Auth integration tests for both server and client packages
- Documentation for auth in protocol spec, server docs, client docs, and examples

[v1.6.0]: https://github.com/mtingers/dflockd/releases/tag/v1.6.0

## [v1.4.0] - 2026-02-18

### Added

- Runtime `stats` protocol command returning a JSON snapshot of active connections, held locks, semaphores, and idle entries awaiting GC
- `Stats()` method on `LockManager` for programmatic access to server state
- Documentation for `stats` command in protocol spec, server docs, and examples

[v1.4.0]: https://github.com/mtingers/dflockd/releases/tag/v1.4.0

## [v1.3.0] - 2026-02-18

### Added

- Optional TLS encryption for client-server communication
- Server flags `--tls-cert` / `--tls-key` and env vars `DFLOCKD_TLS_CERT` / `DFLOCKD_TLS_KEY` to enable TLS
- `DialTLS` client function for low-level TLS connections
- `TLSConfig` field on `Lock` and `Semaphore` high-level types
- TLS integration tests for both server and client packages
- `internal/testutil` package with ephemeral self-signed certificate helper for tests

[v1.3.0]: https://github.com/mtingers/dflockd/releases/tag/v1.3.0

## [v1.2.0] - 2026-02-16

### Added

- Distributed key-based semaphore support allowing up to N concurrent holders per key
- Five new protocol commands: `sl` (acquire), `sr` (release), `sn` (renew), `se` (enqueue), `sw` (wait)
- Per-request `limit` parameter; first acquirer sets the limit, subsequent requests must match or receive `error_limit_mismatch`
- `ErrLimitMismatch` sentinel error and `error_limit_mismatch` protocol status
- Protocol error code 13 for zero or negative semaphore limit
- Low-level client functions: `SemAcquire`, `SemRelease`, `SemRenew`, `SemEnqueue`, `SemWait`
- High-level `Semaphore` type with `Acquire`, `Enqueue`, `Wait`, `Release`, `Close`, `Token` and automatic background lease renewal
- Semaphore lease expiry, GC pruning, and disconnect cleanup integrated into existing background loops
- Semaphore keys share the `--max-locks` budget with lock keys
- Comprehensive test suite for semaphore functionality (62 new tests across all layers)
- Documentation for semaphore commands in protocol spec, client docs, server docs, and README

### Fixed

- `CleanupConnection` early return when a connection had no lock keys, which would skip cleanup of other connection state

[v1.2.0]: https://github.com/mtingers/dflockd/releases/tag/v1.2.0

## [v1.1.0] - 2026-02-16

### Added

- Go client package (`client/`) with low-level protocol API (`Acquire`, `Release`, `Renew`, `Enqueue`, `Wait`) and high-level `Lock` type
- Automatic background lease renewal in `Lock` type with configurable `RenewRatio`
- CRC32-based sharding (`CRC32Shard`) matching the Python client's `stable_hash_shard`
- Functional options pattern (`WithLeaseTTL`) for optional protocol parameters
- Context cancellation support for `Lock.Acquire` and `Lock.Wait`
- Integration test suite for the Go client (12 tests)

[v1.1.0]: https://github.com/mtingers/dflockd/releases/tag/v1.1.0

## [v1.0.0] - 2026-02-15

### Added

- Complete server rewrite from Python to Go, delivering a single static binary with no runtime dependencies
- Standard Go project layout (`cmd/dflockd/`, `internal/config/`, `internal/lock/`, `internal/protocol/`, `internal/server/`)
- Comprehensive test suite (49 tests) covering lock operations, protocol parsing, and integration scenarios
- `--version` flag printing the embedded version string
- Cross-platform binary builds via GoReleaser (linux/darwin/windows on amd64/arm64, tar.gz archives, zip for Windows, SHA256 checksums)
- Automated GitHub Releases workflow triggered on `v*` tag push
- Dependabot configuration for Go module updates

### Changed

- CI and docs workflows updated for Go codebase
- Makefile simplified for Go build toolchain with `VERSION` ldflags support

### Removed

- Python server, async client, and sync client
- TypeScript client
- Python test suite, benchmark scripts, and example scripts
- Sharding module and documentation
- Built `site/` artifacts

[v1.0.0]: https://github.com/mtingers/dflockd/releases/tag/v1.0.0

## [v0.5.0] - 2026-02-14

### Added

- Two-phase lock acquisition with `e` (enqueue) and `w` (wait) protocol commands
- `fifo_enqueue()` and `fifo_wait()` server functions for split enqueue/wait flow
- `enqueue()` and `wait()` module-level functions in async and sync clients
- `DistributedLock.enqueue()` and `DistributedLock.wait()` methods in async and sync clients
- `Status.acquired` and `Status.queued` server response statuses
- `NotEnqueuedError` and `EnqueuedState` server internals for two-phase state tracking
- Connection cleanup for two-phase enqueued state on disconnect
- Two-phase example script (`examples/two_phase_demo.py`)
- Documentation for two-phase flow in protocol, client, architecture, and examples docs

[v0.5.0]: https://github.com/mtingers/dflockd/releases/tag/v0.5.0

## [v0.4.1] - 2026-02-07

### Added

- `--auto-release-on-disconnect` / `--no-auto-release-on-disconnect` CLI flag

### Fixed

- `DFLOCKD_DFLOCKD_READ_TIMEOUT_S` env var typo in README (now `DFLOCKD_READ_TIMEOUT_S`)
- Server configuration env var names in README missing `DFLOCKD_` prefix
- `MAX_LOCKS` default in README corrected from `256` to `1024`

[v0.4.1]: https://github.com/mtingers/dflockd/releases/tag/v0.4.1

## [v0.4.0] - 2026-02-07

### Added

- Documentation site (MkDocs Material) with architecture, configuration, client, protocol, and sharding guides
- `DFLOCKD_AUTO_RELEASE_ON_DISCONNECT` documented in server configuration

### Fixed

- Pyright CI dependency
- Ruff dev dependency

### Changed

- Bump `actions/checkout` from 4 to 6
- Bump `actions/setup-python` from 5 to 6
- Bump `actions/upload-pages-artifact` from 3 to 4
- Bump `astral-sh/setup-uv` from 4 to 7
- Update `uv-build` requirement from >=0.9.28,<0.10.0 to >=0.9.28,<0.11.0

[v0.4.0]: https://github.com/mtingers/dflockd/releases/tag/v0.4.0

## [v0.3.0] - 2026-02-07

### Added

- Async lock server with strict FIFO ordering per key
- Automatic lease expiry with configurable TTL and sweep interval
- Background garbage collection of idle lock state
- Automatic lock release on client disconnect
- Async client (`dflockd.client`) with `DistributedLock` context manager
- Sync client (`dflockd.sync_client`) with `DistributedLock` context manager
- Background lease renewal for both async and sync clients
- Multi-server sharding with `stable_hash_shard` (CRC-32)
- Custom sharding strategy support via `ShardingStrategy` callable
- Configurable `renew_ratio` for controlling renewal frequency
- CLI with `--host`, `--port`, `--default-lease-ttl`, `--max-locks`, and other flags
- Environment variable configuration with `DFLOCKD_` prefix (overrides CLI flags)
- TypeScript client (`ts/`)
- Async and sync benchmark scripts (`examples/bench_async.py`, `examples/bench_sync.py`)
- CI workflow with linting, type checking, and tests (Python 3.13, 3.14)
- GitHub Pages documentation deployment workflow

[v0.3.0]: https://github.com/mtingers/dflockd/releases/tag/0.3.0
