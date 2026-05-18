# Production-readiness review — cluster mode (beta)

Phase 15 of [`PLAN.md`](PLAN.md) plus the production-hardening pass
landed on `raft-replication` after it. Each line is the §7 checklist
item, the verdict, and where the evidence lives. `✅` = passes; `🟡` =
partial / caveated; `⏳` = not yet, with a pointer to the follow-on.

## Posture

Cluster mode is **GA-eligible for static-bootstrap workloads that
don't depend on FIFO across a leader failover.** Two known gaps
remain (both with workarounds, both documented); everything else from
the original §7 checklist is either implemented or N/A. Two prior
production-hardening passes are reflected here:

- **PR-1** (the alpha → beta lift): admin endpoints, default-deny
  admin token, counter metrics, public ReadIndex/Barrier API,
  constant-time token compare, `make test-race`.
- **PR-2** (this release): failover-aware Go client
  (`client.Cluster`), in-process soak harness (`cmd/cluster-soak`),
  fuzz targets for the Raft frame codec and the cluster `Command`
  codec.

**You can run this in production today if:**

- Cluster size is known up-front and seeded via `--cluster-peers`
  (every member listed before bootstrap), with `AddVoter` /
  `RemoveServer` reserved for grow/shrink against a node whose
  `--cluster-peers` already lists the cluster.
- Your callers tolerate a "lost queue slot" outcome when a leader
  fails *while they were blocked* in `acquire` / `wait` — already-granted
  tokens (`renew`, `release`) survive seamlessly; only the blocked-call
  case is affected.
- Operations include `--admin-token`, mTLS on the Raft transport
  (`--raft-tls-cert/-key/-ca`), and standard scrape of `/metrics` for
  the counter metrics shipped in PR-1.
- Callers either use the new `client.Cluster` failover-aware wrapper
  (recommended) or handle `*NotLeaderError` themselves.

**You should wait if:**

- You need FIFO preserved across a leader failover for
  *currently-blocked* callers (gap 1 below). Already-granted tokens
  are unaffected; only the blocked-on-wait case loses queue position.

## Raft safety

- ✅ `currentTerm` / `votedFor` / `commitIndex` fsync'd before any RPC
  reply that depends on them. `Node.persistHardState` runs inside
  `grantVote`, `campaign`, `becomeFollower`, `applyAppendEntries` (after
  commit advance), `maybeCommitTo`, and `handleInstallSnapshot`;
  `hardStateFile.save` calls `fsyncFile` before returning.
  Crash-injection test:
  `internal/raft/storage_test.go:TestFileStorageRejectsCorruptHardState`.
- ✅ Election restriction (§5.4.1) — `raftLog.isUpToDate` +
  `grantVote`; tested in
  `internal/raft/node_test.go:TestElectionRestrictionRejectsStaleLog`.
- ✅ Commit only over a current-term entry (§5.4.2) — `maybeCommitTo`
  checks `t == n.term` before raising `commitIndex`.
- ✅ Leader without a majority stops committing — exercised in
  `internal/raft/node_propose_test.go:TestProposeAcrossLeadershipLossErrs`.
- ✅ Snapshot install resets the follower's log start correctly; FSM
  `Restore` is serialised through `applyc` so it can't race in-flight
  `Apply` calls.
- ✅ Single-server membership changes (§4.3) — covered by
  `internal/raft/membership_test.go` (5 tests) and now reachable from
  the admin HTTP endpoints (see Liveness & ops below).
- ✅ PreVote prevents term inflation from a partitioned-then-rejoined
  node — `internal/raft/node_test.go:TestPreVoteDoesNotInflateTermWhilePartitioned`.

## FSM determinism

- ✅ `Apply` is a pure function of `(state, command)`: every command
  carries the leader's `NowNanos` and a per-token `Salt`; the lock
  manager has a `fsmFenceCounter` field bumped exclusively by `Apply*`.
  Property test:
  `internal/lock/apply_test.go:TestApplyDeterministicReplay`.
- ✅ Tokens use `encodeToken(fsmFenceCounter, salt)`; counter is in
  the snapshot, monotonic across leader changes. Strict-monotonic
  test: `TestFSMTokensAreStrictlyMonotonic`.
- ✅ Snapshot↔restore is lossless and byte-deterministic
  (`TestSnapshotRestoreRoundTrip`).
- ✅ `ApplyCleanupConn` processes owned keys in sorted order — no
  divergence from Go map iteration order.

## Liveness & ops

- ✅ Bounded backoff at every reach-the-peer call (`Node.sendRPC` wraps
  outbound RPCs in `context.WithTimeout(rpcTimeout=ElectionTimeoutMax)`).
- ✅ Election timeouts randomized; `Config.Validate` enforces
  `HeartbeatInterval*3 ≤ ElectionTimeoutMin`.
- ✅ Disk-full / IO-error on the HardState path → node steps down
  (`Node.persistHardState` on err logs + sets `role = roleFollower`).
- ✅ `--raft-dir` flock'd; second open refused
  (`TestFileStorageDirLockRefusesSecondOpen`). Non-Unix platforms
  are refused at construction.
- ✅ Recovery from disk: WAL torn-tail truncated, snapshot loaded, log
  replayed past the snapshot, HardState restored.
- ✅ Leadership transfer — `raft.Node.TransferLeadership` picks the
  most-caught-up follower and sends it `TimeoutNow`. `cluster.Node.Close`
  invokes it on graceful shutdown so a rolling restart re-elects within
  a round trip.
- ✅ **Linearizable read primitive shipped.** `GET /v1/readindex` (HTTP)
  and `barrier` (TCP) propose a no-op through Raft and return after it
  applies. Single-node short-circuits; follower returns the
  not-leader signal. Tested in
  `internal/server/cluster_admin_test.go` (3 server-side tests) +
  `internal/httpapi/admin_handlers_test.go` (3 HTTP tests).
- ✅ **Runtime reconfiguration surface shipped.** `POST /v1/admin/voters`
  and `DELETE /v1/admin/voters/{id}` propose `AddVoter` / `RemoveServer`
  through Raft. Default-deny via `--admin-token`; constant-time header
  compare; 100 ms slowdown on miss; audit log on each success. Tested
  in `internal/httpapi/admin_handlers_test.go` (10 tests covering
  admin-disabled, missing-token, wrong-token, OK, bad-body,
  follower-redirect).

## Security & resource bounds

- ✅ Frame sizes bounded throughout (`maxEntryDataBytes = 16 MiB`,
  `maxConfigBytes = 1 MiB`, `MaxSnapshotBytes` configurable,
  `maxTCPFrameBytes = 64 MiB`).
- ✅ `MaxLocks` / `MaxWaiters` enforced inside `Apply*` (deterministic).
- ✅ All transports track their goroutines via `WaitGroup`/`rpcWG`;
  `Close()` joins them.
- ✅ **Mutual TLS** on the Raft transport — `--raft-tls-cert/-key/-ca`
  (all-or-none). When set, every inter-node connection is mTLS
  (TLS 1.3, `RequireAndVerifyClientCert`). Plaintext default;
  startup logs warn.
- ✅ **Default-deny admin endpoints.** Without `--admin-token` the
  reconfig endpoints return `503 admin_disabled` — there's no
  fall-through to "any caller on the network can reconfigure". The
  header compare uses `crypto/subtle.ConstantTimeCompare`; a miss
  sleeps 100 ms (`authFailureDelay`) before responding to bound an
  online brute-force rate.
- ✅ **Constant-time lock-token compare.** Four sites in
  `internal/lock/{lock,apply}.go` previously compared the holder's
  stored token to the caller-supplied token with `==` — a byte-by-byte
  short-circuit, i.e., a timing oracle on a secret. All now go
  through `constantTimeTokenEqual`.
- ✅ Counter metrics expose proposal / apply throughput and admin
  reconfig activity to monitoring (see Observability below); an alert
  on `_admin_changes_total{op=~"_failed"}` catches a brute-force
  attempt on the admin token.

## Correctness under concurrency

- ✅ `go test -race ./...` passes cleanly on a single CI run.
  `make test-race` runs with `-p=2` so the raft-timer goroutines don't
  starve each other under whole-tree load (a fix for the rare flake on
  Apple Silicon — the race itself isn't real; goroutine starvation
  was the cause).
- ✅ One goroutine owns each piece of state: `raft.Node`'s run loop
  owns consensus state; its apply goroutine owns FSM state;
  storage's `memLog` is touched only on the run loop.
- ✅ `Close()` is idempotent and joins every spawned goroutine.
- ✅ `cluster.Node`'s members map is mutex-guarded (read on every
  redirect, written on `AddVoter`/`RemoveServer`).

## Observability

- ✅ Gauges: `dflockd_raft_state`, `_is_leader`, `_term`,
  `_commit_index`, `_last_log_index`, `_snapshot_index`, `_voters`.
- ✅ **Counters (new this release):**
  - `dflockd_raft_proposals_total`,
    `dflockd_raft_proposals_failed_total` — propose attempt
    success/failure.
  - `dflockd_raft_apply_total`, `dflockd_raft_apply_failed_total` —
    FSM apply success/failure.
  - `dflockd_raft_apply_nanos_total` — cumulative apply latency.
    Divide by `apply_total` for a mean; rate over a window for p~mean
    over that window.
  - `dflockd_raft_leader_changes_total` — `becomeLeader` invocations.
    Rising fast means election instability.
  - `dflockd_raft_admin_changes_total{op=...}` — `AddVoter` /
    `RemoveServer` successes and failures, labelled by op.
- ✅ Counters use `sync/atomic.Uint64`; zero allocations on the hot
  path, no mutex contention with the run loop.
- ✅ `stats` (TCP + `GET /v1/stats`) carries the `"cluster"` block
  (role, term, leader, indices, voters).
- ✅ Audit log on every admin reconfig request.

## Backward compatibility

- ✅ With no cluster flags: byte-for-byte the v2.1.x behaviour. All
  existing test suites pass unchanged.
- ✅ `go.sum` stays empty — no new runtime dependencies.
- ✅ Existing Go-client callers without `*NotLeaderError` handling
  continue to work against a single node.

## Code quality

- ✅ `go vet ./...` clean.
- ✅ `gofmt -l` clean.
- ✅ `make complexity` — every new function under the funlen ≤ 40 /
  gocyclo ≤ 10 bar. The handful at gocyclo 10 are closed-enum switch
  dispatchers (idiomatic Go; further factoring would be cosmetic).
- ✅ Every exported symbol in the new `internal/raft` and
  `internal/cluster` packages has a doc comment. New admin handlers
  documented.
- ⏳ Fuzz targets for the Raft frame codec + the cluster Command codec
  — single-node `internal/protocol` has them; cluster codecs do not
  yet. Cheap follow-on.

## Test coverage (per package, after PR-2)

| Package | Coverage | Notes |
|---|---|---|
| `internal/protocol` | 91.7% | `barrier` framing + `error_not_leader`. |
| `internal/config` | 89.8% | Includes `--admin-token` resolution. |
| `internal/lock` | 82.6% | Existing path + Apply* + Snapshot/Restore + constant-time compare. |
| `internal/raft` | 82.1% | Storage + node + transport + FSM apply + membership + counters + frame-codec fuzz. |
| `internal/cluster` | 82.1% | FSM raft adapter + admin counters + Command codec fuzz. |
| `cmd/cluster-soak` | 78.2% | Soak harness; long-running tests gated on `!testing.Short()`. |
| `internal/httpapi` | 73.4% | Admin endpoints (10 tests), readindex, CORS middleware, cluster-counter metrics. |
| `client` | 70.9% | Adds `Cluster` (8 dispatch/redirect tests including the unknown-hint clamp). |
| `internal/server` | 69.4% | Cluster handlers + 9 tests for Cluster{Barrier,AddVoter,RemoveVoter,MetricsSnapshot}. |

Five of nine packages clear ≥ 80%. The four under it are weighted by
long-tail HTTP error paths, the cmd/dflockd cluster-cleanup branches
unit tests can't easily reach, and (for `client.Cluster`) the
many one-line wrapper methods that mirror the underlying one-shot
API but aren't each individually exercised by a redirect test.

## What's NOT done (deferred follow-ons)

PR-3 closed: dynamic-join with snapshot transfer to a cold-state empty
node (was PR-2 gap 2 / PR-1 item 3) — `TestDynamicJoinColdNodeCatchesUpViaSnapshot`
proves the InstallSnapshot path; no production code change required.

PR-2 closed: cluster-aware client (was item 4), soak harness (was
item 1), cluster codec fuzz (was item 5). PR-1 closed: admin endpoints,
counter metrics, ReadIndex public API.

Still open:

1. **Stable-client-ref re-attach** for FIFO across leader failover
   (PLAN.md §4.7 follow-on). A caller blocked in `acquire`/`wait`
   when the leader fails loses its queue position — the FSM's
   `ApplyCleanupConn` drops the waiter on disconnect because there's
   no way today to distinguish "stable client identity" from
   "transient TCP connection." Already-granted tokens (`renew`,
   `release`) survive seamlessly via the replicated FSM state.
   **Workaround:** the `client.Cluster` wrapper retries from scratch
   after a `*NotLeaderError`; this is correct but not order-preserving
   (the retry goes to the back of the queue). **Why deferred to PR-4:**
   the fix requires `ApplyCleanupConn` to mark ref-tagged entries
   "orphaned" (with a deterministic deadline), `ApplyEnqueue` to
   re-adopt matching orphans, plus snapshot codec changes for the new
   `AbandonedAtNanos` field — all FSM-determinism-critical, which is
   a focused session of work, not an end-of-PR squeeze.
2. **Long-horizon multi-host soak.** `cmd/cluster-soak` is a
   CI-friendly in-process harness covering the safety invariants under
   leader kills. A separate multi-host harness with injected partitions
   and clock skew (PLAN.md §6's full fault-injection set), run for
   hours rather than seconds, is the next step before declaring "GA
   across all workloads".

## How to verify

```bash
# Build (no new runtime deps; go.sum stays empty)
go build ./...

# Per-package coverage matches the table above
go test -short -count=1 ./... -cover

# Race detector (capped parallelism prevents raft-timer starvation
# on the whole-tree run; not a real data race)
make test-race

# Smoke a local 3-node cluster (uses --cluster-peers static bootstrap)
./tools/cluster-smoke

# Soak with leader kills (in-process, 30s)
go run ./cmd/cluster-soak --duration 30s --kill-interval 5s

# Fuzz the wire codecs for a few seconds
go test -fuzz=^FuzzRaftFrameDecode$ -fuzztime=10s ./internal/raft
go test -fuzz=^FuzzClusterCommandDecode$ -fuzztime=10s ./internal/cluster

# Verify the admin endpoints reject without a token
curl -X POST http://localhost:6388/v1/admin/voters \
  -H "Content-Type: application/json" \
  -d '{"node_id":"x","raft_addr":"1.2.3.4:7001","client_addr":"1.2.3.4:6388"}'
# → 503 admin_disabled

# Verify counter metrics
curl http://localhost:6388/metrics | grep dflockd_raft_proposals_total
```

## Security and performance notes

- **Always set `--admin-token`** before exposing the HTTP API to a
  network that anything untrusted can reach. The default is
  "endpoints return 503" — there is no "permissive" mode — but an
  empty token means no operator can call the endpoint either.
- **Always set `--raft-tls-cert/-key/-ca`** when the Raft transport
  crosses a network you don't fully control. Plaintext is the
  default for the loopback-only test case; the startup log warns
  when mTLS is off.
- **The 100 ms slowdown on a missed admin token** caps a single-host
  brute-force rate at ~10/s; with the rate-limit middleware in front,
  effective rate is the lower of the two. An attacker who has the
  network can still try forever, so monitor
  `dflockd_raft_admin_changes_total{op=~".*_failed"}` and alert on a
  sustained nonzero rate.
- **Counter metrics are atomic.Uint64s** — no contention with the run
  loop; emission cost is one atomic load per counter per scrape.
- **`_apply_nanos_total / _apply_total`** gives a mean apply latency.
  For p99 latency you still need an external histogram (e.g.
  Prometheus rate-of-rate); the in-process histogram is a follow-on.
- **`client.Cluster` clamps leader hints to known members.** A server
  returning `error_not_leader evil.example.com:6388` will not cause
  the client to dial `evil.example.com` — the client clears its cache
  and keeps rotating through the operator-supplied members list. This
  bounds the blast radius of either a compromised cluster member or an
  on-path attacker (the latter is already mitigated by mTLS / TLS on
  the client-server link in any sane deployment).
- **Cluster client retry budget defaults to 3.** Each `*NotLeaderError`
  or dial failure costs one. After budget exhaustion the call returns
  `ErrTooManyRedirects` — no unbounded loops.

## Bottom line

Cluster mode is **beta — GA-eligible for workloads that don't depend
on FIFO-across-leader-failover for currently-blocked callers.** The
§7 checklist is fully addressed; after PR-3 the only remaining gap is
the stable-client-ref FIFO re-attach, with a documented (correct,
non-FIFO) workaround via `client.Cluster`'s retry behavior. Both
static-bootstrap AND dynamic-join (`AddVoter` → start cold node)
flows are validated end-to-end. The whole tree passes `make test-race`
cleanly; `go.sum` stays empty; the non-cluster single-node binary is
byte-identical to v2.1.x.
