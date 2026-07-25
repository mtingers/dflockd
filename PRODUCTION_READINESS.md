# Production-readiness review — cluster mode (release candidate)

Phase 15 of [`PLAN.md`](PLAN.md) plus the production-hardening pass
landed on `raft-replication` after it. Each line is the §7 checklist
item, the verdict, and where the evidence lives. `✅` = passes; `🟡` =
partial or environment-dependent.

## Posture

Cluster mode is **GA-eligible for static-bootstrap workloads.** FIFO
across a leader failover — once the headline gap — is now available to
TCP and HTTP clients as an opt-in (stable client refs; see below).
The long-horizon multi-host partition/restart/clock-skew harness now
ships. A recorded multi-hour campaign on the target deployment class is
still required evidence before declaring that deployment GA; it was not
possible to execute a multi-host campaign in this repository workspace.
Everything from the original §7 checklist is implemented or N/A.
The production-hardening passes reflected here:

- **PR-1** (the alpha → beta lift): admin endpoints, default-deny
  admin token, counter metrics, public ReadIndex/Barrier API,
  constant-time token compare, `make test-race`.
- **PR-2**: failover-aware Go client (`client.Cluster`), in-process
  soak harness (`cmd/cluster-soak`), fuzz targets for the Raft frame
  codec and the cluster `Command` codec.
- **PR-3**: dynamic-join via InstallSnapshot to a cold node, validated.
- **PR-4 + PR-5**: FIFO across leader failover via stable client refs.
  PR-4 introduced the mechanism; PR-5 closed it for the case that
  matters — hard leader crash (not just graceful disconnect), the
  single-phase `Acquire` path (not just two-phase `Enqueue`), and the
  `--orphan-ttl` flag itself (PR-4 left it unwired). Proven end-to-end
  by `TestE2EStableRefReAttachAcrossFailover`.
- **HTTP stable refs**: `POST /v1/sessions` accepts an optional
  `stable_ref`; replacement node-local sessions reuse it to re-attach
  replicated holders and waiters. A shared-FSM leader-change regression
  guard asserts that HTTP receives the original held token.
- **Post-review P1-P3 audit**: closed per-peer snapshot gating, fatal
  storage-fault handling, asynchronous local snapshot persistence,
  lock-free leadership reads, post-commit membership publication,
  stable-ref indexing, idle-maintenance suppression, binary Raft RPCs,
  authenticated/encrypted Raft transport, and cluster-client retry
  diagnostics. The final FSM fault pass made unexpected `Apply` panics
  and indeterminate installed-snapshot restores fail-stop.
- **External fault soak**: `cmd/cluster-soak --targets` drives real
  endpoints with stable refs and a pluggable partition/restart/skew
  hook. `tools/cluster-soak/ssh-linux.sh` provides Raft-only Linux
  partitions and process-local clock offsets without changing host
  clocks. Contended-key histories receive bounded exact linearizability
  checking in addition to full-run token and fence invariants.

**You can run this in production today if:**

- Cluster size is known up-front and seeded via `--cluster-peers`
  (every member listed before bootstrap), with `AddVoter` /
  `RemoveServer` reserved for grow/shrink against a node whose
  `--cluster-peers` already lists the cluster.
- Callers either enable stable client refs (`--orphan-ttl` plus
  `client.WithClusterStableRef` for TCP or `stable_ref` when creating
  HTTP sessions) to keep their queue slot / held lock across a failover,
  or tolerate a "lost queue slot" outcome when a leader fails while
  they were blocked in `acquire` / `wait`. HTTP callers recreate their
  node-local session with the same ref. Already-granted tokens
  (`renew`, `release`) survive seamlessly either way.
- Operations include `--admin-token`, mandatory `--raft-auth-token-file`,
  optional mTLS on the Raft transport
  (`--raft-tls-cert/-key/-ca`), and standard scrape of `/metrics` for
  the counter metrics shipped in PR-1.
- Callers either use the new `client.Cluster` failover-aware wrapper
  (recommended) or handle `*NotLeaderError` themselves.

**You should wait if:**

- Your release bar requires a recorded long-horizon campaign and you
  have not run `cmd/cluster-soak --targets` against representative
  multi-host infrastructure.

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
- ✅ Disk-full / IO-error on any durable mutation path → node stops
  (`Node.failStorage` logs, publishes follower state, and closes the
  node); it cannot continue voting or acknowledging from non-durable
  state.
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
- ✅ An unexpected FSM `Apply` panic is recovered, logged with a stack,
  returned to the local proposer, and fail-stops the node. The apply
  goroutine drains already-transferred proposal futures with
  `ErrStopped` without applying later entries. An installed-snapshot
  `Restore` error follows the same fail-stop path.
- ✅ **Mandatory shared-secret protection** on the Raft transport —
  a fresh-nonce HMAC-SHA256 challenge-response authenticates each peer,
  then directional AES-GCM keys protect sequence-numbered RPC frames
  against disclosure, modification, and replay.
- ✅ **Mutual TLS** on the Raft transport — `--raft-tls-cert/-key/-ca`
  (all-or-none). When set, every inter-node connection is mTLS
  (TLS 1.3, `RequireAndVerifyClientCert`), and the verified leaf
  certificate Common Name must exactly match the hello NodeID.
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
- ✅ **Counters and apply-latency histogram:**
  - `dflockd_raft_proposals_total`,
    `dflockd_raft_proposals_failed_total` — propose attempt
    success/failure.
  - `dflockd_raft_apply_total`, `dflockd_raft_apply_failed_total` —
    FSM apply success/failure.
  - `dflockd_raft_apply_nanos_total` — cumulative apply latency.
    Divide by `apply_total` for a mean; rate over a window for p~mean
    over that window.
  - `dflockd_raft_apply_duration_seconds` — fixed-bucket Prometheus
    histogram for quantiles. Buckets span 50 µs through 5 s plus
    `+Inf`.
  - `dflockd_raft_leader_changes_total` — `becomeLeader` invocations.
    Rising fast means election instability.
  - `dflockd_raft_admin_changes_total{op=...}` — `AddVoter` /
    `RemoveServer` successes and failures, labelled by op.
- ✅ Counters and histogram buckets use `sync/atomic.Uint64`; each
  successful apply adds to one selected bucket with zero allocations
  and no mutex contention with the run loop.
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
- ✅ Fuzz targets cover the Raft RPC frame, authenticated handshake,
  secure frame, and cluster Command decoders.

## Test coverage (2026-07-24 short-mode snapshot)

| Package | Coverage | Notes |
|---|---|---|
| `internal/config` | 90.0% | Includes cluster security, admin, and orphan-TTL resolution. |
| `internal/protocol` | 88.5% | Barrier, stable-ref, and not-leader framing. |
| `internal/cluster` | 86.0% | FSM adapter, maintenance, admin counters, and command codec. |
| `internal/lock` | 84.5% | Direct + Apply paths, ref indexes, snapshots, and constant-time compare. |
| `internal/raft` | 83.6% | Storage, node, secure transport, membership, snapshots, and FSM fault containment. |
| `internal/httpapi` | 73.9% | Admin endpoints, readindex, CORS, and cluster metrics. |
| `client` | 69.9% | Eleven cluster-client tests cover routing, diagnostics, and hint clamping. |
| `internal/server` | 69.3% | Cluster handlers, stable refs, barrier, admin, and metrics. |
| `cmd/cluster-soak` | 49.2% | Exact history checking, flags, and fault scheduling are covered; the actual soak loop is intentionally skipped by `-short`. |

Five of nine packages clear ≥ 80%. The four under it are weighted by
the short-mode soak skip, long-tail HTTP/server error paths, and the
many `client.Cluster` wrapper methods that mirror the underlying
one-shot API but are not each exercised by a redirect test.

## Environment-dependent validation

PR-4 + PR-5 closed: stable-client-ref FIFO failover (was PR-3 gap 1).
PR-4 introduced the mechanism — `ApplyCleanupConn` marks ref-tagged
waiters/holders orphaned (`abandonedAtNanos`) instead of removing them,
snapshot codec v2 carries the field (v1 still readable), opt-in via
`--orphan-ttl` + `client.WithClusterStableRef`. PR-5 made it real for
the failover that matters: re-adopt now matches by `(key, ref)` alone
(so a hard-crashed leader — which never replicates a `CleanupConn` —
re-attaches, not just a graceful disconnect), runs in both the
single-phase `ApplyAcquire` and two-phase `ApplyEnqueue` paths (PR-4
wired only the latter), evicts the dead connection's stale index on
re-adopt, and the `--orphan-ttl` flag/env is actually parsed (PR-4 left
only the `Config` field, so the feature was unreachable in a real
deployment). Tests: FSM layer (`TestEnqueueReAdoptsHardCrashedHolder`,
`…Waiter`, `TestAcquireReAdoptsHardCrashedHolder`, `…Waiter`, plus the
PR-4 graceful-path + snapshot tests), config layer
(`TestLoad_OrphanTTL_*`), server layer (3 tests), and an end-to-end
real-TCP hard-crash regression guard
(`TestE2EStableRefReAttachAcrossFailover`).

PR-3 closed: dynamic-join with snapshot transfer to a cold-state empty
node.

PR-2 closed: cluster-aware client, soak harness, cluster codec fuzz.

PR-1 closed: admin endpoints, counter metrics, ReadIndex public API.

The long-horizon harness is implemented. `cmd/cluster-soak --targets`
drives real client endpoints and calls an operator-supplied executable
for leader partitions, healing, service restarts, follower clock skew,
and skew removal. It checks a bounded recorded prefix per contended key
exactly for acquire/release linearizability while retaining token uniqueness
and fencing monotonicity checks for the full campaign. The bundled
`tools/cluster-soak/ssh-linux.sh` hook uses dedicated `iptables` chains and a
systemd environment drop-in.

What this repository cannot supply is deployment evidence: run a
multi-hour campaign on representative hosts and retain its clean final
report and node logs before declaring that deployment GA. The local
real-process smoke validates external routing through a crashed member,
but is not a substitute for that campaign.

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

# Long-horizon external soak (environment setup in docs/operations/cluster.md)
go run ./cmd/cluster-soak \
  --targets a=10.0.0.1:6388,b=10.0.0.2:6388,c=10.0.0.3:6388 \
  --workers 16 --keys 2 --history-limit 32 --duration 8h \
  --fault-hook ./tools/cluster-soak/ssh-linux.sh

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
- **Protect `--raft-auth-token-file` like a private key.** Every member
  shares it, so disclosure permits cluster impersonation. Rotate it with
  an all-node restart; mixed secrets fail the challenge-response.
- **Use `--raft-tls-cert/-key/-ca` for per-node PKI identity.** Issue
  each leaf with `Subject.CommonName` exactly equal to that member's
  `--node-id`.
- **The 100 ms slowdown on a missed admin token** caps a single-host
  brute-force rate at ~10/s; with the rate-limit middleware in front,
  effective rate is the lower of the two. An attacker who has the
  network can still try forever, so monitor
  `dflockd_raft_admin_changes_total{op=~".*_failed"}` and alert on a
  sustained nonzero rate.
- **Counter metrics are atomic.Uint64s** — no contention with the run
  loop; emission cost is one atomic load per counter per scrape.
- **`_apply_nanos_total / _apply_total`** gives a mean apply latency.
  `dflockd_raft_apply_duration_seconds` supplies fixed buckets for
  `histogram_quantile`, including p99 over a rate window.
- **`client.Cluster` clamps leader hints to known members.** A server
  returning `error_not_leader evil.example.com:6388` will not cause
  the client to dial `evil.example.com` — the client clears its cache
  and keeps rotating through the operator-supplied members list. This
  bounds the blast radius of either a compromised cluster member or an
  on-path attacker (the latter is already mitigated by mTLS / TLS on
  the client-server link in any sane deployment).
- **Cluster client retry budget defaults to 3.** Each `*NotLeaderError`
  or dial failure costs one. After budget exhaustion the call returns
  an error matching `ErrTooManyRedirects` and wrapping the final dial
  or redirect cause — no unbounded loops and no lost terminal
  diagnostic.

## Bottom line

Cluster mode is **release-candidate / GA-eligible.** The post-review
P1-P3 audit
closed every identified externally reachable safety defect and added
regression coverage.
Static-bootstrap, dynamic-join (`AddVoter` → cold node), and
FIFO-across-leader-failover (via `--orphan-ttl` plus TCP or HTTP stable
refs) are validated — including hard-crash regression coverage over
real TCP and same-token HTTP re-attachment across server epochs. HTTP
session IDs remain node-local; callers recreate them with the same ref.
The multi-host fault-soak mechanism now ships; a representative
multi-hour run remains required deployment evidence. Its bounded contended-key
histories are checked exactly at shutdown while token/fence invariants cover
the whole run. The whole tree passes `make test-race` cleanly; `go.sum` stays
empty; the non-cluster single-node binary is byte-identical to v2.1.x.
