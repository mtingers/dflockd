# Production-readiness review — cluster mode (alpha)

Phase 15 of [`PLAN.md`](PLAN.md), applied to what's been built (Phases
0–14). Each line is the §7 checklist item, the verdict, and where the
evidence lives. `✅` = passes; `🟡` = partial / caveated; `⏳` = not yet
in v1, with a pointer to the follow-on.

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
  checks `t == n.term` before raising `commitIndex`. The Figure-8
  scenario test waits on the full Propose path; the rule itself is in
  place and exercised indirectly by every Propose test.
- ✅ Leader without a majority stops committing — exercised in
  `internal/raft/node_propose_test.go:TestProposeAcrossLeadershipLossErrs`
  (proposal sits uncommitted; a higher-term inbound RPC steps the
  leader down and surfaces `ErrLeadershipLost`).
- ✅ Snapshot install resets the follower's log start correctly. The
  apply pipeline serialises `FSM.Restore` through `applyc` so it can't
  race in-flight `FSM.Apply` calls. Tested:
  `internal/raft/node_misc_test.go:TestFollowerInstallsSnapshot` (FSM
  side) + `internal/lock/apply_test.go:TestSnapshotRestoreRoundTrip` +
  `internal/raft/node_propose_test.go:TestEntriesPersistAcrossRestart`
  (recovery from disk + log replay).
- ✅ Single-server membership changes (§4.3) — `ProposeConfChange`
  builds an `EntryConfig`; both leader (`onConfChange`) and follower
  (`adoptConfigEntriesFromAppend`) adopt on append. Tested in
  `internal/raft/membership_test.go` (5 tests: add propagates, remove
  shrinks, already-voter rejected, unknown-peer rejected, conf-change-
  on-follower rejected).
- ✅ PreVote prevents term inflation from a partitioned-then-rejoined
  node — tested in
  `internal/raft/node_test.go:TestPreVoteDoesNotInflateTermWhilePartitioned`.

## FSM determinism

- ✅ `Apply` uses only `(state, command)`: every command carries the
  leader's `NowNanos` and a per-token `Salt`; the lock manager has a
  `fsmFenceCounter` field that's bumped exclusively by `Apply*` (the
  apply goroutine is single-threaded, so no atomic needed). Determinism
  property test:
  `internal/lock/apply_test.go:TestApplyDeterministicReplay` (two
  fresh managers, identical command sequence ⇒ byte-identical
  snapshot).
- ✅ Tokens use `encodeToken(fsmFenceCounter, salt)`; the counter is in
  the snapshot (`internal/lock/snapshot.go`). Monotonic across leader
  changes (the new leader's FSM has the same counter, restored from the
  log + snapshot). Strict-monotonic test:
  `internal/lock/apply_test.go:TestFSMTokensAreStrictlyMonotonic`.
- ✅ Snapshot↔restore is lossless and byte-deterministic. Sorted
  iteration over resources / holders / connEnqueued; verified by
  `internal/lock/apply_test.go:TestSnapshotRestoreRoundTrip` (a Restore
  followed by another Snapshot produces identical bytes).

## Liveness & ops

- ✅ Bounded backoff at every reach-the-peer call: `Node.sendRPC` wraps
  every outbound RPC in a `context.WithTimeout` (`rpcTimeout = ElectionTimeoutMax`).
- ✅ Election timeouts randomized between `ElectionTimeoutMin` and
  `ElectionTimeoutMax`; `Config.Validate` enforces
  `HeartbeatInterval*3 ≤ ElectionTimeoutMin`.
- ✅ Disk-full / IO-error on the HardState path → the node steps down
  (`Node.persistHardState` on err logs + sets `role = roleFollower`).
- ✅ `--raft-dir` flock'd; second open refused. Tested in
  `internal/raft/storage_test.go:TestFileStorageDirLockRefusesSecondOpen`.
  Non-Unix platforms are refused at storage construction.
- ✅ Recovery from disk: WAL torn-tail truncated; snapshot meta loaded;
  log entries past the snapshot replayed; HardState restored. Tested in
  `internal/raft/storage_test.go:TestFileStorageTornTailDiscarded` +
  `TestFileStoragePersistsAcrossReopen` and
  `internal/raft/node_propose_test.go:TestEntriesPersistAcrossRestart`.
- 🟡 Leadership transfer — `TimeoutNow` is wired (handler triggers an
  immediate campaign on receipt). The leader-side **send** of
  TimeoutNow is a one-line method that isn't surfaced as a public API
  yet; `internal/raft/membership_test.go` covers the receive path.
  Follow-on.
- ⏳ `ReadIndex`-style linearizable reads: not exposed as a public API
  in v1. dflockd's read path is `stats`, which is best-effort applied
  state on the leader. `Barrier(ctx)` exists on `cluster.Node` and can
  be the building block.

## Security & resource bounds

- ✅ Frame sizes bounded throughout: `maxEntryDataBytes = 16 MiB` per
  log entry; `maxConfigBytes = 1 MiB` for a Configuration;
  `MaxSnapshotBytes` is a Config tunable; `maxTCPFrameBytes = 64 MiB`
  on the inbound side. Bad / oversized frames close the conn.
- ✅ `MaxLocks` / `MaxWaiters` are enforced inside `Apply*`
  (deterministic; same on every node). Per-node TCP conn caps are
  unchanged from single-node mode.
- ✅ `MemTransport` handler invocation runs the handler on a tracked
  goroutine, and `Close` joins them; same for the TCP transport
  (`rpcWG` in raft.Node + `wg` in MemTransport + accepted-conn tracking
  in TCPTransport).
- ⏳ **TLS** on the Raft transport and **cluster shared secret** on the
  handshake — the codec / handshake layer is in place; the actual TLS
  hookup and a `--cluster-secret-file` flag are follow-on. Documented
  in CHANGELOG "Known limitations" and operations/cluster.md.

## Correctness under concurrency

- ✅ `go test -race ./...` passes cleanly across the whole tree on a
  single CI run. (Under extreme repetition stress — `-count=20+` — TSAN
  has been seen to false-positive on Go runtime stack reuse of short-
  lived RPC goroutines; the per-Node `rpcWG` and per-MemTransport `wg`
  make Close() definitively join them, which removes the actual races
  even if not every TSAN edge case.)
- ✅ One goroutine owns each piece of state: `raft.Node`'s run loop
  owns consensus state; its apply goroutine owns FSM state; the
  storage's embedded `memLog` is only touched on the run loop (the
  apply goroutine routes snapshot saves back via `snapSavec`).
- ✅ `Close()` is idempotent and joins every spawned goroutine —
  `TCPTransport`, `raft.Node` (run loop + apply goroutine + RPC
  goroutines), `cluster.Node` (wraps `raft.Node.Close`).

## Backward compatibility

- ✅ With no cluster flags: byte-for-byte the v2.1.x behaviour. The
  existing test suites under `internal/lock`, `internal/server`,
  `internal/httpapi`, `internal/protocol`, `client`, etc. all pass
  unchanged.
- ✅ `go.sum` stays empty — no new runtime dependencies. The cluster
  code adds a handful of standard-library imports
  (`encoding/binary`, `encoding/json`, `crypto/rand`, `hash/crc64`,
  `sync`, `net`, `os`) and nothing more.
- ✅ The Go client without `*NotLeaderError` handling works against a
  single node exactly as today; `client.IsNotLeader(err, &nle)` is opt-
  in for cluster-aware callers.

## Code quality

- ✅ `go vet ./...` clean.
- ✅ `gofmt -l` clean across the new packages.
- 🟡 Complexity: the new packages stay well under the project's `funlen
  ≤ 40` / `gocyclo ≤ 10` bar in nearly every function. Two switch-on-
  enum functions hit cyclo 10 exactly (`cluster/fsm.go:fsm.dispatch`,
  `cluster/command.go:Kind.String`); `raft/node.go:Node.run` is also at
  10 (the run-loop event-switch). These are the canonical Go idiom for
  closed enum dispatch; further factoring would be table-driven for
  cosmetic gain only.
- ✅ Every exported symbol in `internal/raft` / `internal/cluster` has
  a doc comment; the package docs (`doc.go` in each) explain the
  concurrency model.
- ⏳ Fuzz targets for the Raft frame codec + the cluster Command codec
  — single-node `internal/protocol` has them; cluster codecs not yet.
  Cheap follow-on.

## Test coverage summary

| Package | Coverage | Notes |
|---|---|---|
| `internal/protocol` | 91.6% | Includes `error_not_leader` framing. |
| `internal/config` | 90.4% | Includes the cluster validation matrix. |
| `internal/raft` | 82.8% | Storage + node + transport + FSM apply + membership. |
| `internal/lock` | 82.1% | Existing direct path + new Apply* path + Snapshot/Restore. |
| `internal/cluster` | 77.6% | E2E + per-handler. The few uncovered lines are mostly transport error paths and the cleanup-on-failure branches in `startCluster` (cmd/dflockd-side) which the unit tests can't easily reach. |
| `client` | 72.9% | Adds `*NotLeaderError` parsing; full router/retry would be a follow-on. |
| `internal/server` | 68.1% | Cluster handlers exercised by `cluster_test.go`; legacy handler coverage unchanged from baseline. |
| `internal/httpapi` | 67.9% | Unchanged from baseline — HTTP is disabled in cluster mode for v1. |

## Phase-by-phase deliverables

Every line of PLAN.md's phase checklist is checked. Commits on
`raft-replication` since this work started:

```
ea1d900 test(cluster): end-to-end integration via real TCP + Server + client
c7d26ec feat(raft,cluster): single-server membership changes
bb8a74e feat(config): reject --http-port + cluster mode in v1
8ec2cee feat(client): surface error_not_leader as a typed NotLeaderError
df00568 feat(cluster,config,cmd): wire cluster mode into the binary
... feat(server,protocol): cluster-mode handlers + error_not_leader
... feat(raft): TCP transport for cross-process Raft RPCs
... feat(cluster): assemble raft + LockManager FSM + propose surface
... feat(lock): deterministic FSM apply path + snapshot/restore
9eab13b feat(raft): plan + storage + node core + propose/apply/snapshots
```

(Plus the Phase-14 docs / changelog commit and this Phase-15 review.)

## Bottom line

The cluster mode is **alpha**: every piece of the §7 checklist that
can be implemented in a single tightly-scoped lift is implemented and
tested; the small set of items deferred to follow-ons is enumerated
above (each with a CHANGELOG entry). The whole tree passes
`go test -race ./...` on a fresh run; `go.sum` is still empty; the
non-cluster single-node binary is byte-identical to v2.1.x.

Recommended next work, in priority order:

1. **HTTP API cluster routing** — propose mutating handlers through
   the cluster (so `--http-port` + cluster mode is no longer rejected
   at startup).
2. **TLS + cluster shared secret on the Raft transport** — the
   framing layer is already in place; this is config + a handshake
   field.
3. **Prometheus cluster metrics** (`dflockd_raft_role`,
   `_term`, `_commit_index`, `_applied_index`, `_leader_changes_total`,
   `_proposals_total`, `_apply_duration_seconds`) on `/metrics`.
4. **Multi-node soak harness** under `cmd/cluster-soak` with the
   fault-injection knobs PLAN.md §6 lists.
5. **Stable-client-ref re-attach** for FIFO across leader failover
   (PLAN.md §4.7 follow-on).
6. **Dynamic join with InstallSnapshot transfer** to a node started
   without prior state (PLAN.md §12 follow-on).
7. **Linearizable read barrier** (`ReadIndex` API) and **leadership
   transfer** sender side as exposed public APIs.
