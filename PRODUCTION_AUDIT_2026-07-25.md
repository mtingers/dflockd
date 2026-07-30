# Production Audit - 2026-07-25

## Verdict

**Cluster mode is not production ready. Do not describe this revision as
bug-free or safe for production deployment.**

The normal test, race, fuzz, documentation, complexity, and three-node smoke
paths are healthy. The review nevertheless found multiple reachable safety
failures in membership handling, snapshot configuration, cancellation cleanup,
durable recovery, and the public cluster client. Several can produce divergent
replicas, overlapping lock ownership, an invalid leader, or silent loss of
committed state.

This audit was read-only except for this report. No production fixes were made.

Audit target:

- Branch: `raft-replication`
- Revision: `3e3cf5b` (`test(cluster): remove obsolete import shims`)
- Untracked user files were excluded from the review.

Severity:

- **P0:** Can violate consensus, replicated-state, lock ownership, or committed
  durability guarantees through a supported workflow or plausible fault.
- **P1:** Release blocker for security, lifecycle, operability, or documented
  behavior, without a demonstrated immediate consensus violation.
- **P2:** Important hardening or design debt with a narrower trigger.

## P0 Findings

### P0-01: Cluster HTTP cancellation mutates only the local FSM

Locations:

- `internal/httpapi/cluster.go:68-104`
- `internal/httpapi/handlers.go:415-457`
- `internal/httpapi/handlers.go:488-518`

The clustered acquire, enqueue, and wait handlers reuse render-time
cancellation cleanup written for the single-node lock manager. On cancellation,
`bestEffortRelease` calls `LockManager.Release` directly, and
`dequeueAfterPromote` calls `LockManager.Wait` directly. Neither operation is
proposed through Raft.

Impact:

- A grant can commit on all replicas and then be released only on the leader.
- Local promotion can mint fences or move queue state on only one replica.
- A new leader can still consider the old token held while the old leader has
  granted the same slot to another waiter.
- Cancellation before a proposal future returns can also leave an ambiguous
  committed operation with no token available to the cleanup caller.

Required correction:

- Split single-node and clustered cleanup paths.
- Represent cancel/dequeue/release cleanup as idempotent Raft commands.
- Give client operations stable request identities so a timed-out caller can
  reconcile an ambiguous proposal result.
- Add cancellation tests that compare serialized FSM state on every replica
  after acquire, enqueue, promotion, and wait races.

### P0-02: Overlapping membership changes are accepted

Locations:

- `internal/raft/node_propose.go:173-198`
- `internal/raft/node_propose.go:200-215`
- `internal/raft/node.go:58`
- `internal/raft/errors.go:21`

`cfgIndex` and `ErrConfigChangeInProgress` exist, but `onConfChange` does not
reject a new change while `cfgIndex > committed`. A second configuration entry
is appended and adopted while the first is still uncommitted.

Impact:

The single-server membership algorithm requires one configuration change at a
time. Overlapping changes can cause nodes to calculate quorums from different
intermediate voter sets and violate the safety assumptions of the algorithm.

Focused audit probe:

1. Start a one-voter leader.
2. Append an add-voter change that cannot commit under the new quorum.
3. Submit a second add-voter change.
4. Both configuration entries are appended and the second is adopted.

Required correction:

- Reject changes whenever an effective configuration entry is uncommitted.
- Reserve `ErrConfigChangeInProgress` for that condition; use a separate
  already-member error for no-op additions.
- Test concurrent admin calls, leader changes during a pending change, restart,
  and retry after commit.

### P0-03: An overwritten uncommitted configuration is not rolled back

Locations:

- `internal/raft/node_replication.go:198-225`
- `internal/raft/node.go:306-320`

When AppendEntries truncates a conflicting `EntryConfig`, the follower scans
only the newly appended entries. If the replacement batch contains normal
entries, the node retains the voter set and `cfgIndex` from the deleted
configuration until restart or some later configuration entry.

Impact:

The live node can run elections, authorize peers, and calculate commit quorums
from a configuration that no longer exists in its log.

Focused audit probe:

1. Recover a follower with an uncommitted configuration at index 1.
2. Replace index 1 with a normal entry from a higher-term leader.
3. The log is corrected, but the removed configuration remains active.

Required correction:

- After any suffix truncation that can remove `cfgIndex`, reconstruct the
  effective configuration from the latest surviving config entry or snapshot.
- Centralize `configurationAt(index)` and configuration-recovery logic.
- Add partitioned-leader overwrite tests with adds and removals.

### P0-04: Snapshot configuration does not match the snapshot index

Locations:

- `internal/raft/node_apply.go:100-131`
- `internal/raft/node_apply.go:145-160`
- `internal/raft/node_replication.go:272-293`
- `internal/raft/raftlog.go:165-170`
- `internal/raft/node.go:291-303`

There are two independent faults:

1. `shipApplyBatch` attaches `n.config`, the latest appended configuration, to
   a batch of committed entries. If a later configuration is uncommitted, a
   snapshot at an earlier applied index records that future configuration.
2. A live `InstallSnapshot` persists and restores the snapshot but never adopts
   `SnapshotMeta.Configuration`. Snapshot configuration is adopted only during
   process startup.

Impact:

- An uncommitted voter set can become durable snapshot metadata.
- A lagging follower caught up by snapshot keeps stale voters and transport
  peers until restart or a later config entry.
- Quorum, elections, peer authorization, and subsequent snapshot metadata can
  all be wrong.

Focused audit probe:

1. Install a snapshot whose voters differ from the follower's current voters.
2. The snapshot index and data advance.
3. The follower's in-memory voter set remains unchanged.

Required correction:

- Derive snapshot metadata from the configuration effective at exactly the
  snapshot's last included index.
- Atomically adopt and synchronize the installed snapshot configuration before
  acknowledging installation.
- Test snapshots before, at, and after add/remove entries, including restart and
  live catch-up.

### P0-05: Unknown or removed peers can disrupt and participate in Raft

Locations:

- `internal/raft/tcptransport.go:219-250`
- `internal/raft/node_rpc.go:17-42`
- `internal/raft/node_election.go:54-104`
- `internal/raft/node_replication.go:177-182`
- `internal/raft/node_replication.go:272-276`

Transport authentication proves possession of the cluster secret and optional
TLS node identity, but it does not prove that the node is a current voter. The
Raft layer then:

- applies a higher term before checking membership,
- does not bind authenticated `from` to `CandidateID` or `LeaderID`,
- grants votes and pre-votes to non-voters, and
- accepts leader, snapshot, and timeout-now RPCs from non-voters.

Impact:

A removed node that still possesses its secret or certificate can repeatedly
raise terms, depose leaders, suppress elections, request votes, or impersonate
another message-level node ID. This can prevent removal from taking effect and
can invalidate membership safety.

Focused audit probe:

An AppendEntries message at a higher term from an ID absent from the voter set
immediately changes a leader into a follower and raises its durable term.

Required correction:

- Authorize inbound requests against the effective voter set before term or
  election state changes.
- Pass authenticated `from` into vote and timeout handlers.
- Require `from == CandidateID` and `from == LeaderID` where applicable.
- Define and test the exact bootstrap rule for a newly added voter.
- Test removed-node traffic under partitions, stale connections, and retained
  credentials.

### P0-06: A leader that removes itself does not step down

Locations:

- `internal/raft/node_propose.go:218-259`
- `internal/cluster/node.go:393-407`
- `internal/server/cluster.go:42-44`
- `docs/operations/cluster.md:120-128`

The documented contract says a self-removing leader steps down once the entry
commits. `adoptConfig` only updates the voter set, peer transport, and progress.
There is no committed-removal check that changes the leader's role.

Impact:

The removed process can remain leader, continue accepting proposals, and keep
the remaining voters' election timers reset. Because inbound leader RPCs are
not membership-authorized, the invalid leadership can persist.

Required correction:

- Detect commit of a configuration that excludes the local ID.
- Resolve the configuration future, publish non-leadership, and transition to a
  non-campaigning removed state.
- Reject all later proposals and leader RPC emission from a removed node.
- Test self-removal with healthy peers, a partition, restart, and an active
  client workload.

### P0-07: Corrupt committed WAL tails are silently discarded

Locations:

- `internal/raft/wal.go:36-52`
- `internal/raft/wal.go:73-113`
- `internal/raft/storage_file.go:92-102`
- `internal/raft/raftlog.go:16-34`

Every malformed, truncated, or CRC-invalid WAL suffix is treated as an
uncommitted torn tail and truncated before HardState is cross-checked. Later,
`newRaftLog` clamps a durable `CommitIndex` down to the now-shorter log.

Impact:

If a record at or below the durable commit index is corrupted, startup silently
forgets committed commands. In this lock service that can resurrect or remove
ownership incorrectly and permit duplicate critical-section execution.

Required correction:

- Load recovery metadata without mutating files first.
- Classify a bad suffix using the snapshot index and durable commit index.
- Permit truncation only when every discarded index is provably uncommitted.
- Refuse startup when committed state is unavailable or internally
  inconsistent.
- Add byte-level corruption tests at committed, uncommitted, and compacted
  indices.

### P0-08: Total HardState corruption resets term and vote to zero

Locations:

- `internal/raft/hardstate.go:30-70`

When neither journal slot validates, `bestHardStateSlot` returns zero
HardState. The loader does not distinguish a new all-zero file from a
previously written file with two corrupt slots.

Impact:

A node with an existing WAL or snapshot can restart at term zero with no vote.
It may vote again in a term in which it already voted, violating a core Raft
persistence invariant.

Required correction:

- Accept zero state only for a genuinely new all-zero journal.
- Fail closed when nonzero journal bytes exist but neither slot validates.
- Cross-check recovered term against snapshot and WAL terms.
- Add corruption matrices for each slot, both slots, truncation, and stale
  sequence numbers.

### P0-09: Replicated FSM behavior depends on unchecked local flags

Locations:

- `internal/lock/apply.go:183`
- `internal/lock/apply.go:443`
- `internal/lock/apply.go:527`
- `internal/lock/apply.go:593`
- `internal/lock/apply.go:617`
- `internal/lock/apply.go:655`
- `internal/lock/lock.go:372-375`
- `docs/operations/cluster.md:237-241`

Replicated apply logic reads node-local `OrphanTTL`, `GCMaxIdleTime`,
`MaxLocks`, and `MaxWaiters`. Only `OrphanTTL` is documented as requiring an
identical value, and no startup, join, or transport check enforces equality.

Impact:

The same committed command can be accepted on one replica and rejected on
another, or can orphan, remove, garbage-collect, and promote different state.
Fence counters can then diverge. This is a state-machine determinism failure
caused by an ordinary configuration mismatch.

Required correction:

- Make FSM policy part of replicated, versioned cluster state, or exchange and
  reject a policy fingerprint before a node can vote or apply.
- Persist the policy in snapshots.
- Define which settings are immutable and how they are changed.
- Run differential tests that intentionally start replicas with different
  values and require a fail-fast error rather than divergent state.

### P0-10: The public `client.Cluster` destroys its own lock session

Locations:

- `client/cluster_client.go:89-100`
- `client/cluster_client.go:139-165`
- `client/cluster_client.go:198-215`
- `internal/server/conn.go:75-103`
- `internal/lock/apply.go:398-417`
- `docs/operations/cluster.md:301-313`

Every `client.Cluster` operation dials a fresh connection and closes it before
returning. In cluster mode, the leader proposes `CleanupConn` when that
connection closes.

With defaults (`OrphanTTL=0`, no stable ref), `Cluster.Acquire` can return a
token and then immediately cause that token to be released. A later
`Cluster.Release` runs on another connection and can report not-held.
`Enqueue` and `Wait` likewise cannot preserve their connection identity.

A stable ref plus nonzero `OrphanTTL` only delays the failure. `ApplyRenew`
extends the lease but does not clear the orphan timestamp, and the fresh
connection is closed again, so repeated `Cluster.Renew` cannot keep a session
alive past the orphan deadline.

The documented cluster-client example omits a stable ref and shows the broken
Acquire/use/Release lifecycle.

Required correction:

- Redesign the high-level client around a persistent logical session.
- Reuse its connection for acquire/renew/release and enqueue/wait.
- On failover, reconnect and explicitly reattach a cryptographically random
  stable identity before continuing.
- Return a session or lease handle that owns reconnection, renewal, and close
  semantics.
- Add end-to-end tests using default settings, failover, reconnect, concurrent
  operations, and leases longer than `OrphanTTL`.

## P1 Findings

### P1-01: Cluster cleanup ignores `AutoReleaseOnDisconnect=false`

Locations:

- `internal/lock/lock.go:796-820`
- `internal/lock/apply.go:398-417`
- `internal/lock/apply.go:488-532`
- `docs/server.md:17`

Single-node cleanup releases held slots only when the flag is enabled.
Replicated `ApplyCleanupConn` always drops non-stable holders or orphans stable
holders, regardless of the flag.

The focused audit probe acquired a lock with auto-release disabled, applied a
replicated cleanup, and observed that the holder was removed.

Required correction:

- Either replicate and honor the flag in `ApplyCleanupConn`, or reject the
  unsupported setting in cluster mode and update the contract.
- Add parity tests that run the same disconnect sequence in both modes.

### P1-02: `client.Cluster` cannot connect to TLS-enabled client ports

Locations:

- `client/cluster_client.go:29-35`
- `client/cluster_client.go:83-87`

The failover-aware client exposes auth and stable-ref options, but no exported
TLS configuration or dial option. Its production dialer is plaintext. The
low-level client has TLS support, but callers cannot combine it with
`client.Cluster` without reimplementing failover.

Required correction:

- Add an exported TLS option with correct server-name and root handling.
- Keep arbitrary dial injection test-only.
- Exercise the public cluster client in the mTLS smoke path.

### P1-03: An unauthenticated Raft connection can allocate 64 MiB

Locations:

- `internal/raft/tcptransport.go:219-250`
- `internal/raft/tcpframe.go:120-145`

The server reads a generic frame for the client hello before validating the
cluster proof. The generic reader permits and allocates up to 64 MiB. Accept
handling also has no explicit handshake concurrency bound.

Impact:

An unauthenticated network peer can open many connections, send a four-byte
large-frame length, and force large allocations until handshake deadlines
expire.

Required correction:

- Use a small hello-specific frame cap before authentication.
- Bound concurrent handshakes and accepted connections.
- Add allocation and slow-client denial-of-service tests.

### P1-04: Transport close/removal races an in-progress dial

Locations:

- `internal/raft/tcptransport.go:137-164`
- `internal/raft/tcptransport.go:346-404`

The closed/address checks happen before the blocking dial and handshake.
`dialFresh` can publish an outbound connection and call `wg.Add(1)` after
`Close` has ranged the map and begun or completed `wg.Wait`. `RemovePeer` can
also be undone by a dial already in progress.

Required correction:

- Serialize close, peer removal, and outbound publication.
- Recheck closed state and peer generation after handshake, before publication.
- Ensure all positive WaitGroup adds happen before the close path can wait.
- Add blocked-dial race tests under `-race`.

### P1-05: Snapshot size configuration is not enforced

Locations:

- `internal/raft/config.go:34-37`
- `internal/raft/config.go:123-127`
- `internal/raft/storage_file.go:223-246`
- `internal/raft/tcpcodec.go:105-118`
- `internal/raft/tcpframe.go:32-34`

`MaxSnapshotBytes` is validated but never used at capture, storage, encode, or
receive time. Independent storage and transport limits are both 64 MiB, but a
64 MiB snapshot cannot fit inside a 64 MiB secure RPC after metadata and
encryption overhead.

Impact:

A snapshot can be stored successfully but can never be transferred, leaving a
compacted, lagging follower unable to catch up.

Required correction:

- Define one effective payload budget derived from configured transport
  overhead.
- Enforce it before durable snapshot publication and on receive.
- Test exact-boundary and over-boundary snapshots.

### P1-06: Fatal Raft failure is not supervised or reflected in readiness

Locations:

- `internal/raft/node.go:595-612`
- `cmd/dflockd/main.go:191-205`
- `internal/httpapi/handlers.go:32-42`

HardState/storage failures stop the Raft node, but the process supervisor waits
only for TCP and HTTP servers. `/ready` checks only the draining flag and
`/health` always returns OK.

Impact:

A process can stay alive, pass readiness checks, and receive traffic after its
consensus engine has permanently stopped.

Required correction:

- Expose node termination and fatal cause to the process supervisor.
- Cancel servers and exit nonzero on unexpected node termination.
- Make readiness require a running node and a valid local cluster role; document
  whether leadership is required.
- Add injected storage/FSM failure process tests.

### P1-07: The selected Go toolchain has reachable vulnerabilities

Location:

- `go.mod:3-5`

`govulncheck ./...` found three reachable standard-library vulnerabilities with
the selected `go1.26.3` toolchain:

- `GO-2026-5856` in `crypto/tls`, fixed in Go 1.26.5.
- `GO-2026-5039` in `net/textproto`, fixed in Go 1.26.4.
- `GO-2026-5037` in `crypto/x509`, fixed in Go 1.26.4.

Required correction:

- Update the toolchain to at least Go 1.26.5.
- Rerun the full test, race, smoke, and vulnerability gates.
- Make reachable vulnerability findings fail CI and release packaging.

## P2 Findings

### P2-01: AppendEntries batching is count-only

Locations:

- `internal/raft/config.go:25-27`
- `internal/raft/codec.go:23-26`
- `internal/raft/tcpcodec.go:73-78`

Up to 256 entries are selected without a byte budget. Individual entries may be
16 MiB while one frame is 64 MiB. If a selected batch exceeds the codec limit,
the leader retries the same batch without reducing it.

Current dflockd commands are small, so this is primarily a liveness defect in
the Raft package contract.

Required correction:

- Batch by both count and encoded bytes.
- Retry an oversize batch with fewer entries.
- Reject a single entry that cannot fit before appending it.

### P2-02: `raft.Node` lifecycle is not state-guarded

Locations:

- `internal/raft/node.go:326-346`

Calling `Start` twice launches duplicate run, apply, and ticker goroutines.
Calling `Close` before `Start` blocks forever waiting for channels that no
goroutine can close. Public submissions before start wait until context expiry.

Required correction:

- Add explicit created/running/stopping/stopped states.
- Make invalid transitions return deterministic errors.
- Test close-before-start, duplicate start, concurrent close, and submission in
  every lifecycle state.

### P2-03: Cluster connection IDs wrap after 32 bits

Location:

- `internal/server/cluster.go:80-85`

The FSM identity combines a process epoch with only the low 32 bits of the local
connection counter. At 1,000 accepted connections per second, it wraps in about
49.7 days. A collision with a long-lived or orphaned identity can clean up or
re-adopt unrelated state.

Required correction:

- Allocate enough bits for the expected process lifetime and connection rate,
  or rotate epochs before exhaustion while proving no old IDs remain.
- Add an explicit exhaustion check rather than silent truncation.

### P2-04: Lock snapshots do not actually copy state under shard locks

Locations:

- `internal/lock/snapshot.go:76-115`
- `internal/cluster/fsm.go:113-116`

`collectSnapshotData` copies pointers to mutable resource and enqueued objects,
unlocks the shards, and serializes the pointed-to values afterward. This
contradicts the documented stable-copy contract.

The current Raft apply pipeline serializes snapshots synchronously on its sole
FSM writer, so the production path is protected by a stronger external
assumption. Direct concurrent use or future asynchronous capture would race and
produce an inconsistent image.

Required correction:

- Deep-copy snapshot DTOs while holding each shard lock, or serialize each
  shard under lock into immutable buffers.
- Add concurrent mutation snapshot tests under `-race`.

### P2-05: Dynamic member client metadata is not replicated

Locations:

- `internal/cluster/node.go:43-50`
- `internal/cluster/node.go:378-405`
- `docs/operations/cluster.md:109-113`

Raft configuration entries carry only Raft addresses. The `clientAddr` supplied
to `AddVoter` is added only to the requesting leader's local map after commit.
Followers depend on static startup configuration and removal metadata is also
updated only on the requester.

Impact:

After runtime membership changes and failover, status and redirect information
can be missing or stale unless operators pre-provision future members on every
node.

Required correction:

- Replicate durable member metadata with the configuration, or explicitly
  remove runtime client metadata from the API and require a separately
  validated discovery system.
- Test redirect/status behavior after add, remove, leader change, and restart.

## Minor API and Documentation Debt

- `client.WithClusterStableRef` silently ignores invalid input
  (`client/cluster_client.go:65-80`). Construction should return validation
  errors instead of silently falling back to unsafe connection-derived
  identity.
- The comment at `internal/server/server.go:250-254` says cluster leader loops
  are stubbed although they are implemented.
- The external smoke/soak path uses stable refs and nonzero orphan TTL, which
  masks the default `client.Cluster` lifecycle failure.

## Validation Performed

Passed:

- `go test ./... -race -p=2 -count=1 -timeout=240s`
- `go test ./... -cover -count=1 -timeout=240s`
- `go vet ./...`
- `make docs-build` in strict mode
- `go run ./tools/complexity -prod -top 40`
- Plain three-node cluster smoke with redirect, persistent acquire/release,
  SIGKILL failover, and external soak
- The same cluster smoke with mTLS enabled
- All 12 fuzz targets across client, protocol, Raft, cluster, HTTP, and lock;
  approximately 1.2 million executions with no crash

Coverage snapshot:

| Package | Coverage |
|---|---:|
| `client` | 70.3% |
| `cmd/cluster-soak` | 69.6% |
| `cmd/dflockd` | 9.3% |
| `internal/cluster` | 87.0% |
| `internal/config` | 90.0% |
| `internal/httpapi` | 74.0% |
| `internal/lock` | 84.6% |
| `internal/protocol` | 91.7% |
| `internal/raft` | 84.0% |
| `internal/server` | 69.2% |

Failed:

- `govulncheck ./...` due to the three reachable toolchain vulnerabilities
  listed in P1-07.

Focused audit probes in an isolated temporary copy confirmed:

- live snapshot installation retains stale voters,
- truncating a config entry retains that deleted configuration,
- a second uncommitted membership change is accepted,
- an unknown peer can raise the term and depose a leader, and
- replicated disconnect cleanup ignores disabled auto-release policy.

The focused probes were not added to the production tree because this pass was
requested as a report, not an implementation.

## Execution Plan

### Phase 0: Containment

1. Mark cluster mode experimental and block a production release from this
   revision.
2. Preserve the current passing test baseline.
3. Turn every P0 scenario above into a failing regression test before changing
   implementation.
4. Decide whether dynamic membership remains a supported release feature. If
   not, disable its admin endpoints until the membership work is complete.

Exit criteria:

- Every P0 has a deterministic failing test or fault-injection harness.
- No public documentation claims production readiness for cluster mode.

### Phase 1: Consensus Membership and Snapshot Invariants

Implement as one coherent Raft change:

1. Introduce a single source of truth for the configuration effective at a log
   index.
2. Reject overlapping configuration changes.
3. Restore configuration after conflict truncation.
4. Stamp snapshots with the configuration at the snapshot index.
5. Adopt configuration atomically during live snapshot installation.
6. Bind authenticated peer identity to message IDs and current membership.
7. Transition a committed self-removal into a removed, non-leader state.

Required test matrix:

- Add/remove with the old leader partitioned before append, after append, and
  before commit.
- Conflicting leader overwrites an uncommitted add and remove.
- Snapshot catch-up crosses both add and remove entries.
- Removed peer retains credentials and sends every RPC type.
- Self-removing leader under active proposals and failover.
- Restart at every configuration transition.

Exit criteria:

- No two live nodes derive different effective voter sets for the same durable
  log/snapshot state.
- A removed node cannot affect term, votes, leadership, or commit.
- Model/history tests show one-at-a-time configuration safety.

### Phase 2: Durable Recovery

1. Refactor FileStorage recovery into parse, validate, and mutate stages.
2. Cross-check snapshot index, WAL continuity, term, and durable commit index
   before truncating anything.
3. Fail closed on unavailable committed records.
4. Fail closed when both nonempty HardState slots are invalid.
5. Add fault injection around every write, fsync, rename, and directory fsync.

Exit criteria:

- Every single-byte corruption in committed WAL/HardState data either recovers
  from a valid redundant copy or prevents startup.
- Only provably uncommitted torn tails are truncated.
- Crash tests preserve every acknowledged lock command.

### Phase 3: Deterministic Lock FSM and Client Semantics

1. Make cluster FSM policy replicated or strictly compatibility-checked.
2. Route all clustered cancellation cleanup through Raft.
3. Define and implement cluster behavior for
   `AutoReleaseOnDisconnect=false`.
4. Replace per-operation `client.Cluster` connections with a durable logical
   session and explicit lease ownership.
5. Add public TLS configuration to the high-level client.
6. Correct the cluster-client documentation after the API is safe.

Exit criteria:

- Byte-identical snapshots and results on replicas under every supported
  configuration.
- Acquire/renew/release and enqueue/wait survive redirect and leader failure
  without duplicate ownership or lost queue position.
- Default high-level client usage is valid without hidden flag prerequisites.
- Public client works against the mTLS smoke cluster.

### Phase 4: Transport, Supervision, and Limits

1. Cap pre-authentication frames and handshake concurrency.
2. Fix dial/close/remove publication synchronization.
3. Enforce one derived snapshot payload budget end to end.
4. Batch AppendEntries by encoded bytes and count.
5. Supervise unexpected Raft termination and make readiness truthful.
6. Guard Node lifecycle transitions and connection-ID exhaustion.
7. Deep-copy lock snapshot data.

Exit criteria:

- Close/remove race tests pass repeatedly with `-race`.
- Allocation tests bound unauthenticated memory.
- Boundary-size snapshots transfer successfully or are rejected before
  publication.
- Any fatal consensus failure removes readiness and exits nonzero.

### Phase 5: Release Qualification

1. Upgrade to Go 1.26.5 or newer and require a clean `govulncheck`.
2. Run full unit, race, vet, docs, complexity, and fuzz gates.
3. Add a 24-72 hour three- and five-node soak with:
   - default and non-default FSM policies,
   - mTLS and auth,
   - leader kill/restart loops rather than permanent shrink,
   - membership add/remove during load,
   - snapshot catch-up,
   - disk-full, torn-write, and corruption injection,
   - client cancellation and reconnect storms, and
   - an independent linearizability checker with complete histories.
4. Review every remaining P1 and either fix it or document a narrowly justified
   release exception.

Production release gate:

- Zero open P0 findings.
- Zero reachable known vulnerabilities.
- No unexplained race, fuzz, soak, corruption, or linearizability failure.
- Recovery fails closed for unavailable committed state.
- Cluster client and administrative workflows pass public-API end-to-end tests.
- Runbooks describe supported configuration, recovery, replacement, and
  membership procedures without relying on undocumented invariants.

## Final Assessment

The implementation has a strong test baseline and many good defensive details,
but the currently passing suite does not exercise several critical distributed
state transitions. The P0 issues are architectural correctness failures, not
polish items. Production qualification should resume only after Phases 1-3 are
complete and the new failure tests pass.
