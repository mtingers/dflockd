# Production readiness - cluster mode

This document records the current release posture after remediation of
[`PRODUCTION_AUDIT_2026-07-25.md`](PRODUCTION_AUDIT_2026-07-25.md).
The original audit applies to revision `3e3cf5b`; it is retained as the
finding record.

## Posture

All code findings from that audit have remediations and regression tests.
Repository-local release gates must be clean before merge. A production
deployment still requires the environment-dependent qualification below.

Do not describe any distributed system as "bug-free." The supportable claim
after clean gates is narrower: no open finding from the 2026-07-25 audit and no
known failure in the exercised test matrix.

## Supported envelope

- Use an odd cluster of three or five voting members on local filesystems that
  support `flock(2)`.
- Every Raft connection requires the shared high-entropy authentication secret.
- Static bootstrap may use shared-secret-only Raft transport.
- Runtime `AddVoter` and `RemoveServer` require Raft mutual TLS
  (`--raft-tls-cert`, `--raft-tls-key`, and `--raft-tls-ca`). A common secret
  alone cannot provide revocable per-node identity.
- Protect administrative endpoints with `--admin-token`; they are disabled
  when it is absent.
- Use `client.Cluster` for Go clients. It owns a persistent logical session
  over two connections (a blocking session lane and a non-blocking control
  lane, so renew/release cannot be starved by a blocked acquire), generates a
  random stable identity, restores TLS/auth/identity after failover, and must
  be closed when the logical session ends. Budget up to two connections per
  client in `--max-conns` / `--max-conns-per-ip`.
- Low-level TCP and HTTP clients need an explicit stable ref to preserve
  holder/waiter identity across failover. `OrphanTTL` controls retention after
  graceful disconnect; hard-failover reattachment does not require it.
- Monitor `/ready`. In cluster mode it requires a running local voter but not
  leadership. Fatal Raft failure, local removal, or shutdown detach makes it
  return `not_ready`.

## Closed audit areas

- Membership changes are serialized; conflict truncation and snapshots derive
  and adopt the configuration effective at the exact log index.
- Inbound Raft RPCs are authorized against effective membership before term
  changes, with transport identity bound to message IDs.
- Self-removal stops leadership and voter readiness.
- WAL and HardState recovery fail closed when committed or election state is
  unavailable; only provably uncommitted torn WAL tails are truncated.
- FSM policy is versioned, replicated, and snapshotted. Cluster cleanup honors
  `AutoReleaseOnDisconnect`.
- HTTP cancellation cleanup and ambiguous grants are reconciled through
  idempotent Raft `Cancel` and `Attach` commands.
- `client.Cluster` is a persistent logical session with generated stable
  identity, public TLS configuration, and separate blocking/non-blocking
  lanes so a held lease can be renewed while another key is being waited on.
- Pre-authentication frames and handshake concurrency are bounded. Transport
  close/removal cannot publish a stale in-flight dial.
- Snapshot capture, persistence, receive, and wire encoding share a derived
  safe payload budget.
- AppendEntries batches are bounded by count and encoded bytes.
- Raft lifecycle transitions, fatal supervision, process exit, and readiness
  are explicit.
- Connection IDs use a fixed randomized 24-bit process tag plus a 40-bit
  monotonic counter with explicit exhaustion failure.
- Lock snapshots deep-copy mutable state under shard locks.
- Dynamic client-facing member metadata is replicated with membership and
  persists through snapshot/restart.
- The repository selects Go 1.26.5 and CI/release run `govulncheck`.

The item-by-item evidence and final command output belong in the remediation
report generated with this change.

## Repository gates

Run from a clean checkout:

```bash
go test ./... -p=2 -count=1 -timeout=240s
go test ./... -race -p=2 -count=1 -timeout=240s
go test ./... -cover -count=1 -timeout=240s
go vet ./...
govulncheck ./...
make docs-build
go run ./tools/complexity -prod -top 40
bash tools/cluster-smoke/smoke.sh
DFLOCKD_SMOKE_TLS=1 bash tools/cluster-smoke/smoke.sh
```

Run every fuzz target for at least the release campaign duration. The short
developer gate may use 10 seconds per target; retain the logs from the longer
release run.

## Deployment qualification

Before declaring a target environment production-ready, retain a clean 24-72
hour three- and five-node campaign covering:

- default and non-default replicated FSM policies,
- Raft mTLS plus client authentication/TLS,
- repeated leader kill, restart, partition, and heal,
- add/remove membership under load,
- compacted follower and cold-node snapshot catch-up,
- disk-full, torn-write, WAL/HardState corruption, and restart,
- cancellation, reconnect, and session churn,
- bounded complete histories checked for linearizability, and
- token uniqueness plus per-key fencing monotonicity.

The repository includes `cmd/cluster-soak --targets` and
`tools/cluster-soak/ssh-linux.sh` for the network/process-clock portion.
Storage fault injection and representative filesystem behavior must be
validated on disposable hosts matching the deployment class.

## Release gate

A production release requires:

- zero open P0/P1 findings,
- zero reachable known vulnerabilities,
- no unexplained unit, race, fuzz, smoke, soak, corruption, or
  linearizability failure,
- fail-closed recovery for unavailable committed state,
- passing public-client and administrative end-to-end workflows, and
- runbooks that match the supported security and recovery envelope.

Repository-local success alone is necessary but not sufficient evidence for
the environment-dependent campaign.
