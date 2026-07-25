# Operating a dflockd cluster

This page covers bringing up a 3-node Raft cluster, growing / shrinking
it, and recovering from common failures. See
[architecture/cluster.md](../architecture/cluster.md) for the design.

## Required flags

| Flag | Description |
|---|---|
| `--raft-dir <path>` | Directory for the Raft log, snapshots, and HardState journal. The presence of this flag is what switches on cluster mode. Must be on a local filesystem; `flock(2)` is required. |
| `--node-id <id>` | Stable identifier for this node (any string; must be unique within the cluster and never reused). |
| `--raft-addr <host:port>` | This node's Raft transport bind (peer-to-peer consensus traffic). |
| `--cluster-peers <list>` | Every member of the cluster as `id=raftHost:raftPort@clientHost:clientPort,...`. Must include this node. |
| `--raft-auth-token-file <path>` | File containing the shared high-entropy cluster secret (minimum 32 bytes). Every member must use the same value. |
| `--advertise-addr <host:port>` (optional) | This node's client-facing address. Defaults to `--host:--port`. Clients connect here when redirected. |

`--http-port` works in cluster mode — see [HTTP API in cluster
mode](#http-api-in-cluster-mode) below.

### Securing the Raft transport

Raft always performs a replay-resistant HMAC challenge-response and
protects subsequent frames with sequence-numbered AES-GCM. Generate one
random secret and install the same root-readable file on every member:

```bash
umask 077
openssl rand -hex 32 > /etc/dflockd/raft-auth-token
```

`--raft-auth-token` and `DFLOCKD_RAFT_AUTH_TOKEN` are also supported,
but a file avoids exposing the secret in the process list.

For PKI-backed per-node identity in addition to the shared secret, give
every node a certificate signed by a shared CA and pass:

| Flag | Description |
|---|---|
| `--raft-tls-cert <pem>` | This node's certificate. Its Common Name must exactly equal `--node-id`. |
| `--raft-tls-key <pem>` | Its private key. |
| `--raft-tls-ca <pem>` | The CA bundle used to verify peers. |

All three must be set together (or all omitted). When set, every
connection is also mutual TLS (TLS 1.3, `RequireAndVerifyClientCert`).
The transport rejects a hello whose NodeID differs from the verified
leaf certificate's Common Name.

The authenticated wire format is `raft.v3`; upgrading from an older
release requires an all-node restart because mixed protocol versions
intentionally refuse the handshake.

## Bringing up a 3-node cluster

Pick stable node ids (`n1`, `n2`, `n3`), free Raft ports (`7001` /
`7002` / `7003`), and free client ports (`6388` / `6389` / `6390`).
On each host run dflockd with the same `--cluster-peers` list:

```bash
# Host A (n1)
dflockd \
  --node-id n1 \
  --raft-dir /var/lib/dflockd/n1 \
  --raft-addr 10.0.0.1:7001 \
  --raft-auth-token-file /etc/dflockd/raft-auth-token \
  --port 6388 --advertise-addr 10.0.0.1:6388 \
  --cluster-peers "n1=10.0.0.1:7001@10.0.0.1:6388,n2=10.0.0.2:7002@10.0.0.2:6389,n3=10.0.0.3:7003@10.0.0.3:6390"

# Host B (n2) — same --cluster-peers value
dflockd --node-id n2 --raft-dir /var/lib/dflockd/n2 \
        --raft-addr 10.0.0.2:7002 --raft-auth-token-file /etc/dflockd/raft-auth-token \
        --port 6389 --advertise-addr 10.0.0.2:6389 \
        --cluster-peers "n1=10.0.0.1:7001@10.0.0.1:6388,n2=10.0.0.2:7002@10.0.0.2:6389,n3=10.0.0.3:7003@10.0.0.3:6390"

# Host C (n3) — same again
dflockd --node-id n3 --raft-dir /var/lib/dflockd/n3 \
        --raft-addr 10.0.0.3:7003 --raft-auth-token-file /etc/dflockd/raft-auth-token \
        --port 6390 --advertise-addr 10.0.0.3:6390 \
        --cluster-peers "n1=10.0.0.1:7001@10.0.0.1:6388,n2=10.0.0.2:7002@10.0.0.2:6389,n3=10.0.0.3:7003@10.0.0.3:6390"
```

Within a couple of hundred milliseconds the cluster will elect a
leader. From a client:

```bash
$ nc 10.0.0.1:6388
l
deploy-job
60
ok 0001a3...b9 60      # if 10.0.0.1 is the leader
# OR:
error_not_leader 10.0.0.2:6389
# -> reconnect to that address and retry; the Go client does this
# automatically when you check for *client.NotLeaderError.
```

## Adding a node

Add a voting member via the leader's admin endpoint:

```bash
curl -X POST https://leader:6388/v1/admin/voters \
  -H "X-Dflockd-Admin: $DFLOCKD_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"node_id":"d","raft_addr":"10.0.0.4:7001","client_addr":"10.0.0.4:6388"}'
# → 200 {"status":"ok","node_id":"d"}
```

The leader proposes a `ConfigEntry`; the change takes effect immediately
on append, and the new node starts being counted toward quorum. The new
node must be **started** before it can apply incoming AppendEntries —
boot it with `--cluster-peers` listing every member (including itself)
so its membership view matches.

Requires the operator to have set `--admin-token` (or
`DFLOCKD_ADMIN_TOKEN`) on the leader; without it the endpoint returns
`503 admin_disabled`. On a follower the endpoint returns `503 not_leader`
with the leader's raft client address in `X-Dflockd-Leader`.

## Removing a node

```bash
curl -X DELETE https://leader:6388/v1/admin/voters/d \
  -H "X-Dflockd-Admin: $DFLOCKD_ADMIN_TOKEN"
# → 200 {"status":"ok","node_id":"d"}
```

A leader removing itself steps down once the entry commits.

## Linearizable reads

The `GET /v1/readindex` endpoint (HTTP) and the `barrier` TCP command
both propose a no-op through Raft and return only after it applies.
Use this as a "wait for the cluster to catch up" primitive before a
read that must reflect every preceding committed write:

```bash
# HTTP
curl https://leader:6388/v1/readindex
# → 200 {"status":"ok"}  (or 503 not_leader if you hit a follower)

# TCP (Go client)
err := client.Barrier(conn)
```

In single-node mode this is a no-op (returns immediately). On a
follower the HTTP returns `503 not_leader` and the TCP returns
`error_not_leader <addr>`.

## Restart / recovery

- **Single node restart** — the node opens its `--raft-dir`, replays
  the WAL into the FSM (restoring from the latest snapshot first if
  one is present), and rejoins. Catches up via AppendEntries; far-
  behind nodes receive an InstallSnapshot.
- **Full cluster restart** — every node recovers from disk; once a
  quorum is back, an election runs and writes resume.
- **Disk wipe on one node** — start it with an empty `--raft-dir`
  and the same `--node-id`; the leader will detect divergence and
  install a fresh snapshot. A brand-new node ID follows the dynamic-
  join flow above: add the voter, then start it with empty storage.
- **Lost a majority** — the cluster cannot make progress until a
  majority is restored. There is no automatic single-survivor recovery
  or force-reconfigure tooling; restore enough original members, with
  their existing `--raft-dir` contents, to regain quorum.
- **Graceful leader restart** — shutting down the leader (SIGTERM)
  first hands leadership to a caught-up follower via `TimeoutNow`, so
  the successor is elected within a round trip rather than after an
  election timeout. A `kill -9` skips that; the cluster re-elects on
  the usual timeout.

## Health / observability

- The TCP `stats` command (and `GET /v1/stats` when `--http-port` is
  enabled) return the local node's lock state, and in cluster mode an
  extra `"cluster"` object: this node's id, role
  (`leader`/`follower`/`candidate`/`pre-candidate`), term, leader id +
  client address, commit index, last-log index, snapshot index, and the
  voter set. Poll any node — a follower's view is consistent enough for
  monitoring.
- `GET /metrics` (HTTP enabled) exposes `dflockd_raft_state`,
  `dflockd_raft_is_leader`, `dflockd_raft_term`,
  `dflockd_raft_commit_index`, `dflockd_raft_last_log_index`,
  `dflockd_raft_snapshot_index`, and `dflockd_raft_voters` alongside the
  usual `dflockd_*` gauges. It also exposes leader-change, proposal,
  apply, cumulative apply-time, and admin-change counters, plus the
  `dflockd_raft_apply_duration_seconds` apply-latency histogram. A p99
  query across scraped nodes is:

  ```promql
  histogram_quantile(
    0.99,
    sum by (le) (rate(dflockd_raft_apply_duration_seconds_bucket[5m]))
  )
  ```
- A follower also reveals the leader via the `error_not_leader <addr>`
  TCP redirect (or the `503 {"error":"not_leader"}` HTTP response with
  an `X-Dflockd-Leader` header) on any mutating command.
- Log output (`level=INFO`) shows role changes, elections, leadership
  transfers, and snapshot events.

## HTTP API in cluster mode

The HTTP API works in cluster mode: mutating endpoints
(`acquire`/`enqueue`/`wait`/`release`/`renew`, session delete) are
proposed through Raft. On a **follower** they return
`503 {"error":"not_leader","detail":...}` with the leader's *raft client
address* in an `X-Dflockd-Leader` header — note that's the leader's TCP
port, not its HTTP port (the cluster config doesn't carry HTTP
addresses), so an HTTP client should retry against another node it knows
rather than blindly following the header. Read-only endpoints
(`/health`, `/ready`, `/v1/stats`, `/metrics`, `/openapi.json`) work on
any node.

HTTP session IDs remain node-local. `POST /v1/sessions` accepts an
optional body containing a caller-generated stable identity:

```json
{"stable_ref":"worker-01-6f1b7e8c"}
```

With `--orphan-ttl > 0`, recreate the session on the new leader with the
same `stable_ref`, then retry `acquire` or `enqueue`. The new session
re-attaches the replicated holder or waiter, preserving its token or
FIFO position. Empty-body session creation keeps the default behavior:
after failover the caller creates a new session and re-enters at the
back. A token the caller already observed remains valid for `renew` or
`release` either way.

## FIFO across leader failover (stable client refs)

A caller holding — or blocked waiting on — a lock when the leader fails
ordinarily loses its place because the holder/waiter slot is keyed by a
node-local connection id. **Stable client refs** give TCP connections
and HTTP replacement sessions the same replicated identity.

1. Set `--orphan-ttl 30` (seconds; or `DFLOCKD_ORPHAN_TTL_S`) on every
   node so the FSM retains a ref-tagged holder/waiter that long after
   its connection drops. The value must be identical on every member —
   it's read in the replicated apply path — and cluster-mode only (the
   loader rejects `--orphan-ttl > 0` without `--raft-dir`).
2. A Go TCP client opts in via
   `client.WithClusterStableRef("session-X")`. An HTTP caller supplies
   `{"stable_ref":"session-X"}` when creating each replacement session.

```go
cl, _ := client.NewCluster(members,
    client.WithClusterStableRef(uuid.NewString()), // any opaque per-session id
)
// Works for both the single-phase Acquire and the two-phase Enqueue/Wait:
token, _, _ := cl.Acquire(ctx, "deploy", 30*time.Second)
// ... if the leader dies, the Cluster wrapper reconnects to the new
// leader, re-sends the stable ref, and re-attaches to the original
// holder — the SAME token comes back. A queued caller keeps its FIFO
// position the same way.
```

Under the hood: re-adopt matches on `(key, ref)` in **both** the
`ApplyAcquire` and `ApplyEnqueue` paths, but only for a slot whose
owner is demonstrably gone. The FSM accepts one of two proofs, both of
which it replicates:

- the previous connection closed **gracefully** — `ApplyCleanupConn`
  stamped the slot `abandonedAtNanos` and cleared its conn id; or
- the slot's conn id was minted by a **different server process**. Conn
  ids carry a per-process epoch in their high 32 bits, so a slot the
  new leader inherited from the crashed one (no `CleanupConn` ever ran;
  `abandonedAtNanos` is still 0) is recognisable as belonging to a
  process that is no longer serving that client.

The reconnect then rebinds the slot to the new connection, renews the
lease, and evicts the dead connection's stale index entries.

A slot held by a **live** connection on the node handling the request
matches neither proof, so naming its ref does not take it over: the
request queues normally. The trade-off is that a client which vanishes
without its TCP connection being reaped cannot re-attach on the *same*
leader until the connection teardown stamps the slot (or the lease
lapses) — the conservative direction.

Slots never reclaimed by a reconnect still go away: the `EvictExpired`
sweep retires gracefully-orphaned slots past `OrphanTTL`; a
hard-crashed holder is bounded by its lease, and a hard-crashed waiter
by promotion-then-lease.

**Security note:** stable refs are caller-supplied identifiers, not
authenticated credentials. They are not a capability for taking over a
live session (see above), but anyone who knows your ref can still claim
a slot your process left behind — on another node, or after your
connection closed. Treat refs like session tokens: generate randomly
(e.g. `uuid.NewString()`), don't log them, don't share them. The
`--auth-token` mechanism, when set, gates the connection before the ref
is accepted.

**One ref, one logical session.** Do not share one across concurrent
TCP connections or HTTP sessions. Two operations on the same key under
one ref are two claims on the same slot, and `Cluster` dials a fresh
connection per call, so give each concurrent worker its own `Cluster`
(or its own ref).

## Using the cluster-aware Go client

```go
cl, err := client.NewCluster(
    []string{"10.0.0.1:6388", "10.0.0.2:6389", "10.0.0.3:6390"},
    client.WithClusterRedirectBudget(3), // default
    client.WithClusterAuthToken(os.Getenv("DFLOCKD_AUTH_TOKEN")),
)
ctx := context.Background()
token, ttl, err := cl.Acquire(ctx, "deploy-job", 60*time.Second)
// ... use lock ...
_ = cl.Release(ctx, "deploy-job", token)
```

`client.Cluster` keeps a process-local leader cache and transparently
follows `*NotLeaderError` redirects up to the configured budget. The
cache only honors hints that name an address in the operator-supplied
members list — a hostile or stale `error_not_leader evil:6388` does
not cause the client to dial an arbitrary host. An exhausted budget
surfaces an error matching `client.ErrTooManyRedirects` and wrapping
the final dial or `*client.NotLeaderError` cause. Use `errors.Is` /
`errors.As` to inspect both the retry exhaustion and its terminal
diagnostic. The operation context bounds connection establishment,
authentication, request I/O, and the complete retry loop.

The single-`*Conn` package-level API (`client.Acquire(conn, …)`) is
unchanged and still works against any node — callers that need to
handle redirects themselves can use it directly with `client.IsNotLeader`.

## Soak testing

`cmd/cluster-soak` has two modes. The default is a CI-sized in-process
N-node soak with optional periodic leader termination:

```bash
go run ./cmd/cluster-soak --nodes 3 --workers 4 --duration 30s --kill-interval 5s
# soak: clean run: writes=... successes=... killed=... duration=...
```

External mode drives an already-running odd-sized cluster through every
member's client address. Each worker uses a unique stable ref and workers
share a bounded pool of contended keys (`--keys`, default `1`), so every node
must run with the same positive `--orphan-ttl` and that TTL should cover the
expected fault and reconnect window. Use `--fault-interval 0` for
workload-only validation:

```bash
go run ./cmd/cluster-soak \
  --targets a=10.0.0.1:6388,b=10.0.0.2:6388,c=10.0.0.3:6388 \
  --workers 8 --keys 2 --duration 10m \
  --fault-interval 0 --lease-ttl 10s
```

For a long-horizon campaign, `--fault-hook` names an executable that
accepts one of these argument forms:

```text
HOOK partition NODE
HOOK heal NODE
HOOK restart NODE
HOOK skew NODE OFFSET
HOOK unskew NODE
```

The driver repeatedly partitions the writable leader, heals it, applies
an alternating clock offset to a follower, restarts the writable leader,
then removes the offset. It probes all client addresses with a
linearizable `barrier`, tracks token uniqueness and per-key fencing
monotonicity for the full run, and records the first `--history-limit`
invocations per key (default and maximum `32`). It also records the first
release attempt for each successful acquire in that initial set, closing the
history at no more than `64` operations per key. At shutdown it uses exact
state-space search to check each history against real-time ordering, stable-ref
reattachment, ambiguous lost release replies, and the earliest lease expiry
permitted by the configured clock skew.

`--lease-ttl` must exceed twice `--clock-skew` when fault injection is enabled,
leaving a positive client-observable validity interval for the checker.
Failed acquires may be omitted because their effects are unobserved; a failed
release is checked both as unapplied and as committed before its reply was
lost. The final clean report includes `history_ops=...`. Any online invariant
or final history violation makes the command exit non-zero. The hook is
executed directly, never through a shell.

`tools/cluster-soak/ssh-linux.sh` is a hook for dedicated Linux/systemd
hosts. It isolates only Raft ports with dedicated `iptables` chains,
restarts the configured service, and injects clock skew through the
process-local `DFLOCKD_UNSAFE_TEST_CLOCK_OFFSET` setting. It does not
change host clocks.

```bash
export DFLOCKD_SOAK_SSH_TARGETS='a=ops@host-a,b=ops@host-b,c=ops@host-c'
export DFLOCKD_SOAK_RAFT_ADDRS='a=10.0.0.1:7000,b=10.0.0.2:7000,c=10.0.0.3:7000'
export DFLOCKD_SOAK_SERVICE='dflockd.service'

go run ./cmd/cluster-soak \
  --targets a=10.0.0.1:6388,b=10.0.0.2:6388,c=10.0.0.3:6388 \
  --workers 16 --duration 8h \
  --keys 2 --history-limit 32 \
  --fault-hook ./tools/cluster-soak/ssh-linux.sh \
  --fault-interval 2m --fault-hold 30s --clock-skew 2s \
  --lease-ttl 10s --redirect-budget 8
```

Run the bundled hook only against disposable IPv4 hosts. The SSH user
must have non-interactive SSH access and permission to run
`sudo -n bash`. Normal cancellation heals the active partition and
removes an active offset. If the driver itself is killed ungracefully,
invoke `heal NODE` and `unskew NODE` manually for any affected node.

When session authentication is enabled, pass `--auth-token-file`.
Exit code is non-zero on a workload invariant, an unavailable writable
leader, or a failed fault action. Operation failures during expected
failover are counted in the final report but are not themselves safety
violations.

## v1 caveats

- **FIFO across leader failover** — *by default* a client blocked in
  `acquire` / `wait` when the leader fails loses its queue position;
  the holder entry (if any was minted) expires via its lease.
  Already-granted tokens survive seamlessly (the client can `renew` /
  `release` them against the new leader). To preserve queue position
  and re-attach to a held lock across failover — including a
  hard-crashed leader — enable stable client refs (`--orphan-ttl` plus
  `WithClusterStableRef` for TCP or `stable_ref` at HTTP session
  creation); see "FIFO across leader failover" above.
- **Dynamic-join with snapshot transfer** to a node started with
  empty `--raft-dir` works as of PR-3. Operator flow: `AddVoter` on
  the leader, then start the new node with empty storage and
  `--cluster-peers` listing the full cluster. The leader's
  `sendAppendEntries → sendInstallSnapshot` fallback detects the cold
  follower (`nextIndex < firstIndex`) and ships a snapshot. Validated
  by `TestDynamicJoinColdNodeCatchesUpViaSnapshot` in `internal/cluster`.

## Pointers

- [Architecture / Cluster](../architecture/cluster.md) — the model.
- `PLAN.md` — full 15-phase design + risks + the §7 production-
  readiness checklist.
