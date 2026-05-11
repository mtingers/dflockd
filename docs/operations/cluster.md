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
| `--advertise-addr <host:port>` (optional) | This node's client-facing address. Defaults to `--host:--port`. Clients connect here when redirected. |

`--http-port` is currently rejected when `--raft-dir` is set — see
the [v1 caveats below](#v1-caveats).

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
  --port 6388 --advertise-addr 10.0.0.1:6388 \
  --cluster-peers "n1=10.0.0.1:7001@10.0.0.1:6388,n2=10.0.0.2:7002@10.0.0.2:6389,n3=10.0.0.3:7003@10.0.0.3:6390"

# Host B (n2) — same --cluster-peers value
dflockd --node-id n2 --raft-dir /var/lib/dflockd/n2 \
        --raft-addr 10.0.0.2:7002 --port 6389 --advertise-addr 10.0.0.2:6389 \
        --cluster-peers "n1=10.0.0.1:7001@10.0.0.1:6388,n2=10.0.0.2:7002@10.0.0.2:6389,n3=10.0.0.3:7003@10.0.0.3:6390"

# Host C (n3) — same again
dflockd --node-id n3 --raft-dir /var/lib/dflockd/n3 \
        --raft-addr 10.0.0.3:7003 --port 6390 --advertise-addr 10.0.0.3:6390 \
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

The cluster operator runs `AddVoter` against the leader (via a future
admin endpoint; for now via a small Go program importing
`internal/cluster`). The new entry adds the member to the
configuration immediately on append; the new node should be started
with the **same** `--cluster-peers` list so its membership view
matches.

## Removing a node

`RemoveServer` on the leader proposes a `ConfigEntry` that drops the
named node. A leader removing itself steps down once the change
commits.

## Restart / recovery

- **Single node restart** — the node opens its `--raft-dir`, replays
  the WAL into the FSM (restoring from the latest snapshot first if
  one is present), and rejoins. Catches up via AppendEntries; far-
  behind nodes receive an InstallSnapshot.
- **Full cluster restart** — every node recovers from disk; once a
  quorum is back, an election runs and writes resume.
- **Disk wipe on one node** — start it with an empty `--raft-dir`
  and the same `--node-id`; the leader will detect divergence and
  install a fresh snapshot. (Note: dynamic-join is a follow-on; for
  v1 the simpler path is to remove + re-add the node id.)
- **Lost a majority** — the cluster cannot make progress until a
  majority is restored. There is no automatic single-survivor recovery
  in v1; the operator should restart enough members or, as a last
  resort, force-reconfigure (manual, unsafe).

## Health / observability

- The TCP `stats` command returns the local node's lock state.
- A cluster admin HTTP endpoint is a follow-on (see PLAN.md §6 /
  Phase 11). For now log output (`level=INFO`) shows role changes,
  elections, and snapshot events.

## v1 caveats

- **`--http-port` is rejected with cluster mode.** The HTTP API
  currently calls the LockManager directly; routing it through the
  cluster's propose path is a follow-on. Use the TCP API in cluster
  mode.
- **TLS / cluster-shared-secret** on the Raft transport are not yet
  wired. Run on a trusted network.
- **FIFO across leader failover** — a client blocked in `acquire` /
  `wait` when the leader fails loses its queue position; the holder
  entry (if any was minted) expires via its lease. Already-granted
  tokens survive seamlessly (the client can `renew` / `release` them
  against the new leader). Stable-client-ref re-attach across failover
  is laid out in PLAN.md §4.7 as a follow-on.
- **Dynamic-join with snapshot transfer** to a node started without
  prior state is not yet implemented. The supported flows are static
  bootstrap (all members listed in `--cluster-peers`) and `AddVoter`
  against a node whose `--cluster-peers` already lists the cluster.

## Pointers

- [Architecture / Cluster](../architecture/cluster.md) — the model.
- `PLAN.md` — full 15-phase design + risks + the §7 production-
  readiness checklist.
