# Server Configuration

## Running the server

```bash
# Default: listens on 0.0.0.0:6388
dflockd

# Custom port
dflockd --port 7000

# Multiple options
dflockd --host 127.0.0.1 --port 7000 --max-locks 512
```

## CLI flags

| Flag | Default | Description |
|---|---|---|
| `--host` | `0.0.0.0` | Bind address |
| `--port` | `6388` | Bind port |
| `--default-lease-ttl` | `33` | Default lock lease duration (seconds) |
| `--lease-sweep-interval` | `1` | How often to check for expired leases (seconds) |
| `--gc-interval` | `5` | How often to prune idle lock state (seconds) |
| `--gc-max-idle` | `60` | Seconds before idle lock state is pruned |
| `--max-locks` | `1024` | Maximum number of unique lock keys |
| `--read-timeout` | `23` | Client read timeout (seconds) |

## Environment variables

All settings can be configured via environment variables. Environment variables take precedence over CLI flags.

| Variable | Default | Description |
|---|---|---|
| `DFLOCKD_HOST` | `0.0.0.0` | Bind address |
| `DFLOCKD_PORT` | `6388` | Bind port |
| `DFLOCKD_DEFAULT_LEASE_TTL_S` | `33` | Default lock lease duration (seconds) |
| `DFLOCKD_LEASE_SWEEP_INTERVAL_S` | `1` | How often to check for expired leases |
| `DFLOCKD_GC_LOOP_SLEEP` | `5` | How often to prune idle lock state |
| `DFLOCKD_GC_MAX_UNUSED_TIME` | `60` | Seconds before idle lock state is pruned |
| `DFLOCKD_MAX_LOCKS` | `1024` | Maximum number of unique lock keys |
| `DFLOCKD_READ_TIMEOUT_S` | `23` | Client read timeout (seconds) |
| `DFLOCKD_AUTO_RELEASE_ON_DISCONNECT` | `1` | Release locks when a client disconnects |

```bash
# Example: configure via environment
export DFLOCKD_PORT=7000
export DFLOCKD_MAX_LOCKS=2048
dflockd
```

## Tuning guide

### Lease TTL

The `default-lease-ttl` controls how long a lock is held before it expires if not renewed. Clients automatically renew at `lease * renew_ratio` (default 0.5), so a 33-second TTL renews every ~16 seconds.

- **Shorter TTL** (e.g. 10s): faster failover when clients crash, but more renewal traffic.
- **Longer TTL** (e.g. 60s): less renewal traffic, but slower failover.

### Max locks

The `max-locks` setting caps the number of unique lock keys tracked by the server. When the limit is reached, new lock requests for unknown keys return `error_max_locks`. Existing keys are unaffected.

### Garbage collection

Idle lock state (no owner, no waiters) is pruned after `gc-max-idle` seconds. The GC runs every `gc-interval` seconds. For workloads with many transient keys, lower `gc-max-idle` to reclaim memory faster.

### Read timeout

The `read-timeout` controls how long the server waits for a client to send a complete request line. Idle connections that send no data within this window are disconnected. This prevents resource exhaustion from abandoned connections.

### Auto release on disconnect

When `DFLOCKD_AUTO_RELEASE_ON_DISCONNECT` is enabled (the default), the server automatically releases any locks held by a client when its TCP connection closes — whether gracefully or due to a crash. Pending waiters from that connection are also cancelled. The released lock is transferred to the next FIFO waiter, if any.

Accepts `1`, `yes`, or `true` (case-insensitive) to enable; any other value disables it.

```bash
# Disable auto-release (locks persist until lease expiry)
export DFLOCKD_AUTO_RELEASE_ON_DISCONNECT=0
```

!!! warning
    Disabling this means locks from disconnected clients will only be freed when their lease expires. This increases the window where a lock is held by a dead client.
