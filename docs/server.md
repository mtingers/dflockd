# Server

## Running the server

```bash
# Default: listens on 0.0.0.0:6388
./dflockd

# Custom port
./dflockd --port 7000

# Multiple options
./dflockd --host 127.0.0.1 --port 7000 --max-locks 512

# Debug logging
./dflockd --debug
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
| `--tls-cert` | *(unset)* | Path to TLS certificate PEM file |
| `--tls-key` | *(unset)* | Path to TLS private key PEM file |
| `--auto-release-on-disconnect` / `--no-auto-release-on-disconnect` | `true` | Release locks when a client disconnects |
| `--debug` | `false` | Enable debug logging |

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
| `DFLOCKD_TLS_CERT` | *(unset)* | Path to TLS certificate PEM file |
| `DFLOCKD_TLS_KEY` | *(unset)* | Path to TLS private key PEM file |
| `DFLOCKD_AUTO_RELEASE_ON_DISCONNECT` | `1` | Release locks when a client disconnects |
| `DFLOCKD_DEBUG` | *(unset)* | Enable debug logging (`1`, `yes`, or `true`) |

```bash
# Example: configure via environment
export DFLOCKD_PORT=7000
export DFLOCKD_MAX_LOCKS=2048
./dflockd
```

## Tuning guide

### Lease TTL

The `default-lease-ttl` controls how long a lock is held before it expires if not renewed. Clients are responsible for sending renew (`n`) commands before the lease expires.

- **Shorter TTL** (e.g. 10s): faster failover when clients crash, but more renewal traffic.
- **Longer TTL** (e.g. 60s): less renewal traffic, but slower failover.

### Max locks

The `max-locks` setting caps the total number of unique lock keys **and** semaphore keys tracked by the server. Lock keys and semaphore keys share the same budget. When the limit is reached, new lock or semaphore requests for unknown keys return `error_max_locks`. Existing keys are unaffected.

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

## TLS

To enable TLS encryption, provide both a PEM certificate and private key file:

```bash
./dflockd --tls-cert /path/to/cert.pem --tls-key /path/to/key.pem
```

Or via environment variables:

```bash
export DFLOCKD_TLS_CERT=/path/to/cert.pem
export DFLOCKD_TLS_KEY=/path/to/key.pem
./dflockd
```

Both `--tls-cert` and `--tls-key` must be provided together. If only one is set, the server exits with an error. When TLS is enabled, the server requires all clients to connect using TLS — plain TCP connections will fail the TLS handshake and be dropped.

The server enforces a minimum TLS version of 1.2.

## Runtime stats

The `stats` protocol command returns a JSON snapshot of the server's current state. This is useful for monitoring, debugging, and building health checks.

```bash
printf 'stats\n_\n\n' | nc localhost 6388
# ok {"connections":2,"locks":[...],"semaphores":[...],"idle_locks":[],"idle_semaphores":[]}
```

The response includes:

- **connections** — number of currently connected TCP clients
- **locks** — held locks with key, owner connection ID, seconds until lease expires, and waiter count
- **semaphores** — semaphores with at least one holder, showing key, limit, holder count, and waiter count
- **idle_locks** / **idle_semaphores** — entries with no owner/holders (cached state awaiting GC), with seconds since last activity

See [Wire Protocol](architecture/protocol.md#stats-stats) for the full JSON schema.
