# Server

## Running the server

```bash
# Default: listens on 127.0.0.1:6388
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
| `--host` | `127.0.0.1` | Bind address |
| `--port` | `6388` | Bind port |
| `--default-lease-ttl` | `33` | Default lock lease duration (seconds) |
| `--lease-sweep-interval` | `1` | How often to check for expired leases (seconds) |
| `--gc-interval` | `5` | How often to prune idle lock state (seconds) |
| `--gc-max-idle` | `60` | Seconds before idle lock state is pruned |
| `--max-locks` | `1024` | Maximum number of unique lock keys |
| `--max-connections` | `0` | Maximum concurrent connections (0 = unlimited) |
| `--max-waiters` | `0` | Maximum waiters per lock/semaphore key (0 = unlimited) |
| `--read-timeout` | `23` | Client read timeout (seconds) |
| `--write-timeout` | `5` | Client write timeout (seconds) |
| `--tls-cert` | *(unset)* | Path to TLS certificate PEM file |
| `--tls-key` | *(unset)* | Path to TLS private key PEM file |
| `--auth-token` | *(unset)* | Shared secret for client authentication |
| `--auto-release-on-disconnect` / `--no-auto-release-on-disconnect` | `true` | Release locks when a client disconnects |
| `--debug` | `false` | Enable debug logging |

## Environment variables

All settings can be configured via environment variables. Environment variables take precedence over CLI flags.

| Variable | Default | Description |
|---|---|---|
| `DFLOCKD_HOST` | `127.0.0.1` | Bind address |
| `DFLOCKD_PORT` | `6388` | Bind port |
| `DFLOCKD_DEFAULT_LEASE_TTL_S` | `33` | Default lock lease duration (seconds) |
| `DFLOCKD_LEASE_SWEEP_INTERVAL_S` | `1` | How often to check for expired leases |
| `DFLOCKD_GC_LOOP_SLEEP` | `5` | How often to prune idle lock state |
| `DFLOCKD_GC_MAX_UNUSED_TIME` | `60` | Seconds before idle lock state is pruned |
| `DFLOCKD_MAX_LOCKS` | `1024` | Maximum number of unique lock keys |
| `DFLOCKD_MAX_CONNECTIONS` | `0` | Maximum concurrent connections (0 = unlimited) |
| `DFLOCKD_MAX_WAITERS` | `0` | Maximum waiters per lock/semaphore key (0 = unlimited) |
| `DFLOCKD_READ_TIMEOUT_S` | `23` | Client read timeout (seconds) |
| `DFLOCKD_WRITE_TIMEOUT_S` | `5` | Client write timeout (seconds) |
| `DFLOCKD_TLS_CERT` | *(unset)* | Path to TLS certificate PEM file |
| `DFLOCKD_TLS_KEY` | *(unset)* | Path to TLS private key PEM file |
| `DFLOCKD_AUTH_TOKEN` | *(unset)* | Shared secret for client authentication |
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

### Write timeout

The `write-timeout` controls how long the server waits for a response write to complete. If a client is reading slowly (or not at all), the write will fail after this deadline and the connection is closed. This prevents slow-reading clients from blocking server goroutines indefinitely.

### Max connections

The `max-connections` setting caps the total number of concurrent TCP connections. When the limit is reached, new connections are accepted and immediately closed. Set to `0` (the default) for unlimited connections.

### Max waiters

The `max-waiters` setting caps the number of pending waiters **per key** for both locks and semaphores. When the limit is reached, new acquire or enqueue requests for that key return `error_max_waiters`. This prevents unbounded memory growth from waiter queues on a single contended key. Set to `0` (the default) for unlimited waiters.

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

## Authentication

To require token-based authentication, set a shared secret:

```bash
./dflockd --auth-token my-secret-token
```

Or via environment variable:

```bash
export DFLOCKD_AUTH_TOKEN=my-secret-token
./dflockd
```

When `--auth-token` is set, every new connection must send an `auth` command as its **first** message. If the token matches, the server responds with `ok` and the connection proceeds normally. If the token is wrong or a non-auth command is sent first, the server responds with `error_auth` and closes the connection.

If `--auth-token` is not set (the default), no authentication is required and all connections are accepted as before.

The token comparison uses constant-time comparison (`crypto/subtle.ConstantTimeCompare`) to prevent timing attacks.

!!! warning
    The auth token is sent in plaintext over the wire. Use together with TLS (`--tls-cert` / `--tls-key`) to protect the token in transit.

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
