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

## Configuration

CLI flags take precedence over environment variables when explicitly set; otherwise the environment variable is used, falling back to the built-in default. Boolean env vars accept `1`, `yes`, or `true` (case-insensitive).

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--host` | `DFLOCKD_HOST` | `127.0.0.1` | Bind address |
| `--port` | `DFLOCKD_PORT` | `6388` | Bind port |
| `--default-lease-ttl` | `DFLOCKD_DEFAULT_LEASE_TTL_S` | `33` | Default lease duration (seconds) |
| `--lease-sweep-interval` | `DFLOCKD_LEASE_SWEEP_INTERVAL_S` | `1` | Lease expiry check interval (seconds) |
| `--gc-interval` | `DFLOCKD_GC_LOOP_SLEEP` | `5` | Idle state GC interval (seconds) |
| `--gc-max-idle` | `DFLOCKD_GC_MAX_UNUSED_TIME` | `60` | Seconds before idle state is pruned |
| `--max-locks` | `DFLOCKD_MAX_LOCKS` | `1024` | Max total lock and semaphore resource entries |
| `--max-connections` | `DFLOCKD_MAX_CONNECTIONS` | `0` | Max concurrent connections (0 = unlimited) |
| `--max-waiters` | `DFLOCKD_MAX_WAITERS` | `0` | Max waiters per key (0 = unlimited) |
| `--max-subscriptions` | `DFLOCKD_MAX_SUBSCRIPTIONS` | `0` | Max signal subscriptions per connection (0 = unlimited) |
| `--read-timeout` | `DFLOCKD_READ_TIMEOUT_S` | `23` | Client read timeout (seconds) |
| `--write-timeout` | `DFLOCKD_WRITE_TIMEOUT_S` | `5` | Client write timeout (seconds) |
| `--shutdown-timeout` | `DFLOCKD_SHUTDOWN_TIMEOUT_S` | `30` | Graceful shutdown timeout (seconds, 0 = wait forever) |
| `--tls-cert` | `DFLOCKD_TLS_CERT` | *(unset)* | TLS certificate PEM file |
| `--tls-key` | `DFLOCKD_TLS_KEY` | *(unset)* | TLS private key PEM file |
| `--auth-token` | `DFLOCKD_AUTH_TOKEN` | *(unset)* | Shared secret for authentication |
| `--auth-token-file` | `DFLOCKD_AUTH_TOKEN_FILE` | *(unset)* | File containing the auth token |
| `--auto-release-on-disconnect` | `DFLOCKD_AUTO_RELEASE_ON_DISCONNECT` | `true` | Release locks on disconnect |
| `--version` | — | | Print version and exit |
| `--debug` | `DFLOCKD_DEBUG` | `false` | Enable debug logging |

## Tuning guide

### Lease TTL

The `default-lease-ttl` controls how long a lock is held before it expires if not renewed. Clients are responsible for sending renew (`n`) commands before the lease expires.

- **Shorter TTL** (e.g. 10s): faster failover when clients crash, but more renewal traffic.
- **Longer TTL** (e.g. 60s): less renewal traffic, but slower failover.

### Max locks

The `max-locks` setting caps the total number of resource entries (lock keys + semaphore keys) tracked by the server. Lock keys and semaphore keys are in separate namespaces — the same key string used for both a lock and a semaphore counts as two entries. When the limit is reached, new lock or semaphore requests for unknown keys return `error_max_locks`. Existing keys are unaffected.

### Garbage collection

Idle lock state (no owner, no waiters) is pruned after `gc-max-idle` seconds. The GC runs every `gc-interval` seconds. For workloads with many transient keys, lower `gc-max-idle` to reclaim memory faster.

### Read timeout

The `read-timeout` controls how long the server waits for a client to send a complete request line. Idle connections that send no data within this window are disconnected. This prevents resource exhaustion from abandoned connections.

The Go client's `SignalConn` automatically sends `ping` heartbeats (default: every 15s) to keep signal listener connections alive. External clients that hold long-lived signal connections should send periodic `ping` commands to avoid read-timeout disconnections.

### Write timeout

The `write-timeout` controls how long the server waits for a response write to complete. If a client is reading slowly (or not at all), the write will fail after this deadline and the connection is closed. This prevents slow-reading clients from blocking server goroutines indefinitely.

### Shutdown timeout

The `shutdown-timeout` controls the maximum time the server waits for active connections to finish during graceful shutdown (SIGINT/SIGTERM). When the timeout expires, any remaining connections are force-closed.

- **Default (30s)**: suitable for most workloads. Gives clients time to finish in-flight operations.
- **Shorter timeout** (e.g. 5s): faster shutdown, useful in container orchestration where a SIGKILL follows after a grace period.
- **Longer timeout** (e.g. 120s): for workloads with long-running lock acquisitions.
- **0**: wait forever (no deadline). The server blocks until all clients disconnect naturally. This matches pre-1.8.0 behavior.

### Max connections

The `max-connections` setting caps the total number of concurrent TCP connections. When the limit is reached, new connections are accepted and immediately closed. Set to `0` (the default) for unlimited connections.

### Max waiters

The `max-waiters` setting caps the number of pending waiters **per key** for both locks and semaphores. When the limit is reached, new acquire or enqueue requests for that key return `error_max_waiters`. This prevents unbounded memory growth from waiter queues on a single contended key. Set to `0` (the default) for unlimited waiters.

### Max subscriptions

The `max-subscriptions` setting caps the number of signal subscriptions (listen registrations) per connection. When the limit is reached, additional `listen` commands return `error`. Set to `0` (the default) for unlimited subscriptions.

### Go runtime tuning

For latency-sensitive deployments, increasing `GOGC` reduces garbage collection frequency at the cost of higher memory usage. The default `GOGC=100` triggers ~100 GC cycles during a sustained burst of 500 concurrent workers; `GOGC=400` cuts that to ~25 cycles and reduces p99 latency by ~12%.

```bash
GOGC=400 ./dflockd
```

Alternatively, use `GOMEMLIMIT` to set a soft memory ceiling and let the runtime schedule GC less aggressively within that budget:

```bash
GOMEMLIMIT=128MiB ./dflockd
```

### Auto release on disconnect

When enabled (the default), the server automatically releases any locks held by a client when its TCP connection closes — whether gracefully or due to a crash. Pending waiters from that connection are also cancelled. The released lock is transferred to the next FIFO waiter, if any.

!!! warning
    Disabling this means locks from disconnected clients will only be freed when their lease expires.

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

Or load the token from a file (avoids leaking the secret in the process list):

```bash
./dflockd --auth-token-file /run/secrets/dflockd-token
```

Or via environment variables:

```bash
export DFLOCKD_AUTH_TOKEN=my-secret-token
./dflockd
# or
export DFLOCKD_AUTH_TOKEN_FILE=/run/secrets/dflockd-token
./dflockd
```

Token resolution priority: `DFLOCKD_AUTH_TOKEN` env var > `--auth-token` flag > `--auth-token-file` flag > `DFLOCKD_AUTH_TOKEN_FILE` env var.

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
- **signal_channels** — active signal subscriptions with pattern, optional group name, and listener count
- **idle_locks** / **idle_semaphores** — entries with no owner/holders (cached state awaiting GC), with seconds since last activity

See [Wire Protocol](architecture/protocol.md#stats-stats) for the full JSON schema.
