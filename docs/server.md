# Server configuration

Every flag has a matching environment variable. Precedence: explicit
CLI flag > environment variable > flag default.

## TCP server

| Flag | Env | Default | Description |
|---|---|---|---|
| `--host` | `DFLOCKD_HOST` | `127.0.0.1` | Bind address |
| `--port` | `DFLOCKD_PORT` | `6388` | TCP port |
| `--read-timeout` | `DFLOCKD_READ_TIMEOUT_S` | `23` | Per-line read timeout (seconds) |
| `--write-timeout` | `DFLOCKD_WRITE_TIMEOUT_S` | `5` | Per-line write timeout (seconds) |
| `--shutdown-timeout` | `DFLOCKD_SHUTDOWN_TIMEOUT_S` | `30` | Graceful drain timeout on SIGTERM (0 = forever) |
| `--max-connections` | `DFLOCKD_MAX_CONNECTIONS` | `0` | Cap on concurrent TCP connections (0 = unlimited) |
| `--max-connections-per-ip` | `DFLOCKD_MAX_CONNECTIONS_PER_IP` | `0` | Cap per remote IP (0 = unlimited) |
| `--auto-release-on-disconnect` | `DFLOCKD_AUTO_RELEASE_ON_DISCONNECT` | `true` | Release held tokens when a connection closes |

`--read-timeout` is per-line on the line-based protocol, *not*
per-request. Each of the three lines (cmd / key / arg) gets its own
deadline. The default is large enough for human typing during ad-hoc
`nc` debugging.

## Lock manager

| Flag | Env | Default | Description |
|---|---|---|---|
| `--default-lease-ttl` | `DFLOCKD_DEFAULT_LEASE_TTL_S` | `33` | Lease TTL when the request doesn't specify one |
| `--lease-sweep-interval` | `DFLOCKD_LEASE_SWEEP_INTERVAL_S` | `1` | How often to evict expired holders |
| `--gc-interval` | `DFLOCKD_GC_INTERVAL_S` | `5` | How often to prune unused resource state |
| `--gc-max-idle` | `DFLOCKD_GC_MAX_IDLE_S` | `60` | Idle seconds before a no-holder, no-waiter resource is GCed |
| `--max-locks` | `DFLOCKD_MAX_LOCKS` | `1024` | Cluster-wide cap on unique active keys |
| `--max-waiters` | `DFLOCKD_MAX_WAITERS` | `0` | Per-key waiter cap (0 = unlimited) |
| `--fence-state-file` | `DFLOCKD_FENCE_STATE_FILE` | *(unset)* | Path to the fence-counter state file (8-byte uint64). Empty = best-effort wall-clock seeding. Set for strict cross-restart fencing — see [protocol → token format](architecture/protocol.md#token-format). |

`--max-locks` is the global cap on resource state. Once it's hit,
new keys return `error_max_locks` even on the fast path. Idle
resources (no holders, no waiters) are GCed after `--gc-max-idle`,
so the cap is a steady-state limit, not a high-water mark.

### Fence state file

Without `--fence-state-file`, the fencing prefix in every issued
token starts from `time.Now().UnixNano()` at boot — strictly
monotonic across restarts only as long as the wall clock doesn't
regress.

With `--fence-state-file=/path`, dflockd pre-allocates fence
ranges to disk: each ~1M grants triggers one `fsync(2)` of an
8-byte file. After a crash, the next instance reads the persisted
ceiling (which is always ≥ any fence ever issued) and seeds above
it. Up to ~1M fence values are skipped per restart, but
monotonicity is preserved unconditionally.

Measured overhead vs. the in-memory path is ~0.4% (Apple M1,
single-threaded, `BenchmarkNewToken_*` in `internal/lock/`). The
file is created if missing, must be 8 bytes if present, and
resides in a directory the dflockd user can write. On clean
shutdown the file is closed (FD released); the durable state is
already on disk at that point.

## TLS and authentication

| Flag | Env | Default | Description |
|---|---|---|---|
| `--tls-cert` | `DFLOCKD_TLS_CERT` | *(unset)* | TLS certificate file (PEM) |
| `--tls-key` | `DFLOCKD_TLS_KEY` | *(unset)* | TLS private key file (PEM) |
| `--auth-token` | `DFLOCKD_AUTH_TOKEN` | *(unset)* | Shared secret (visible in `ps`; prefer the `-file` form) |
| `--auth-token-file` | `DFLOCKD_AUTH_TOKEN_FILE` | *(unset)* | File containing the token (one line, trailing whitespace stripped) |

`--tls-cert` and `--tls-key` must be set together or both unset.
The same cert/key is used for the TCP listener and the HTTP
listener (when enabled). TLS 1.2 is the minimum.

Auth is a single shared secret compared in constant time:

- TCP: the first command on a new connection must be `auth\n_\n<token>\n`. Anything else gets `error_auth`.
- HTTP: every request must carry `Authorization: Bearer <token>`,
  except `/health` and `/ready`.

Auth-token resolution order:

1. `--auth-token` (if explicitly set on the CLI)
2. `--auth-token-file` (if explicitly set on the CLI)
3. `DFLOCKD_AUTH_TOKEN`
4. `DFLOCKD_AUTH_TOKEN_FILE`

Newlines in tokens are rejected at startup.

## HTTP REST API

The HTTP listener is opt-in. `--http-port=0` (the default) disables
it; any non-zero port enables it on `--http-host` (which defaults to
`--host`).

| Flag | Env | Default | Description |
|---|---|---|---|
| `--http-port` | `DFLOCKD_HTTP_PORT` | `0` (disabled) | HTTP listen port |
| `--http-host` | `DFLOCKD_HTTP_HOST` | same as `--host` | HTTP bind address |
| `--http-session-idle-timeout` | `DFLOCKD_HTTP_SESSION_IDLE_S` | `20` | Idle timeout reported to clients |
| `--http-max-sessions` | `DFLOCKD_HTTP_MAX_SESSIONS` | `0` | Cap on concurrent HTTP sessions |
| `--http-max-sessions-per-ip` | `DFLOCKD_HTTP_MAX_SESSIONS_PER_IP` | `0` | Per-IP session cap |
| `--http-max-connections-per-ip` | `DFLOCKD_HTTP_MAX_CONNECTIONS_PER_IP` | `0` | Per-IP transport-conn cap |
| `--http-rate-limit-per-ip` | `DFLOCKD_HTTP_RATE_LIMIT_PER_IP` | `0` | Token-bucket rate per IP (req/s) |
| `--http-rate-limit-burst` | `DFLOCKD_HTTP_RATE_LIMIT_BURST` | `0` | Bucket burst (defaults to rate when rate is set) |
| `--http-cors-allowed-origins` | `DFLOCKD_HTTP_CORS_ALLOWED_ORIGINS` | *(disabled)* | Comma-separated origins; `*` allows any |

The session sweeper reaps any session whose `lastSeen` falls behind
2× `--http-session-idle-timeout`. In-flight requests (held via
`BeginRequest`) are immune to reaping for the duration of the
request, so a long-poll `/wait` won't be killed mid-flight.

## Diagnostics

| Flag | Env | Default | Description |
|---|---|---|---|
| `--debug` | `DFLOCKD_DEBUG` | `false` | Enable debug-level logs |
| `--version` | — | — | Print version and exit |

Logs are slog text format on stderr.

## Operational endpoints

When `--http-port` is set:

- `GET /health` — unauthenticated. Returns `{"status":"ok"}`.
- `GET /ready` — unauthenticated. Returns `{"status":"draining"}`
  with HTTP 503 during graceful shutdown; otherwise
  `{"status":"ok"}`.
- `GET /metrics` — Prometheus exposition. Includes per-route
  request counts/durations and lock-manager gauges.
- `GET /v1/openapi.json` — unauthenticated. Serves the embedded
  OpenAPI 3.1 spec for codegen tools.

## Graceful shutdown

SIGINT or SIGTERM cancels the parent context. Both listeners stop
accepting new connections; in-flight handlers run to completion
bounded by `--shutdown-timeout`. A timeout expiry force-closes
remaining connections so the process can exit. Set
`--shutdown-timeout=0` to wait forever.
