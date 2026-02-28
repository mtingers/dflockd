# Server Configuration

dflockd is configured via command-line flags and/or environment variables. When both are provided, **explicit CLI flags take precedence** over environment variables, which take precedence over defaults.

## All Flags

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--host` | `DFLOCKD_HOST` | `127.0.0.1` | Bind address |
| `--port` | `DFLOCKD_PORT` | `6388` | Bind port |
| `--default-lease-ttl` | `DFLOCKD_DEFAULT_LEASE_TTL_S` | `33` | Default lock lease duration (seconds) |
| `--lease-sweep-interval` | `DFLOCKD_LEASE_SWEEP_INTERVAL_S` | `1` | Lease expiry check interval (seconds) |
| `--gc-interval` | `DFLOCKD_GC_LOOP_SLEEP` | `5` | Lock state GC interval (seconds) |
| `--gc-max-idle` | `DFLOCKD_GC_MAX_UNUSED_TIME` | `60` | Idle seconds before pruning lock state |
| `--max-locks` | `DFLOCKD_MAX_LOCKS` | `1024` | Maximum number of unique lock/semaphore keys |
| `--max-keys` | `DFLOCKD_MAX_KEYS` | `0` | Maximum aggregate keys across all types (0 = unlimited) |
| `--max-list-length` | `DFLOCKD_MAX_LIST_LENGTH` | `0` | Maximum items per list (0 = unlimited) |
| `--max-connections` | `DFLOCKD_MAX_CONNECTIONS` | `0` | Maximum concurrent connections (0 = unlimited) |
| `--max-waiters` | `DFLOCKD_MAX_WAITERS` | `0` | Maximum waiters per lock/semaphore key (0 = unlimited) |
| `--max-subscriptions` | `DFLOCKD_MAX_SUBSCRIPTIONS` | `0` | Maximum watch + listen registrations per connection (0 = unlimited) |
| `--read-timeout` | `DFLOCKD_READ_TIMEOUT_S` | `23` | Client read timeout (seconds) |
| `--write-timeout` | `DFLOCKD_WRITE_TIMEOUT_S` | `5` | Client write timeout (seconds) |
| `--shutdown-timeout` | `DFLOCKD_SHUTDOWN_TIMEOUT_S` | `30` | Graceful shutdown drain timeout (seconds, 0 = wait forever) |
| `--auto-release-on-disconnect` | `DFLOCKD_AUTO_RELEASE_ON_DISCONNECT` | `true` | Release locks when a client disconnects |
| `--tls-cert` | `DFLOCKD_TLS_CERT` | _(empty)_ | Path to TLS certificate PEM file |
| `--tls-key` | `DFLOCKD_TLS_KEY` | _(empty)_ | Path to TLS private key PEM file |
| `--auth-token` | — | _(empty)_ | Shared secret for client authentication (visible in process list; prefer `--auth-token-file`) |
| `--auth-token-file` | `DFLOCKD_AUTH_TOKEN_FILE` | _(empty)_ | Path to file containing the auth token |
| `--debug` | `DFLOCKD_DEBUG` | `false` | Enable debug logging |
| `--version` | — | — | Print version and exit |

## Validation Rules

The server rejects configurations that violate these constraints:

| Constraint | Rule |
|------------|------|
| `--max-locks` | Must be > 0 |
| `--default-lease-ttl` | Must be > 0 |
| `--lease-sweep-interval` | Must be > 0 |
| `--gc-interval` | Must be > 0 |
| `--gc-max-idle` | Must be > 0 |
| `--read-timeout` | Must be > 0 |
| `--write-timeout` | Must be >= 0 |
| `--shutdown-timeout` | Must be >= 0 |
| `--port` | Must be 0-65535 |
| `--max-keys` | Must be >= 0 |
| `--max-list-length` | Must be >= 0 |
| `--max-connections` | Must be >= 0 |
| `--max-waiters` | Must be >= 0 |
| `--max-subscriptions` | Must be >= 0 |

## Auth Token Resolution

The auth token is resolved in this priority order:

1. `DFLOCKD_AUTH_TOKEN` environment variable
2. `--auth-token` flag value
3. Contents of file at `--auth-token-file` path (trailing whitespace stripped)
4. Contents of file at `DFLOCKD_AUTH_TOKEN_FILE` environment variable

If no token is configured, authentication is disabled and `auth` commands are rejected.

## TLS Setup

Both `--tls-cert` and `--tls-key` must be provided together. The server enforces a minimum of TLS 1.2.

```bash
# Generate a self-signed cert for testing
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem \
  -days 365 -nodes -subj '/CN=localhost'

# Start with TLS
dflockd --tls-cert cert.pem --tls-key key.pem
```

## Environment Variable Examples

```bash
export DFLOCKD_HOST=0.0.0.0
export DFLOCKD_PORT=6388
export DFLOCKD_AUTH_TOKEN=my-secret-token
export DFLOCKD_DEFAULT_LEASE_TTL_S=60
export DFLOCKD_MAX_CONNECTIONS=1000
export DFLOCKD_DEBUG=true

dflockd
```

## Boolean Environment Variables

Boolean env vars accept: `1`, `yes`, `true` (true) and `0`, `no`, `false` (false). Unrecognized values fall back to the flag default.
