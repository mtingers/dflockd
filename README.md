# dflockd

A lightweight distributed lock server with FIFO ordering, automatic lease expiry, semaphores, and pub/sub signals. Single Go binary, zero dependencies.

[Documentation](https://mtingers.github.io/dflockd/) · [Changelog](https://mtingers.github.io/dflockd/changelog/)

## Build & run

```bash
go build -o dflockd ./cmd/dflockd
./dflockd  # listens on 127.0.0.1:6388
```

Or install directly:

```bash
go install github.com/mtingers/dflockd/cmd/dflockd@latest
```

## Usage

### Go

```go
import "github.com/mtingers/dflockd/client"

l := &client.Lock{
    Key:            "my-resource",
    AcquireTimeout: 10 * time.Second,
    Servers:        []string{"127.0.0.1:6388"},
}
ok, err := l.Acquire(context.Background())
if err != nil || !ok { /* handle */ }
defer l.Release(context.Background())
```

See the [Go client reference](https://mtingers.github.io/dflockd/client/) for the full API (renewal, sharding, TLS, auth, SSE signal subscriptions).

### HTTP

Start the server with `--http-port 6389` to enable the optional REST + SSE layer, then:

```bash
sid=$(curl -sX POST http://localhost:6389/v1/sessions | jq -r .session_id)
curl -sX POST http://localhost:6389/v1/locks/my-job \
     -H "X-Dflockd-Session: $sid" \
     -d '{"acquire_timeout_s": 10, "lease_ttl_s": 60}'
# → {"status":"ok","token":"...","lease_ttl_s":60}
curl -sX DELETE http://localhost:6389/v1/sessions/$sid
```

See the [HTTP API docs](https://mtingers.github.io/dflockd/http-api/) and [OpenAPI spec](https://mtingers.github.io/dflockd/openapi.json).

### Raw TCP protocol

The wire format is three newline-terminated UTF-8 lines (`command\nkey\narg\n`). Useful for debugging or minimal-footprint clients in languages without a ready-made library:

```bash
$ nc localhost 6388
l
my-key
10
ok abc123... 33
r
my-key
abc123...
ok
```

The connection must stay open — by default, locks are auto-released on disconnect. See the [protocol reference](https://mtingers.github.io/dflockd/architecture/protocol/) for all commands.

## Performance

Each operation is one lock acquire + release over a persistent TCP connection. Measured on an Apple M1 (MacBook Air, 8 GB RAM) with server and clients on localhost.

| Workers | Rounds | Ops | Throughput | Mean | p50 | p99 |
|---|---|---|---|---|---|---|
| 1 | 1,000 | 1,000 | 14,030 ops/s | 0.071 ms | 0.055 ms | 0.223 ms |
| 10 | 1,000 | 10,000 | 49,974 ops/s | 0.199 ms | 0.191 ms | 0.430 ms |
| 50 | 1,000 | 50,000 | 95,741 ops/s | 0.504 ms | 0.386 ms | 2.977 ms |
| 100 | 1,000 | 100,000 | 92,948 ops/s | 1.042 ms | 0.766 ms | 6.606 ms |
| 200 | 1,000 | 200,000 | 92,895 ops/s | 2.079 ms | 1.503 ms | 13.354 ms |
| 500 | 1,000 | 500,000 | 93,460 ops/s | 5.172 ms | 4.295 ms | 29.387 ms |

All workers use unique keys (no contention). Run your own benchmarks with `go run ./cmd/bench --help`.

## Configuration

CLI flags take precedence over environment variables.

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--host` | `DFLOCKD_HOST` | `127.0.0.1` | Bind address |
| `--port` | `DFLOCKD_PORT` | `6388` | Bind port |
| `--default-lease-ttl` | `DFLOCKD_DEFAULT_LEASE_TTL_S` | `33` | Default lease duration (seconds) |
| `--max-locks` | `DFLOCKD_MAX_LOCKS` | `1024` | Max unique lock+semaphore keys |
| `--max-connections` | `DFLOCKD_MAX_CONNECTIONS` | `0` | Max concurrent connections (0 = unlimited) |
| `--max-connections-per-ip` | `DFLOCKD_MAX_CONNECTIONS_PER_IP` | `0` | Max concurrent TCP connections per remote IP (0 = unlimited) |
| `--max-waiters` | `DFLOCKD_MAX_WAITERS` | `0` | Max waiters per key (0 = unlimited) |
| `--max-subscriptions` | `DFLOCKD_MAX_SUBSCRIPTIONS` | `0` | Max signal subscriptions per connection (0 = unlimited) |
| `--read-timeout` | `DFLOCKD_READ_TIMEOUT_S` | `23` | Client read timeout (seconds) |
| `--write-timeout` | `DFLOCKD_WRITE_TIMEOUT_S` | `5` | Client write timeout (seconds) |
| `--shutdown-timeout` | `DFLOCKD_SHUTDOWN_TIMEOUT_S` | `30` | Graceful shutdown timeout (seconds, 0 = wait forever) |
| `--tls-cert` | `DFLOCKD_TLS_CERT` | *(unset)* | TLS certificate PEM file |
| `--tls-key` | `DFLOCKD_TLS_KEY` | *(unset)* | TLS private key PEM file |
| `--auth-token` | `DFLOCKD_AUTH_TOKEN` | *(unset)* | Shared secret for authentication |
| `--auth-token-file` | `DFLOCKD_AUTH_TOKEN_FILE` | *(unset)* | File containing the auth token |
| `--auto-release-on-disconnect` | `DFLOCKD_AUTO_RELEASE_ON_DISCONNECT` | `true` | Release held locks/semaphore slots on disconnect |
| `--http-port` | `DFLOCKD_HTTP_PORT` | `0` | HTTP API port (0 = disabled) |
| `--http-host` | `DFLOCKD_HTTP_HOST` | same as `--host` | HTTP API bind address |
| `--http-session-idle-timeout` | `DFLOCKD_HTTP_SESSION_IDLE_S` | `20` | HTTP session idle timeout (seconds) |
| `--http-max-sessions` | `DFLOCKD_HTTP_MAX_SESSIONS` | `0` | Max concurrent HTTP sessions (0 = unlimited) |
| `--http-max-sessions-per-ip` | `DFLOCKD_HTTP_MAX_SESSIONS_PER_IP` | `0` | Max concurrent HTTP sessions per remote IP (0 = unlimited) |
| `--http-max-connections-per-ip` | `DFLOCKD_HTTP_MAX_CONNECTIONS_PER_IP` | `0` | Max concurrent HTTP transport connections per remote IP (0 = unlimited) |
| `--http-rate-limit-per-ip` | `DFLOCKD_HTTP_RATE_LIMIT_PER_IP` | `0` | HTTP requests per second per remote IP (0 = unlimited) |
| `--http-rate-limit-burst` | `DFLOCKD_HTTP_RATE_LIMIT_BURST` | `0` | HTTP per-IP burst size (0 = same as rate when rate is set) |
| `--http-cors-allowed-origins` | `DFLOCKD_HTTP_CORS_ALLOWED_ORIGINS` | *(unset)* | Comma-separated CORS origins; `*` allows any origin |
| `--http-sse-ping-interval` | `DFLOCKD_HTTP_SSE_PING_S` | `15` | Internal pinger interval for SSE streams (seconds) |
| `--debug` | `DFLOCKD_DEBUG` | `false` | Enable debug logging |

See the [server docs](https://mtingers.github.io/dflockd/server/) for GC tuning, TLS setup, authentication, and other advanced options.

## Client libraries

- **Go** (in-repo) — `go get github.com/mtingers/dflockd/client` ([docs](https://mtingers.github.io/dflockd/client/))
- **Python** — [dflockd-client-py](https://github.com/mtingers/dflockd-client-py) ([docs](https://mtingers.github.io/dflockd-client-py/))
- **TypeScript** — [dflockd-client-ts](https://github.com/mtingers/dflockd-client-ts) ([docs](https://mtingers.github.io/dflockd-client-ts/))

## Tests

```bash
go test ./... -v
```

## Benchmarking

```bash
go run ./cmd/bench --workers 100 --rounds 500
```

See `go run ./cmd/bench --help` for all options.
