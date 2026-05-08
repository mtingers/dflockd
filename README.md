# dflockd

A distributed FIFO lock server with leases and semaphores. Single Go binary, zero runtime dependencies. Speaks a line-based TCP protocol and an optional HTTP REST API — both backed by the same in-memory `LockManager`, so a TCP client and an HTTP session contending on the same key are ordered together in a single FIFO queue.

[Documentation](https://mtingers.github.io/dflockd/) · [HTTP API](https://mtingers.github.io/dflockd/http-api/) · [OpenAPI](https://mtingers.github.io/dflockd/openapi.json) · [Changelog](https://github.com/mtingers/dflockd/blob/main/CHANGELOG.md)

## Build & run

```bash
go install github.com/mtingers/dflockd/cmd/dflockd@latest
dflockd                                       # TCP only, on 127.0.0.1:6388
dflockd --http-port 6389                      # TCP + HTTP REST
```

Or from a checkout:

```bash
make build && ./dflockd
```

## Usage

### Go

```go
import (
    "context"
    "time"

    "github.com/mtingers/dflockd/client"
)

l := &client.Lock{
    Key:            "deploy-job",
    AcquireTimeout: 10 * time.Second,
    LeaseTTL:       60,                       // seconds; 0 = server default
    Servers:        []string{"127.0.0.1:6388"},
}
got, err := l.Acquire(context.Background())
if err != nil { /* network error */ }
if !got     { /* timed out */ }
defer l.Release(context.Background())
```

`Lock.Acquire` runs background lease renewal at half the TTL, so a holder that crashes loses the lock within `LeaseTTL` seconds without explicit pings. Same shape for `Semaphore` plus a `Limit` field. See the [Go client docs](https://mtingers.github.io/dflockd/client/) for the full API (two-phase enqueue/wait, sharding, TLS, auth).

### HTTP

```bash
sid=$(curl -sX POST http://localhost:6389/v1/sessions | jq -r .session_id)
curl -sX POST http://localhost:6389/v1/locks/deploy-job \
     -H "X-Dflockd-Session: $sid" \
     -d '{"acquire_timeout_s": 10, "lease_ttl_s": 60}'
# → {"status":"ok","token":"...","lease_ttl_s":60}
curl -sX DELETE http://localhost:6389/v1/sessions/$sid
```

See the [HTTP API docs](https://mtingers.github.io/dflockd/http-api/). The [OpenAPI 3.1 spec](https://mtingers.github.io/dflockd/openapi.json) is also served live by the running server at `GET /v1/openapi.json` (auth-exempt) for codegen tools.

### Raw TCP protocol

The wire format is three newline-terminated UTF-8 lines (`command\nkey\narg\n`). Useful for debugging or for languages without a ready-made client:

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

The connection must stay open. By default, locks are auto-released on disconnect. See the [protocol reference](https://mtingers.github.io/dflockd/architecture/protocol/) for every command.

## Fencing tokens

Tokens are 32 lowercase hex chars: `<16-hex monotonic prefix><16-hex random salt>`. The prefix is a server-monotonic uint64 (big-endian) that strictly increases on every grant — including across server restarts on a non-regressing wall clock — so a token also serves as a [fencing token](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html). Compare two tokens **for the same key** lexicographically (or parse the prefix as a uint64): the larger token was issued later, so a downstream resource can store the most recent token it has seen and reject any write whose token compares less.

```go
tok, _, _ := client.Acquire(c, "row:42", 5*time.Second)
fence, _ := client.FenceFromToken(tok)        // uint64 — pass to your DB / blob store
```

Fences are per-server-instance: each dflockd process is the source of truth for the keys clients route to it (CRC32 sharding). Caveats: tokens for *different* keys aren't meaningfully ordered against each other; a `Limit>1` semaphore issues a distinct fence per grant, not per resource; and cross-restart monotonicity assumes the wall clock doesn't regress between processes.

## Performance

One acquire + release per operation over a persistent TCP connection. Median of three runs on v1.16.0; Apple M1 (MacBook Air, 8 GB RAM) with server and clients on localhost. Unique keys, no contention.

| Workers | Rounds | Ops | Throughput | Mean | p50 | p99 |
|---|---|---|---|---|---|---|
| 1 | 1,000 | 1,000 | 13,440 ops/s | 0.074 ms | 0.062 ms | 0.178 ms |
| 10 | 1,000 | 10,000 | 45,861 ops/s | 0.217 ms | 0.215 ms | 0.382 ms |
| 50 | 1,000 | 50,000 | 83,516 ops/s | 0.590 ms | 0.513 ms | 2.096 ms |
| 100 | 1,000 | 100,000 | 87,448 ops/s | 1.124 ms | 0.935 ms | 4.192 ms |
| 200 | 1,000 | 200,000 | 89,706 ops/s | 2.200 ms | 1.864 ms | 7.892 ms |
| 500 | 1,000 | 500,000 | 85,124 ops/s | 5.814 ms | 5.117 ms | 18.554 ms |

The HTTP transport adds ~2× latency vs. raw TCP at the same concurrency (extra round trip overhead, JSON encode/decode, middleware chain). Run your own benchmarks with `go run ./cmd/bench --help`.

## Configuration

CLI flags take precedence over environment variables. The full table is in the [server docs](https://mtingers.github.io/dflockd/server/); the most common knobs:

| Flag | Env | Default | Description |
|---|---|---|---|
| `--host` | `DFLOCKD_HOST` | `127.0.0.1` | Bind address |
| `--port` | `DFLOCKD_PORT` | `6388` | TCP port |
| `--http-port` | `DFLOCKD_HTTP_PORT` | `0` (disabled) | HTTP REST port |
| `--default-lease-ttl` | `DFLOCKD_DEFAULT_LEASE_TTL_S` | `33` | Default lease TTL (seconds) |
| `--max-locks` | `DFLOCKD_MAX_LOCKS` | `1024` | Cluster-wide cap on unique keys |
| `--max-waiters` | `DFLOCKD_MAX_WAITERS` | `0` | Per-key waiter cap (0 = unlimited) |
| `--max-connections-per-ip` | `DFLOCKD_MAX_CONNECTIONS_PER_IP` | `0` | Per-IP TCP cap |
| `--http-rate-limit-per-ip` | `DFLOCKD_HTTP_RATE_LIMIT_PER_IP` | `0` | Per-IP HTTP req/s |
| `--tls-cert` / `--tls-key` | `DFLOCKD_TLS_CERT` / `_KEY` | *(unset)* | Enable TLS on both listeners |
| `--auth-token-file` | `DFLOCKD_AUTH_TOKEN_FILE` | *(unset)* | Shared-secret auth token |
| `--auto-release-on-disconnect` | `DFLOCKD_AUTO_RELEASE_ON_DISCONNECT` | `true` | Release tokens on disconnect |

## Client libraries

- **Go** (in-repo) — `go get github.com/mtingers/dflockd/client` ([docs](https://mtingers.github.io/dflockd/client/))
- **Python** — [dflockd-client-py](https://github.com/mtingers/dflockd-client-py)
- **TypeScript** — [dflockd-client-ts](https://github.com/mtingers/dflockd-client-ts)

The TCP wire format is stable; client libraries hash keys with CRC-32 (IEEE) so the same key resolves to the same server in any language.

## Tests

```bash
go test ./...             # full suite
go test -race ./...       # with the race detector
make complexity           # per-function lines + cyclomatic complexity
make openapi-sync         # mirror internal/httpapi/openapi.json → docs/
```

## Versioning

dflockd v2 dropped the pub/sub / SSE layer that earlier releases shipped. The pre-refactor source is preserved under `old/` (gitignored, build-tagged out) for reference. Current docs describe only the v2 surface.
