# dflockd

A lightweight distributed lock server with FIFO ordering, automatic lease expiry, semaphores, and pub/sub signals. Speaks both a simple line-based TCP protocol and an optional HTTP REST + SSE API on top of the same shared state.

## Features

- **Two transports, one shared state** — connect over the native line-based TCP protocol or, opt-in, an HTTP REST + SSE API ([HTTP API docs](http-api.md), [OpenAPI spec](openapi.json)). HTTP sessions and TCP clients contending on the same key share a single FIFO queue.
- **Strict FIFO ordering** — waiters are granted locks in the order they enqueue, per key
- **Two-phase lock acquisition** — split enqueue and wait to notify external systems between joining the queue and blocking
- **Automatic lease expiry** — held locks expire if not renewed, preventing deadlocks
- **Disconnect cleanup** — held locks and semaphore slots are released automatically when a client disconnects, unless that safety net is disabled
- **Pub/sub signals** — publish messages to channels with wildcard pattern matching (`*`, `>`) and optional queue groups for load-balanced delivery
- **Operations endpoints** — unauthenticated `/health` and `/ready` plus authenticated Prometheus `/metrics` (when `--http-port` is set)
- **Per-IP guardrails** — TCP and HTTP connection caps, HTTP session caps, and an HTTP token-bucket rate limiter, all tunable per remote IP
- **Zero dependencies** — single Go binary
- **Go client library** — high-level `Lock` type with automatic renewal and sharding, plus low-level protocol API
- **Runtime stats** — query active connections, held locks, semaphores, signal channels, and idle entries via the `stats` command or `GET /v1/stats`
- **Built-in benchmarking** — `cmd/bench` measures lock throughput and latency under concurrent load
- **Simple wire protocol** — line-based UTF-8 over TCP, easy to integrate from any language

## Quick example

Native TCP protocol:

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

By default, locks are auto-released on disconnect, so the connection must stay open.

HTTP API (start the server with `--http-port 6389`):

```bash
sid=$(curl -sX POST http://localhost:6389/v1/sessions | jq -r .session_id)
curl -sX POST http://localhost:6389/v1/locks/my-key \
     -H "X-Dflockd-Session: $sid" \
     -d '{"acquire_timeout_s": 10, "lease_ttl_s": 60}'
# → {"status":"ok","token":"...","lease_ttl_s":60}
curl -sX DELETE http://localhost:6389/v1/sessions/$sid
```

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

All workers use unique keys (no contention). Run your own: `go run ./cmd/bench --help`

## Getting started

- [Installation](getting-started/installation.md) — build from source or `go install`
- [Quick Start](getting-started/quickstart.md) — run the server and acquire your first lock
- [Examples](getting-started/examples.md) — TCP protocol examples and Go client usage

## Client libraries

- [Go](client.md) — in-repo `client` package with automatic renewal, sharding, two-phase locking, and signals ([docs](client.md))
- [Python](https://github.com/mtingers/dflockd-client-py) — async/sync client with `DistributedLock` context manager ([docs](https://mtingers.github.io/dflockd-client-py/))
- [TypeScript](https://github.com/mtingers/dflockd-client-ts) — TypeScript/JavaScript client ([docs](https://mtingers.github.io/dflockd-client-ts/))

## Reference

- [Server Configuration](server.md) — CLI flags, environment variables, TLS, authentication, and tuning
- [HTTP REST + SSE API](http-api.md) — sessions, locks, semaphores, two-phase enqueue/wait, signal pub/sub via SSE, plus `/health`, `/ready`, `/metrics`
- [OpenAPI 3.1 spec](openapi.json) — machine-readable contract; also served at `GET /v1/openapi.json` when the HTTP API is enabled
- [Wire Protocol](architecture/protocol.md) — line-based TCP protocol specification, commands, and error codes
- [Architecture](architecture/overview.md) — server internals, lock state, concurrency model, and signal delivery
- [Changelog](changelog.md) — release history
