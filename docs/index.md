# dflockd

A lightweight distributed lock server using a simple line-based TCP protocol with FIFO ordering, automatic lease expiry, and background renewal.

## Features

- **Strict FIFO ordering** — waiters are granted locks in the order they enqueue, per key
- **Two-phase lock acquisition** — split enqueue and wait to notify external systems between joining the queue and blocking
- **Automatic lease expiry** — held locks expire if not renewed, preventing deadlocks
- **Disconnect cleanup** — held locks and semaphore slots are released automatically when a client disconnects, unless that safety net is disabled
- **Pub/sub signals** — publish messages to channels with wildcard pattern matching (`*`, `>`) and optional queue groups for load-balanced delivery
- **Zero dependencies** — single Go binary
- **Go client library** — high-level `Lock` type with automatic renewal and sharding, plus low-level protocol API
- **Runtime stats** — query active connections, held locks, semaphores, signal channels, and idle entries via the `stats` command
- **Built-in benchmarking** — `cmd/bench` measures lock throughput and latency under concurrent load
- **Simple wire protocol** — line-based UTF-8 over TCP, easy to integrate from any language

## Quick example

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
- [Wire Protocol](architecture/protocol.md) — line-based TCP protocol specification, commands, and error codes
- [Architecture](architecture/overview.md) — server internals, lock state, concurrency model, and signal delivery
- [Changelog](changelog.md) — release history
