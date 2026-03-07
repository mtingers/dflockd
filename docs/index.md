# dflockd

A lightweight distributed lock server using a simple line-based TCP protocol with FIFO ordering, automatic lease expiry, and background renewal.

## Features

- **Strict FIFO ordering** — waiters are granted locks in the order they enqueue, per key
- **Two-phase lock acquisition** — split enqueue and wait to notify external systems between joining the queue and blocking
- **Automatic lease expiry** — held locks expire if not renewed, preventing deadlocks
- **Disconnect cleanup** — locks are released automatically when a client disconnects
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

Locks are auto-released on disconnect, so the connection must stay open.

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
