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

Each request is 3 newline-terminated UTF-8 lines (`command\nkey\narg\n`). The connection must stay open — locks are auto-released on disconnect. See the [protocol reference](https://mtingers.github.io/dflockd/architecture/protocol/) for all commands.

## Performance

Each operation is one lock acquire + release over a persistent TCP connection. Measured on an Apple M1 (MacBook Air, 8 GB RAM) with server and clients on localhost.

| Workers | Ops | Throughput | Mean | p50 | p99 |
|---|---|---|---|---|---|
| 1 | 5,000 | 21,404 ops/s | 0.047 ms | 0.043 ms | 0.161 ms |
| 10 | 10,000 | 45,980 ops/s | 0.215 ms | 0.204 ms | 0.462 ms |
| 50 | 25,000 | 81,798 ops/s | 0.591 ms | 0.458 ms | 2.648 ms |
| 100 | 50,000 | 87,093 ops/s | 1.115 ms | 0.892 ms | 5.094 ms |
| 200 | 50,000 | 84,276 ops/s | 2.128 ms | 1.747 ms | 7.638 ms |
| 500 | 50,000 | 77,781 ops/s | 5.512 ms | 5.110 ms | 14.755 ms |

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
| `--debug` | `DFLOCKD_DEBUG` | `false` | Enable debug logging |

See the [server docs](https://mtingers.github.io/dflockd/server/) for GC tuning, TLS setup, authentication, and other advanced options.

## Client libraries

- **Go** (in-repo) — `go get github.com/mtingers/dflockd/client` ([docs](https://mtingers.github.io/dflockd/client/))
- **Python** — [dflockd-client-py](https://github.com/mtingers/dflockd-client-py) ([docs](https://mtingers.github.io/dflockd-client-py/))
- **TypeScript** — [dflockd-client-ts](https://github.com/mtingers/dflockd-client-ts) ([docs](https://mtingers.github.io/dflockd-client-ts/))

### Go quick start

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/mtingers/dflockd/client"
)

func main() {
    l := &client.Lock{
        Key:            "my-resource",
        AcquireTimeout: 10 * time.Second,
        Servers:        []string{"127.0.0.1:6388"},
    }

    ok, err := l.Acquire(context.Background())
    if err != nil {
        log.Fatal(err)
    }
    if !ok {
        log.Fatal("timed out waiting for lock")
    }
    defer l.Release(context.Background())

    fmt.Println("lock acquired, doing work...")
}
```

## Tests

```bash
go test ./... -v
```

## Benchmarking

```bash
go run ./cmd/bench --workers 100 --rounds 500
```

See `go run ./cmd/bench --help` for all options.
