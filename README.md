# dflockd (Go)

<!--toc:start-->

- [dflockd (Go)](#dflockd-go)
  - [Build](#build)
  - [Run](#run)
  - [Configuration](#configuration)
  - [Tests](#tests)
  - [Protocol](#protocol)
    - [Commands](#commands)
    - [Example session with netcat](#example-session-with-netcat)
    - [Two-phase example](#two-phase-example)
  - [Client Libraries](#client-libraries) - [Go client quick start](#go-client-quick-start)
      <!--toc:end-->

Go implementation of the dflockd distributed lock server.

[Read the docs here](https://mtingers.github.io/dflockd/)

## Build

```bash
go build -o dflockd ./cmd/dflockd
```

## Run

```bash
./dflockd
```

The server listens on `0.0.0.0:6388` by default.

## Configuration

All settings can be passed as CLI flags or environment variables. Environment variables take precedence.

| Flag                                                               | Env var                              | Default   | Description                            |
| ------------------------------------------------------------------ | ------------------------------------ | --------- | -------------------------------------- |
| `--host`                                                           | `DFLOCKD_HOST`                       | `0.0.0.0` | Bind address                           |
| `--port`                                                           | `DFLOCKD_PORT`                       | `6388`    | Bind port                              |
| `--default-lease-ttl`                                              | `DFLOCKD_DEFAULT_LEASE_TTL_S`        | `33`      | Default lock lease duration (seconds)  |
| `--lease-sweep-interval`                                           | `DFLOCKD_LEASE_SWEEP_INTERVAL_S`     | `1`       | Lease expiry check interval (seconds)  |
| `--gc-interval`                                                    | `DFLOCKD_GC_LOOP_SLEEP`              | `5`       | Lock state GC interval (seconds)       |
| `--gc-max-idle`                                                    | `DFLOCKD_GC_MAX_UNUSED_TIME`         | `60`      | Idle seconds before pruning lock state |
| `--max-locks`                                                      | `DFLOCKD_MAX_LOCKS`                  | `1024`    | Maximum number of unique lock keys     |
| `--read-timeout`                                                   | `DFLOCKD_READ_TIMEOUT_S`             | `23`      | Client read timeout (seconds)          |
| `--tls-cert`                                                       | `DFLOCKD_TLS_CERT`                   | *(unset)* | Path to TLS certificate PEM file       |
| `--tls-key`                                                        | `DFLOCKD_TLS_KEY`                    | *(unset)* | Path to TLS private key PEM file       |
| `--auto-release-on-disconnect` / `--no-auto-release-on-disconnect` | `DFLOCKD_AUTO_RELEASE_ON_DISCONNECT` | `true`    | Release locks on client disconnect     |
| `--debug`                                                          | `DFLOCKD_DEBUG`                      | `false`   | Enable debug logging                   |

Example:

```bash
./dflockd --port 7000 --max-locks 512
# or
DFLOCKD_PORT=7000 DFLOCKD_MAX_LOCKS=512 ./dflockd
```

## TLS

To enable TLS encryption, provide both a certificate and private key:

```bash
./dflockd --tls-cert /path/to/cert.pem --tls-key /path/to/key.pem
# or
DFLOCKD_TLS_CERT=/path/to/cert.pem DFLOCKD_TLS_KEY=/path/to/key.pem ./dflockd
```

Both `--tls-cert` and `--tls-key` must be provided together. When TLS is enabled, clients must connect using TLS:

```go
l := &client.Lock{
    Key:       "my-resource",
    Servers:   []string{"127.0.0.1:6388"},
    TLSConfig: &tls.Config{},  // configure CA, etc.
}
```

## Tests

```bash
go test ./... -v
```

## Protocol

The wire protocol is identical to the Python server. Each request is 3 newline-terminated UTF-8 lines (`command\nkey\narg\n`). Each response is a single newline-terminated line.

### Commands

**Lock (`l`)** — acquire a lock, blocking up to `timeout_s` seconds.

```
l\n<key>\n<timeout_s> [<lease_ttl_s>]\n
```

Response: `ok <token> <lease_ttl>\n` | `timeout\n` | `error_max_locks\n`

**Release (`r`)** — release a held lock.

```
r\n<key>\n<token>\n
```

Response: `ok\n` | `error\n`

**Renew (`n`)** — renew the lease on a held lock.

```
n\n<key>\n<token> [<lease_ttl_s>]\n
```

Response: `ok <seconds_remaining>\n` | `error\n`

**Enqueue (`e`)** — join the lock queue without blocking (two-phase step 1).

```
e\n<key>\n[<lease_ttl_s>]\n
```

Response: `acquired <token> <lease_ttl>\n` | `queued\n` | `error_max_locks\n`

**Wait (`w`)** — block until the enqueued lock is granted (two-phase step 2).

```
w\n<key>\n<timeout_s>\n
```

Response: `ok <token> <lease_ttl>\n` | `timeout\n` | `error\n`

**Semaphore Acquire (`sl`)** — acquire a semaphore slot, blocking up to `timeout_s` seconds.

```
sl\n<key>\n<timeout_s> <limit> [<lease_ttl_s>]\n
```

Response: `ok <token> <lease_ttl>\n` | `timeout\n` | `error_max_locks\n` | `error_limit_mismatch\n`

**Semaphore Release (`sr`)** — release a held semaphore slot.

```
sr\n<key>\n<token>\n
```

Response: `ok\n` | `error\n`

**Semaphore Renew (`sn`)** — renew the lease on a held semaphore slot.

```
sn\n<key>\n<token> [<lease_ttl_s>]\n
```

Response: `ok <seconds_remaining>\n` | `error\n`

**Semaphore Enqueue (`se`)** — join the semaphore queue without blocking (two-phase step 1).

```
se\n<key>\n<limit> [<lease_ttl_s>]\n
```

Response: `acquired <token> <lease_ttl>\n` | `queued\n` | `error_max_locks\n` | `error_limit_mismatch\n`

**Semaphore Wait (`sw`)** — block until the enqueued semaphore slot is granted (two-phase step 2).

```
sw\n<key>\n<timeout_s>\n
```

Response: `ok <token> <lease_ttl>\n` | `timeout\n` | `error\n`

**Stats (`stats`)** — query server runtime state (connections, locks, semaphores, idle entries).

```
stats\n_\n\n
```

Response: `ok <json>\n`

The JSON payload includes `connections`, `locks`, `semaphores`, `idle_locks`, and `idle_semaphores`. Key and arg lines are read but ignored.

### Example session with netcat

```bash
# Terminal 1: start the server
./dflockd

# Terminal 2: acquire and release a lock
$ nc localhost 6388
l
mykey
10
ok a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4 33
r
mykey
a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4
ok
```

### Two-phase example

```bash
$ nc localhost 6388
e
mykey

acquired a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4 33
w
mykey
10
ok a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4 33
r
mykey
a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4
ok
```

### Semaphore example

```bash
$ nc localhost 6388
sl
worker-pool
10 3
ok a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4 33
sr
worker-pool
a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4
ok
```

### Stats example

```bash
$ nc localhost 6388
stats
_

ok {"connections":1,"locks":[],"semaphores":[],"idle_locks":[],"idle_semaphores":[]}
```

### Go semaphore quick start

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
    s := &client.Semaphore{
        Key:            "worker-pool",
        Limit:          3,
        AcquireTimeout: 10 * time.Second,
        Servers:        []string{"127.0.0.1:6388"},
    }

    ok, err := s.Acquire(context.Background())
    if err != nil {
        log.Fatal(err)
    }
    if !ok {
        log.Fatal("timed out waiting for semaphore slot")
    }
    defer s.Release(context.Background())

    fmt.Println("semaphore slot acquired, doing work...")
}
```

## Client Libraries

- **Go** (in-repo) — `go get github.com/mtingers/dflockd/client` ([docs](https://mtingers.github.io/dflockd/client/))
- [Python client](https://github.com/mtingers/dflockd-client-py)
- [TypeScript client](https://github.com/mtingers/dflockd-client-ts)

### Go client quick start

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

The `Lock` type handles server selection (optional sharding), lease renewal in
the background, and context cancellation. For lower-level control,
use `client.Dial` with `client.Acquire`/`client.Release`/`client.Renew` directly.
