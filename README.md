# dflockd (Go)

Go implementation of the dflockd distributed lock server.

## Build

```bash
cd go
go build -o dflockd ./cmd/dflockd
```

## Run

```bash
./dflockd
```

The server listens on `0.0.0.0:6388` by default.

## Configuration

All settings can be passed as CLI flags or environment variables. Environment variables take precedence.

| Flag | Env var | Default | Description |
|------|---------|---------|-------------|
| `--host` | `DFLOCKD_HOST` | `0.0.0.0` | Bind address |
| `--port` | `DFLOCKD_PORT` | `6388` | Bind port |
| `--default-lease-ttl` | `DFLOCKD_DEFAULT_LEASE_TTL_S` | `33` | Default lock lease duration (seconds) |
| `--lease-sweep-interval` | `DFLOCKD_LEASE_SWEEP_INTERVAL_S` | `1` | Lease expiry check interval (seconds) |
| `--gc-interval` | `DFLOCKD_GC_LOOP_SLEEP` | `5` | Lock state GC interval (seconds) |
| `--gc-max-idle` | `DFLOCKD_GC_MAX_UNUSED_TIME` | `60` | Idle seconds before pruning lock state |
| `--max-locks` | `DFLOCKD_MAX_LOCKS` | `1024` | Maximum number of unique lock keys |
| `--read-timeout` | `DFLOCKD_READ_TIMEOUT_S` | `23` | Client read timeout (seconds) |
| `--auto-release-on-disconnect` / `--no-auto-release-on-disconnect` | `DFLOCKD_AUTO_RELEASE_ON_DISCONNECT` | `true` | Release locks on client disconnect |
| `--debug` | `DFLOCKD_DEBUG` | `false` | Enable debug logging |

Example:

```bash
./dflockd --port 7000 --max-locks 512
# or
DFLOCKD_PORT=7000 DFLOCKD_MAX_LOCKS=512 ./dflockd
```

## Tests

```bash
cd go
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

## Project structure

```

cmd/dflockd/main.go           # entrypoint
internal/
  config/config.go             # configuration (env vars + CLI flags)
  protocol/protocol.go         # wire protocol parsing and formatting
  protocol/protocol_test.go    # protocol unit tests
  lock/lock.go                 # lock manager (all lock logic)
  lock/lock_test.go            # lock unit tests
  server/server.go             # TCP server and connection handler
  server/server_test.go        # integration tests
go.mod
```
