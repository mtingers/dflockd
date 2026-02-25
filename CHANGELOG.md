# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v1.10.0] - 2026-02-24

### Changed

- **Server: 64-shard lock manager** — replaced single `sync.Mutex` with 64 `fnv32`-keyed shards and a separate `connMu` for connection tracking, reducing contention under concurrent load
- **Server: buffered CSPRNG token generation** — token generation now buffers 4096 bytes (256 tokens) per `crypto/rand` syscall, amortising syscall overhead by 256×
- **Server: pre-formatted protocol responses** — common response status lines (`ok`, `timeout`, `error_*`, `queued`) are pre-computed `[]byte` constants; dynamic responses use `strconv.AppendInt` instead of `fmt.Sprintf`
- **Server: O(1) waiter queue grant** — replaced O(n) `copy` on every waiter grant with a `WaiterHead` index; the slice is compacted when more than half is consumed
- **Server: deferred waiter allocation** — `Acquire` no longer allocates a waiter struct on the fast path when capacity is immediately available
- **Benchmark: persistent connections** — `cmd/bench` now uses `client.Dial` once per worker goroutine and calls the low-level `Acquire`/`Release` API, measuring lock latency instead of TCP connection overhead
- **Benchmark: `--connections` flag** — optional flag to control the number of persistent connections per worker (default: 1)

[v1.10.0]: https://github.com/mtingers/dflockd/releases/tag/v1.10.0

## [v1.9.0] - 2026-02-24

### Added

- `cmd/bench` — Go benchmark tool for measuring lock acquire/release latency and throughput under concurrent load
  - Configurable workers, rounds, key prefix, acquire timeout, lease TTL, and server addresses
  - Reports total ops, wall time, throughput (ops/s), mean, min, max, p50, p99, and stdev latencies

[v1.9.0]: https://github.com/mtingers/dflockd/releases/tag/v1.9.0

## [v1.8.1] - 2026-02-24

### Fixed

- **Client: data races in `Lock` and `Semaphore` types** — multiple fields (`conn`, `token`, `lease`, `cancelRenew`) were accessed without holding the mutex in `Acquire`, `Enqueue`, `Wait`, `Release`, and `Close` methods
- **Client: `Lock.Wait` and `Semaphore.Wait` connection leak on timeout** — the connection was left open in an ambiguous state after a Wait timeout (enqueue state consumed, no lock held); now properly closed
- **Client: `Lock.Wait` and `Semaphore.Wait` connection leak on non-timeout errors** — connections were not closed on server errors, only on context cancellation
- **Client: `Enqueue` and `SemEnqueue` missing `error_max_waiters` handling** — the `error_max_waiters` server response was not parsed, causing a generic `ErrServer` instead of `ErrMaxWaiters`
- **Client: `ErrMaxWaiters` sentinel error added** — new sentinel for the `error_max_waiters` protocol status, consistent with `ErrMaxLocks` and `ErrLimitMismatch`
- **Client: `startRenew` goroutine leak** — old renewal goroutines were cancelled but not waited on before starting new ones, risking concurrent renewals; now calls `stopRenew()` which waits for the old goroutine to exit
- **Client: `readLine` off-by-one buffer overflow** — buffer was `[maxResponseBytes + 1]byte` but the overflow check used `>` instead of `>=`, allowing one extra byte to be written past the intended limit
- **Server: protocol error recovery disconnect** — read-level errors (timeout, line too long) now disconnect the client since the protocol stream may be desynchronized; parse-level errors continue safely
- **Server: `handleConn` auth error could ignore `ReadRequest` error** — when the auth read returned an error, the nil `req` was still accessed for `req.Cmd`; now returns `error_auth` on any read failure during auth
- **Server: concurrent map access in `connOwned` tracking** — lock ownership tracking was not consistently protected by the mutex in all code paths
- **Config: `GCMaxIdleTime` validation** — negative values for `--gc-max-idle` were silently accepted; now `0` is explicitly allowed (meaning "prune immediately") while the env var override is correctly applied
- **Config: `WriteTimeout` of 0 handling** — a zero write timeout no longer sets an immediate deadline on writes; it correctly disables write deadlines

### Added

- `--auth-token-file` flag and `DFLOCKD_AUTH_TOKEN_FILE` env var for loading the auth token from a file instead of passing it on the command line (avoids leaking the secret in the process list)

### Documentation

- Added `ErrMaxWaiters` to the client error table
- Added `--auth-token-file` and `DFLOCKD_AUTH_TOKEN_FILE` to README, server docs, and configuration tables
- Documented auth token resolution priority order

[v1.8.1]: https://github.com/mtingers/dflockd/releases/tag/v1.8.1

## [v1.8.0] - 2026-02-24

### Added

- Graceful shutdown drain with configurable timeout: active connections are given time to finish before being force-closed
- `--shutdown-timeout` flag and `DFLOCKD_SHUTDOWN_TIMEOUT_S` env var (default: 30 seconds, 0 = wait forever)
- Connection tracking via `sync.Map` to enable force-close on shutdown deadline
- Integration tests for graceful shutdown drain and force-close scenarios

### Fixed

- Indentation bug in protocol error write block (`server.go:202-204`)

### Changed

- Extracted shared accept-loop and shutdown logic into private `serve()` method, eliminating duplication between `Run` and `RunOnListener`

[v1.8.0]: https://github.com/mtingers/dflockd/releases/tag/v1.8.0

## [v1.7.0] - 2026-02-24

### Added

- `--max-connections` flag and `DFLOCKD_MAX_CONNECTIONS` env var to limit concurrent connections (default: 0 = unlimited)
- `--max-waiters` flag and `DFLOCKD_MAX_WAITERS` env var to limit waiter queue depth per lock/semaphore key (default: 0 = unlimited)
- `ErrMaxWaiters` sentinel error and `error_max_waiters` protocol status for rejected waiters
- `--write-timeout` flag and `DFLOCKD_WRITE_TIMEOUT_S` env var to set write deadlines on responses (default: 5 seconds)
- `writeResponse` server helper that sets and clears `SetWriteDeadline` on every write
- Unit tests for max waiters across all four enqueue paths (FIFOAcquire, FIFOEnqueue, SemAcquire, SemEnqueue)
- Integration tests for max connections, max waiters, and write timeout

### Changed

- Default bind address changed from `0.0.0.0` to `127.0.0.1` for safer defaults

[v1.7.0]: https://github.com/mtingers/dflockd/releases/tag/v1.7.0

## [v1.6.0] - 2026-02-18

### Added

- Optional token-based authentication for client connections
- Server flag `--auth-token` and env var `DFLOCKD_AUTH_TOKEN` to set a shared secret
- `auth` protocol command for clients to authenticate before issuing other commands
- `Authenticate` client function for low-level auth on a `*Conn`
- `AuthToken` field on `Lock` and `Semaphore` high-level types for automatic auth on connect
- `ErrAuth` sentinel error returned when authentication fails
- Constant-time token comparison using `crypto/subtle` to prevent timing attacks
- Auth integration tests for both server and client packages
- Documentation for auth in protocol spec, server docs, client docs, and examples

[v1.6.0]: https://github.com/mtingers/dflockd/releases/tag/v1.6.0

## [v1.4.0] - 2026-02-18

### Added

- Runtime `stats` protocol command returning a JSON snapshot of active connections, held locks, semaphores, and idle entries awaiting GC
- `Stats()` method on `LockManager` for programmatic access to server state
- Documentation for `stats` command in protocol spec, server docs, and examples

[v1.4.0]: https://github.com/mtingers/dflockd/releases/tag/v1.4.0

## [v1.3.0] - 2026-02-18

### Added

- Optional TLS encryption for client-server communication
- Server flags `--tls-cert` / `--tls-key` and env vars `DFLOCKD_TLS_CERT` / `DFLOCKD_TLS_KEY` to enable TLS
- `DialTLS` client function for low-level TLS connections
- `TLSConfig` field on `Lock` and `Semaphore` high-level types
- TLS integration tests for both server and client packages
- `internal/testutil` package with ephemeral self-signed certificate helper for tests

[v1.3.0]: https://github.com/mtingers/dflockd/releases/tag/v1.3.0

## [v1.2.0] - 2026-02-16

### Added

- Distributed key-based semaphore support allowing up to N concurrent holders per key
- Five new protocol commands: `sl` (acquire), `sr` (release), `sn` (renew), `se` (enqueue), `sw` (wait)
- Per-request `limit` parameter; first acquirer sets the limit, subsequent requests must match or receive `error_limit_mismatch`
- `ErrLimitMismatch` sentinel error and `error_limit_mismatch` protocol status
- Protocol error code 13 for zero or negative semaphore limit
- Low-level client functions: `SemAcquire`, `SemRelease`, `SemRenew`, `SemEnqueue`, `SemWait`
- High-level `Semaphore` type with `Acquire`, `Enqueue`, `Wait`, `Release`, `Close`, `Token` and automatic background lease renewal
- Semaphore lease expiry, GC pruning, and disconnect cleanup integrated into existing background loops
- Semaphore keys share the `--max-locks` budget with lock keys
- Comprehensive test suite for semaphore functionality (62 new tests across all layers)
- Documentation for semaphore commands in protocol spec, client docs, server docs, and README

### Fixed

- `CleanupConnection` early return when a connection had no lock keys, which would skip cleanup of other connection state

[v1.2.0]: https://github.com/mtingers/dflockd/releases/tag/v1.2.0

## [v1.1.0] - 2026-02-16

### Added

- Go client package (`client/`) with low-level protocol API (`Acquire`, `Release`, `Renew`, `Enqueue`, `Wait`) and high-level `Lock` type
- Automatic background lease renewal in `Lock` type with configurable `RenewRatio`
- CRC32-based sharding (`CRC32Shard`) matching the Python client's `stable_hash_shard`
- Functional options pattern (`WithLeaseTTL`) for optional protocol parameters
- Context cancellation support for `Lock.Acquire` and `Lock.Wait`
- Integration test suite for the Go client (12 tests)

[v1.1.0]: https://github.com/mtingers/dflockd/releases/tag/v1.1.0

## [v1.0.0] - 2026-02-15

### Added

- Complete server rewrite from Python to Go, delivering a single static binary with no runtime dependencies
- Standard Go project layout (`cmd/dflockd/`, `internal/config/`, `internal/lock/`, `internal/protocol/`, `internal/server/`)
- Comprehensive test suite (49 tests) covering lock operations, protocol parsing, and integration scenarios
- `--version` flag printing the embedded version string
- Cross-platform binary builds via GoReleaser (linux/darwin/windows on amd64/arm64, tar.gz archives, zip for Windows, SHA256 checksums)
- Automated GitHub Releases workflow triggered on `v*` tag push
- Dependabot configuration for Go module updates

### Changed

- CI and docs workflows updated for Go codebase
- Makefile simplified for Go build toolchain with `VERSION` ldflags support

### Removed

- Python server, async client, and sync client
- TypeScript client
- Python test suite, benchmark scripts, and example scripts
- Sharding module and documentation
- Built `site/` artifacts

[v1.0.0]: https://github.com/mtingers/dflockd/releases/tag/v1.0.0

## [v0.5.0] - 2026-02-14

### Added

- Two-phase lock acquisition with `e` (enqueue) and `w` (wait) protocol commands
- `fifo_enqueue()` and `fifo_wait()` server functions for split enqueue/wait flow
- `enqueue()` and `wait()` module-level functions in async and sync clients
- `DistributedLock.enqueue()` and `DistributedLock.wait()` methods in async and sync clients
- `Status.acquired` and `Status.queued` server response statuses
- `NotEnqueuedError` and `EnqueuedState` server internals for two-phase state tracking
- Connection cleanup for two-phase enqueued state on disconnect
- Two-phase example script (`examples/two_phase_demo.py`)
- Documentation for two-phase flow in protocol, client, architecture, and examples docs

[v0.5.0]: https://github.com/mtingers/dflockd/releases/tag/v0.5.0

## [v0.4.1] - 2026-02-07

### Added

- `--auto-release-on-disconnect` / `--no-auto-release-on-disconnect` CLI flag

### Fixed

- `DFLOCKD_DFLOCKD_READ_TIMEOUT_S` env var typo in README (now `DFLOCKD_READ_TIMEOUT_S`)
- Server configuration env var names in README missing `DFLOCKD_` prefix
- `MAX_LOCKS` default in README corrected from `256` to `1024`

[v0.4.1]: https://github.com/mtingers/dflockd/releases/tag/v0.4.1

## [v0.4.0] - 2026-02-07

### Added

- Documentation site (MkDocs Material) with architecture, configuration, client, protocol, and sharding guides
- `DFLOCKD_AUTO_RELEASE_ON_DISCONNECT` documented in server configuration

### Fixed

- Pyright CI dependency
- Ruff dev dependency

### Changed

- Bump `actions/checkout` from 4 to 6
- Bump `actions/setup-python` from 5 to 6
- Bump `actions/upload-pages-artifact` from 3 to 4
- Bump `astral-sh/setup-uv` from 4 to 7
- Update `uv-build` requirement from >=0.9.28,<0.10.0 to >=0.9.28,<0.11.0

[v0.4.0]: https://github.com/mtingers/dflockd/releases/tag/v0.4.0

## [v0.3.0] - 2026-02-07

### Added

- Async lock server with strict FIFO ordering per key
- Automatic lease expiry with configurable TTL and sweep interval
- Background garbage collection of idle lock state
- Automatic lock release on client disconnect
- Async client (`dflockd.client`) with `DistributedLock` context manager
- Sync client (`dflockd.sync_client`) with `DistributedLock` context manager
- Background lease renewal for both async and sync clients
- Multi-server sharding with `stable_hash_shard` (CRC-32)
- Custom sharding strategy support via `ShardingStrategy` callable
- Configurable `renew_ratio` for controlling renewal frequency
- CLI with `--host`, `--port`, `--default-lease-ttl`, `--max-locks`, and other flags
- Environment variable configuration with `DFLOCKD_` prefix (overrides CLI flags)
- TypeScript client (`ts/`)
- Async and sync benchmark scripts (`examples/bench_async.py`, `examples/bench_sync.py`)
- CI workflow with linting, type checking, and tests (Python 3.13, 3.14)
- GitHub Pages documentation deployment workflow

[v0.3.0]: https://github.com/mtingers/dflockd/releases/tag/0.3.0
