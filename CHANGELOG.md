# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
