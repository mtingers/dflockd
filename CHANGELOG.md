# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
