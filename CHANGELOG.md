# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2026-02-07

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

[0.3.0]: https://github.com/mtingers/dflockd/releases/tag/0.3.0
