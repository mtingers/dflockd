# dflockd

A lightweight distributed lock server using a simple line-based TCP protocol with FIFO ordering, automatic lease expiry, and background renewal.

## What is dflockd?

dflockd is a standalone TCP server that provides distributed coordination primitives. Clients communicate using a minimal 3-line text protocol, making it easy to integrate from any language or even from the command line using `nc` or `telnet`.

## Feature Matrix

| Feature | Protocol Commands | Go Client API |
|---------|-------------------|---------------|
| Exclusive Locks | `l`, `r`, `n`, `e`, `w` | `Lock`, `Acquire`, `Release`, `Renew`, `Enqueue`, `Wait` |
| Semaphores | `sl`, `sr`, `sn`, `se`, `sw` | `Semaphore`, `SemAcquire`, `SemRelease`, `SemRenew` |
| Read-Write Locks | `rl`, `wl`, `rr`, `wr`, `rn`, `wn`, `re`, `we`, `rw`, `ww` | `RWLock`, `RLock`, `WLock`, `RUnlock`, `WUnlock` |
| Leader Election | `elect`, `resign`, `observe`, `unobserve` | `LeaderConn`, `Elect`, `Resign`, `Observe` |
| Atomic Counters | `incr`, `decr`, `get`, `cset` | `Incr`, `Decr`, `GetCounter`, `SetCounter` |
| KV Store | `kset`, `kget`, `kdel`, `kcas` | `KVSet`, `KVGet`, `KVDel`, `KVCAS` |
| Lists/Queues | `lpush`, `rpush`, `lpop`, `rpop`, `llen`, `lrange`, `blpop`, `brpop` | `LPush`, `RPush`, `LPop`, `RPop`, `LLen`, `LRange`, `BLPop`, `BRPop` |
| Pub/Sub Signaling | `listen`, `unlisten`, `signal` | `SignalConn`, `Listen`, `Unlisten`, `Emit` |
| Key Watch | `watch`, `unwatch` | `WatchConn`, `Watch`, `Unwatch` |
| Barriers | `bwait` | `BarrierWait` |
| Authentication | `auth` | `Authenticate` |
| Stats | `stats` | `Stats` |

## Key Design Decisions

- **FIFO ordering** -- waiters are granted locks in the order they enqueued
- **Lease-based expiry** -- locks auto-expire if the holder disconnects or fails to renew
- **Fencing tokens** -- monotonically increasing tokens to detect stale holders
- **Auto-release on disconnect** -- configurable; releases all locks held by a disconnected client
- **64-way internal sharding** -- low-contention concurrent access across keys
- **Simple text protocol** -- 3 lines per request (command, key, argument), one-line response

## Getting Started

Head to the [Installation](getting-started/installation.md) guide, then try the [Quick Start](getting-started/quickstart.md).
