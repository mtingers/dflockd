# dflockd

A lightweight distributed lock server with a simple TCP protocol.

## What is dflockd?

dflockd coordinates access to shared resources across processes and machines. It runs as a single TCP server and speaks a plain-text, line-based protocol -- no special client library required. Connect with `nc`, `telnet`, or the included Go client.

## Why dflockd?

- **Dead simple protocol.** Every request is 3 lines (command, key, argument). Every response is 1 line. You can debug with netcat.
- **Locks that don't get stuck.** Leases auto-expire when holders crash or disconnect. No manual cleanup.
- **Fair queuing.** Waiters get the lock in the order they asked for it (FIFO).
- **Fencing tokens.** Every acquisition returns a monotonically increasing token so downstream systems can reject stale writes.
- **Batteries included.** Beyond locks you get semaphores, read-write locks, leader election, counters, a KV store, queues, pub/sub, key-change watches, and barriers -- all through the same connection.
- **Secure by default.** Optional TLS and shared-secret authentication.

## Features

**Coordination**

- **Distributed Locks** -- exclusive access with automatic lease renewal and FIFO ordering
- **Semaphores** -- bounded concurrency (e.g. limit a worker pool to N)
- **Read-Write Locks** -- many readers or one writer
- **Leader Election** -- elect a leader, get notified on failover
- **Barriers** -- block until N participants arrive

**Data**

- **Atomic Counters** -- increment, decrement, get, and compare-and-set
- **KV Store** -- set/get/delete with optional TTL and atomic compare-and-swap
- **Lists / Queues** -- push, pop (blocking and non-blocking), range queries

**Messaging**

- **Pub/Sub Signaling** -- subscribe to channels with wildcard patterns and optional queue groups for load-balanced delivery
- **Key Watch** -- get notified when any key changes (set, delete, acquire, release, ...)

**Safety & Operations**

- **Fencing Tokens** -- monotonic counters to prevent stale-holder writes
- **Two-Phase Locking** -- enqueue without blocking, then wait separately
- **Auto-Release on Disconnect** -- configurable; cleans up after crashed clients
- **TLS & Authentication** -- encrypted transport and shared-secret auth
- **Graceful Shutdown** -- drains connections before stopping

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

## Getting Started

Head to the [Installation](getting-started/installation.md) guide, then try the [Quick Start](getting-started/quickstart.md).
