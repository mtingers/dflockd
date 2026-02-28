# Architecture Overview

## System Architecture

```
                 ┌─────────────┐
                 │  TCP Client  │
                 └──────┬──────┘
                        │
                 ┌──────▼──────┐
                 │   Server    │
                 │  (server/)  │
                 └──────┬──────┘
                        │
          ┌─────────────┼─────────────┐
          │             │             │
    ┌─────▼─────┐ ┌────▼─────┐ ┌────▼─────┐
    │   Lock    │ │  Signal  │ │  Watch   │
    │ Manager   │ │ Manager  │ │ Manager  │
    │  (lock/)  │ │(signal/) │ │ (watch/) │
    └───────────┘ └──────────┘ └──────────┘
```

## Internal Packages

| Package | Responsibility |
|---------|---------------|
| `cmd/dflockd` | Entry point, signal handling, server startup |
| `internal/server` | TCP listener, connection handling, command dispatch, auth, TLS |
| `internal/lock` | Lock manager: locks, semaphores, RW locks, counters, KV, lists, barriers, elections |
| `internal/protocol` | Wire protocol parsing, request/response formatting |
| `internal/signal` | Pub/sub signal delivery, pattern matching, queue groups |
| `internal/watch` | Key change notifications, pattern matching |
| `internal/config` | Flag parsing, env var resolution, validation |
| `client` | Go client library (high-level types + low-level protocol functions) |

## 64-Way Sharding

The `LockManager` partitions state across 64 shards using FNV-1a hashing on the key:

```
key → FNV-1a hash → hash % 64 → shard index
```

Each shard has its own `sync.Mutex`, so operations on different keys rarely contend. A single shard holds:

- `resources` -- lock/semaphore/RW lock state (`ResourceState`)
- `counters` -- atomic counter values
- `kvStore` -- KV entries with optional TTL
- `lists` -- list/queue data with blocking pop waiters
- `barriers` -- barrier participant tracking
- `leaderWatchers` -- leader election observers

Global state (connection-level tracking) uses a separate `connMu` mutex.

## Lease Expiry

Locks, semaphores, and RW locks use lease-based expiry to handle client failures:

1. On acquire, the server records `leaseExpires = now + leaseTTL`
2. A background **lease sweep loop** runs every `--lease-sweep-interval` (default 1s)
3. Expired holders are removed and the next FIFO waiter is granted the lock
4. Clients must periodically **renew** their lease before it expires

The high-level Go client types (`Lock`, `Semaphore`, `RWLock`) automatically run a background renewal goroutine at `renewRatio * leaseTTL` intervals (default 50% of lease TTL).

## Garbage Collection

A background GC loop runs every `--gc-interval` (default 5s) and prunes:

- Lock/semaphore resources with no holders and no waiters that have been idle for `--gc-max-idle` (default 60s)
- Expired KV entries
- Empty counter state past the idle threshold
- Empty list state past the idle threshold
- Tripped or stale barrier state past the idle threshold

## Fencing Tokens

Every lock acquisition returns a **fencing token** -- a globally monotonic `uint64` counter. Fencing tokens prevent stale-holder bugs:

1. Client A acquires lock, gets fence=1
2. Client A's lease expires (network partition)
3. Client B acquires lock, gets fence=2
4. Client A's delayed write arrives at the storage system
5. Storage system rejects the write because fence=1 < current fence=2

The fencing token is incremented atomically (`atomic.Uint64`) across all shards.

## Two-Phase Locking

The enqueue/wait pattern allows non-blocking queue entry:

```
Phase 1: Enqueue
  → Returns "acquired" (immediate) or "queued" (FIFO position reserved)

Phase 2: Wait (only if queued)
  → Blocks until lock is granted or timeout
  → Returns token + lease + fence on success
```

This is useful when a client wants to check queue position or perform other work between enqueue and wait.

## FIFO Ordering

Waiters are granted locks in strict FIFO order. The implementation uses a slice with a head pointer (`WaiterHead`) to avoid shifting:

- New waiters append to the end
- Grants pop from `WaiterHead`
- When more than half the slice is consumed, the waiters are compacted

## Connection Management

Each TCP connection gets:

- A unique `connID` (monotonically increasing `atomic.Uint64`)
- A per-connection write channel for async push delivery (signals, watch events, leader events)
- A per-connection context that cancels on server shutdown

When a connection disconnects:

1. All owned locks/semaphores/RW locks are released (if `--auto-release-on-disconnect` is true)
2. All signal listeners are cleaned up
3. All watch registrations are removed
4. All enqueued waiters are dequeued
5. All leader election observations are unregistered

## Signal Delivery

The signal manager supports:

- **Exact matching**: channel name maps directly to listeners
- **Wildcard matching**: `*` (single token) and `>` (trailing tokens)
- **Queue groups**: within a named group, signals are delivered round-robin to one member

Signals are delivered asynchronously via buffered channels. Slow consumers (full write channel) are disconnected to prevent backpressure.

## Watch Delivery

The watch manager fires on key state changes (KV set/delete, lock acquire/release). It supports the same wildcard pattern syntax as signals. Watch events are delivered via `watch <event_type> <key>\n` push messages.

## Connection Limits

The server enforces `--max-connections` using atomic compare-and-swap on an `int64` counter, preventing TOCTOU races in the accept loop.

## Graceful Shutdown

On `SIGINT` or `SIGTERM`:

1. The listener stops accepting new connections
2. Existing connections are allowed to drain for `--shutdown-timeout`
3. If the timeout expires, all remaining connections are force-closed
4. Background loops (lease sweep, GC) exit via context cancellation
