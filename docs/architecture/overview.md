# Architecture Overview

## Overview

dflockd is a single-process Go server that manages named locks with FIFO ordering, automatic lease expiry, and garbage collection of idle state.

```
┌───────────┐    TCP     ┌─────────────────────────────────────┐
│ Go client │◄──────────►│            dflockd server           │
│ (client/) │  line-     │                                     │
│           │  based     │  ┌──────────┐  ┌────────────────┐   │
└───────────┘  UTF-8     │  │  Lock    │  │  Background    │   │
                         │  │  State   │  │  Goroutines    │   │
┌───────────┐            │  │          │  │                │   │
│ TCP client│◄──────────►│  │  key →   │  │  • lease       │   │
│ (any lang)│            │  │   owner  │  │    expiry      │   │
└───────────┘            │  │   waiter │  │  • lock GC     │   │
                         │  │   queue  │  │                │   │
                         │  └──────────┘  └────────────────┘   │
                         └─────────────────────────────────────┘
```

An in-repo Go client (`github.com/mtingers/dflockd/client`) provides a high-level `Lock` type with automatic lease renewal and CRC32-based sharding, as well as low-level protocol functions. External clients exist for [Python](https://github.com/mtingers/dflockd-client-py) and [TypeScript](https://github.com/mtingers/dflockd-client-ts). Any TCP client that speaks the line-based protocol can also interact with the server directly.

## Lock state

Each named lock key maintains a `LockState`:

- **owner_token** — the UUID token of the current holder (or empty if free)
- **owner_conn_id** — connection ID of the current holder
- **lease_expires_at** — timestamp when the lease expires
- **waiters** — FIFO queue of pending acquire requests
- **last_activity** — timestamp of the most recent operation (used for GC)

## FIFO acquire flow

1. A client sends a lock request for key `K` with timeout `T` and optional lease TTL.
2. If `K` is free and has no waiters, the lock is granted immediately (fast path).
3. Otherwise, the client is appended to the waiter queue and blocks until:
    - The lock is granted (previous holder released or lease expired), or
    - The timeout `T` elapses (client receives `timeout`).
4. When a lock is released or expires, the next waiter in FIFO order is granted the lock.

## Two-phase acquire flow

The two-phase flow splits acquisition into enqueue (`e`) and wait (`w`), allowing application logic between joining the queue and blocking:

1. A client sends an enqueue request (`e`) for key `K` with optional lease TTL.
2. If `K` is free and has no waiters, the lock is granted immediately (fast path). The server returns `acquired <token> <lease>` and the client can begin renewal.
3. Otherwise, the client is appended to the waiter queue and the server returns `queued` immediately (non-blocking).
4. The client performs application logic (e.g. notifying an external system).
5. The client sends a wait request (`w`) for key `K` with timeout `T`.
6. If the lock was already acquired (fast path), the server resets the lease and returns `ok <token> <lease>`.
7. Otherwise, the client blocks until the lock is granted or timeout elapses.
8. On success, the lease is reset to `now + lease_ttl_s`, giving the client the full TTL from the moment `w` returns.

The two-phase flow uses an `EnqueuedState` tracked per `(conn_id, key)`. This state is cleaned up on disconnect, timeout, or successful wait.

## Background goroutines

### Lease expiry loop

Runs every `LEASE_SWEEP_INTERVAL_S` seconds (default: 1s). For each held lock:

- If the lease has expired, the owner is evicted and the lock passes to the next FIFO waiter.
- This prevents deadlocks from crashed or hung clients.

### Lock garbage collection

Runs every `GC_LOOP_SLEEP` seconds (default: 5s). Prunes lock state entries where:

- No owner is holding the lock
- No waiters are queued
- The key has been idle longer than `GC_MAX_UNUSED_TIME` (default: 60s)

This prevents unbounded memory growth from transient keys.

## Connection cleanup

When `DFLOCKD_AUTO_RELEASE_ON_DISCONNECT` is enabled (the default), the server performs cleanup when a TCP connection closes (graceful or abrupt):

1. Cleans up any two-phase enqueued state for the connection, cancelling pending waiters and removing them from lock queues.
2. Cancels any pending waiter futures belonging to that connection.
3. Releases any locks held by that connection.
4. Transfers released locks to the next FIFO waiter, if any.

If disabled, locks from disconnected clients are only freed when their lease expires.

## Signal system

The signal manager (`internal/signal`) provides a pub/sub signaling layer with optional queue groups for work distribution.

### Data structures

```
Manager
├── exact         map[channel] → map[connID] → *Listener     (non-grouped)
├── wildcards     []*Listener                                  (non-grouped)
├── exactGroups   map[channel] → map[groupName] → *queueGroup (grouped)
├── wildGroups    []*wildGroupEntry                            (grouped)
└── connListeners map[connID] → []*listenerEntry              (reverse index)
```

### Queue groups

Queue groups enable unicast delivery: within a named group, only **one** member receives each signal via atomic round-robin. Different groups each get one delivery. Non-grouped listeners still receive individually.

```
Signal on "tasks.email"
         │
         ├──► D (non-grouped)          → receives
         ├──► group "worker-pool" [A,B] → one of A/B receives (round-robin)
         └──► group "audit" [C]         → C receives
                                          ─────────
                                          3 deliveries total
```

The round-robin counter uses `atomic.Uint64` for lock-free increment under `RLock`, so signal dispatch never takes a write lock. Deduplication ensures a connection appearing in multiple paths receives at most one copy.

### Disconnect cleanup

When a connection closes, `UnlistenAll(connID)` iterates the connection's reverse index and removes it from all non-grouped and grouped structures. Empty queue groups and wildcard entries are pruned inline.

## Concurrency model

Lock state is distributed across 64 shards, keyed by `fnv32(key) % 64`. Each shard has its own `sync.Mutex` protecting its `resources` map, so operations on different keys rarely contend. A separate `connMu` mutex protects connection-level tracking (`connOwned`, `connEnqueued`).

**Lock ordering protocol:** `connMu` is always acquired before any shard lock. Two shard locks are never held simultaneously. This prevents deadlocks while allowing high concurrency across keys.

Each client connection is handled in its own goroutine. On the fast path (uncontended acquire), only the relevant shard lock and `connMu` are held briefly. Background loops (lease expiry, GC) iterate shards sequentially, locking one at a time.
