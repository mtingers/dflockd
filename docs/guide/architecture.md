# Architecture

## Overview

dflockd is a single-process asyncio server that manages named locks with FIFO ordering, automatic lease expiry, and garbage collection of idle state.

```
┌──────────┐    TCP     ┌─────────────────────────────────────┐
│  Client   │◄─────────►│            dflockd server           │
│  (async   │  line-    │                                     │
│   or sync)│  based    │  ┌──────────┐  ┌────────────────┐  │
└──────────┘  UTF-8     │  │  Lock     │  │  Background    │  │
                        │  │  State    │  │  Tasks         │  │
┌──────────┐            │  │          │  │                │  │
│  Client   │◄─────────►│  │  key →   │  │  • lease       │  │
└──────────┘            │  │   owner  │  │    expiry      │  │
                        │  │   waiter │  │  • lock GC     │  │
┌──────────┐            │  │   queue  │  │                │  │
│  Client   │◄─────────►│  └──────────┘  └────────────────┘  │
└──────────┘            └─────────────────────────────────────┘
```

## Lock state

Each named lock key maintains a `LockState`:

- **owner_token** — the UUID token of the current holder (or `None` if free)
- **owner_conn_id** — connection ID of the current holder
- **lease_expires_at** — monotonic timestamp when the lease expires
- **waiters** — FIFO deque of pending acquire requests
- **last_activity** — timestamp of the most recent operation (used for GC)

## FIFO acquire flow

1. A client sends a lock request for key `K` with timeout `T` and optional lease TTL.
2. If `K` is free and has no waiters, the lock is granted immediately (fast path).
3. Otherwise, the client is appended to the waiter deque and blocks until:
    - The lock is granted (previous holder released or lease expired), or
    - The timeout `T` elapses (client receives `timeout`).
4. When a lock is released or expires, the next waiter in FIFO order is granted the lock.

## Two-phase acquire flow

The two-phase flow splits acquisition into enqueue (`e`) and wait (`w`), allowing application logic between joining the queue and blocking:

1. A client sends an enqueue request (`e`) for key `K` with optional lease TTL.
2. If `K` is free and has no waiters, the lock is granted immediately (fast path). The server returns `acquired <token> <lease>` and the client can begin renewal.
3. Otherwise, the client is appended to the waiter deque and the server returns `queued` immediately (non-blocking).
4. The client performs application logic (e.g. notifying an external system).
5. The client sends a wait request (`w`) for key `K` with timeout `T`.
6. If the lock was already acquired (fast path), the server resets the lease and returns `ok <token> <lease>`.
7. Otherwise, the client blocks until the lock is granted or timeout elapses.
8. On success, the lease is reset to `now + lease_ttl_s`, giving the client the full TTL from the moment `w` returns.

The two-phase flow uses an `EnqueuedState` tracked per `(conn_id, key)`. This state is cleaned up on disconnect, timeout, or successful wait.

## Background tasks

### Lease expiry loop

Runs every `LEASE_SWEEP_INTERVAL_S` seconds (default: 1s). For each held lock:

- If `now >= lease_expires_at`, the owner is evicted and the lock passes to the next FIFO waiter.
- This prevents deadlocks from crashed or hung clients.

### Lock garbage collection

Runs every `GC_LOOP_SLEEP` seconds (default: 5s). Prunes lock state entries where:

- No owner is holding the lock
- No waiters are queued
- The key has been idle longer than `GC_MAX_UNUSED_TIME` (default: 60s)

This prevents unbounded memory growth from transient keys.

## Connection cleanup

When `DFLOCKD_AUTO_RELEASE_ON_DISCONNECT` is enabled (the default), the server performs cleanup when a TCP connection closes (graceful or abrupt):

1. Cleans up any two-phase enqueued state (`_conn_enqueued`) for the connection, cancelling pending waiters and removing them from lock queues.
2. Cancels any pending waiter futures belonging to that connection.
3. Releases any locks held by that connection.
4. Transfers released locks to the next FIFO waiter, if any.

If disabled, locks from disconnected clients are only freed when their lease expires.

## Concurrency model

All lock state mutations are serialized through a single `asyncio.Lock` (`tracking_lock`). This ensures consistency without complex fine-grained locking, while asyncio's cooperative scheduling keeps throughput high for the I/O-bound workload.
