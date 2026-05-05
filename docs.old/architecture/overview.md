# Architecture Overview

## Overview

dflockd is a single-process Go server that manages named locks with FIFO ordering, automatic lease expiry, garbage collection of idle state, and pub/sub signal delivery. The core lock manager is shared across two transports — the native line-based TCP protocol and an optional HTTP REST + SSE layer — so contenders on the same key are ordered together regardless of how they connected.

```
┌───────────┐    TCP     ┌────────────────────────────────────────┐
│ Go client │◄──────────►│             dflockd server             │
│ (client/) │  line-     │                                        │
│           │  based     │  ┌──────────┐  ┌────────────────────┐  │
└───────────┘  UTF-8     │  │  Lock    │  │  Background        │  │
                         │  │  Manager │  │  Goroutines        │  │
┌───────────┐            │  │          │  │                    │  │
│ TCP client│◄──────────►│  │  key →   │  │  • lease expiry    │  │
│ (any lang)│            │  │   owner  │  │  • state GC        │  │
└───────────┘            │  │   waiter │  │  • HTTP session    │  │
                         │  │   queue  │  │    sweeper         │  │
┌───────────┐   HTTP/    │  ├──────────┤  └────────────────────┘  │
│ HTTP      │   SSE      │  │  Signal  │                          │
│ client    │◄──────────►│  │  Manager │  ┌────────────────────┐  │
│ (curl,    │            │  │          │  │  HTTP bridge       │  │
│  webhook, │            │  │  pattern→│◄─┤  (internal/httpapi)│  │
│  codegen) │            │  │  listener│  │                    │  │
└───────────┘            │  └──────────┘  │  session map →     │  │
                         │                │   net.Pipe →       │  │
                         │                │   ServeConn        │  │
                         │                └────────────────────┘  │
                         └────────────────────────────────────────┘
```

### Transports

- **Native TCP protocol.** Line-based UTF-8 (`command\nkey\narg\n`). Each request is three lines; responses are one line plus asynchronous `sig ...` push frames on signal subscriptions. Lock ownership is tied to the TCP connection — locks auto-release on disconnect when enabled.
- **HTTP REST + SSE (optional).** Enabled with `--http-port`. Every HTTP session owns an in-process virtual connection (`net.Pipe`) that feeds into the same `ServeConn` handler used by TCP. The bridge is a pure translation layer: HTTP requests become protocol lines, responses become JSON, and SSE streams fan out `sig ...` push frames as `text/event-stream` events. Because both transports share one `LockManager`, a TCP client and an HTTP session contending on the same key are ordered together in a single FIFO queue.

### Clients

An in-repo Go client (`github.com/mtingers/dflockd/client`) provides a high-level `Lock` type with automatic lease renewal and CRC32-based sharding, as well as low-level protocol functions. External clients exist for [Python](https://github.com/mtingers/dflockd-client-py) and [TypeScript](https://github.com/mtingers/dflockd-client-ts). Any TCP client that speaks the line-based protocol can also interact with the server directly; any HTTP client can use the REST + SSE layer (see the [HTTP API reference](../http-api.md) and [OpenAPI spec](../openapi.json)).

## Resource state

Each named key (lock or semaphore) maintains a `ResourceState`:

- **limit** — maximum concurrent holders (`1` for locks, `N` for semaphores)
- **holders** — map of active holder tokens to their connection ID and lease expiry
- **waiters** — FIFO queue of pending acquire requests
- **last_activity** — timestamp of the most recent operation (used for GC)

A lock is simply a resource with `limit = 1`. Lock keys and semaphore keys are stored in **separate namespaces** (internally prefixed), so the same user-visible key string can be used for both a lock and a semaphore without conflict.

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

The two-phase flow uses an `enqueuedState` tracked per `(conn_id, key)`. This state is cleaned up on disconnect, timeout, or successful wait.

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

## Signal manager

The signal manager (`internal/signal`) handles pub/sub message delivery with pattern matching and optional queue groups.

### Pattern matching

Signal patterns use NATS-style wildcards:

- `*` — matches exactly one dot-separated token (e.g. `events.*.login` matches `events.user.login`)
- `>` — matches one or more trailing tokens (e.g. `events.>` matches `events.user.login`, `events.order.created`)

Literal channel names (no wildcards) are used when publishing signals.

### Queue groups

Listeners can join a named queue group via the `listen` command's group argument. Within a group, each signal is delivered to exactly one member via round-robin. This enables load-balanced signal processing across multiple consumers.

Multiple queue groups on the same pattern are independent — each group receives its own delivery. Non-grouped listeners always receive every matching signal.

### Delivery

When a signal is published:

1. Exact non-grouped listeners on the channel receive the signal (deduplicated per connection)
2. Exact queue groups on the channel each deliver to one member
3. Wildcard non-grouped listeners matching the channel receive the signal (deduplicated against exact deliveries)
4. Wildcard queue groups matching the channel each deliver to one member

If a listener's write buffer is full, its connection is cancelled (slow consumer eviction).

### Cleanup

All signal subscriptions for a connection are automatically removed on disconnect via `UnlistenAll`.

## HTTP bridge

When `--http-port` is set, `internal/httpapi` runs alongside the TCP listener. Each HTTP session maps to:

- A 32-char hex session ID (minted by the server from `crypto/rand`, sent back to the client on `POST /v1/sessions`).
- A `net.Pipe()` pair — the server end is handed to `ServeConn`, which treats it identically to a TCP connection.
- A multiplexer goroutine that splits response lines from async `sig ...` push frames for SSE.
- A unique `connID` from the same `connSeq` counter the TCP accept loop uses, so cross-transport FIFO holds.

Sessions have three teardown paths: explicit `DELETE /v1/sessions/{id}`, the virtual connection's read timeout (same as TCP), and the bridge's idle-session sweeper (which skips sessions with in-flight HTTP commands). See the [HTTP API reference](../http-api.md) for flow details and the [OpenAPI spec](../openapi.json) for the full contract.

## Connection cleanup

When a TCP connection or an HTTP-bridge virtual connection closes (graceful or abrupt), the server performs cleanup for that connection:

1. Cleans up any two-phase enqueued state for the connection, cancelling pending waiters and removing them from queues.
2. Cancels any pending waiter futures belonging to that connection.
3. When `DFLOCKD_AUTO_RELEASE_ON_DISCONNECT` is enabled (the default), releases any locks or semaphore slots held by that connection.
4. Transfers released resources to the next FIFO waiter, if any.

If disabled, held locks and semaphore slots from disconnected clients are only freed when their leases expire. Pending waiters and two-phase enqueue state are still removed immediately because a disconnected waiter cannot observe a later grant.

## Concurrency model

Lock state is distributed across 64 shards, keyed by `fnv32a(key) % 64`. Each shard has its own `sync.Mutex` protecting its `resources` map plus the per-connection indexes for keys in that shard (`connOwned`, `connEnqueued`). Operations on different shards rarely contend.

Operations hold at most one shard lock at a time. Connection cleanup and background loops iterate shards sequentially, so there is no cross-shard lock ordering requirement.

Each client connection is handled in its own goroutine. On the fast path (uncontended acquire), only the relevant shard lock is held briefly. Background loops (lease expiry, GC) iterate shards sequentially, locking one at a time.
