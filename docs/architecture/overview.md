# Architecture overview

dflockd is one process with three concerns:

1. **`LockManager`** — the in-memory lock state. Sharded by key for
   parallel-key throughput.
2. **TCP server** — the line-based wire protocol. One goroutine per
   client connection.
3. **HTTP server** (optional) — REST handlers that call `LockManager`
   directly. Sessions provide the connID a two-phase request needs.

```
┌─────────────────┐        ┌─────────────────┐
│  TCP transport  │        │  HTTP transport │
│  (line proto)   │        │   (REST/JSON)   │
└────────┬────────┘        └────────┬────────┘
         │                          │
         │   per-conn / per-session connID
         │                          │
         └────────────┬─────────────┘
                      ▼
             ┌───────────────────┐
             │   LockManager     │
             │  (sharded, 64×)   │
             └───────────────────┘
```

Both transports talk to the same `LockManager`, so a TCP client and
an HTTP session contending on the same key are ordered together in a
single FIFO queue.

## LockManager

State is sharded into 64 stripes by `FNV-1a(key) % 64`. Each shard
owns its own mutex; cross-shard operations (only
`CleanupConnection`) iterate one shard at a time without ever
holding two mutexes.

Per shard:

- `resources` — map of key → `ResourceState` (limit, holders,
  waiter queue, last-activity).
- `connOwned` — map of connID → key → set of held tokens. Used by
  `CleanupConnection` to release everything a disconnected
  connection held without scanning every key.
- `connEnqueued` — map of (connID, key) → enqueued state. Backs
  the two-phase `Wait` lookup.

Tokens are 32 lowercase hex chars: an 8-byte big-endian
**fence prefix** drawn from a per-manager monotonic counter,
followed by 8 random bytes. The counter strictly increases on
every grant, so a token doubles as a [fencing token](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html).
By default the counter is seeded at startup from
`time.Now().UnixNano()` (best-effort cross-restart monotonicity);
with `--fence-state-file=/path`, fence ranges are pre-allocated
to disk (one `fsync` per ~1M grants), giving unconditional
cross-restart monotonicity even through crashes and clock
regressions. The salt is drawn from a per-manager `randBuf` that
reads 4 KiB from `crypto/rand` at a time and dispenses 8 bytes
per token, amortising the syscall.

Two background goroutines run for the manager's lifetime:

- **Lease expiry sweep** — every `--lease-sweep-interval`, evicts
  any holder past its lease deadline and grants the freed slot to
  the head waiter.
- **Idle GC** — every `--gc-interval`, prunes resources that have
  been idle (no holders, no waiters, no recent activity) for longer
  than `--gc-max-idle`. The cap on unique keys
  (`--max-locks`) is steady-state, not high-water.

## TCP server

The accept loop:

- Bounded exponential backoff on transient accept errors (so FD
  exhaustion doesn't busy-spin).
- Global cap (`--max-connections`) and per-IP cap
  (`--max-connections-per-ip`) gates before allocating a connID.
- Each accepted conn runs `ServeConn` in its own goroutine.

Per connection, the lifecycle:

1. **Auth handshake** if `--auth-token` is set. First command must
   be `auth`; anything else gets `error_auth` and a 100 ms cooldown
   before close.
2. **Request loop.** Read three lines (cmd / key / arg), dispatch
   to the LockManager, write one response line.
3. **Peer-close watcher.** While a blocking command (long-poll
   `acquire` / `wait`) runs, a watcher goroutine peeks the read
   buffer; if it observes EOF or a full-buffer pipeline (abuse), it
   cancels the connection so the in-flight waiter can exit promptly.
4. **Disconnect cleanup.** The handler's `defer` calls
   `LockManager.CleanupConnection(connID)`. With
   `--auto-release-on-disconnect=true` (the default), every held
   token is released and granted to the next waiter. Pending
   waiters and two-phase enqueued state are *always* cleaned up
   regardless of that flag.

## HTTP server

The HTTP layer doesn't speak protocol bytes. Each session is
metadata (`{ID, ConnID, OwnerIP, lastSeen, inFlight, closed}`);
handlers parse JSON, claim the session, then call `LockManager`
methods directly using the session's connID.

Two coordination primitives keep the session safe:

- **`BeginRequest` / `done`.** Every lock-modifying handler claims
  the session's mutex before calling `LockManager`, and bumps an
  `inFlight` counter. The idle sweeper skips any session with
  `inFlight > 0`, so a long-poll `/wait` isn't reaped mid-flight.
  Session DELETE / sweeper / shutdown all set a `closed` flag and
  drain the mutex before calling `CleanupConnection`, so a handler
  already mid-`Acquire` finishes before its connID's state is
  wiped.
- **Body parse before claim.** Handlers parse and validate the
  JSON body *before* `BeginRequest`. A slow / malicious body never
  bumps `inFlight`, so the sweeper can still reap a stalled
  session.

A background sweeper reaps sessions whose `lastSeen` falls behind
2× `--http-session-idle-timeout`. `lastSeen` is refreshed on each
`Lookup` and on each `BeginRequest` exit.

## Connection IDs

Both transports allocate connIDs from a single shared counter
(`Server.NextConnID`). That gives `LockManager` one flat namespace
of connections regardless of transport — `CleanupConnection(id)`
works the same way whether the id came from a TCP accept or an
HTTP session create.

connID 0 is reserved as a "skip per-connection bookkeeping"
sentinel. The accept loop allocates from 1.

## Two-phase operations

A two-phase acquire (`enqueue` then `wait`) splits the FIFO grant
into two requests so the caller can observe its queue position
before blocking. The pair share state via `connEnqueued[(connID,
key)]`:

- `Enqueue` either grants immediately (status `"acquired"`, token
  in the response) or registers a waiter (status `"queued"`, no
  token yet).
- `Wait` looks up the per-(connID, key) state. If the waiter has
  already been promoted to holder (the same-tick fast path), it
  returns the token. Otherwise it blocks on the waiter's channel.
- If `Enqueue` returned `"queued"` and the caller never calls
  `Wait`, the connection's eventual disconnect (or session
  cleanup) closes the waiter channel.

The connID *must* be the same for the matching enqueue and wait.
On TCP that's automatic (same connection). On HTTP it's enforced
by carrying the same `X-Dflockd-Session: <id>` header.

## Pre-refactor source

Older releases shipped a pub/sub layer (`signal` / `listen` /
`unlisten` commands, `/v1/signals` SSE) and an HTTP "bridge" that
ran the line protocol over `net.Pipe` per session. Both were
removed in v2; the source is preserved at `old/` for reference but
excluded from the build via `//go:build ignore`. The current docs
describe only the v2 surface.
