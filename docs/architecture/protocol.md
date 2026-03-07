# Wire Protocol

dflockd uses a line-based UTF-8 protocol over TCP. Each request is exactly **3 lines**: `command\nkey\narg\n`. Each response is a single line.

## Transport

The protocol runs over plain TCP by default. The server optionally supports TLS encryption — when the server is started with `--tls-cert` and `--tls-key`, all connections must use TLS. The framing and command semantics are identical over both plain TCP and TLS; TLS is a transparent transport layer.

## Authentication

### Auth (`auth`)

Authenticate the connection with a shared secret token. When the server is started with `--auth-token`, this must be the **first** command sent on every new connection. If the server has no auth token configured, this command is not recognized and will return an error.

**Request:**
```
auth
_
<token>
```

The key line is ignored (use `_` by convention).

**Response:**

- Success: `ok\n`
- Failure: `error_auth\n` (connection is closed by the server)

**Example:**
```
auth
_
my-secret-token
```
→ `ok`

After a successful `auth`, the connection proceeds normally and all other commands are available.

## Commands

### Lock (acquire)

Request a lock on a key with a timeout. Optionally specify a lease TTL.

**Request:**
```
l
<key>
<acquire_timeout_s> [<lease_ttl_s>]
```

**Response:**

- Success: `ok <token> <lease_ttl>\n`
- Timeout: `timeout\n`
- Max locks reached: `error_max_locks\n`
- Max waiters reached: `error_max_waiters\n`

**Example:**
```
l
my-job
10
```
→ `ok a1b2c3d4e5f6... 33`

```
l
my-job
10 60
```
→ `ok a1b2c3d4e5f6... 60`

### Enqueue (two-phase step 1)

Join the FIFO queue for a key and return immediately. If the lock is free, it is acquired immediately. Otherwise the caller is enqueued and must call `w` (wait) to block.

**Request:**
```
e
<key>
[<lease_ttl_s>]
```

The 3rd line is an optional positive integer. If empty, the server default lease TTL is used.

**Response:**

- Immediate acquire: `acquired <token> <lease_ttl>\n`
- Queued: `queued\n`
- Max locks reached: `error_max_locks\n`
- Max waiters reached: `error_max_waiters\n`
- Already enqueued: `error_already_enqueued\n`

**Example:**
```
e
my-job

```
→ `acquired a1b2c3d4e5f6... 33` or `queued`

### Wait (two-phase step 2)

Block until the lock is granted (after a prior `e` enqueue) or timeout. The lease is reset to `now + lease_ttl_s` on success, so the client gets the full TTL from the moment `w` returns.

**Request:**
```
w
<key>
<timeout_s>
```

The 3rd line is a required integer >= 0 (same semantics as acquire timeout).

**Response:**

- Success: `ok <token> <lease_ttl>\n`
- Timeout: `timeout\n`
- Not enqueued: `error_not_enqueued\n`
- Lease expired before consumption: `error_lease_expired\n`

**Example:**
```
e
my-job

```
→ `queued`

```
w
my-job
10
```
→ `ok a1b2c3d4e5f6... 33`

### Release

Release a held lock. The token must match the current owner.

**Request:**
```
r
<key>
<token>
```

**Response:**

- Success: `ok\n`
- Token mismatch or unknown key: `error\n`

### Renew

Extend the lease on a held lock. Optionally specify a new lease TTL. The lease expiry is reset to `now + lease_ttl_s`.

**Request:**
```
n
<key>
<token> [<lease_ttl_s>]
```

**Response:**

- Success: `ok <seconds_remaining>\n`
- Token mismatch, unknown key, or already expired: `error\n`

!!! note
    A renew request for an already-expired lease will fail. The server does not resurrect expired locks — the lock transfers to the next waiter on expiry.

## Semaphore Commands

Semaphore commands mirror the lock commands but allow up to N concurrent holders per key (where N = `limit`). The first acquirer sets the limit for a key; subsequent requests must match or receive `error_limit_mismatch`.

### Semaphore Acquire (`sl`)

Request a semaphore slot on a key with a timeout and limit.

**Request:**
```
sl
<key>
<acquire_timeout_s> <limit> [<lease_ttl_s>]
```

**Response:**

- Success: `ok <token> <lease_ttl>\n`
- Timeout: `timeout\n`
- Max locks reached: `error_max_locks\n`
- Limit mismatch: `error_limit_mismatch\n`
- Max waiters reached: `error_max_waiters\n`

**Example:**
```
sl
worker-pool
10 3
```
→ `ok a1b2c3d4e5f6... 33`

### Semaphore Enqueue (`se`) — two-phase step 1

Join the semaphore queue for a key and return immediately.

**Request:**
```
se
<key>
<limit> [<lease_ttl_s>]
```

**Response:**

- Immediate acquire: `acquired <token> <lease_ttl>\n`
- Queued: `queued\n`
- Max locks reached: `error_max_locks\n`
- Limit mismatch: `error_limit_mismatch\n`
- Max waiters reached: `error_max_waiters\n`
- Already enqueued: `error_already_enqueued\n`

### Semaphore Wait (`sw`) — two-phase step 2

Block until a semaphore slot is granted (after a prior `se` enqueue) or timeout.

**Request:**
```
sw
<key>
<timeout_s>
```

**Response:**

- Success: `ok <token> <lease_ttl>\n`
- Timeout: `timeout\n`
- Not enqueued: `error_not_enqueued\n`
- Lease expired before consumption: `error_lease_expired\n`

### Semaphore Release (`sr`)

Release a held semaphore slot. The token must match a current holder.

**Request:**
```
sr
<key>
<token>
```

**Response:**

- Success: `ok\n`
- Token mismatch or unknown key: `error\n`

### Semaphore Renew (`sn`)

Extend the lease on a held semaphore slot.

**Request:**
```
sn
<key>
<token> [<lease_ttl_s>]
```

**Response:**

- Success: `ok <seconds_remaining>\n`
- Token mismatch, unknown key, or already expired: `error\n`

## Signal Commands

### Listen (`listen`)

Subscribe to signals matching a pattern. Patterns support NATS-style wildcards: `*` matches a single dot-separated token, `>` matches one or more trailing tokens. Optionally join a queue group for load-balanced delivery.

**Request:**
```
listen
<pattern>
[<group>]
```

The 3rd line is an optional queue group name. If empty, the listener receives all matching signals individually. If set, only one member of the group receives each signal (round-robin).

**Response:**

- Success: `ok\n`
- Error (e.g. max subscriptions reached): `error\n`

**Examples:**
```
listen
events.user.login

```
→ `ok` (subscribe to exact channel)

```
listen
events.*.login

```
→ `ok` (subscribe with single-token wildcard)

```
listen
events.>

```
→ `ok` (subscribe to all events.* channels)

```
listen
events.>
workers
```
→ `ok` (subscribe with queue group "workers")

### Unlisten (`unlisten`)

Remove a signal subscription. The pattern and group must match the original `listen` call.

**Request:**
```
unlisten
<pattern>
[<group>]
```

**Response:**

- Success: `ok\n`

**Example:**
```
unlisten
events.>

```
→ `ok`

### Signal (`signal`)

Publish a signal to a literal channel (no wildcards allowed). The payload is delivered to all matching listeners.

**Request:**
```
signal
<channel>
<payload>
```

The channel must not contain `*` or `>`. The payload is the 3rd line and must be non-empty.

**Response:**

- Success: `ok <num_delivered>\n`
- Error (e.g. wildcards in channel): `error\n`

**Example:**
```
signal
events.user.login
{"user":"alice","time":"2025-01-01T00:00:00Z"}
```
→ `ok 3` (delivered to 3 listeners)

### Push messages

Signals are delivered to subscribed clients as asynchronous push messages outside the normal request-response flow:

```
sig <channel> <payload>\n
```

For example:
```
sig events.user.login {"user":"alice"}
```

Push messages can arrive at any time on a connection that has active `listen` subscriptions. Client libraries must distinguish `sig ...` push lines from command responses.

## Stats Command

### Stats (`stats`)

Query server runtime state: active connections, held locks, held semaphores, pending waiters, and idle entries awaiting GC. The key and arg lines are read (to keep the wire format uniform) but ignored.

**Request:**
```
stats
_

```

The key and arg lines can be any value (they are discarded).

**Response:**

- Success: `ok <json>\n`

The JSON payload contains:

| Field | Type | Description |
|---|---|---|
| `connections` | int | Number of currently connected TCP clients |
| `locks` | array | Held locks (owner token is set) |
| `locks[].key` | string | Lock key |
| `locks[].owner_conn_id` | int | Connection ID of the lock holder |
| `locks[].lease_expires_in_s` | float | Seconds until the lease expires |
| `locks[].waiters` | int | Number of clients waiting for this lock |
| `semaphores` | array | Semaphores with at least one holder |
| `semaphores[].key` | string | Semaphore key |
| `semaphores[].limit` | int | Maximum concurrent holders |
| `semaphores[].holders` | int | Current number of holders |
| `semaphores[].waiters` | int | Number of clients waiting for a slot |
| `idle_locks` | array | Lock entries with no owner (awaiting GC) |
| `idle_locks[].key` | string | Lock key |
| `idle_locks[].idle_s` | float | Seconds since last activity |
| `signal_channels` | array | Active signal subscriptions |
| `signal_channels[].pattern` | string | Subscription pattern |
| `signal_channels[].group` | string | Queue group name (omitted if non-grouped) |
| `signal_channels[].listeners` | int | Number of listeners on this pattern/group |
| `idle_semaphores` | array | Semaphore entries with no holders (awaiting GC) |
| `idle_semaphores[].key` | string | Semaphore key |
| `idle_semaphores[].idle_s` | float | Seconds since last activity |

**Example:**
```
stats
_

```
→ `ok {"connections":2,"locks":[{"key":"my-job","owner_conn_id":3,"lease_expires_in_s":25.4,"waiters":1}],"semaphores":[],"signal_channels":[],"idle_locks":[],"idle_semaphores":[]}`

## Protocol constraints

| Constraint | Value |
|---|---|
| Max line length | 256 bytes |
| Encoding | UTF-8 |
| Key | Non-empty string without whitespace (within line limit) |
| Token | UUID hex string (32 chars) |
| Timeout | Integer >= 0 |
| Lease TTL | Integer > 0 |

## Error codes

Protocol violations cause the server to respond with `error\n` and close the connection:

| Code | Meaning |
|---|---|
| 3 | Invalid command (not `auth`, `l`, `r`, `n`, `e`, `w`, `sl`, `sr`, `sn`, `se`, `sw`, `listen`, `unlisten`, `signal`, or `stats`) |
| 4 | Invalid integer in argument |
| 5 | Invalid key (empty, contains whitespace, or wildcards in signal channel) |
| 6 | Negative timeout |
| 7 | Empty token |
| 8 | Wrong argument count |
| 9 | Zero or negative lease TTL |
| 10 | Read timeout (no data within server's read timeout) |
| 11 | Client disconnected |
| 12 | Line too long (exceeds 256 bytes) |
| 13 | Zero or negative semaphore limit |

### Semantic error responses

In addition to the protocol error codes above (which close the connection), the following named error responses are returned inline without closing the connection:

| Response | Meaning |
|---|---|
| `error_auth` | Authentication failed (connection is closed) |
| `error_max_locks` | Server lock+semaphore key limit reached |
| `error_max_waiters` | Waiter queue full for this key |
| `error_limit_mismatch` | Semaphore limit doesn't match existing key |
| `error_already_enqueued` | Connection already has a pending enqueue for this key |
| `error_not_enqueued` | Wait called without a prior enqueue for this key |
| `error_lease_expired` | Lock/slot lease expired before the waiter consumed the grant |

## Behavior

- Locks are granted in **strict FIFO order** per key.
- Semaphore slots are granted in **strict FIFO order** per key, up to the configured limit.
- If a lease expires without renewal, the lock/slot automatically passes to the next waiter.
- When a connection closes, all locks and semaphore slots held by that connection are released and transferred to waiters. All signal subscriptions are removed.
- Signal patterns support `*` (single token) and `>` (one or more trailing tokens) wildcards.
- Signal channels (for publishing) must be literal — no wildcards.
- Queue group members receive signals via round-robin. If all members' buffers are full, the primary member is disconnected (slow consumer eviction).
- The server prunes idle lock and semaphore state (no owner/holders, no waiters) after a configurable idle period.
- Lock keys and semaphore keys share the same `--max-locks` budget.

See the [examples page](../getting-started/examples.md) for interactive protocol sessions.
