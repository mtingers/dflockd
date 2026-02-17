# Wire Protocol

dflockd uses a line-based UTF-8 protocol over TCP. Each request is exactly **3 lines**: `command\nkey\narg\n`. Each response is a single line.

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
- Already enqueued: `error\n`

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
- Not enqueued: `error\n`

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
- Already enqueued: `error\n`

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
- Not enqueued: `error\n`

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

## Protocol constraints

| Constraint | Value |
|---|---|
| Max line length | 256 bytes |
| Encoding | UTF-8 |
| Key | Non-empty string (within line limit) |
| Token | UUID hex string (32 chars) |
| Timeout | Integer >= 0 |
| Lease TTL | Integer > 0 |

## Error codes

Protocol violations cause the server to respond with `error\n` and close the connection:

| Code | Meaning |
|---|---|
| 3 | Invalid command (not `l`, `r`, `n`, `e`, `w`, `sl`, `sr`, `sn`, `se`, or `sw`) |
| 4 | Invalid integer in argument |
| 5 | Empty key |
| 6 | Negative timeout |
| 7 | Empty token |
| 8 | Wrong argument count |
| 9 | Zero or negative lease TTL |
| 10 | Read timeout (no data within server's read timeout) |
| 11 | Client disconnected |
| 12 | Line too long (exceeds 256 bytes) |
| 13 | Zero or negative semaphore limit |

## Behavior

- Locks are granted in **strict FIFO order** per key.
- Semaphore slots are granted in **strict FIFO order** per key, up to the configured limit.
- If a lease expires without renewal, the lock/slot automatically passes to the next waiter.
- When a connection closes, all locks and semaphore slots held by that connection are released and transferred to waiters.
- The server prunes idle lock and semaphore state (no owner/holders, no waiters) after a configurable idle period.
- Lock keys and semaphore keys share the same `--max-locks` budget.

## Example session

```
→ l\nmy-key\n10\n
← ok abc123def456... 33\n

→ n\nmy-key\nabc123def456... \n
← ok 32\n

→ r\nmy-key\nabc123def456...\n
← ok\n
```

## Two-phase example session

```
→ e\nmy-key\n\n
← queued\n

(notify external system here)

→ w\nmy-key\n10\n
← ok abc123def456... 33\n

→ r\nmy-key\nabc123def456...\n
← ok\n
```

## Interoperability

The protocol is language-agnostic. Any TCP client that can send and receive UTF-8 lines can interact with dflockd:

```bash
# netcat example
printf 'l\nmy-key\n10\n' | nc localhost 6388
```

Client libraries are available for:

- **Go** — in-repo `client/` package (`github.com/mtingers/dflockd/client`). See [Go Client](../client.md).
- **Python** — [dflockd-client-py](https://github.com/mtingers/dflockd-client-py)
- **TypeScript** — [dflockd-client-ts](https://github.com/mtingers/dflockd-client-ts)
