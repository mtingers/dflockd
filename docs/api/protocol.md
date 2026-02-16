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
| 3 | Invalid command (not `l`, `r`, `n`, `e`, or `w`) |
| 4 | Invalid integer in argument |
| 5 | Empty key |
| 6 | Negative timeout |
| 7 | Empty token |
| 8 | Wrong argument count |
| 9 | Zero or negative lease TTL |
| 10 | Read timeout (no data within server's read timeout) |
| 11 | Client disconnected |
| 12 | Line too long (exceeds 256 bytes) |

## Behavior

- Locks are granted in **strict FIFO order** per key.
- If a lease expires without renewal, the lock automatically passes to the next waiter.
- When a connection closes, all locks held by that connection are released and transferred to waiters.
- The server prunes idle lock state (no owner, no waiters) after a configurable idle period.

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

- **Go** — in-repo `client/` package (`github.com/mtingers/dflockd/client`). See [Go Client](../guide/client.md).
- **Python** — [dflockd-client-py](https://github.com/mtingers/dflockd-client-py)
- **TypeScript** — [dflockd-client-ts](https://github.com/mtingers/dflockd-client-ts)
