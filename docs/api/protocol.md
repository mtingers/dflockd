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
| 3 | Invalid command (not `l`, `r`, or `n`) |
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

## Interoperability

The protocol is language-agnostic. Any TCP client that can send and receive UTF-8 lines can interact with dflockd:

```bash
# netcat example
printf 'l\nmy-key\n10\n' | nc localhost 6388
```
