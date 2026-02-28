# Wire Protocol

dflockd uses a simple line-based TCP protocol. Every request is exactly **3 lines**, and every response is **1 line**.

## Request Format

```
<command>\n
<key>\n
<argument>\n
```

- **command** -- the operation to perform (see below)
- **key** -- the resource key (no whitespace allowed)
- **argument** -- command-specific argument (may be empty)

Each line is terminated by `\n`. Lines are limited to 256 bytes. Carriage returns (`\r`) are stripped.

## Response Format

```
<status> [<fields...>]\n
```

Responses are a single line. The first word is a status code, optionally followed by space-separated fields.

## Response Statuses

| Status | Meaning |
|--------|---------|
| `ok` | Success (may include additional fields) |
| `acquired` | Lock/semaphore acquired (may include token, lease, fence) |
| `queued` | Enqueued in FIFO wait queue |
| `timeout` | Operation timed out |
| `nil` | Key/value not found |
| `cas_conflict` | Compare-and-swap value mismatch |
| `error` | Generic server error |
| `error_auth` | Authentication failed |
| `error_max_locks` | Maximum lock keys reached |
| `error_max_keys` | Maximum aggregate keys reached |
| `error_max_waiters` | Maximum waiters per key reached |
| `error_limit_mismatch` | Semaphore limit doesn't match existing key |
| `error_type_mismatch` | Key exists as a different type |
| `error_not_enqueued` | Wait called without prior enqueue |
| `error_already_enqueued` | Duplicate enqueue for same connection+key |
| `error_lease_expired` | Lease expired before operation completed |
| `error_list_full` | List at max length |
| `error_barrier_count_mismatch` | Barrier count differs from existing barrier |

## Commands by Feature

### Exclusive Locks

| Command | Argument | Response | Description |
|---------|----------|----------|-------------|
| `l` | `<timeout> [<lease_ttl>]` | `acquired <token> <lease> <fence>` or `timeout` | Acquire lock (blocking) |
| `r` | `<token>` | `ok` | Release lock |
| `n` | `<token> [<lease_ttl>]` | `ok <remaining> <fence>` | Renew lease |
| `e` | `[<lease_ttl>]` | `acquired <token> <lease> <fence>` or `queued` | Enqueue (non-blocking) |
| `w` | `<timeout>` | `ok <token> <lease> <fence>` or `timeout` | Wait after enqueue |

### Semaphores

| Command | Argument | Response | Description |
|---------|----------|----------|-------------|
| `sl` | `<timeout> <limit> [<lease_ttl>]` | `acquired <token> <lease> <fence>` or `timeout` | Acquire semaphore slot (blocking) |
| `sr` | `<token>` | `ok` | Release semaphore slot |
| `sn` | `<token> [<lease_ttl>]` | `ok <remaining> <fence>` | Renew semaphore lease |
| `se` | `<limit> [<lease_ttl>]` | `acquired <token> <lease> <fence>` or `queued` | Enqueue for semaphore (non-blocking) |
| `sw` | `<timeout>` | `ok <token> <lease> <fence>` or `timeout` | Wait after semaphore enqueue |

### Read-Write Locks

| Command | Argument | Response | Description |
|---------|----------|----------|-------------|
| `rl` | `<timeout> [<lease_ttl>]` | `ok <token> <lease> <fence>` or `timeout` | Acquire read lock |
| `wl` | `<timeout> [<lease_ttl>]` | `ok <token> <lease> <fence>` or `timeout` | Acquire write lock |
| `rr` | `<token>` | `ok` | Release read lock |
| `wr` | `<token>` | `ok` | Release write lock |
| `rn` | `<token> [<lease_ttl>]` | `ok <remaining> <fence>` | Renew read lock lease |
| `wn` | `<token> [<lease_ttl>]` | `ok <remaining> <fence>` | Renew write lock lease |
| `re` | `[<lease_ttl>]` | `acquired <token> <lease> <fence>` or `queued` | Enqueue for read lock |
| `we` | `[<lease_ttl>]` | `acquired <token> <lease> <fence>` or `queued` | Enqueue for write lock |
| `rw` | `<timeout>` | `ok <token> <lease> <fence>` or `timeout` | Wait after read enqueue |
| `ww` | `<timeout>` | `ok <token> <lease> <fence>` or `timeout` | Wait after write enqueue |

### Atomic Counters

| Command | Argument | Response | Description |
|---------|----------|----------|-------------|
| `incr` | `<delta>` | `ok <new_value>` | Increment counter by delta |
| `decr` | `<delta>` | `ok <new_value>` | Decrement counter by delta |
| `get` | _(empty)_ | `ok <value>` | Get counter value (0 if nonexistent) |
| `cset` | `<value>` | `ok` | Set counter to specific value |

### KV Store

| Command | Argument | Response | Description |
|---------|----------|----------|-------------|
| `kset` | `<value>\t<ttl>` | `ok` | Set key-value pair (TTL in seconds, 0 = no expiry) |
| `kget` | _(empty)_ | `ok <value>` or `nil` | Get value by key |
| `kdel` | _(empty)_ | `ok` | Delete key |
| `kcas` | `<old>\t<new>\t<ttl>` | `ok` or `cas_conflict` | Compare-and-swap |

!!! note "Tab separator"
    `kset` and `kcas` use a tab character (`\t`) to separate value from TTL, since values may contain spaces. Values must not contain tab characters.

### Lists / Queues

| Command | Argument | Response | Description |
|---------|----------|----------|-------------|
| `lpush` | `<value>` | `ok <length>` | Prepend to list |
| `rpush` | `<value>` | `ok <length>` | Append to list |
| `lpop` | _(empty)_ | `ok <value>` or `nil` | Pop from left |
| `rpop` | _(empty)_ | `ok <value>` or `nil` | Pop from right |
| `llen` | _(empty)_ | `ok <length>` | Get list length |
| `lrange` | `<start> <stop>` | `ok <json_array>` | Get range (0-based, negative from end, inclusive) |
| `blpop` | `<timeout>` | `ok <value>` or `nil` | Blocking left pop |
| `brpop` | `<timeout>` | `ok <value>` or `nil` | Blocking right pop |

### Pub/Sub Signaling

| Command | Argument | Response | Description |
|---------|----------|----------|-------------|
| `listen` | `[<group>]` | `ok` | Subscribe to channel/pattern (optional queue group) |
| `unlisten` | `[<group>]` | `ok` | Unsubscribe from channel/pattern |
| `signal` | `<payload>` | `ok <receiver_count>` | Send signal to a channel (no wildcards) |

**Push format** (delivered asynchronously to listeners):

```
sig <channel> <payload>\n
```

### Key Watch

| Command | Argument | Response | Description |
|---------|----------|----------|-------------|
| `watch` | _(empty)_ | `ok` | Subscribe to key/pattern change events |
| `unwatch` | _(empty)_ | `ok` | Unsubscribe from key/pattern |

**Push format** (delivered asynchronously to watchers):

```
watch <event_type> <key>\n
```

Event types include: `kset`, `kdel`, `acquire`, `release`, etc.

### Barriers

| Command | Argument | Response | Description |
|---------|----------|----------|-------------|
| `bwait` | `<count> <timeout>` | `ok` or `timeout` | Wait for N participants to arrive |

### Leader Election

| Command | Argument | Response | Description |
|---------|----------|----------|-------------|
| `elect` | `<timeout> [<lease_ttl>]` | `acquired <token> <lease> <fence>` or `timeout` | Run for leader (blocking) |
| `resign` | `<token>` | `ok` | Step down as leader |
| `observe` | _(empty)_ | `ok` | Subscribe to leader events for key |
| `unobserve` | _(empty)_ | `ok` | Unsubscribe from leader events |

**Push format** (delivered to observers and the elected leader):

```
leader <event> <key>\n
```

Events: `elected`, `resigned`, `failover`

### Authentication

| Command | Argument | Response | Description |
|---------|----------|----------|-------------|
| `auth` | `<token>` | `ok` or `error_auth` | Authenticate connection |

The key field is ignored for `auth` (conventionally `_`).

### Stats

| Command | Argument | Response | Description |
|---------|----------|----------|-------------|
| `stats` | — | `ok <json>` | Server statistics (JSON) |

Both key and argument are ignored for `stats`.

## Pattern Syntax (Signal and Watch)

Signal `listen` and `watch` commands accept wildcard patterns using dot-separated tokens:

| Pattern | Matches |
|---------|---------|
| `foo.bar` | Exact match only |
| `foo.*` | `foo.bar`, `foo.baz` (single token wildcard) |
| `foo.>` | `foo.bar`, `foo.bar.baz` (one or more trailing tokens) |
| `*` | Any single-token channel |
| `>` | Any channel with 1+ tokens |

Rules:

- `*` must be an entire dot-separated token (not `fo*`)
- `>` must be the last token in the pattern
- Signal channels (the key in `signal`) must not contain wildcards

## Protocol Error Codes

| Code | Meaning |
|------|---------|
| 3 | Invalid command |
| 4 | Invalid argument (bad integer, overflow) |
| 5 | Invalid key (empty or contains whitespace) |
| 6 | Negative timeout |
| 7 | Empty token |
| 8 | Malformed argument |
| 9 | Invalid lease TTL (must be > 0) |
| 10 | Read timeout or failed to set deadline |
| 11 | Client disconnected |
| 12 | Line too long (> 256 bytes) |
| 13 | Invalid limit or count (must be > 0) |
