# Wire Protocol

dflockd uses a simple line-based TCP protocol. Every request is exactly **3 lines**, and every response is **1 line**.

## Request Format

```
<command>\n
<key>\n
<argument>\n
```

- **command** (line 1) -- the operation to perform (see below)
- **key** (line 2) -- the resource key (no whitespace allowed)
- **argument** (line 3) -- command-specific argument (may be empty)

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

| Command | Key | Argument | Response | Description |
|---------|-----|----------|----------|-------------|
| `l` | `<lock_name>` | `<timeout> [<lease_ttl>]` | `acquired <token> <lease> <fence>` or `timeout` | Acquire lock (blocking) |
| `r` | `<lock_name>` | `<token>` | `ok` | Release lock |
| `n` | `<lock_name>` | `<token> [<lease_ttl>]` | `ok <remaining> <fence>` | Renew lease |
| `e` | `<lock_name>` | `[<lease_ttl>]` | `acquired <token> <lease> <fence>` or `queued` | Enqueue (non-blocking) |
| `w` | `<lock_name>` | `<timeout>` | `ok <token> <lease> <fence>` or `timeout` | Wait after enqueue |

### Semaphores

| Command | Key | Argument | Response | Description |
|---------|-----|----------|----------|-------------|
| `sl` | `<sem_name>` | `<timeout> <limit> [<lease_ttl>]` | `acquired <token> <lease> <fence>` or `timeout` | Acquire semaphore slot (blocking) |
| `sr` | `<sem_name>` | `<token>` | `ok` | Release semaphore slot |
| `sn` | `<sem_name>` | `<token> [<lease_ttl>]` | `ok <remaining> <fence>` | Renew semaphore lease |
| `se` | `<sem_name>` | `<limit> [<lease_ttl>]` | `acquired <token> <lease> <fence>` or `queued` | Enqueue for semaphore (non-blocking) |
| `sw` | `<sem_name>` | `<timeout>` | `ok <token> <lease> <fence>` or `timeout` | Wait after semaphore enqueue |

### Read-Write Locks

| Command | Key | Argument | Response | Description |
|---------|-----|----------|----------|-------------|
| `rl` | `<lock_name>` | `<timeout> [<lease_ttl>]` | `ok <token> <lease> <fence>` or `timeout` | Acquire read lock |
| `wl` | `<lock_name>` | `<timeout> [<lease_ttl>]` | `ok <token> <lease> <fence>` or `timeout` | Acquire write lock |
| `rr` | `<lock_name>` | `<token>` | `ok` | Release read lock |
| `wr` | `<lock_name>` | `<token>` | `ok` | Release write lock |
| `rn` | `<lock_name>` | `<token> [<lease_ttl>]` | `ok <remaining> <fence>` | Renew read lock lease |
| `wn` | `<lock_name>` | `<token> [<lease_ttl>]` | `ok <remaining> <fence>` | Renew write lock lease |
| `re` | `<lock_name>` | `[<lease_ttl>]` | `acquired <token> <lease> <fence>` or `queued` | Enqueue for read lock |
| `we` | `<lock_name>` | `[<lease_ttl>]` | `acquired <token> <lease> <fence>` or `queued` | Enqueue for write lock |
| `rw` | `<lock_name>` | `<timeout>` | `ok <token> <lease> <fence>` or `timeout` | Wait after read enqueue |
| `ww` | `<lock_name>` | `<timeout>` | `ok <token> <lease> <fence>` or `timeout` | Wait after write enqueue |

### Atomic Counters

| Command | Key | Argument | Response | Description |
|---------|-----|----------|----------|-------------|
| `incr` | `<counter_name>` | `<delta>` | `ok <new_value>` | Increment counter by delta |
| `decr` | `<counter_name>` | `<delta>` | `ok <new_value>` | Decrement counter by delta |
| `get` | `<counter_name>` | _(empty)_ | `ok <value>` | Get counter value (0 if nonexistent) |
| `cset` | `<counter_name>` | `<value>` | `ok` | Set counter to specific value |

### KV Store

| Command | Key | Argument | Response | Description |
|---------|-----|----------|----------|-------------|
| `kset` | `<key>` | `<value>\t<ttl>` | `ok` | Set value (TTL in seconds, 0 = no expiry) |
| `kget` | `<key>` | _(empty)_ | `ok <value>` or `nil` | Get value |
| `kdel` | `<key>` | _(empty)_ | `ok` | Delete key |
| `kcas` | `<key>` | `<old>\t<new>\t<ttl>` | `ok` or `cas_conflict` | Compare-and-swap |

!!! note "Tab separator"
    `kset` and `kcas` use a tab character (`\t`) to separate value from TTL, since values may contain spaces. Values must not contain tab characters.

### Lists / Queues

| Command | Key | Argument | Response | Description |
|---------|-----|----------|----------|-------------|
| `lpush` | `<list_name>` | `<value>` | `ok <length>` | Prepend to list |
| `rpush` | `<list_name>` | `<value>` | `ok <length>` | Append to list |
| `lpop` | `<list_name>` | _(empty)_ | `ok <value>` or `nil` | Pop from left |
| `rpop` | `<list_name>` | _(empty)_ | `ok <value>` or `nil` | Pop from right |
| `llen` | `<list_name>` | _(empty)_ | `ok <length>` | Get list length |
| `lrange` | `<list_name>` | `<start> <stop>` | `ok <json_array>` | Get range (0-based, negative from end, inclusive) |
| `blpop` | `<list_name>` | `<timeout>` | `ok <value>` or `nil` | Blocking left pop |
| `brpop` | `<list_name>` | `<timeout>` | `ok <value>` or `nil` | Blocking right pop |

### Pub/Sub Signaling

| Command | Key | Argument | Response | Description |
|---------|-----|----------|----------|-------------|
| `listen` | `<channel_or_pattern>` | `[<group>]` | `ok` | Subscribe to channel/pattern (optional queue group) |
| `unlisten` | `<channel_or_pattern>` | `[<group>]` | `ok` | Unsubscribe from channel/pattern |
| `signal` | `<channel>` | `<payload>` | `ok <receiver_count>` | Send signal to a channel (no wildcards) |

**Push format** (delivered asynchronously to listeners):

```
sig <channel> <payload>\n
```

### Key Watch

| Command | Key | Argument | Response | Description |
|---------|-----|----------|----------|-------------|
| `watch` | `<key_or_pattern>` | _(empty)_ | `ok` | Subscribe to key/pattern change events |
| `unwatch` | `<key_or_pattern>` | _(empty)_ | `ok` | Unsubscribe from key/pattern |

**Push format** (delivered asynchronously to watchers):

```
watch <event_type> <key>\n
```

Event types include: `kset`, `kdel`, `acquire`, `release`, etc.

### Barriers

| Command | Key | Argument | Response | Description |
|---------|-----|----------|----------|-------------|
| `bwait` | `<barrier_name>` | `<count> <timeout>` | `ok` or `timeout` | Wait for N participants to arrive |

### Leader Election

| Command | Key | Argument | Response | Description |
|---------|-----|----------|----------|-------------|
| `elect` | `<election_name>` | `<timeout> [<lease_ttl>]` | `acquired <token> <lease> <fence>` or `timeout` | Run for leader (blocking) |
| `resign` | `<election_name>` | `<token>` | `ok` | Step down as leader |
| `observe` | `<election_name>` | _(empty)_ | `ok` | Subscribe to leader events |
| `unobserve` | `<election_name>` | _(empty)_ | `ok` | Unsubscribe from leader events |

**Push format** (delivered to observers and the elected leader):

```
leader <event> <key>\n
```

Events: `elected`, `resigned`, `failover`

### Authentication

| Command | Key | Argument | Response | Description |
|---------|-----|----------|----------|-------------|
| `auth` | `_` _(ignored)_ | `<token>` | `ok` or `error_auth` | Authenticate connection |

### Stats

| Command | Key | Argument | Response | Description |
|---------|-----|----------|----------|-------------|
| `stats` | `_` _(ignored)_ | `_` _(ignored)_ | `ok <json>` | Server statistics (JSON) |

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
