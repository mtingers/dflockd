# Wire protocol

The TCP protocol is line-based UTF-8. Every request is exactly three
newline-terminated lines:

```
<command>\n
<key>\n
<arg>\n
```

Every response is one newline-terminated line:

```
<status>[ <field1>[ <field2>]]\n
```

`\r\n` is accepted as a line ending; trailing `\r` is stripped.

## Caps

| Constant | Value | Notes |
|---|---|---|
| `MaxLineBytes` | 256 | Cap on cmd, key, and most arg lines. |
| `MaxAuthTokenBytes` | 64 KiB | Larger cap for the `auth` token line. |

Lines longer than the cap return `error` with code `12`. The
oversized line is drained to newline so the framing stays in sync.

## Authentication

When `--auth-token` is set, the first command on a fresh connection
must be `auth`:

```
auth
_           # the key field is unused; convention is to send "_"
<token>
ok          # success
```

Anything else returns `error_auth`. The server adds a 100 ms cool-
down before closing on auth failure.

The key for `auth` is irrelevant; `_` is convention. The token is
compared in constant time.

## Commands

### `l` — lock acquire (single-phase)

```
l
<key>
<timeout> [<lease_ttl>]      # both in seconds
ok <token> <lease_ttl>       # granted
timeout                      # acquire timeout fired
error_max_locks              # unique-key cap reached
error_max_waiters            # per-key waiter cap reached
error_lease_expired          # rare; granted slot's lease expired before observation
error_limit_mismatch         # this key was already created as a sem with limit != 1
```

`timeout=0` is non-blocking (try-lock).

### `r` — lock release

```
r
<key>
<token>
ok                           # success
error                        # token doesn't match a held slot
```

### `n` — lock renew

```
n
<key>
<token> [<lease_ttl>]
ok <remaining_seconds>
error                        # token not held, or lease already expired
```

If `lease_ttl` is omitted, the server uses `--default-lease-ttl`.

### `e` — lock enqueue (two-phase, phase 1)

```
e
<key>
[<lease_ttl>]
acquired <token> <lease_ttl> # capacity available; you hold the lock
queued                       # waiter registered; call `w` next
error_already_enqueued       # this connection already has phase-1 state for this key
error_max_locks
error_max_waiters
error_limit_mismatch
```

### `w` — lock wait (two-phase, phase 2)

```
w
<key>
<timeout>
ok <token> <lease_ttl>       # grant arrived
timeout                      # wait timeout fired (waiter still in queue if connection stays open? no — popped)
error_not_enqueued           # no matching `e` on this connection
error_lease_expired
```

Must be called from the same connection that issued the matching
`e`. The connection is what binds the two phases.

### Semaphores: `sl`, `sr`, `sn`, `se`, `sw`

Identical to `l`/`r`/`n`/`e`/`w` but for semaphores. The differences
are the arg shapes:

```
sl                           se
<key>                        <key>
<timeout> <limit> [<lease>]  <limit> [<lease>]

sr                           sn                          sw
<key>                        <key>                       <key>
<token>                      <token> [<lease>]           <timeout>
```

The same key cannot be used as both a lock and a semaphore;
mismatching `limit` returns `error_limit_mismatch`. A semaphore
with `limit=1` is wire-equivalent to a lock but the namespaces are
separate (`lock:` vs `sem:` prefix internally).

### `ping` — keepalive

```
ping
_
_
ok
```

The two trailing fields are unused. Convention: `_`.

### `stats` — server snapshot

```
stats
_
_
ok <json>
```

Body of the `<json>` is the same shape as `GET /v1/stats`:

```json
{
  "connections": 14,
  "locks": [{"key":"...","owner_conn_id":42,"lease_expires_in_s":18.5,"waiters":0}],
  "semaphores": [...],
  "idle_locks": [...],
  "idle_semaphores": [...]
}
```

## Status table

| Status | Meaning |
|---|---|
| `ok` | Generic success. May carry a token + lease, or stats JSON. |
| `acquired` | Two-phase fast path: enqueue immediately granted you the slot. |
| `queued` | Two-phase: waiter registered; call `w`/`sw` next. |
| `timeout` | Acquire/wait timeout fired without a grant. |
| `error` | Generic error. Token not held, or unknown protocol error. |
| `error_auth` | Auth handshake failed. |
| `error_max_locks` | `--max-locks` cap reached. |
| `error_max_waiters` | `--max-waiters` cap reached for this key. |
| `error_limit_mismatch` | Sem `limit` doesn't match the existing key's limit. |
| `error_not_enqueued` | `wait` without a matching `enqueue` on this connection. |
| `error_already_enqueued` | `enqueue` while this connection already has phase-1 state for this key. |
| `error_lease_expired` | Granted slot's lease expired before we could observe it. |
| `error_draining` | Server is in graceful shutdown. |

## Disconnect semantics

Closing the TCP connection triggers `LockManager.CleanupConnection`:

- Pending waiters (`l`/`sl` blocking, or `e`/`se` followed by no
  `w`/`sw`) are cancelled. The disconnected client can never
  observe a future grant.
- Held tokens are released only when
  `--auto-release-on-disconnect=true` (the default). With it set
  to `false`, held slots remain reachable via lease expiry.

## Read errors and framing

If `--read-timeout` fires mid-frame, or a single line exceeds
`MaxLineBytes`, the server writes `error` and disconnects. The
framing can't be safely resumed after either condition.
