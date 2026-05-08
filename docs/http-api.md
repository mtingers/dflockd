# HTTP REST API

The HTTP listener is opt-in. Enable with `--http-port`. TCP and HTTP
share the same `LockManager`, so a TCP client and an HTTP session
contending on the same key are ordered together in a single FIFO
queue.

The machine-readable contract is at
[`/v1/openapi.json`](./openapi.json) — point any OpenAPI codegen at
it. The same document is served live by the running server on the
HTTP listener (auth-exempt, since the spec describes auth):

```bash
curl -sS http://localhost:6389/v1/openapi.json | jq .info.version
# → "2.0.0"
```

## Sessions

Two-phase operations (`/enqueue` + `/wait`) require a stable
identity that survives across requests. That identity is a
**session**: a server-side token bound to a unique connID in the
LockManager.

```
POST   /v1/sessions               → 200 {"session_id":"...","idle_timeout_s":20}
DELETE /v1/sessions/{id}          → 204 (releases anything held)
POST   /v1/sessions/{id}/ping     → 204 (refreshes idle timer)
```

Carry the session ID on every lock-modifying request via
`X-Dflockd-Session: <id>`. A request without the header gets HTTP
400; an unknown or expired session gets HTTP 410 (`session_gone`).

Sessions die in three ways:

1. **Explicit `DELETE`.** Synchronous; held tokens are released
   before the response returns.
2. **Idle timeout.** A background sweeper reaps any session whose
   `lastSeen` falls behind 2× `--http-session-idle-timeout`.
   In-flight handlers are immune for the duration of the request.
3. **Server shutdown.** All sessions are closed and their state
   cleaned up.

## Endpoints

### Operational (unauthenticated)

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Liveness probe. Always 200 unless the server is hard-down. |
| `GET` | `/ready` | Readiness. 503 with `{"status":"draining"}` during graceful shutdown. |
| `GET` | `/metrics` | Prometheus exposition. |

### Stats

```
GET /v1/stats
```

Returns a snapshot of the lock-manager state:

```json
{
  "connections": 14,
  "locks": [
    {"key":"deploy","owner_conn_id":42,"lease_expires_in_s":18.5,"waiters":0}
  ],
  "semaphores": [
    {"key":"rate-limit","limit":5,"holders":3,"waiters":7}
  ],
  "idle_locks": [],
  "idle_semaphores": []
}
```

`connections` is the sum of TCP connections plus active HTTP
sessions.

### Locks

```
POST /v1/locks/{key}                request: {"acquire_timeout_s": int, "lease_ttl_s": int (optional)}
                                    response: {"status":"ok|timeout","token":"...","lease_ttl_s":int}

POST /v1/locks/{key}/release        request: {"token":"..."}
                                    response: 204 No Content

POST /v1/locks/{key}/renew          request: {"token":"...", "lease_ttl_s": int (optional)}
                                    response: {"remaining_s": int}

POST /v1/locks/{key}/enqueue        request: {"lease_ttl_s": int (optional)}
                                    response: {"status":"acquired|queued","token":"...","lease_ttl_s":int}

POST /v1/locks/{key}/wait           request: {"timeout_s": int}
                                    response: {"status":"ok|timeout","token":"...","lease_ttl_s":int}
```

Single-phase: `POST /v1/locks/{key}` blocks up to
`acquire_timeout_s` server-side. `0` is non-blocking
(try-lock).

Two-phase: `enqueue` is non-blocking and returns either
`"acquired"` (you got the slot immediately) or `"queued"` (you have
a queue position; call `wait` next). The session connID is what
binds enqueue and wait — they don't share a token.

`lease_ttl_s` defaults to `--default-lease-ttl` when omitted.

### Semaphores

Same shape, with a required `limit`:

```
POST /v1/semaphores/{key}            request: {"acquire_timeout_s": int, "limit": int, "lease_ttl_s": int (optional)}
POST /v1/semaphores/{key}/release    request: {"token":"..."}
POST /v1/semaphores/{key}/renew      request: {"token":"...", "lease_ttl_s": int (optional)}
POST /v1/semaphores/{key}/enqueue    request: {"limit": int, "lease_ttl_s": int (optional)}
POST /v1/semaphores/{key}/wait       request: {"timeout_s": int}
```

The same key cannot be used as both a lock and a semaphore.
Mismatching `limit` for an existing key returns `409 limit_mismatch`.

## Tokens and fencing

Every grant response carries a `token` field that is 32 lowercase
hex chars, structured as:

```
<16 hex: fence prefix (uint64, big-endian)><16 hex: random salt>
```

The fence prefix is a server-monotonic `uint64` seeded from
`time.Now().UnixNano()` at startup and incremented atomically on
every grant. It strictly increases across grants and across server
restarts on a non-regressing wall clock, so the token doubles as a
[fencing token](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html):
a downstream resource can store the most recent token it has seen
for a key and reject any write whose token compares lex-less.
Comparison is meaningful **per key** — fences from different keys
aren't ordered against one another. A `Limit>1` semaphore issues a
distinct fence per grant, not per resource. The salt preserves ~64
bits of unguessability.

The Go client exposes `client.FenceFromToken(token) (uint64, error)`
to extract the fence as a typed integer.

By default the prefix is seeded from `time.Now().UnixNano()` at
boot — strictly monotonic across restarts only while the wall
clock does not regress. Run dflockd with `--fence-state-file=/path`
for unconditional cross-restart monotonicity (one fsync per ~1M
grants, ~0.4% overhead). See [server config](server.md#fence-state-file).

## Authentication

When `--auth-token` is set, every request except `/health` and
`/ready` must carry:

```
Authorization: Bearer <token>
```

Unauthorised requests get `401 unauthorized`.

## Error responses

Every non-2xx response is a JSON body:

```json
{"error": "<code>", "detail": "<optional human-readable>"}
```

The `error` field is the stable machine-readable code. Status →
code mapping:

| HTTP | Code | When |
|---|---|---|
| 400 | `bad_request` | Malformed input, missing required field, invalid key/token |
| 401 | `unauthorized` | Missing or wrong `Authorization: Bearer` |
| 404 | `not_found` | Path doesn't match a route |
| 404 | `not_held` | Release/renew on a token the server doesn't have |
| 405 | `method_not_allowed` | Path matches a different method |
| 408 | `client_canceled` | Client disconnected while a long-poll was in flight |
| 409 | `limit_mismatch` | Same key was already created with a different `limit` |
| 409 | `already_enqueued` | Two-phase: caller already has an enqueued state for this key |
| 409 | `not_enqueued` | Two-phase: `wait` without a matching `enqueue` |
| 409 | `lease_expired` | Promoted holder's lease expired before observation |
| 410 | `session_gone` | Session id unknown, expired, or DELETEd |
| 429 | `rate_limited` | Per-IP request rate exceeded |
| 503 | `max_locks` | Server-wide unique-key cap reached |
| 503 | `max_waiters` | Per-key waiter cap reached |
| 503 | `max_sessions` | Cluster-wide HTTP session cap reached |
| 503 | `max_sessions_per_ip` | Per-IP session cap reached |
| 503 | `draining` | Server is in graceful shutdown |

## Cancellation

The server respects HTTP request context cancellation. A `/wait`
that times out or whose client disconnects releases any
just-granted slot back to the queue rather than stranding it until
lease expiry.

## CORS

Disabled by default. Set `--http-cors-allowed-origins` to a
comma-separated list of origins (`*` allows any). Preflight
`OPTIONS` requests with `Access-Control-Request-Method` are
short-circuited with `204`.
