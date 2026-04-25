# HTTP REST API

dflockd ships an optional HTTP + SSE interface alongside the native TCP protocol. It's designed for environments where a long-lived TCP client is awkward — curl from scripts, serverless functions, webhook publishers, and languages without a native client library.

The full machine-readable contract is [`/v1/openapi.json`](./openapi.json) (OpenAPI 3.1). Point any codegen tool at it (`openapi-generator`, Orval, Stoplight, etc.) to build a typed client.

## Enabling the HTTP server

HTTP is disabled by default. Set `--http-port` (or `DFLOCKD_HTTP_PORT`) to start the listener:

```bash
./dflockd --http-port 6389
```

TCP and HTTP can run concurrently on different ports, and they share the same `LockManager` — so a TCP client and an HTTP session contending on the same key are ordered together in a single FIFO queue.

| Flag | Env | Default | Description |
|---|---|---|---|
| `--http-port` | `DFLOCKD_HTTP_PORT` | `0` (disabled) | Port for the HTTP API |
| `--http-host` | `DFLOCKD_HTTP_HOST` | same as `--host` | Bind address for the HTTP API |
| `--http-session-idle-timeout` | `DFLOCKD_HTTP_SESSION_IDLE_S` | `20` | Advisory idle timeout reported to clients (seconds) |
| `--http-max-sessions` | `DFLOCKD_HTTP_MAX_SESSIONS` | `0` (unlimited) | Cap on concurrent HTTP sessions |
| `--http-sse-ping-interval` | `DFLOCKD_HTTP_SSE_PING_S` | `15` | Internal pinger interval for SSE streams (seconds) |

TLS and authentication reuse `--tls-cert` / `--tls-key` / `--auth-token` from the TCP server — same credentials, served on both listeners.

## The session model

The native protocol ties lock ownership to a persistent TCP connection. HTTP requests are independent, so the bridge introduces a **session** — a server-generated ID that maps to an in-process virtual connection. Every state-touching request carries `X-Dflockd-Session` so the bridge routes it to the correct virtual connection.

```
┌────────┐                          ┌────────────────────────┐
│ client │  POST /v1/sessions       │  virtual conn (pipe)   │
│        ├─────────────────────────►│  ↓                     │
│        │◄───── session_id         │  ServeConn(connID=42)  │
│        │                          │  ↓                     │
│        │  POST /v1/locks/foo      │  LockManager.Acquire   │
│        │  X-Dflockd-Session: ...  │                        │
└────────┘                          └────────────────────────┘
```

Two-phase `enqueue`/`wait` flows work across requests on the same session because they share one virtual connection.

## Quick start

```bash
# 1. Mint a session.
sid=$(curl -sX POST http://localhost:6389/v1/sessions | jq -r .session_id)

# 2. Acquire a lock.
curl -sX POST http://localhost:6389/v1/locks/my-job \
     -H "X-Dflockd-Session: $sid" \
     -H "Content-Type: application/json" \
     -d '{"acquire_timeout_s": 10, "lease_ttl_s": 60}'
# → {"status":"ok","token":"a1b2c3d4...","lease_ttl_s":60}

# 3. Do work, then release.
curl -sX POST http://localhost:6389/v1/locks/my-job/release \
     -H "X-Dflockd-Session: $sid" \
     -H "Content-Type: application/json" \
     -d '{"token": "a1b2c3d4..."}'

# 4. Close the session (with default settings, releases anything still held).
curl -sX DELETE http://localhost:6389/v1/sessions/$sid
```

## Staying alive

Every HTTP operation on a session resets the virtual connection's read deadline. Active sessions do not need pings. For **idle** lock or semaphore sessions, clients should call `POST /v1/sessions/{id}/ping` at roughly half the advertised `idle_timeout_s`.

Keep `--http-session-idle-timeout` lower than `--read-timeout` so the advertised heartbeat cadence is comfortably inside the protocol read deadline. The defaults do this (`20s` vs `23s`).

SSE streams use a dedicated internal session and the server sends protocol-level pings for that session, so SSE clients do not need to call the session ping endpoint.

If a client crashes without calling `DELETE`, cleanup comes from three paths:

1. **Lease expiry** — the lock is reclaimed `lease_ttl_s` seconds after the last renew.
2. **Virtual connection read timeout** — if no HTTP command reaches the session before the server's read timeout, the underlying protocol connection exits.
3. **Idle session sweeper** — the bridge reaps dead sessions and sessions that have been idle for about `2 × http-session-idle-timeout`. Active in-flight commands are not swept.

When `--auto-release-on-disconnect` is enabled (the default), deleting, timing out, or sweeping a session releases held locks and semaphore slots immediately. When it is disabled, session cleanup still cancels pending waiters and signal subscriptions, but held resources are released only by explicit `release` calls or lease expiry.

For quick recovery, keep lease TTLs short and renew aggressively.

## Authentication

When `--auth-token` is set, every request must carry `Authorization: Bearer <token>`. The one exception is `GET /v1/openapi.json`, which is always reachable so tools can fetch the schema without credentials.

## Validation limits

Keys, signal channels, tokens, and SSE queue groups are capped at 256 bytes to match the native TCP protocol's command/key/arg line limit. Signal payloads must be non-empty after trimming, cannot contain newlines, and must fit the 64 KiB pushed-frame limit after `sig <channel> ` framing. Violations return `400 bad_request`.

## Error mapping

Lock and semaphore wait timeouts are domain outcomes: they return HTTP `200` with `{"status":"timeout"}` so clients can distinguish "the request was valid, but acquisition timed out" from "the server rejected the request." Protocol-level conflicts such as already enqueued, not enqueued, lease expired, and semaphore limit mismatch return `409`.

| HTTP | When |
|---|---|
| `200` | Request processed (including domain timeouts) |
| `204` | Success with no body |
| `400` | Malformed JSON, bad parameter, missing header |
| `401` | Missing or invalid bearer token |
| `404` | Token doesn't match any holder |
| `409` | Already enqueued, not enqueued, lease expired, limit mismatch |
| `410` | Session ID unknown — you need to create a new session |
| `503` | Server capacity hit (max_locks, max_waiters, max_sessions) |

Error bodies have the shape `{"error": "<stable_code>", "detail": "<optional prose>"}`.

## Subscribing to signals

`GET /v1/signals?pattern=events.>` opens a Server-Sent Events stream. Each matched signal arrives as:

```
event: sig
data: {"channel":"events.user.login","payload":"{\"user\":\"alice\"}"}
```

The bridge uses a **dedicated** session per SSE stream so its internal pinger (configured by `--http-sse-ping-interval`, default 15 seconds) doesn't contend with any commands the caller may be running on a different session. When the HTTP client disconnects, the bridge unregisters the subscription automatically.

Pattern syntax matches the native protocol:

- `events.user.login` — exact channel
- `events.*.login` — single-token wildcard
- `events.>` — multi-token wildcard (tail)

Append `&group=<name>` to join a queue group — each matching signal is delivered to exactly one member, round-robin.

## Publishing signals

`POST /v1/signals/{channel}` is **sessionless** — the bridge creates a transient session, publishes, and tears it down. Ideal for webhook handlers and cron-style publishers.

```bash
curl -sX POST http://localhost:6389/v1/signals/events.user.login \
     -H "Content-Type: application/json" \
     -d '{"payload": "{\"user\":\"alice\"}"}'
# → {"delivered": 3}
```

## Full endpoint reference

See [`/v1/openapi.json`](./openapi.json) for the complete contract, or run:

```bash
curl http://localhost:6389/v1/openapi.json | jq .
```

Paste the URL into any OpenAPI viewer (Swagger UI, Redoc, Stoplight, `editor.swagger.io`) for an interactive reference.

## Cross-transport ordering

The TCP and HTTP transports share one `LockManager`, so acquires across transports are ordered together:

```
t=0.00  TCP client A: acquire("shared")           → ok, holds
t=0.10  TCP client B: enqueue("shared")           → queued
t=0.20  HTTP session C: enqueue("shared")         → queued
t=0.30  TCP client A: release                     → ok
t=0.31  TCP client B: wait                        → ok, grants to B (FIFO)
t=5.00  TCP client B: release
t=5.01  HTTP session C: wait                      → ok, grants to C
```

This is tested by the `TestCrossTransport_FIFOPreservation` test in `internal/httpapi/phase2_test.go`.

## What's not supported over HTTP

- **Binary payloads.** Signal payloads are UTF-8 text (same as TCP).
- **The raw `auth` command.** Authentication is handled by the bridge using the `Authorization` header; the protocol-level `auth` command is sent transparently.
- **Arbitrary listener reconnect.** SSE clients must re-subscribe after a reconnect; the server doesn't buffer missed signals. This matches the native protocol's slow-consumer eviction behavior.
