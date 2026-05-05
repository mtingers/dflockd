# Proposal: HTTP REST API via In-Process Protocol Bridge

!!! note "Historical proposal"
    This page records the design proposal that led to the HTTP API. It is not the current API contract. For implemented behavior, use the [HTTP API guide](../http-api.md) and [OpenAPI spec](../openapi.json).

## Summary

Add an opt-in HTTP REST + SSE layer alongside the existing TCP server, backed by an in-process "virtual connection" bridge that replays each HTTP request as a line-based protocol command through the unchanged `handleConn` code path.

The HTTP server is a pure translation layer. `internal/lock` and `internal/server` are not modified. The TCP protocol remains the canonical source of truth.

## Goals

- Full behavioral parity with TCP: FIFO ordering, lease expiry, two-phase `e`/`w`, signal pub/sub, auto-release on disconnect.
- Zero changes to `internal/lock` and `internal/server`.
- Usable by curl, webhook senders, serverless functions, and languages without a TCP client library.
- Cross-transport correctness: a TCP client and an HTTP client contending on the same key respect the same FIFO ordering.
- Off by default; purely additive.

## Non-goals

- Replacing the TCP protocol.
- Matching TCP throughput (HTTP has inherently higher per-op overhead).
- Metrics, Prometheus, OpenAPI schema generation — post-v1.

## Architecture

```
┌────────┐   HTTP   ┌──────────────┐  protocol lines  ┌────────────┐
│ client ├─────────►│  httpapi     ├──net.Pipe───────►│ handleConn │
│        │◄──JSON───┤  (bridge)    │                  │ (unchanged)│
└────────┘          │ ┌──sessions─┐│                  └─────┬──────┘
                    │ │ connID=42 ││                        │
                    │ │ pipes     ││                        ▼
                    │ │ mux       ││                  ┌────────────┐
                    │ └───────────┘│                  │LockManager │
                    └──────────────┘                  │  (shared)  │
                                                      └────────────┘
```

Every HTTP session owns:

- A `net.Pipe()` pair (client side + server side)
- A synthetic `connID` minted from the existing `connSeq` counter
- A `handleConn` goroutine blocked on the server-side pipe, reading protocol lines
- A multiplexer goroutine on the client-side pipe that splits command responses from `sig` push messages
- A `reqMu` mutex serializing protocol round-trips for that session

HTTP requests with a session header are translated to protocol lines, written to the pipe, and the response line is parsed into a JSON response.

## Session lifecycle

### Create

```
POST /v1/sessions
Authorization: Bearer <token>          (if server has --auth-token)
→ 200 {"session_id": "a3f...", "idle_timeout_s": 20}
```

Server:
1. Mints a 32-char hex session ID from `tokenBuf` (`lock.go:29`).
2. Calls `net.Pipe()` to create a pipe pair.
3. Allocates `connID := s.connSeq.Add(1)`.
4. Spawns `go s.handleConn(ctx, serverSidePipe, connID)` — identical to TCP handler.
5. Stores the client side of the pipe + metadata in the session map.
6. Returns the session ID and an advisory idle timeout (= server `ReadTimeout` minus safety margin).

If auth is enabled, the bridge writes `auth\n_\n<token>\n` to the new virtual conn before returning the session to the caller. A failing auth closes the virtual conn and returns `401`.

### Use

Any HTTP operation with `X-Dflockd-Session: <id>` is routed to that session. The bridge:
1. `session.reqMu.Lock()`
2. Writes protocol lines to `toSrv`
3. Reads one response line from `session.respCh` (fed by the multiplexer)
4. `reqMu.Unlock()`
5. Returns JSON

Every command write resets the virtual conn's read deadline, so active sessions never time out.

### Explicit close

```
DELETE /v1/sessions/{id}  →  204
```

Closes both pipe halves. `handleConn`'s `ReadLine` returns `Code: 11` (client disconnected), the deferred `s.lm.CleanupConnection(connID)` releases any held locks and transfers them to waiters. Synchronous — the HTTP 204 is returned after cleanup completes.

### Idle cleanup

Two layers of safety:

1. **Virtual conn read timeout** (already built in, `protocol.go:73`). If no HTTP requests arrive for the session in `ReadTimeout` (default 23s), `ReadLine` times out with code 10, `handleConn` breaks out of the loop (`server.go:276`), deferred cleanup runs.
2. **HTTP session sweeper** (new, ~30 lines). Prunes session map entries where the virtual conn has exited or `lastSeen > 2 * idle_timeout_s`. Subsequent requests to those IDs return `410 Gone`.

### Orphan detection in the multiplexer

If the multiplexer's `ReadLine` returns EOF (because `handleConn` exited), it marks the session dead, drains pending `respCh` waiters with an error, and removes the session from the map.

## Request/response multiplexing

The protocol interleaves synchronous responses (`ok ...`, `timeout`, `error*`) with asynchronous `sig ...` push frames on the same connection. The bridge replicates what TCP client libraries already do:

```go
// Per-session multiplexer goroutine
for {
    line, err := session.fromSrv.ReadLine()
    if err != nil {
        session.markDead(err)
        return
    }
    if bytes.HasPrefix(line, []byte("sig ")) {
        select {
        case session.sigCh <- line:
        default:
            // SSE consumer is slow; drop or evict.
        }
    } else {
        session.respCh <- line  // buffered 1; paired with reqMu
    }
}
```

`reqMu` guarantees only one command is in flight per session, so `respCh` never mixes responses from different requests.

## API surface

All endpoints prefixed `/v1`. JSON request/response bodies. Auth via `Authorization: Bearer <token>` header.

| Method | Path | Protocol cmd | Notes |
|---|---|---|---|
| POST | `/sessions` | — | Mint session, open virtual conn |
| DELETE | `/sessions/{id}` | — | Close virtual conn, release locks |
| POST | `/sessions/{id}/ping` | `ping` | Keepalive |
| GET | `/stats` | `stats` | Sessionless — uses a shared bridge conn |
| POST | `/locks/{key}` | `l` | Single-phase acquire |
| POST | `/locks/{key}/release` | `r` | |
| POST | `/locks/{key}/renew` | `n` | |
| POST | `/locks/{key}/enqueue` | `e` | Two-phase step 1 |
| POST | `/locks/{key}/wait` | `w` | Two-phase step 2 |
| POST | `/semaphores/{key}` | `sl` | |
| POST | `/semaphores/{key}/release` | `sr` | |
| POST | `/semaphores/{key}/renew` | `sn` | |
| POST | `/semaphores/{key}/enqueue` | `se` | |
| POST | `/semaphores/{key}/wait` | `sw` | |
| POST | `/signals/{channel}` | `signal` | Sessionless — one-shot virtual conn |
| GET | `/signals` (SSE) | `listen` / `unlisten` | Server-Sent Events stream |
| GET | `/openapi.json` | — | OpenAPI 3.1 spec (static, embedded) |

`X-Dflockd-Session` is required on all endpoints except `/sessions` (create), `/stats`, `/openapi.json`, and `POST /signals/{channel}`. Those use short-lived internal virtual conns owned by the bridge, or are fully static.

## Request/response schemas

### Acquire lock
```
POST /v1/locks/my-job
{"acquire_timeout_s": 10, "lease_ttl_s": 60}
→ 200 {"status": "ok", "token": "a1b2...", "lease_ttl_s": 60}
→ 200 {"status": "timeout"}
→ 503 {"error": "max_locks"} | {"error": "max_waiters"}
```

### Release
```
POST /v1/locks/my-job/release
{"token": "a1b2..."}
→ 204
→ 404 {"error": "not_held"}
```

### Renew
```
POST /v1/locks/my-job/renew
{"token": "a1b2...", "lease_ttl_s": 60}
→ 200 {"remaining_s": 60}
→ 404 {"error": "not_held_or_expired"}
```

### Two-phase enqueue / wait
```
POST /v1/locks/my-job/enqueue
{"lease_ttl_s": 60}
→ 200 {"status": "acquired", "token": "...", "lease_ttl_s": 60}
→ 200 {"status": "queued"}
→ 409 {"error": "already_enqueued"}
→ 503 {"error": "max_locks"} | {"error": "max_waiters"}

POST /v1/locks/my-job/wait
{"timeout_s": 10}
→ 200 {"status": "ok", "token": "...", "lease_ttl_s": 60}
→ 200 {"status": "timeout"}
→ 409 {"error": "not_enqueued"} | {"error": "lease_expired"}
```

### Semaphore acquire
```
POST /v1/semaphores/worker-pool
{"acquire_timeout_s": 10, "limit": 3, "lease_ttl_s": 60}
→ 200 {"status": "ok", "token": "...", "lease_ttl_s": 60}
→ 409 {"error": "limit_mismatch"}
```

### Signal publish
```
POST /v1/signals/events.user.login
{"payload": "{\"user\":\"alice\"}"}
→ 200 {"delivered": 3}
```

### Signal subscribe (SSE)
```
GET /v1/signals?pattern=events.>&group=workers
Accept: text/event-stream
X-Dflockd-Session: a3f...

→ HTTP/1.1 200 OK
  Content-Type: text/event-stream

  event: sig
  data: {"channel":"events.user.login","payload":"{\"user\":\"alice\"}"}

  event: sig
  data: {"channel":"events.order.created","payload":"..."}
```

The bridge's SSE handler writes `listen\n<pattern>\n<group>\n` to the virtual conn on stream open and `unlisten` on close. An internal 15s `ping` pumper on that session keeps the virtual conn from hitting its read timeout while idle.

Recommendation: SSE subscribers use a **dedicated session** (the bridge can open an implicit one per SSE stream). This avoids the internal pinger contending with user commands on `reqMu`.

### Ping
```
POST /v1/sessions/{id}/ping
→ 204
```

### Stats
```
GET /v1/stats
→ 200 {<same JSON as TCP stats>}
```

## Error mapping

Domain outcomes live in the response body. HTTP status codes are reserved for transport-level correctness.

| Protocol response | HTTP status | Body |
|---|---|---|
| `ok` (with data) | 200 | `{"status":"ok", ...}` |
| `ok` (no data) | 204 | — |
| `timeout` | 200 | `{"status":"timeout"}` |
| `queued` | 200 | `{"status":"queued"}` |
| `acquired` | 200 | `{"status":"acquired", ...}` |
| `error` on release/renew (not held) | 404 | `{"error":"not_held"}` |
| `error_auth` | 401 | `{"error":"unauthorized"}` |
| `error_max_locks` | 503 | `{"error":"max_locks"}` |
| `error_max_waiters` | 503 | `{"error":"max_waiters"}` |
| `error_limit_mismatch` | 409 | `{"error":"limit_mismatch"}` |
| `error_already_enqueued` | 409 | `{"error":"already_enqueued"}` |
| `error_not_enqueued` | 409 | `{"error":"not_enqueued"}` |
| `error_lease_expired` | 409 | `{"error":"lease_expired"}` |
| malformed JSON / missing field | 400 | `{"error":"bad_request", "detail":"..."}` |
| unknown session | 410 | `{"error":"session_gone"}` |
| session map full | 503 | `{"error":"max_sessions"}` |

`timeout` uses **200** not 408: the HTTP request itself succeeded; the lock-level timeout is a domain result, and clients should distinguish it from transport timeouts.

## OpenAPI specification

The HTTP API ships with a hand-authored OpenAPI 3.1.0 spec (`openapi.json`) that is the user-facing contract document. It is served at `GET /v1/openapi.json` and is also committed to the repo so that codegen tools (client generators, Postman, Stoplight, etc.) can be pointed at it directly.

### Why hand-authored, not generated

- **Zero-dependency policy.** Generator libraries (`kin-openapi`, `swaggo`, `go-swagger`) all pull in large dep trees. dflockd's `go.mod` is currently empty beyond stdlib and we want to keep it that way.
- **Descriptions matter more than schemas.** The value of this spec is the prose — explaining session semantics, lease behavior, SSE pinger, two-phase flow — which generators do poorly.
- **Low churn.** Once the endpoints stabilize, the spec changes rarely. The cost of hand-maintenance is small.

### Source and serving

- **Source file:** `internal/httpapi/openapi.json` — single JSON document, ~600 lines, pretty-printed (2-space indent) for diff review.
- **Embedding:** `//go:embed openapi.json` — baked into the binary so deployment is one artifact.
- **Endpoint:** `GET /v1/openapi.json` returns the file with `Content-Type: application/json`. No auth required (the spec describes auth but isn't itself protected).
- **Mirror copy:** `docs/openapi.json` — identical content, copied during build via a `go generate` directive or a Makefile target. Used by mkdocs and external tooling.

### Required content

The spec must include, for every endpoint:

- `summary` — one-line description, shows in tool TOCs.
- `description` — multi-paragraph markdown. Covers: purpose, when to use it, session/lease implications, cross-references to TCP protocol command (link to `docs/architecture/protocol.md`), any gotchas.
- `parameters` — path params, query params, `X-Dflockd-Session` header where applicable, `Authorization` header via `security`.
- `requestBody` — JSON schema with `example` and `description` for each field (including units — "seconds", "unix epoch", etc.).
- `responses` — every status code from the error-mapping table, each with a schema and at least one `example`.
- `tags` — grouped: `sessions`, `locks`, `semaphores`, `signals`, `introspection`.

At the document level:

- `info.title`, `info.version` (matches the server version), `info.description` (prose explaining the session model, lease renewal pattern, SSE, security, and linking to the full architecture docs).
- `servers` — with a `{host}` and `{port}` template so generated clients pick up the right URL.
- `components.securitySchemes.bearerAuth` — HTTP bearer auth referencing `--auth-token`.
- `components.schemas` — shared schemas for `Token`, `LockToken`, `SessionId`, `ErrorResponse`, `StatsResponse` (mirror of `lock.Stats` struct), `SemaphoreInfo`, etc. Keep them DRY.
- `components.parameters.SessionHeader` — reusable definition of `X-Dflockd-Session`.
- `components.responses.Unauthorized`, `SessionGone`, `BadRequest`, `CapacityExceeded` — reusable error envelopes.

### SSE documentation

OpenAPI 3.1 doesn't have first-class SSE support, but the `GET /v1/signals` endpoint should still be listed. Use:

- Response `content` type `text/event-stream` with a schema describing the `sig` event envelope.
- A clear `description` block explaining: the stream is long-lived, events have the form `event: sig\ndata: {...}\n\n`, reconnection is the client's responsibility, server pings the underlying conn every 15s (not visible to SSE consumers).
- An `x-sse: true` vendor extension so tooling that cares can flag it.

### Drift prevention

A test (`internal/httpapi/openapi_test.go`) enforces:

1. **Every registered handler path appears in the spec.** Walk the `http.ServeMux` routes (or a central `routes` slice) and assert each is present as a `paths` entry.
2. **Every spec path has a handler.** Conversely, iterate `spec.paths` and assert each has a route.
3. **Spec parses as valid JSON** and has required top-level keys (`openapi`, `info`, `paths`, `components`).
4. **Every response status code used in handler code appears in the spec for that path.** Optional: grep handler source for `w.WriteHeader(NNN)` and assert the spec declares that status.
5. **`info.version` matches the build version** (or a known placeholder in dev).

If drift is detected, the test fails with a diff pointing to the missing entry.

### Client generation (informational)

Downstream users can run `openapi-generator` against `https://server/v1/openapi.json` or the committed `docs/openapi.json`. We don't ship generated clients — the existing hand-written Go/Python/TS clients stay authoritative for TCP; OpenAPI-generated clients are for HTTP users who want one.

## Configuration

New flags/env vars (all safe defaults; disabled unless `--http-port` is set).

| Flag | Env | Default | Description |
|---|---|---|---|
| `--http-port` | `DFLOCKD_HTTP_PORT` | `0` (disabled) | HTTP listen port (e.g. 6389) |
| `--http-host` | `DFLOCKD_HTTP_HOST` | same as `--host` | HTTP bind address |
| `--http-session-idle-timeout` | `DFLOCKD_HTTP_SESSION_IDLE_S` | `20` | Session idle timeout (advisory; clients should ping sooner) |
| `--http-max-sessions` | `DFLOCKD_HTTP_MAX_SESSIONS` | `0` (unlimited) | Cap on concurrent sessions |
| `--http-sse-ping-interval` | `DFLOCKD_HTTP_SSE_PING_S` | `15` | Internal pinger interval for SSE streams |

Reused:
- `--tls-cert` / `--tls-key` — same cert is served on both TCP and HTTP listeners.
- `--auth-token` — required on every HTTP request if set. The bridge sends `auth\n_\n<token>\n` into each new virtual conn automatically (client never sees the protocol-level auth).
- `--read-timeout` — applied to the virtual conn just like TCP. Governs the orphan detection watchdog.

## Implementation phases

### Phase 1 — Bridge skeleton + single-phase ops

**Files to add:**
- `internal/httpapi/bridge.go` — session struct, `net.Pipe()` wiring, multiplexer goroutine, protocol command helpers (`writeCmd(session, cmd, key, arg) (string, error)`).
- `internal/httpapi/server.go` — HTTP server, routing (use `net/http` + `http.ServeMux`; no third-party router to stay zero-dep).
- `internal/httpapi/handlers.go` — handlers for sessions, ping, stats, `l`/`r`/`n`, `sl`/`sr`/`sn`, signal publish.
- `internal/httpapi/errors.go` — protocol response → HTTP status/body mapping.

**Files to modify:**
- `internal/config/config.go` — add new HTTP flags/env vars.
- `internal/server/server.go` — export a method to borrow `LockManager`, `signal.Manager`, and the shared `connSeq` counter for the bridge to use. Alternative: construct the bridge from `cmd/dflockd/main.go` with the same shared instances.
- `cmd/dflockd/main.go` — if `--http-port > 0`, start the HTTP server alongside TCP. Both share the same `*lock.LockManager` and `*signal.Manager`.

**Estimated size:** ~450 lines.

**Tests (`internal/httpapi/*_test.go`):**
- Session create → ID is valid hex, entry appears in session map.
- Session DELETE → entry removed, any held locks released (cross-check via TCP `stats`).
- Acquire + release round-trip.
- Acquire timeout (client request takes >= requested timeout, returns `{"status":"timeout"}`).
- Concurrent acquires on different keys parallelise.
- Auth failure returns 401 and closes virtual conn.

### Phase 2 — Two-phase + parity tests

**Files to modify:**
- `internal/httpapi/handlers.go` — add `/enqueue`, `/wait` for both locks and semaphores.

**Estimated size:** ~100 lines.

**Tests:**
- `/enqueue` returns `queued`, `/wait` on the same session returns the token.
- `/enqueue` returns `acquired` on fast path.
- `/wait` without prior `/enqueue` returns 409 `not_enqueued`.
- **Cross-transport FIFO**: one TCP client and one HTTP session enqueue the same key; verify the first one to enqueue wins.

### Phase 3 — SSE signals

**Files to add:**
- `internal/httpapi/sse.go` — SSE handler, internal pinger goroutine, per-SSE dedicated session.

**Estimated size:** ~200 lines.

**Tests:**
- Open SSE stream, publish via HTTP signal endpoint, consumer receives `event: sig`.
- Queue group: two SSE streams in the same group receive round-robin.
- Wildcard patterns (`events.>`) work.
- Idle SSE stream survives past the virtual conn read timeout (verifies internal pinger).
- Stream cancellation triggers `unlisten` + session cleanup.

### Phase 4 — OpenAPI spec + docs + bench

**Files to add:**
- `internal/httpapi/openapi.json` — hand-authored OpenAPI 3.1 spec, embedded via `//go:embed`. Covers every endpoint with prose `description` fields, schemas, examples, and security scheme. See the [OpenAPI specification](#openapi-specification) section above for required content.
- `internal/httpapi/openapi.go` — handler at `GET /v1/openapi.json` serving the embedded bytes.
- `internal/httpapi/openapi_test.go` — drift test asserting every registered handler path appears in the spec and vice versa; validates JSON structure and required top-level keys.
- `docs/openapi.json` — build-time copy of the embedded spec (populated by a `go generate` directive or Makefile target).
- `docs/http-api.md` — client-facing HTTP API reference (modeled on `docs/architecture/protocol.md`). Links to `/v1/openapi.json`.
- `docs/getting-started/http-examples.md` — curl examples for the top 5 flows (create session, acquire lock, release, publish signal, subscribe via SSE).

**Files to modify:**
- `mkdocs.yml` — nav entries for the HTTP API reference and the OpenAPI spec (link, not render).
- `cmd/bench/*` — optional `--transport http` flag to run the same benchmark against HTTP.
- `Makefile` — add `openapi-sync` target copying `internal/httpapi/openapi.json` to `docs/openapi.json`.
- `README.md` — HTTP quick-start section with a one-liner curl example.

**Estimated size:** OpenAPI spec ~600 lines of JSON, handler+test ~100 lines of Go, docs ~200 lines of markdown.

## Testing strategy

- **Unit**: multiplexer dispatch (sig vs response), session lifecycle, error mapping, auth.
- **Integration**: black-box HTTP client against a running server (reuse `internal/testutil` where useful).
- **Parity**: a test matrix running the same scenarios (acquire/release, two-phase, signals, stats, lease expiry) over both transports and asserting equivalent outcomes.
- **Cross-transport**: TCP and HTTP clients contending on the same keys; verify FIFO preservation and correct lease-expiry handoff.
- **Stress**: 1000 concurrent HTTP sessions doing acquire/release loops; verify no goroutine leaks (assert `runtime.NumGoroutine()` returns to baseline after drain).
- **Leak check**: open and close 10k sessions, assert session map empty and `LockManager.ConnEnqueuedCountForTest() == 0`.

## Performance expectations

Per-operation overhead added by the bridge on top of a direct `LockManager` call:
- Pipe write (in-process, no syscall): ~200 ns
- Goroutine wake (handleConn): ~5 µs
- Protocol parse + format (already exists): ~2 µs
- Multiplexer dispatch: ~1 µs
- **Bridge total: ~10 µs per op**

HTTP layer overhead (dominant):
- `net/http` request parse: ~50–100 µs
- `json.Unmarshal` / `Marshal`: ~10–50 µs
- TLS (if on): +200 µs

Bridge adds ~5–10% on top of the already-paid HTTP cost. Unmeasurable in practice for REST use cases.

Expected HTTP throughput: ~20–30k ops/s single-worker, ~50–80k concurrent (vs TCP's 14k / 95k respectively). HTTP will be latency-dominated by the framework, not the bridge.

## Risks and mitigations

| Risk | Mitigation |
|---|---|
| Goroutine growth (one per session) | `--http-max-sessions` cap; idle sweeper; same scaling model as TCP |
| Multiplexer line ordering corruption | `reqMu` serializes requests; `respCh` is size-1 buffered |
| SSE slow consumer stalls `sigCh` | Drop on full (existing TCP behavior); slow-consumer eviction via `CancelConn` path |
| Session ID leak via logs | Treat like auth tokens — redact; log session ID prefix only |
| Bridge protocol drift from TCP | Build protocol lines in one helper used by all handlers; parity tests catch divergence |
| Shutdown ordering | Close HTTP listener first, drain sessions (closes pipes), then TCP |

## Open questions

1. **Session reuse across client restarts.** If a client remembers its session ID, should reconnecting work? **Proposed:** no. Sessions are in-memory and ephemeral. Clients always create a fresh session on startup. Simpler, and avoids a new failure mode where a client "inherits" locks from a ghost.
2. **Multiple virtual conns per session for pipelining.** Should we allow concurrent in-flight requests per session? **Proposed:** not in v1. `reqMu` serializes. Revisit if real users hit the bottleneck.
3. **Dedicated session per SSE stream.** **Proposed:** yes — the bridge opens an implicit session on SSE connect and closes it on disconnect. Avoids mutex contention between the internal pinger and any commands the user runs on the same session.
4. **Metrics endpoint** (`/metrics` Prometheus). Out of scope for v1.

## Acceptance criteria

v1 is done when:

- All phase 1–3 tests pass.
- Parity test suite passes: every TCP protocol command has an HTTP equivalent producing equivalent state transitions.
- Cross-transport FIFO test passes.
- Zero-session baseline: with `--http-port 0` (default), no HTTP goroutines or listeners are started.
- 10k session create/close cycle leaks no goroutines and no lock state.
- `GET /v1/openapi.json` returns a valid OpenAPI 3.1 document describing every registered endpoint.
- OpenAPI drift test passes: spec and handlers are in 1:1 correspondence.
- `docs/http-api.md` and `docs/openapi.json` published in mkdocs.
- `CHANGELOG.md` entry.
