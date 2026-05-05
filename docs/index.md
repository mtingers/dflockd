# dflockd

A distributed FIFO lock server with leases and semaphores. Single Go
binary, zero runtime dependencies. Speaks a line-based TCP protocol
and an optional HTTP REST API — both share the same underlying lock
state.

## What it does

- **FIFO grants.** Per key, the first waiter to enqueue is the first
  to acquire.
- **Leases.** Every grant carries a TTL. Holders must renew or the
  server reclaims the slot.
- **Semaphores.** Same primitive with a configurable concurrent-holder
  limit per key.
- **Two-phase acquire.** Split into enqueue + wait so the caller can
  observe its queue position before blocking.
- **Auto-release on disconnect.** A torn-down TCP connection (or
  closed/idle HTTP session) drops its locks automatically.

## What it doesn't do

dflockd v2 is intentionally narrow: locking only. There is no pub/sub,
no SSE, no event stream. Earlier releases shipped a signal layer that
was removed in the v2 refactor (see the changelog).

## Quick taste

```bash
$ ./dflockd                     # listens on 127.0.0.1:6388
$ nc localhost 6388
l
my-job
10
ok 7f4c... 33
r
my-job
7f4c...
ok
```

Or via the REST API (start with `--http-port 6389`):

```bash
sid=$(curl -sX POST http://localhost:6389/v1/sessions | jq -r .session_id)
curl -sX POST http://localhost:6389/v1/locks/my-job \
     -H "X-Dflockd-Session: $sid" \
     -d '{"acquire_timeout_s": 10, "lease_ttl_s": 60}'
# → {"status":"ok","token":"...","lease_ttl_s":60}
```

## Where to go next

- [Quick start](getting-started/quickstart.md) — run the server, hold
  a lock from Go and curl.
- [HTTP API](http-api.md) — every REST endpoint, request/response
  shapes, error codes.
- [Server configuration](server.md) — every flag and env var.
- [Go client](client.md) — `Lock`, `Semaphore`, low-level `Conn`.
- [Wire protocol](architecture/protocol.md) — every TCP command and
  response.
- [Architecture overview](architecture/overview.md) — sharded
  LockManager, sessions, lifecycle.
