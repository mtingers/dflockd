# Quick start

Run the server, then take a lock from each transport.

## 1. Start the server

```bash
./dflockd                                  # TCP only, on 127.0.0.1:6388
./dflockd --http-port 6389                 # TCP + HTTP REST
./dflockd --auth-token-file /etc/dflockd.token --http-port 6389
```

Both transports talk to the same in-memory `LockManager`, so a TCP
client and an HTTP session contending on the same key are ordered
together in a single FIFO queue.

## 2. Hold a lock from Go

```go
import (
    "context"
    "time"

    "github.com/mtingers/dflockd/client"
)

l := &client.Lock{
    Key:            "deploy-job",
    AcquireTimeout: 10 * time.Second,
    LeaseTTL:       60,                 // seconds; 0 = server default
    Servers:        []string{"127.0.0.1:6388"},
}

got, err := l.Acquire(context.Background())
if err != nil { /* retryable network error */ }
if !got     { /* timed out without acquiring */ }
defer l.Release(context.Background())

// ... critical section ...
```

`Lock.Acquire` runs background lease renewal at half the TTL, so a
holder that crashes loses the lock within `LeaseTTL` seconds — no
explicit ping required during normal operation.

## 3. Hold a lock from curl

```bash
# create a session (one per long-running caller)
sid=$(curl -sX POST http://localhost:6389/v1/sessions | jq -r .session_id)

# acquire
curl -sX POST http://localhost:6389/v1/locks/deploy-job \
     -H "X-Dflockd-Session: $sid" \
     -d '{"acquire_timeout_s": 10, "lease_ttl_s": 60}'
# → {"status":"ok","token":"7f4c...","lease_ttl_s":60}

# release
curl -sX POST http://localhost:6389/v1/locks/deploy-job/release \
     -H "X-Dflockd-Session: $sid" \
     -d '{"token":"7f4c..."}'
# → 204 No Content

# end the session (releases anything still held)
curl -sX DELETE http://localhost:6389/v1/sessions/$sid
```

## 4. Two-phase enqueue + wait

For workflows that need to *observe* the queue before blocking
(e.g. emit a "queued behind 5 jobs" status update):

```go
status, err := l.Enqueue(ctx)               // returns "acquired" or "queued"
if status == "queued" {
    got, err := l.Wait(ctx, 30*time.Second) // blocks until grant or timeout
    ...
}
```

Same shape over HTTP: `POST /v1/locks/{key}/enqueue` then `POST
/v1/locks/{key}/wait`.

## 5. Semaphore

```go
sem := &client.Semaphore{
    Key:     "rate-limited-api",
    Limit:   5,                              // 5 concurrent holders
    Servers: []string{"127.0.0.1:6388"},
}
sem.Acquire(ctx)
defer sem.Release(ctx)
```

A semaphore with `Limit:1` is equivalent to a `Lock`. The same key
cannot be used as both a lock and a semaphore — the second call
returns `error_limit_mismatch`.

## What to read next

- [Examples](examples.md) — common patterns (renewal, sharding, TLS,
  per-IP limits).
- [HTTP API](../http-api.md) — every endpoint and error code.
- [Server config](../server.md) — every flag and env var.
