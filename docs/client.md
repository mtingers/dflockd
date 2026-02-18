# Go Client

The `client` package (`github.com/mtingers/dflockd/client`) provides a Go client for dflockd with two API levels: low-level protocol functions and a high-level `Lock` type with automatic lease renewal and sharding.

## Installation

```bash
go get github.com/mtingers/dflockd/client
```

## Quick start

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/mtingers/dflockd/client"
)

func main() {
    l := &client.Lock{
        Key:            "my-resource",
        AcquireTimeout: 10 * time.Second,
        Servers:        []string{"127.0.0.1:6388"},
    }

    ok, err := l.Acquire(context.Background())
    if err != nil {
        log.Fatal(err)
    }
    if !ok {
        log.Fatal("timed out waiting for lock")
    }
    defer l.Release(context.Background())

    fmt.Println("lock acquired, doing work...")
}
```

## High-level API: `Lock`

The `Lock` type manages the full lifecycle: connecting to the correct shard, acquiring the lock, renewing the lease in the background, and releasing on cleanup.

### Creating a Lock

```go
l := &client.Lock{
    Key:            "my-resource",
    AcquireTimeout: 10 * time.Second,      // default: 10s
    LeaseTTL:       60,                     // seconds; 0 = server default
    Servers:        []string{               // default: ["127.0.0.1:6388"]
        "10.0.0.1:6388",
        "10.0.0.2:6388",
        "10.0.0.3:6388",
    },
    ShardFunc:      client.CRC32Shard,      // default: CRC32Shard
    RenewRatio:     0.5,                    // default: 0.5
}
```

| Field | Default | Description |
|---|---|---|
| `Key` | (required) | The lock key name |
| `AcquireTimeout` | `10s` | How long to wait for the lock before timing out |
| `LeaseTTL` | `0` (server default) | Custom lease TTL in seconds |
| `Servers` | `["127.0.0.1:6388"]` | List of dflockd server addresses |
| `ShardFunc` | `CRC32Shard` | Function that maps a key to a server index |
| `RenewRatio` | `0.5` | Fraction of lease TTL at which to renew (e.g. 0.5 = renew at half the lease) |
| `TLSConfig` | `nil` | If non-nil, connect to the server using TLS with this `*tls.Config` |

### Single-phase acquire

```go
ok, err := l.Acquire(ctx)
if err != nil {
    // connection or server error
}
if !ok {
    // timed out
}
// Lock is held; background renewal is running.

err = l.Release(ctx)
```

`Acquire` returns `(false, nil)` on timeout rather than an error, so callers can distinguish timeouts from failures.

### Two-phase acquire

The two-phase flow lets you perform application logic between joining the queue and blocking:

```go
status, err := l.Enqueue(ctx)
if err != nil {
    log.Fatal(err)
}

if status == "queued" {
    // Perform application logic here (e.g. notify external system)
    fmt.Println("queued, notifying coordinator...")

    ok, err := l.Wait(ctx, 30*time.Second)
    if err != nil {
        log.Fatal(err)
    }
    if !ok {
        log.Fatal("timed out waiting for lock")
    }
}

// Lock is held
defer l.Release(ctx)
```

### Context cancellation

Passing a cancellable context to `Acquire` or `Wait` allows you to abort a blocked operation:

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

ok, err := l.Acquire(ctx)
if err != nil {
    // context.DeadlineExceeded or context.Canceled
}
```

### Cleanup without release

`Close` stops the renewal goroutine and closes the connection without sending a release command. The server will auto-release the lock (if configured):

```go
l.Close()
```

### Inspecting the token

```go
fmt.Println("token:", l.Token())
```

## Low-level API

The low-level functions operate on a `*Conn` and map directly to wire protocol commands. Use these when you need fine-grained control.

### Connecting

```go
c, err := client.Dial("127.0.0.1:6388")
if err != nil {
    log.Fatal(err)
}
defer c.Close()
```

### Connecting with TLS

```go
c, err := client.DialTLS("127.0.0.1:6388", &tls.Config{
    // Configure RootCAs, InsecureSkipVerify, etc.
})
if err != nil {
    log.Fatal(err)
}
defer c.Close()
```

For the high-level `Lock` and `Semaphore` types, set the `TLSConfig` field:

```go
l := &client.Lock{
    Key:       "my-resource",
    Servers:   []string{"127.0.0.1:6388"},
    TLSConfig: &tls.Config{RootCAs: pool},
}
```

### Acquire

```go
token, leaseTTL, err := client.Acquire(c, "my-key", 10*time.Second)
// With custom lease TTL:
token, leaseTTL, err := client.Acquire(c, "my-key", 10*time.Second, client.WithLeaseTTL(60))
```

### Release

```go
err := client.Release(c, "my-key", token)
```

### Renew

```go
remaining, err := client.Renew(c, "my-key", token)
// With custom lease TTL:
remaining, err := client.Renew(c, "my-key", token, client.WithLeaseTTL(60))
```

### Enqueue and Wait

```go
status, token, leaseTTL, err := client.Enqueue(c, "my-key")
if status == "queued" {
    token, leaseTTL, err = client.Wait(c, "my-key", 10*time.Second)
}
```

## High-level API: `Semaphore`

The `Semaphore` type manages the full lifecycle for a distributed semaphore slot: connecting, acquiring a slot (up to the limit), renewing the lease, and releasing.

### Creating a Semaphore

```go
s := &client.Semaphore{
    Key:            "worker-pool",
    Limit:          3,                          // max concurrent holders
    AcquireTimeout: 10 * time.Second,           // default: 10s
    LeaseTTL:       60,                         // seconds; 0 = server default
    Servers:        []string{"127.0.0.1:6388"}, // default: ["127.0.0.1:6388"]
    ShardFunc:      client.CRC32Shard,          // default: CRC32Shard
    RenewRatio:     0.5,                        // default: 0.5
}
```

| Field | Default | Description |
|---|---|---|
| `Key` | (required) | The semaphore key name |
| `Limit` | (required) | Maximum concurrent holders for this key |
| `AcquireTimeout` | `10s` | How long to wait for a slot before timing out |
| `LeaseTTL` | `0` (server default) | Custom lease TTL in seconds |
| `Servers` | `["127.0.0.1:6388"]` | List of dflockd server addresses |
| `ShardFunc` | `CRC32Shard` | Function that maps a key to a server index |
| `RenewRatio` | `0.5` | Fraction of lease TTL at which to renew |
| `TLSConfig` | `nil` | If non-nil, connect to the server using TLS with this `*tls.Config` |

### Single-phase acquire

```go
ok, err := s.Acquire(ctx)
if err != nil {
    // connection, server, or limit mismatch error
}
if !ok {
    // timed out
}
// Slot is held; background renewal is running.

err = s.Release(ctx)
```

### Two-phase acquire

```go
status, err := s.Enqueue(ctx)
if status == "queued" {
    ok, err := s.Wait(ctx, 30*time.Second)
    if !ok {
        log.Fatal("timed out")
    }
}
defer s.Release(ctx)
```

## Low-level Semaphore API

### SemAcquire

```go
token, leaseTTL, err := client.SemAcquire(c, "pool", 10*time.Second, 3)
// With custom lease TTL:
token, leaseTTL, err := client.SemAcquire(c, "pool", 10*time.Second, 3, client.WithLeaseTTL(60))
```

### SemRelease

```go
err := client.SemRelease(c, "pool", token)
```

### SemRenew

```go
remaining, err := client.SemRenew(c, "pool", token)
```

### SemEnqueue and SemWait

```go
status, token, leaseTTL, err := client.SemEnqueue(c, "pool", 3)
if status == "queued" {
    token, leaseTTL, err = client.SemWait(c, "pool", 10*time.Second)
}
```

## Error handling

The client defines sentinel errors that can be checked with `errors.Is()`:

```go
_, _, err := client.Acquire(c, "my-key", 0)
if errors.Is(err, client.ErrTimeout) {
    // lock was not available within the timeout
}
```

| Error | Meaning |
|---|---|
| `ErrTimeout` | The server returned `timeout` (lock/slot not acquired within the deadline) |
| `ErrMaxLocks` | The server returned `error_max_locks` (server lock+semaphore key limit reached) |
| `ErrServer` | The server returned an unexpected error response |
| `ErrNotQueued` | A `Wait`/`SemWait` was attempted without a prior `Enqueue`/`SemEnqueue` |
| `ErrLimitMismatch` | The server returned `error_limit_mismatch` (semaphore limit doesn't match existing key) |

## Sharding

When multiple servers are configured, the client uses a shard function to deterministically route each key to a single server. The default `CRC32Shard` uses CRC-32 (IEEE), matching the Python client's `stable_hash_shard`:

```go
idx := client.CRC32Shard("my-key", 3)  // 0, 1, or 2
```

To use a custom shard function:

```go
l := &client.Lock{
    Key:     "my-key",
    Servers: []string{"s1:6388", "s2:6388", "s3:6388"},
    ShardFunc: func(key string, n int) int {
        // your custom sharding logic
        return 0
    },
}
```

The shard function signature is:

```go
type ShardFunc func(key string, numServers int) int
```
