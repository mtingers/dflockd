# Go Client

The `client` package (`github.com/mtingers/dflockd/client`) provides a Go client for dflockd with high-level types (`Lock`, `Semaphore`, `SignalConn`) and low-level protocol functions.

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
| `AuthToken` | `""` | If non-empty, authenticate with this token after connecting |
| `OnRenewError` | `nil` | Optional callback invoked when background lease renewal fails |

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

### Authenticating

When the server requires authentication (`--auth-token`), call `Authenticate` after connecting:

```go
c, err := client.Dial("127.0.0.1:6388")
if err != nil {
    log.Fatal(err)
}
defer c.Close()

if err := client.Authenticate(c, "my-secret-token"); err != nil {
    log.Fatal(err) // ErrAuth if token is wrong
}
```

For the high-level types, set the `AuthToken` field:

```go
l := &client.Lock{
    Key:       "my-resource",
    Servers:   []string{"127.0.0.1:6388"},
    AuthToken: "my-secret-token",
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
| `AuthToken` | `""` | If non-empty, authenticate with this token after connecting |
| `OnRenewError` | `nil` | Optional callback invoked when background lease renewal fails |

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
| `ErrMaxWaiters` | The server returned `error_max_waiters` (waiter queue full for this key) |
| `ErrServer` | The server returned an unexpected error response |
| `ErrNotQueued` | A `Wait`/`SemWait` was attempted without a prior `Enqueue`/`SemEnqueue` |
| `ErrLimitMismatch` | The server returned `error_limit_mismatch` (semaphore limit doesn't match the limit set by the first acquirer of that semaphore key) |
| `ErrAlreadyQueued` | The server returned `error_already_enqueued` (connection already enqueued for this key) |
| `ErrLeaseExpired` | The server returned `error_lease_expired` (lease expired before the grant was consumed) |
| `ErrAuth` | The server returned `error_auth` (authentication failed or wrong token) |

## Signal API: `SignalConn`

The `SignalConn` type wraps a `*Conn` and provides pub/sub signal operations. It runs a background goroutine that separates asynchronous `sig ...` push messages from command responses.

### Creating a SignalConn

```go
c, err := client.Dial("127.0.0.1:6388")
if err != nil {
    log.Fatal(err)
}
sc := client.NewSignalConn(c)
defer sc.Close()
```

`NewSignalConn` accepts variadic `SignalConnOption` arguments. By default it starts a heartbeat goroutine that sends `ping` commands every 15 seconds to prevent the server from timing out idle signal connections:

```go
// Custom heartbeat interval
sc := client.NewSignalConn(c, client.WithHeartbeatInterval(10*time.Second))

// Disable heartbeat (not recommended — idle connections will be disconnected
// after the server's read timeout, typically 23s)
sc := client.NewSignalConn(c, client.WithHeartbeatInterval(0))
```

| Option | Default | Description |
|---|---|---|
| `WithHeartbeatInterval(d)` | `15s` | Interval between heartbeat pings. Set to `0` to disable. |

If the server requires authentication, call `Authenticate` on the `*Conn` before wrapping:

```go
c, err := client.Dial("127.0.0.1:6388")
if err != nil {
    log.Fatal(err)
}
if err := client.Authenticate(c, "my-secret-token"); err != nil {
    log.Fatal(err)
}
sc := client.NewSignalConn(c)
defer sc.Close()
```

### Subscribing to signals

```go
// Listen for exact channel
err := sc.Listen("events.user.login")

// Listen with single-token wildcard
err = sc.Listen("events.*.login")

// Listen with multi-token wildcard
err = sc.Listen("events.>")

// Listen with a queue group (load-balanced delivery)
err = sc.Listen("events.>", client.WithGroup("workers"))
```

Patterns support NATS-style wildcards:

- `*` — matches exactly one dot-separated token
- `>` — matches one or more trailing tokens (must be the last token)

### Receiving signals

```go
for sig := range sc.Signals() {
    fmt.Printf("channel=%s payload=%s\n", sig.Channel, sig.Payload)
}
```

`Signals()` returns a read-only channel of `Signal` structs. The channel is closed when the connection is closed or an error occurs.

### Publishing signals

```go
// Via SignalConn
n, err := sc.Emit("events.user.login", `{"user":"alice"}`)
fmt.Printf("delivered to %d listeners\n", n)

// Or via a regular Conn (no subscription needed for publishing)
n, err = client.Emit(c, "events.user.login", `{"user":"alice"}`)
```

The channel must be a literal name — no wildcards allowed.

### Unsubscribing

```go
err := sc.Unlisten("events.>")

// With queue group
err = sc.Unlisten("events.>", client.WithGroup("workers"))
```

### Closing

```go
err := sc.Close()
```

`Close` closes the underlying connection and waits for the background read loop to exit.

### Signal struct

```go
type Signal struct {
    Channel string // the literal channel the signal was published to
    Payload string // the signal payload
}
```

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
