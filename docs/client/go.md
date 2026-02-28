# Go Client

The `github.com/mtingers/dflockd/client` package provides both high-level types with automatic lease renewal and low-level protocol functions for direct control.

```bash
go get github.com/mtingers/dflockd/client
```

## High-Level Types

### Lock

`Lock` manages exclusive distributed lock acquisition with automatic background lease renewal.

```go
lock := &client.Lock{
    Key:            "my-resource",
    Servers:        []string{"127.0.0.1:6388"},
    AcquireTimeout: 10 * time.Second, // default 10s
    LeaseTTL:       60,               // seconds; 0 = server default (33s)
    RenewRatio:     0.5,              // renew at 50% of lease (default)
    TLSConfig:      nil,              // set for TLS connections
    AuthToken:      "",               // set for authenticated servers
    ShardFunc:      nil,              // defaults to CRC32Shard
    OnRenewError:   func(err error) { log.Println("renew failed:", err) },
}

// Acquire blocks until the lock is held or timeout.
// Returns (false, nil) on timeout.
ok, err := lock.Acquire(ctx)
if err != nil {
    log.Fatal(err)
}
if !ok {
    log.Fatal("timeout")
}

// Fencing token for safe resource access
fmt.Println("Fence:", lock.Fence())

// Release stops renewal, releases the lock, and closes the connection.
err = lock.Release(ctx)
```

**Two-phase locking:**

```go
status, err := lock.Enqueue(ctx)
// status is "acquired" or "queued"

if status == "queued" {
    ok, err := lock.Wait(ctx, 30*time.Second)
    // ok is true if lock acquired, false on timeout
}

lock.Release(ctx)
```

### Semaphore

`Semaphore` manages distributed semaphore slot acquisition with automatic renewal.

```go
sem := &client.Semaphore{
    Key:            "worker-pool",
    Limit:          5,
    Servers:        []string{"127.0.0.1:6388"},
    AcquireTimeout: 10 * time.Second,
}

ok, err := sem.Acquire(ctx)
defer sem.Release(ctx)
```

Supports the same fields as `Lock` plus `Limit` (required, > 0). Also supports `Enqueue`/`Wait` two-phase acquire.

### RWLock

`RWLock` provides distributed read-write locking with automatic renewal.

```go
rw := &client.RWLock{
    Key:     "shared-data",
    Servers: []string{"127.0.0.1:6388"},
}

// Read lock (shared -- multiple readers allowed)
ok, err := rw.RLock(ctx)
defer rw.Unlock(ctx)

// Write lock (exclusive -- blocks all readers and writers)
ok, err := rw.WLock(ctx)
defer rw.Unlock(ctx)
```

`Unlock` automatically sends the correct unlock command (`rr` or `wr`) based on the acquire mode.

### SignalConn

`SignalConn` provides pub/sub signaling with a background reader that separates push signals from command responses.

```go
conn, _ := client.Dial("127.0.0.1:6388")
sc := client.NewSignalConn(conn)
defer sc.Close()

// Subscribe to a pattern
sc.Listen("events.*")

// Subscribe with a queue group (round-robin delivery within the group)
sc.Listen("tasks.>", client.WithGroup("workers"))

// Receive signals
go func() {
    for sig := range sc.Signals() {
        fmt.Printf("Channel: %s, Payload: %s\n", sig.Channel, sig.Payload)
    }
}()

// Emit a signal (returns receiver count)
count, _ := sc.Emit("events.user.login", "user123")
```

!!! warning
    The original `*Conn` must not be used directly after wrapping it with `NewSignalConn`.

### WatchConn

`WatchConn` provides key change notifications with a background reader.

```go
conn, _ := client.Dial("127.0.0.1:6388")
wc := client.NewWatchConn(conn)
defer wc.Close()

wc.Watch("config.*")

for event := range wc.Events() {
    fmt.Printf("Type: %s, Key: %s\n", event.Type, event.Key)
}
```

Event types include `kset`, `kdel`, `acquire`, `release`, etc.

### LeaderConn

`LeaderConn` provides leader election operations with a background reader for leader change events.

```go
conn, _ := client.Dial("127.0.0.1:6388")
lc := client.NewLeaderConn(conn)
defer lc.Close()

// Run for leader (blocking)
token, lease, fence, err := lc.Elect("my-election", 30*time.Second)

// Observe leader events on another connection
lc.Observe("my-election")

for event := range lc.Events() {
    fmt.Printf("Leader event: %s on %s\n", event.Type, event.Key)
    // Types: "elected", "resigned", "failover"
}
```

### Barrier

`BarrierWait` is a standalone function for barrier synchronization.

```go
conn, _ := client.Dial("127.0.0.1:6388")
defer conn.Close()

// Wait until 3 participants arrive (10s timeout)
tripped, err := client.BarrierWait(conn, "sync-point", 3, 10*time.Second)
```

## Options

### WithLeaseTTL

Override the server's default lease TTL for acquire/enqueue/renew operations:

```go
token, lease, err := client.Acquire(conn, "key", 10*time.Second, client.WithLeaseTTL(60))
```

### WithGroup

Set a queue group for signal subscriptions (round-robin delivery within the group):

```go
sc.Listen("tasks.>", client.WithGroup("workers"))
```

## Sharding

For multi-server deployments, the client supports key-based sharding:

```go
lock := &client.Lock{
    Key:       "my-resource",
    Servers:   []string{"server1:6388", "server2:6388", "server3:6388"},
    ShardFunc: client.CRC32Shard, // default
}
```

`CRC32Shard` uses CRC-32 (IEEE) to deterministically map keys to servers. You can provide a custom `ShardFunc`:

```go
type ShardFunc func(key string, numServers int) int
```

## Sentinel Errors

| Error | Meaning |
|-------|---------|
| `ErrTimeout` | Operation timed out |
| `ErrMaxLocks` | Server max lock keys reached |
| `ErrMaxWaiters` | Server max waiters per key reached |
| `ErrMaxKeys` | Server max aggregate keys reached |
| `ErrServer` | Generic server error |
| `ErrNotQueued` | Wait called without prior enqueue |
| `ErrAlreadyQueued` | Duplicate enqueue for same connection+key |
| `ErrLimitMismatch` | Semaphore limit doesn't match existing key |
| `ErrTypeMismatch` | Key exists as a different type |
| `ErrLeaseExpired` | Lease expired before operation completed |
| `ErrAuth` | Authentication failed |
| `ErrNotFound` | KV key doesn't exist |
| `ErrListFull` | List at max length |
| `ErrBarrierCountMismatch` | Barrier count differs from existing barrier |

All errors support `errors.Is()` for matching.

## Connection Management

```go
// Plain TCP
conn, err := client.Dial("127.0.0.1:6388")

// TLS
conn, err := client.DialTLS("127.0.0.1:6388", &tls.Config{})

// Authenticate
err = client.Authenticate(conn, "my-secret-token")

// Close
conn.Close()
```

- Default dial timeout: 10 seconds
- TCP keepalive: 30 seconds
- `Conn` is safe for concurrent use (serializes request/response pairs with a mutex)
