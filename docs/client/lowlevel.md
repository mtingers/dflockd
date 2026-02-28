# Low-Level API

The low-level API provides direct protocol functions that map 1:1 to wire commands. Use these when you need fine-grained control over connections and don't want automatic lease renewal.

## Connection

```go
// Dial connects to a dflockd server.
conn, err := client.Dial("127.0.0.1:6388")
defer conn.Close()

// DialTLS connects using TLS.
conn, err := client.DialTLS("127.0.0.1:6388", &tls.Config{
    InsecureSkipVerify: true, // for testing only
})

// Authenticate after connecting.
err = client.Authenticate(conn, "my-token")
```

`Conn` is safe for concurrent use. A mutex serializes request/response pairs so that concurrent callers cannot interleave their protocol bytes.

## Exclusive Locks

```go
// Acquire -- blocks until lock is held or timeout
token, leaseTTL, err := client.Acquire(conn, "key", 10*time.Second)
token, leaseTTL, fence, err := client.AcquireWithFence(conn, "key", 10*time.Second)

// Release
err := client.Release(conn, "key", token)

// Renew lease
remaining, err := client.Renew(conn, "key", token)
remaining, fence, err := client.RenewWithFence(conn, "key", token)

// Two-phase: Enqueue (non-blocking)
status, token, leaseTTL, err := client.Enqueue(conn, "key")
status, token, leaseTTL, fence, err := client.EnqueueWithFence(conn, "key")

// Two-phase: Wait (blocking)
token, leaseTTL, err := client.Wait(conn, "key", 30*time.Second)
token, leaseTTL, fence, err := client.WaitWithFence(conn, "key", 30*time.Second)
```

## Semaphores

```go
token, leaseTTL, err := client.SemAcquire(conn, "key", 10*time.Second, 5)
token, leaseTTL, fence, err := client.SemAcquireWithFence(conn, "key", 10*time.Second, 5)

err := client.SemRelease(conn, "key", token)

remaining, err := client.SemRenew(conn, "key", token)
remaining, fence, err := client.SemRenewWithFence(conn, "key", token)

status, token, leaseTTL, err := client.SemEnqueue(conn, "key", 5)
status, token, leaseTTL, fence, err := client.SemEnqueueWithFence(conn, "key", 5)

token, leaseTTL, err := client.SemWait(conn, "key", 30*time.Second)
token, leaseTTL, fence, err := client.SemWaitWithFence(conn, "key", 30*time.Second)
```

## Read-Write Locks

```go
// Read lock
token, leaseTTL, fence, err := client.RLock(conn, "key", 10*time.Second)
err := client.RUnlock(conn, "key", token)
remaining, fence, err := client.RRenew(conn, "key", token)

// Write lock
token, leaseTTL, fence, err := client.WLock(conn, "key", 10*time.Second)
err := client.WUnlock(conn, "key", token)
remaining, fence, err := client.WRenew(conn, "key", token)

// Two-phase
status, token, leaseTTL, fence, err := client.REnqueue(conn, "key")
status, token, leaseTTL, fence, err := client.WEnqueue(conn, "key")
token, leaseTTL, fence, err := client.RWait(conn, "key", 30*time.Second)
token, leaseTTL, fence, err := client.WWait(conn, "key", 30*time.Second)
```

## Atomic Counters

```go
newVal, err := client.Incr(conn, "counter", 1)
newVal, err := client.Decr(conn, "counter", 1)
val, err := client.GetCounter(conn, "counter")
err := client.SetCounter(conn, "counter", 100)
```

## KV Store

```go
err := client.KVSet(conn, "key", "value", 60)   // TTL 60s
err := client.KVSet(conn, "key", "value", 0)     // no TTL
val, err := client.KVGet(conn, "key")             // ErrNotFound if missing
err := client.KVDel(conn, "key")

// Compare-and-swap
swapped, err := client.KVCAS(conn, "key", "old", "new", 0)
// swapped=true on success, false on mismatch (no error)
// oldValue="" to create-if-not-exists
```

## Lists / Queues

```go
length, err := client.LPush(conn, "list", "value")
length, err := client.RPush(conn, "list", "value")
val, err := client.LPop(conn, "list")   // ErrNotFound if empty
val, err := client.RPop(conn, "list")   // ErrNotFound if empty
length, err := client.LLen(conn, "list")
items, err := client.LRange(conn, "list", 0, -1) // all items

// Blocking pop
val, err := client.BLPop(conn, "list", 10*time.Second) // ErrTimeout on timeout
val, err := client.BRPop(conn, "list", 10*time.Second)
```

## Signaling

```go
// Emit from a regular connection (no listener needed)
count, err := client.Emit(conn, "channel", "payload")
```

For receiving signals, use `SignalConn` (see [Go Client](go.md#signalconn)).

## Barriers

```go
tripped, err := client.BarrierWait(conn, "sync", 3, 10*time.Second)
```

## WithLeaseTTL Option

All acquire, enqueue, and renew functions accept `client.WithLeaseTTL(seconds)`:

```go
token, lease, err := client.Acquire(conn, "key", 10*time.Second, client.WithLeaseTTL(60))
remaining, err := client.Renew(conn, "key", token, client.WithLeaseTTL(60))
```

## Direct TCP Usage

You can interact with dflockd using any TCP client:

```bash
# Acquire a lock with 10s timeout
printf 'l\nmy-key\n10\n' | nc localhost 6388

# Set a KV pair (tab separates value from TTL)
printf 'kset\nmy-key\nhello\t60\n' | nc localhost 6388

# Increment a counter
printf 'incr\nhits\n1\n' | nc localhost 6388
```
