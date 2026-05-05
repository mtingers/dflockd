# Go client

```bash
go get github.com/mtingers/dflockd/client
```

Three layers, low to high:

1. `Conn` — one TCP/TLS connection, request/response framing.
2. Package-level functions (`Acquire`, `Release`, `Enqueue`,
   `Wait`, `SemAcquire`, …) — raw protocol calls on a `Conn`.
3. `Lock` and `Semaphore` — high-level types that own a connection,
   run lease renewal in the background, and translate the two-phase
   API into a familiar shape.

For most code, use `Lock` or `Semaphore`. Drop down to the lower
layers only when you need explicit connection management (e.g. one
TCP connection holding many short-lived locks).

## High-level: `Lock`

```go
type Lock struct {
    Key            string
    AcquireTimeout time.Duration   // default 10s
    LeaseTTL       int             // seconds; 0 = server default
    Servers        []string        // e.g. ["lock-a:6388", "lock-b:6388"]
    ShardFunc      ShardFunc       // defaults to CRC32Shard
    RenewRatio     float64         // fraction of lease at which to renew; default 0.5
    RenewJitter    float64         // early-only jitter; default 0.10
    TLSConfig      *tls.Config     // non-nil = TLS
    AuthToken      string          // non-empty = Authenticate after connect
    OnRenewError   func(err error) // optional; called when background renewal fails
}
```

### Methods

```go
func (l *Lock) Acquire(ctx context.Context) (bool, error)
func (l *Lock) Enqueue(ctx context.Context) (string, error)
func (l *Lock) Wait(ctx context.Context, timeout time.Duration) (bool, error)
func (l *Lock) Release(ctx context.Context) error
func (l *Lock) Token() string
func (l *Lock) Close() error
```

`Acquire` returns `(true, nil)` on grant, `(false, nil)` on timeout,
or `(false, err)` on any other failure. On success a background
renewal goroutine starts; calling `Release` (or `Close`) stops it.

`Enqueue` returns `"acquired"` (held immediately) or `"queued"`
(call `Wait` next). The same `Lock` value is the queue handle —
don't reuse it across goroutines while a Wait is in flight.

`Wait` returns `(true, nil)` on grant, `(false, nil)` on timeout,
`(false, ErrNotQueued)` if the prior `Enqueue` hasn't been called.
On timeout the connection is closed; you must `Enqueue` again to
re-queue.

`Release` is idempotent. If the caller is `queued` but not yet
granted, `Release` closes the connection (which dequeues the
waiter on the server) and returns nil.

## High-level: `Semaphore`

Same shape as `Lock` plus a required `Limit`:

```go
sem := &client.Semaphore{
    Key:     "rate-limited-api",
    Limit:   5,
    Servers: []string{"127.0.0.1:6388"},
}
```

A `Semaphore{Limit:1}` is equivalent to a `Lock`. Mixing the two
on the same key returns `ErrLimitMismatch`.

## Low-level: `Conn`

```go
conn, err := client.Dial("127.0.0.1:6388")
defer conn.Close()

if err := client.Authenticate(conn, "shared-secret"); err != nil { ... }

token, lease, err := client.Acquire(conn, "k", 10*time.Second)
if err != nil { ... }
defer client.Release(conn, "k", token)
```

`Conn` is safe for concurrent use; an internal mutex serialises
request/response pairs. If you need many keys held concurrently
under a single connection, just call `Acquire` with different keys
from different goroutines.

### Functions

```go
// Locks
func Acquire(c *Conn, key string, timeout time.Duration, opts ...Option) (token string, lease int, err error)
func Release(c *Conn, key, token string) error
func Renew(c *Conn, key, token string, opts ...Option) (remaining int, err error)
func Enqueue(c *Conn, key string, opts ...Option) (status, token string, lease int, err error)
func Wait(c *Conn, key string, timeout time.Duration) (token string, lease int, err error)

// Semaphores
func SemAcquire(c *Conn, key string, timeout time.Duration, limit int, opts ...Option) (token string, lease int, err error)
func SemRelease(c *Conn, key, token string) error
func SemRenew(c *Conn, key, token string, opts ...Option) (remaining int, err error)
func SemEnqueue(c *Conn, key string, limit int, opts ...Option) (status, token string, lease int, err error)
func SemWait(c *Conn, key string, timeout time.Duration) (token string, lease int, err error)

// Auth
func Authenticate(c *Conn, token string) error
```

`Option` is currently just `WithLeaseTTL(seconds)`.

## Sentinel errors

```go
ErrTimeout        // server reported "timeout"
ErrMaxLocks       // unique-key cap reached
ErrMaxWaiters     // per-key waiter cap reached
ErrLimitMismatch  // sem limit doesn't match existing key
ErrAlreadyQueued  // two-phase: enqueue with existing state
ErrNotQueued      // two-phase: wait without enqueue
ErrLeaseExpired   // promoted slot's lease expired before observation
ErrAuth           // server rejected auth
ErrDraining       // server is shutting down
ErrServer         // unknown / unmapped server response
```

Use `errors.Is`. Wrapped errors always wrap one of these.

## Sharding

Multi-server deployments hash each key to a single server:

```go
type ShardFunc func(key string, numServers int) int

l := &client.Lock{
    Key:     "user:42:profile",
    Servers: []string{"a:6388", "b:6388", "c:6388"},
    // ShardFunc defaults to CRC32Shard, which matches the
    // Python and TypeScript client implementations.
}
```

`CRC32Shard(key, n)` returns `crc32.IEEE(key) % n`. Same key always
maps to the same server, so any client (in any language) can find
the right server.

## Context cancellation

Every method that takes `ctx context.Context` honours it: cancellation
closes the underlying connection, which interrupts the in-flight
server I/O. A token granted as the ctx fires is best-effort released
back to the server before `Acquire` returns ctx.Err().

## Dial timeouts and TLS

```go
client.DefaultDialTimeout = 10 * time.Second  // package-level default

// TLS:
conn, err := client.DialTLS("lockd.internal:6388", &tls.Config{
    ServerName: "lockd.internal",
})
```

TCP keepalives are enabled with a 30-second probe interval.
