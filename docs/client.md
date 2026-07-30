# Go client

```bash
go get github.com/mtingers/dflockd/client
```

Four client shapes are available:

1. `Conn` — one TCP/TLS connection, request/response framing.
2. Package-level functions (`Acquire`, `Release`, `Enqueue`,
   `Wait`, `SemAcquire`, …) — raw protocol calls on a `Conn`.
3. `Cluster` — one persistent, failover-aware logical session over a
   Raft member list.
4. `Lock` and `Semaphore` — high-level types that own a connection,
   run lease renewal in the background, and translate the two-phase
   API into a familiar shape.

Use `Cluster` for Raft deployments. Use `Lock` or `Semaphore` for
standalone or explicitly sharded servers.

## Cluster client

```go
cl, err := client.NewCluster(
    []string{"lock-a:6388", "lock-b:6388", "lock-c:6388"},
    client.WithClusterAuthToken(os.Getenv("DFLOCKD_AUTH_TOKEN")),
    client.WithClusterTLS(&tls.Config{RootCAs: roots}),
)
if err != nil { /* handle */ }
defer cl.Close()

token, ttl, err := cl.Acquire(ctx, "deploy", 30*time.Second)
if err != nil { /* handle */ }
defer cl.Release(context.Background(), "deploy", token)
```

`Cluster` caches member-clamped leader hints, rotates through known
members after transport failure, and retries up to its redirect budget.
It generates a random stable ref by default and re-sends it after
failover, preserving held-token and FIFO waiter identity.
`WithClusterStableRef` overrides that identity; one ref must identify one
logical session.

Calls are safe from concurrent goroutines. One session carries two
connections: a *session lane* for the calls that block for a grant
(`Acquire`, `Enqueue`, `Wait`, and their `Sem` variants) and a *control
lane* for the token-authorized lifecycle calls (`Release`, `Renew`) plus
`Barrier`. That split is what lets you renew a lease you already hold
while another goroutine is parked waiting on a different key. Two
consequences: a `Cluster` uses up to two connections, so size
`--max-conns` / `--max-conns-per-ip` accordingly; and a successful
`Renew` rebinds that lock's holder to the control lane, so from then on
it is the control lane's disconnect that auto-releases it. Concurrent
blocking calls still serialize on the session lane — give independent
workers their own `Cluster`.

`WithClusterRedirectBudget` changes the default three-attempt budget.
After exhaustion the error matches `ErrTooManyRedirects` and wraps the
terminal dial or redirect cause. `Close` is idempotent; later operations
return `ErrClusterClosed`.

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

// Fencing
func FenceFromToken(token string) (uint64, error)
```

`Option` is currently just `WithLeaseTTL(seconds)`.

### Fencing tokens

Every grant returns a 32-hex token whose first 16 hex chars are a
server-monotonic `uint64` (big-endian). The prefix strictly
increases on every grant from one dflockd server, so a token also
works as a [fencing token](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html).
Across restarts this is best-effort by default (wall-clock seed),
or strict when the server runs with `--fence-state-file`.
`FenceFromToken` parses the prefix:

```go
tok, _, _ := client.Acquire(conn, "row:42", 5*time.Second)
fence, _ := client.FenceFromToken(tok)   // uint64 to pass downstream
```

Fence comparison is meaningful **per key**: a downstream resource
stores the most recent fence it has observed for a key and rejects
any write whose fence compares less. Fences from different keys
aren't ordered relative to one another. A `Limit>1` semaphore
issues a distinct fence per grant — fencing orders the grants, not
the resource.

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

For `client.Cluster`, the same context controls connection
establishment, authentication, request I/O, and the redirect retry loop.
Cancellation drops the current connection so a later operation
re-establishes the stable logical session on a healthy member.

## Dial timeouts and TLS

```go
client.DefaultDialTimeout = 10 * time.Second  // package-level default

// TLS:
conn, err := client.DialTLS("lockd.internal:6388", &tls.Config{
    ServerName: "lockd.internal",
})
```

TCP keepalives are enabled with a 30-second probe interval.
