# Examples

Common patterns. Each example is independently runnable.

## Lock with auto-renewal

`Lock.Acquire` starts a background renewal goroutine; you only need
to call `Release` when you're done.

```go
l := &client.Lock{
    Key:        "etl-job",
    LeaseTTL:   30,                                 // seconds
    RenewRatio: 0.5,                                // renew at 15s
    Servers:    []string{"127.0.0.1:6388"},
    OnRenewError: func(err error) {
        log.Error("lost lock", "err", err)          // alert / fail closed
    },
}
got, _ := l.Acquire(ctx)
if !got { return }
defer l.Release(ctx)

// critical section can run for hours; lease keeps getting refreshed
// until Release or process exit.
```

## Two-phase queue with status reporting

```go
l := &client.Lock{Key: "deploy", Servers: servers, LeaseTTL: 60}
status, err := l.Enqueue(ctx)
if err != nil { return err }

switch status {
case "acquired":                                    // grabbed it immediately
    defer l.Release(ctx)
case "queued":
    log.Info("waiting in queue", "key", l.Key)
    got, err := l.Wait(ctx, 5*time.Minute)
    if err != nil || !got { return ... }
    defer l.Release(ctx)
}
```

`status == "queued"` means the server has reserved a slot in the
FIFO queue for this connection. You must call `Wait` (or `Release`,
which abandons the queue position) on the same `Lock` value — it
holds the connection.

## Multi-server sharding

```go
l := &client.Lock{
    Key:       "user:42:profile",
    Servers:   []string{
        "lock-a.internal:6388",
        "lock-b.internal:6388",
        "lock-c.internal:6388",
    },
    ShardFunc: client.CRC32Shard,                   // default; explicit here
}
```

`CRC32Shard` matches the official Python and TypeScript clients, so
clients in any language hash a given key to the same server. Drop
in a custom `ShardFunc` if you have a topology-aware mapping.

## TLS

Server:

```bash
./dflockd \
    --tls-cert /etc/dflockd/server.crt \
    --tls-key  /etc/dflockd/server.key \
    --http-port 6389
```

Both TCP and HTTP listeners use the same cert/key.

Client:

```go
l := &client.Lock{
    Key: "k",
    Servers: []string{"lockd.internal:6388"},
    TLSConfig: &tls.Config{
        ServerName: "lockd.internal",
        // RootCAs:   ...,                          // your CA bundle
    },
    AuthToken: "shared-secret",
}
```

## Auth

Tokens are a single shared secret, compared in constant time. Pass
via env, file, or flag — flags expose the token in `ps`, prefer
`--auth-token-file`:

```bash
./dflockd --auth-token-file /etc/dflockd.token
```

```go
l := &client.Lock{Key: "k", AuthToken: "shared-secret", ...}
```

```bash
curl -H "Authorization: Bearer shared-secret" \
     -X POST http://localhost:6389/v1/sessions
```

`/health` and `/ready` are intentionally exempt from auth so probes
work without credentials.

## Per-IP guardrails

```bash
./dflockd \
    --max-connections-per-ip 50 \
    --http-rate-limit-per-ip 100 \
    --http-max-sessions-per-ip 20
```

These caps are evaluated independently — a misbehaving client can
hit the connection cap (TCP refuse), the session cap (HTTP `503
max_sessions_per_ip`), or the rate limit (HTTP `429
rate_limited`).

## Raw TCP from a shell

Useful for one-off ops or for languages without a client library:

```bash
$ nc localhost 6388
l                          # command
my-key                     # key
10                         # acquire timeout, seconds
ok abc123 33               # ← server reply: status, token, lease_ttl
n                          # renew
my-key
abc123 60                  # token, new lease_ttl
ok 60                      # ← remaining seconds after renew
r                          # release
my-key
abc123
ok
```

The connection must stay open. Closing the TCP connection
auto-releases anything still held (unless the server was started
with `--auto-release-on-disconnect=false`).
