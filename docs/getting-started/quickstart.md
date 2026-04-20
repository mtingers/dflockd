# Quick Start

## 1. Start the server

```bash
./dflockd
```

The server listens on `127.0.0.1:6388` by default. See [Server Configuration](../server.md) for tuning options.

## 2. Acquire and release a lock

Using an interactive netcat session (the connection must stay open — locks are auto-released on disconnect by default):

```bash
$ nc localhost 6388
l
my-key
10
ok abc123... 33
r
my-key
abc123...
ok
```

- `l` is the lock command, `my-key` is the lock key, `10` is the acquire timeout in seconds.
- The server responds with `ok <token> <lease_ttl>`. The token identifies this lock hold; `33` is the lease TTL in seconds.
- `r` releases the lock using the token from the acquire response.

## 5. Subscribe to signals

dflockd also supports pub/sub signals with pattern matching:

```bash
# Terminal 1: subscribe
nc localhost 6388
listen
events.>

# Response: ok
# Signals arrive as: sig <channel> <payload>
```

```bash
# Terminal 2: publish
printf 'signal\nevents.hello\nworld\n' | nc localhost 6388
# Response: ok 1
```

Terminal 1 receives: `sig events.hello world`

See [Examples](examples.md) for more signal patterns, queue groups, and Go client usage.

## What happens under the hood

1. The client opens a TCP connection to the server.
2. It sends a lock request with the key and timeout.
3. The server grants the lock immediately if it's free, or enqueues the client in FIFO order.
4. Once acquired, the client is responsible for renewing the lease before it expires (using the `n` command).
5. On release (or disconnect, with the default auto-release setting), the server frees the lock and grants it to the next FIFO waiter.
6. Background goroutines handle lease expiry sweeps and garbage collection of idle lock state.
