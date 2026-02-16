# Quick Start

## 1. Start the server

```bash
./dflockd
```

The server listens on `0.0.0.0:6388` by default. See [Server Configuration](../server.md) for tuning options.

## 2. Acquire a lock

Using netcat (or any TCP client):

```bash
printf 'l\nmy-key\n10\n' | nc localhost 6388
```

This sends a lock request for key `my-key` with a 10-second acquire timeout. On success, the server responds:

```
ok <token> 33
```

The token is a unique identifier for this lock hold, and `33` is the lease TTL in seconds.

## 3. Release a lock

```bash
printf 'r\nmy-key\n<token>\n' | nc localhost 6388
```

Replace `<token>` with the token returned from step 2. The server responds:

```
ok
```

## 4. Interactive session

For an interactive session, use netcat without piping:

```bash
nc localhost 6388
```

Then type each line of the protocol manually:

```
l
my-key
10
```

The server responds with `ok <token> <lease_ttl>`. To release, type:

```
r
my-key
<token>
```

## What happens under the hood

1. The client opens a TCP connection to the server.
2. It sends a lock request with the key and timeout.
3. The server grants the lock immediately if it's free, or enqueues the client in FIFO order.
4. Once acquired, the client is responsible for renewing the lease before it expires (using the `n` command).
5. On release (or disconnect), the server frees the lock and grants it to the next FIFO waiter.
6. Background goroutines handle lease expiry sweeps and garbage collection of idle lock state.
