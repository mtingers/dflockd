# Examples

## Go client

The [Go client reference](../client.md) has complete examples for locks, semaphores, signals, two-phase acquisition, TLS, authentication, and sharding.

## TCP protocol

Each request is 3 newline-terminated lines: `command\nkey\narg\n`. See the [wire protocol reference](../architecture/protocol.md) for all commands and response formats.

### Lock and release

The connection must stay open — locks are auto-released on disconnect by default.

```bash
nc localhost 6388
l
my-key
10
# → ok abc123def456... 33

r
my-key
abc123def456...
# → ok
```

Custom lease TTL (60s):

```bash
nc localhost 6388
l
my-key
10 60
# → ok abc123def456... 60
```

### Renew

```bash
nc localhost 6388
l
my-key
10
# → ok abc123def456... 33

n
my-key
abc123def456...
# → ok 32
```

### Two-phase acquisition

Enqueue first, do application work, then wait:

```bash
nc localhost 6388
e
my-key

# → "acquired <token> 33" or "queued"

# ... application logic here ...

w
my-key
10
# → ok <token> 33

r
my-key
<token>
# → ok
```

### Semaphore

```bash
nc localhost 6388
sl
worker-pool
10 3
# → ok abc123def456... 33

sr
worker-pool
abc123def456...
# → ok
```

### Signals

```bash
# Terminal 1: subscribe
nc localhost 6388
listen
events.>

# → ok
# Signals arrive as: sig <channel> <payload>

# Terminal 2: publish
printf 'signal\nevents.user.login\n{"user":"alice"}\n' | nc localhost 6388
# → ok 1
```

Queue group (load-balanced — each signal goes to one member):

```bash
nc localhost 6388
listen
jobs.>
workers
# → ok
```

### Stats

```bash
printf 'stats\n_\n\n' | nc localhost 6388
# → ok {"connections":1,"locks":[],"semaphores":[],"signal_channels":[],...}
```
