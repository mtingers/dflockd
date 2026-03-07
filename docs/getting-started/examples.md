# Examples

## Go client

The [Go client reference](../client.md) has complete examples for locks, semaphores, signals, two-phase acquisition, TLS, authentication, and sharding.

## TCP protocol

Each request is 3 newline-terminated lines: `command\nkey\narg\n`. See the [wire protocol reference](../architecture/protocol.md) for all commands and response formats.

### Lock and release

```bash
# Acquire (10s timeout)
printf 'l\nmy-key\n10\n' | nc localhost 6388
# → ok abc123def456... 33

# Release
printf 'r\nmy-key\nabc123def456...\n' | nc localhost 6388
# → ok

# Custom lease TTL (60s)
printf 'l\nmy-key\n10 60\n' | nc localhost 6388
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
# Acquire a slot (limit=3, 10s timeout)
printf 'sl\nworker-pool\n10 3\n' | nc localhost 6388
# → ok abc123def456... 33

# Release
printf 'sr\nworker-pool\nabc123def456...\n' | nc localhost 6388
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
