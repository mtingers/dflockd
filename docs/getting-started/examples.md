# Examples

All examples use the raw TCP protocol via netcat. dflockd's line-based protocol works with any TCP client in any language.

## Basic lock and release

Acquire a lock with a 10-second timeout, then release it:

```bash
# Terminal 1: Acquire a lock
printf 'l\nmy-key\n10\n' | nc localhost 6388
# Response: ok abc123def456... 33
```

```bash
# Terminal 2: Release the lock (substitute your token)
printf 'r\nmy-key\nabc123def456...\n' | nc localhost 6388
# Response: ok
```

## Lock with custom lease TTL

Specify a custom lease TTL (60 seconds) after the timeout:

```bash
printf 'l\nmy-key\n10 60\n' | nc localhost 6388
# Response: ok abc123def456... 60
```

## Renewing a lease

After acquiring a lock, renew the lease before it expires:

```bash
# In an interactive netcat session:
nc localhost 6388
# Acquire:
l
my-key
10
# Response: ok abc123def456... 33

# Renew (before lease expires):
n
my-key
abc123def456...
# Response: ok 32
```

## FIFO lock ordering

Multiple clients competing for the same lock are granted access in FIFO order. Open three terminals and run them in quick succession:

```bash
# Terminal 1
printf 'l\nfoo\n30\n' | nc localhost 6388
# Granted immediately: ok <token1> 33
```

```bash
# Terminal 2 (while terminal 1 holds the lock)
printf 'l\nfoo\n30\n' | nc localhost 6388
# Blocks until terminal 1's lease expires or lock is released
# Then granted: ok <token2> 33
```

```bash
# Terminal 3 (while terminal 1 holds the lock)
printf 'l\nfoo\n30\n' | nc localhost 6388
# Blocks until terminal 2 releases
# Then granted: ok <token3> 33
```

Terminal 2 is always granted before terminal 3, regardless of timing — strict FIFO order is maintained.

## Two-phase lock acquisition

Split enqueue and wait to perform application logic between joining the queue and blocking:

```bash
# Interactive session
nc localhost 6388

# Step 1: Enqueue for the lock
e
my-key

# Response: "acquired <token> 33" (if free) or "queued" (if contended)

# ... perform application logic here (e.g. notify external system) ...

# Step 2: Wait for the lock (10s timeout)
w
my-key
10
# Response: ok <token> 33
```

If the lock was free at enqueue time, it is acquired immediately (fast path) and `w` returns `ok` without blocking. The lease is reset to the full TTL from the moment `w` returns.

## Scripted two-phase example

```bash
#!/bin/bash
# two-phase.sh — enqueue, do work, then wait

exec 3<>/dev/tcp/localhost/6388

# Enqueue
printf 'e\nmy-key\n\n' >&3
read -r response <&3
echo "enqueue: $response"

# Application logic between enqueue and wait
echo "notifying external system..."
sleep 1

# Wait for lock
printf 'w\nmy-key\n10\n' >&3
read -r response <&3
echo "wait: $response"

# Extract token and release
token=$(echo "$response" | awk '{print $2}')
printf 'r\nmy-key\n%s\n' "$token" >&3
read -r response <&3
echo "release: $response"

exec 3>&-
```
