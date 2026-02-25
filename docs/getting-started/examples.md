# Examples

Examples are shown using both the raw TCP protocol (via netcat) and the Go client library. dflockd's line-based protocol works with any TCP client in any language.

## Go client examples

### Basic lock and release

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/mtingers/dflockd/client"
)

func main() {
    l := &client.Lock{
        Key:            "my-key",
        AcquireTimeout: 10 * time.Second,
        Servers:        []string{"127.0.0.1:6388"},
    }

    ok, err := l.Acquire(context.Background())
    if err != nil {
        log.Fatal(err)
    }
    if !ok {
        log.Fatal("timed out")
    }
    fmt.Println("acquired lock, token:", l.Token())

    // Lock is automatically renewed in the background.
    // Do work here...

    if err := l.Release(context.Background()); err != nil {
        log.Fatal(err)
    }
    fmt.Println("released")
}
```

### Two-phase acquisition

```go
l := &client.Lock{
    Key:     "my-key",
    Servers: []string{"127.0.0.1:6388"},
}

status, err := l.Enqueue(context.Background())
if err != nil {
    log.Fatal(err)
}
fmt.Println("enqueue status:", status)

if status == "queued" {
    // Application logic between enqueue and wait
    fmt.Println("notifying external system...")

    ok, err := l.Wait(context.Background(), 30*time.Second)
    if err != nil {
        log.Fatal(err)
    }
    if !ok {
        log.Fatal("timed out waiting")
    }
}

defer l.Release(context.Background())
fmt.Println("lock held, doing work...")
```

### Low-level API

```go
c, err := client.Dial("127.0.0.1:6388")
if err != nil {
    log.Fatal(err)
}
defer c.Close()

token, lease, err := client.Acquire(c, "my-key", 10*time.Second)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("acquired: token=%s lease=%ds\n", token, lease)

remaining, err := client.Renew(c, "my-key", token)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("renewed: %ds remaining\n", remaining)

if err := client.Release(c, "my-key", token); err != nil {
    log.Fatal(err)
}
fmt.Println("released")
```

### Sharding across multiple servers

```go
l := &client.Lock{
    Key: "my-key",
    Servers: []string{
        "10.0.0.1:6388",
        "10.0.0.2:6388",
        "10.0.0.3:6388",
    },
    // CRC32Shard is the default; each key is routed to one server
}

ok, err := l.Acquire(context.Background())
// ...
```

### Connecting with TLS

```go
import (
    "context"
    "crypto/tls"
    "crypto/x509"
    "fmt"
    "log"
    "os"
    "time"

    "github.com/mtingers/dflockd/client"
)

func main() {
    // Load CA certificate to verify the server
    caCert, err := os.ReadFile("/path/to/ca.pem")
    if err != nil {
        log.Fatal(err)
    }
    pool := x509.NewCertPool()
    pool.AppendCertsFromPEM(caCert)

    l := &client.Lock{
        Key:            "my-resource",
        AcquireTimeout: 10 * time.Second,
        Servers:        []string{"127.0.0.1:6388"},
        TLSConfig:      &tls.Config{RootCAs: pool},
    }

    ok, err := l.Acquire(context.Background())
    if err != nil {
        log.Fatal(err)
    }
    if !ok {
        log.Fatal("timed out")
    }
    defer l.Release(context.Background())

    fmt.Println("acquired lock over TLS")
}
```

### Connecting with authentication

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/mtingers/dflockd/client"
)

func main() {
    l := &client.Lock{
        Key:            "my-resource",
        AcquireTimeout: 10 * time.Second,
        Servers:        []string{"127.0.0.1:6388"},
        AuthToken:      "my-secret-token",
    }

    ok, err := l.Acquire(context.Background())
    if err != nil {
        log.Fatal(err)
    }
    if !ok {
        log.Fatal("timed out")
    }
    defer l.Release(context.Background())

    fmt.Println("authenticated and acquired lock")
}
```

## Benchmarking

dflockd ships with a built-in benchmark tool (`cmd/bench`) that measures lock acquire/release latency and throughput under concurrent load. Each worker goroutine dials a persistent TCP connection and reuses it for all rounds, so the benchmark measures lock latency rather than TCP connection overhead.

### Quick start

```bash
# Start the server
./dflockd

# Run the benchmark (10 workers, 50 rounds each)
go run ./cmd/bench
```

### Flags

| Flag            | Default          | Description                                          |
| --------------- | ---------------- | ---------------------------------------------------- |
| `--workers`     | `10`             | Number of concurrent goroutines                      |
| `--rounds`      | `50`             | Acquire/release rounds per worker                    |
| `--key`         | `bench`          | Lock key prefix                                      |
| `--timeout`     | `30`             | Acquire timeout (seconds)                            |
| `--lease`       | `10`             | Lease TTL (seconds)                                  |
| `--servers`     | `127.0.0.1:6388` | Comma-separated host:port pairs                      |
| `--connections` | `0`              | Persistent connections per worker (0 = 1 per worker) |

### Example output

```bash
# taken from macbook air m1 benchmark:
$ bench: 100 workers x 500 rounds (key_prefix="bench", conns/worker=1)

  total ops : 50000
  wall time : 0.575s
  throughput: 86922.7 ops/s

  mean      : 1.116 ms
  min       : 0.037 ms
  max       : 21.251 ms
  p50       : 0.895 ms
  p99       : 5.112 ms
  stdev     : 0.942 ms

```

Each worker uses a unique randomized key so all workers run in parallel without contending. To benchmark contended locks, use the same `--key` value with `--workers 1` and increase `--rounds`.

### Multiple servers

```bash
go run ./cmd/bench --servers 10.0.0.1:6388,10.0.0.2:6388,10.0.0.3:6388
```

Keys are sharded across servers using CRC32, matching the client library's default.

## TCP protocol examples

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

## Querying server stats

The `stats` command returns a JSON snapshot of the server's current state — active connections, held locks, semaphores, and idle entries awaiting garbage collection.

```bash
nc localhost 6388
stats
_

# Response: ok {"connections":1,"locks":[],"semaphores":[],"idle_locks":[],"idle_semaphores":[]}
```

With locks and semaphores held:

```bash
# In an interactive session, acquire a lock and semaphore first, then check stats:
nc localhost 6388
l
my-key
10
# Response: ok abc123def456... 33

sl
worker-pool
10 3
# Response: ok 789abc012def... 33

stats
_

# Response: ok {"connections":1,"locks":[{"key":"my-key","owner_conn_id":1,"lease_expires_in_s":32.5,"waiters":0}],"semaphores":[{"key":"worker-pool","limit":3,"holders":1,"waiters":0}],"idle_locks":[],"idle_semaphores":[]}
```

One-liner with printf:

```bash
printf 'stats\n_\n\n' | nc localhost 6388
```

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
