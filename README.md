# dflockd

A lightweight distributed lock server using a simple line-based TCP protocol with FIFO ordering, automatic lease expiry, and background renewal.

## Features

- **Distributed Locks** with FIFO ordering and automatic lease renewal
- **Semaphores** with configurable concurrency limits
- **Read-Write Locks** for shared/exclusive access patterns
- **Leader Election** with observer notifications
- **Atomic Counters** with increment/decrement/compare-and-set
- **KV Store** with optional TTL and compare-and-swap
- **Lists/Queues** with blocking pop operations
- **Pub/Sub Signaling** with wildcard patterns and queue groups
- **Key Watch** notifications for change events
- **Barriers** for multi-process synchronization
- **Fencing Tokens** for safe resource access across failures
- **Two-Phase Locking** (enqueue/wait) for non-blocking queue entry
- **TLS** and **token-based authentication**
- **64-way internal sharding** for low-contention concurrency

## Quick Start

### Install

```bash
go install github.com/mtingers/dflockd/cmd/dflockd@latest
```

### Run the Server

```bash
dflockd
# Listening on 127.0.0.1:6388
```

### Go Client

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/mtingers/dflockd/client"
)

func main() {
    lock := &client.Lock{
        Key:     "my-resource",
        Servers: []string{"127.0.0.1:6388"},
    }

    ok, err := lock.Acquire(context.Background())
    if err != nil {
        log.Fatal(err)
    }
    if !ok {
        log.Fatal("timeout acquiring lock")
    }
    defer lock.Release(context.Background())

    fmt.Println("Lock acquired, doing work...")
}
```

### Raw TCP

```
$ nc localhost 6388
l
my-key
10
acquired abc123def456... 33 1
r
my-key
abc123def456...
ok
```

## Documentation

Full documentation is available at [mtingers.github.io/dflockd](https://mtingers.github.io/dflockd/).

## License

See [LICENSE](LICENSE) for details.
