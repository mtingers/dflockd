# Quick Start

## Start the Server

```bash
dflockd
```

By default, the server listens on `127.0.0.1:6388`.

## Acquire a Lock via Go Client

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

    fmt.Println("Lock acquired! Doing work...")
}
```

## Acquire a Lock via Raw TCP

Every request is exactly 3 lines: **command**, **key**, **argument**.

```
$ nc localhost 6388
l
my-key
10
```

The server responds with:

```
acquired abc123def456... 33 1
```

This means: lock acquired, token is `abc123def456...`, lease TTL is 33 seconds, fencing token is 1.

Release the lock:

```
r
my-key
abc123def456...
```

Response:

```
ok
```

## Two-Phase Locking

For non-blocking queue entry, use enqueue + wait:

=== "Go Client"

    ```go
    lock := &client.Lock{
        Key:     "my-resource",
        Servers: []string{"127.0.0.1:6388"},
    }

    status, err := lock.Enqueue(context.Background())
    if err != nil {
        log.Fatal(err)
    }

    if status == "queued" {
        ok, err := lock.Wait(context.Background(), 30*time.Second)
        if err != nil {
            log.Fatal(err)
        }
        if !ok {
            log.Fatal("timeout waiting for lock")
        }
    }

    defer lock.Release(context.Background())
    fmt.Println("Lock acquired!")
    ```

=== "Raw TCP"

    ```
    e
    my-key

    ```

    Response: `queued` or `acquired <token> <lease> <fence>`

    If queued, wait:

    ```
    w
    my-key
    30
    ```

    Response: `ok <token> <lease> <fence>` or `timeout`

## Use a Semaphore

```go
sem := &client.Semaphore{
    Key:     "worker-pool",
    Limit:   5, // allow 5 concurrent holders
    Servers: []string{"127.0.0.1:6388"},
}

ok, err := sem.Acquire(context.Background())
if err != nil {
    log.Fatal(err)
}
if !ok {
    log.Fatal("timeout")
}
defer sem.Release(context.Background())
```

## Use Atomic Counters

```go
conn, _ := client.Dial("127.0.0.1:6388")
defer conn.Close()

val, _ := client.Incr(conn, "page-views", 1)
fmt.Println("Counter:", val)
```

## Use the KV Store

```go
conn, _ := client.Dial("127.0.0.1:6388")
defer conn.Close()

client.KVSet(conn, "config:timeout", "30", 0) // no TTL
val, _ := client.KVGet(conn, "config:timeout")
fmt.Println("Value:", val) // "30"
```

## Next Steps

- [Server Configuration](../server/configuration.md) -- all flags, env vars, and defaults
- [Wire Protocol](../server/protocol.md) -- complete command reference
- [Go Client](../client/go.md) -- high-level types and examples
