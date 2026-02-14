# dflockd

A lightweight distributed lock server using a simple line-based TCP protocol with FIFO ordering, automatic lease expiry, and background renewal.

## Features

- **Strict FIFO ordering** — waiters are granted locks in the order they enqueue, per key
- **Two-phase lock acquisition** — split enqueue and wait to notify external systems between joining the queue and blocking
- **Automatic lease expiry** — held locks expire if not renewed, preventing deadlocks
- **Background renewal** — both async and sync clients auto-renew leases in the background
- **Disconnect cleanup** — locks are released automatically when a client disconnects
- **Multi-server sharding** — distribute keys across multiple servers with consistent hashing
- **Zero dependencies** — pure Python 3.13+ using only the standard library
- **Simple wire protocol** — line-based UTF-8 over TCP, easy to integrate from any language

## Quick example

```python
from dflockd.sync_client import DistributedLock

with DistributedLock("my-key", acquire_timeout_s=10) as lock:
    print(f"acquired: {lock.token}")
    # critical section — lease auto-renews in background
```

## Getting started

- [Installation](getting-started/installation.md) — install dflockd with pip or uv
- [Quick Start](getting-started/quickstart.md) — run the server and acquire your first lock
- [Examples](getting-started/examples.md) — async, sync, FIFO ordering, and multi-server demos
