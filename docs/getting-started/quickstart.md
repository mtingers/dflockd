# Quick Start

## 1. Start the server

```bash
dflockd
```

The server listens on `0.0.0.0:6388` by default. See [Server Configuration](../server/configuration.md) for tuning options.

## 2. Acquire a lock

### Async client

```python
import asyncio
from dflockd.client import DistributedLock

async def main():
    async with DistributedLock("my-key", acquire_timeout_s=10) as lock:
        print(f"token={lock.token} lease={lock.lease}")
        # critical section — lease auto-renews in background

asyncio.run(main())
```

### Sync client

```python
from dflockd.sync_client import DistributedLock

with DistributedLock("my-key", acquire_timeout_s=10) as lock:
    print(f"token={lock.token} lease={lock.lease}")
    # critical section — lease auto-renews in background thread
```

## 3. Manual acquire/release

Both clients support explicit `acquire()` / `release()` outside of a context manager:

```python
from dflockd.sync_client import DistributedLock

lock = DistributedLock("my-key")
if lock.acquire():
    try:
        pass  # critical section
    finally:
        lock.release()
```

## What happens under the hood

1. The client opens a TCP connection to the server (selected via sharding if multiple servers are configured).
2. It sends a lock request with the key and timeout.
3. The server grants the lock immediately if it's free, or enqueues the client in FIFO order.
4. Once acquired, the client starts a background task/thread that renews the lease at `lease * renew_ratio` intervals.
5. On context manager exit (or explicit `release()`), the client sends a release command and closes the connection.
6. If the client disconnects without releasing, the server cleans up automatically.
