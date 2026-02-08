# Async Client

The async client uses `asyncio` for non-blocking lock operations with automatic background lease renewal.

```python
from dflockd.client import DistributedLock
```

## Context manager

The recommended way to use the client. The lock is acquired on entry and released on exit:

```python
import asyncio
from dflockd.client import DistributedLock

async def main():
    async with DistributedLock("my-key", acquire_timeout_s=10) as lock:
        print(f"token={lock.token} lease={lock.lease}")
        # critical section

asyncio.run(main())
```

If the lock cannot be acquired within the timeout, a `TimeoutError` is raised.

## Manual acquire/release

For cases where a context manager doesn't fit:

```python
lock = DistributedLock("my-key", acquire_timeout_s=10)
acquired = await lock.acquire()
if acquired:
    try:
        # critical section
        pass
    finally:
        await lock.release()
```

`acquire()` returns `False` on timeout instead of raising.

## Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `key` | `str` | *(required)* | Lock name |
| `acquire_timeout_s` | `int` | `10` | Seconds to wait for acquisition |
| `lease_ttl_s` | `int \| None` | `None` | Lease duration (seconds). `None` uses server default |
| `servers` | `list[tuple[str, int]]` | `[("127.0.0.1", 6388)]` | Server addresses |
| `sharding_strategy` | `ShardingStrategy` | `stable_hash_shard` | Key-to-server mapping function |
| `renew_ratio` | `float` | `0.5` | Renew at `lease * ratio` seconds |

## Attributes

After acquiring a lock, these attributes are available:

| Attribute | Type | Description |
|---|---|---|
| `token` | `str \| None` | The lock token (UUID hex). `None` if not held |
| `lease` | `int` | Lease duration in seconds as reported by the server |

## Background renewal

Once a lock is acquired, the client starts an `asyncio.Task` that sends renew requests at `lease * renew_ratio` intervals. If renewal fails (server unreachable, lease already expired), the client logs an error and sets `token = None`.

The renewal task is cancelled automatically on `release()`, context manager exit, or `aclose()`.

## Cleanup

If you use manual `acquire()`, always call `release()` or `aclose()` to clean up the connection:

```python
lock = DistributedLock("my-key")
try:
    if await lock.acquire():
        # work
        await lock.release()
finally:
    await lock.aclose()
```

## Low-level functions

The module also exposes low-level protocol functions for direct use:

```python
from dflockd.client import acquire, release, renew

reader, writer = await asyncio.open_connection("127.0.0.1", 6388)

token, lease = await acquire(reader, writer, "my-key", timeout_s=10)
remaining = await renew(reader, writer, "my-key", token)
await release(reader, writer, "my-key", token)

writer.close()
await writer.wait_closed()
```
