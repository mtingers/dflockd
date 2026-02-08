# Sync Client

The sync client uses standard `socket` and `threading` for blocking lock operations with automatic background lease renewal. No asyncio required.

```python
from dflockd.sync_client import DistributedLock
```

## Context manager

The recommended way to use the client:

```python
from dflockd.sync_client import DistributedLock

with DistributedLock("my-key", acquire_timeout_s=10) as lock:
    print(f"token={lock.token} lease={lock.lease}")
    # critical section
```

If the lock cannot be acquired within the timeout, a `TimeoutError` is raised.

## Manual acquire/release

```python
lock = DistributedLock("my-key", acquire_timeout_s=10)
if lock.acquire():
    try:
        # critical section
        pass
    finally:
        lock.release()
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

| Attribute | Type | Description |
|---|---|---|
| `token` | `str \| None` | The lock token (UUID hex). `None` if not held |
| `lease` | `int` | Lease duration in seconds as reported by the server |

## Background renewal

Once acquired, a daemon thread sends renew requests at `lease * renew_ratio` intervals. If renewal fails, the client logs an error and sets `token = None`.

The renewal thread is stopped automatically on `release()`, context manager exit, or `close()`.

## Cleanup

Always call `release()` or `close()` when using manual acquire:

```python
lock = DistributedLock("my-key")
try:
    if lock.acquire():
        # work
        lock.release()
finally:
    lock.close()
```

## Low-level functions

Direct protocol functions are also available:

```python
import socket
from dflockd.sync_client import acquire, release, renew

sock = socket.create_connection(("127.0.0.1", 6388))
rfile = sock.makefile("r", encoding="utf-8")

token, lease = acquire(sock, rfile, "my-key", acquire_timeout_s=10)
remaining = renew(sock, rfile, "my-key", token)
release(sock, rfile, "my-key", token)

rfile.close()
sock.close()
```

## Async vs sync

| | Async | Sync |
|---|---|---|
| Import | `dflockd.client` | `dflockd.sync_client` |
| Context manager | `async with` | `with` |
| Renewal | `asyncio.Task` | `threading.Thread` (daemon) |
| Cleanup | `await lock.aclose()` | `lock.close()` |
| Best for | asyncio applications, high concurrency | Scripts, threads, simple applications |
