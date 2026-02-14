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

## Two-phase lock acquisition

The `enqueue()` / `wait()` methods split lock acquisition into two steps. This lets you notify an external system after joining the queue but before blocking:

```python
lock = DistributedLock("my-key", acquire_timeout_s=10)

# Step 1: join the queue (returns immediately)
status = lock.enqueue()  # "acquired" or "queued"

# Step 2: notify external system
notify_external_system(status)

# Step 3: block until granted (no-op if already acquired)
if lock.wait(timeout_s=10):
    try:
        # critical section
        pass
    finally:
        lock.release()
```

**`enqueue()`** connects to the server and sends the `e` command. Returns `"acquired"` if the lock was free (fast path) or `"queued"` if there are other holders/waiters. On fast-path acquire, the renewal thread starts immediately.

**`wait(timeout_s=None)`** sends the `w` command and blocks until the lock is granted. Returns `True` on success, `False` on timeout. If the lock was already acquired during `enqueue()`, returns `True` immediately without contacting the server. Uses `acquire_timeout_s` if `timeout_s` is not provided.

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

The two-phase functions are also available at the low level:

```python
from dflockd.sync_client import enqueue, wait, release

sock = socket.create_connection(("127.0.0.1", 6388))
rfile = sock.makefile("r", encoding="utf-8")

status, token, lease = enqueue(sock, rfile, "my-key")
# status is "acquired" or "queued"

if status == "queued":
    token, lease = wait(sock, rfile, "my-key", wait_timeout_s=10)

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
