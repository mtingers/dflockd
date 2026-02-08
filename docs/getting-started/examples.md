# Examples

## Hold a lock with auto-renewal

Acquire a lock, hold it for an extended period while the client automatically renews the lease in the background:

=== "Async"

    ```python
    import asyncio
    from dflockd.client import DistributedLock

    async def main():
        async with DistributedLock("foo", acquire_timeout_s=10, lease_ttl_s=20) as lock:
            print(f"acquired key={lock.key} token={lock.token} lease={lock.lease}")
            await asyncio.sleep(45)  # lease renews automatically
            print("done critical section")

    asyncio.run(main())
    ```

=== "Sync"

    ```python
    import time
    from dflockd.sync_client import DistributedLock

    with DistributedLock("foo", acquire_timeout_s=10, lease_ttl_s=20) as lock:
        print(f"acquired key={lock.key} token={lock.token} lease={lock.lease}")
        time.sleep(45)  # lease renews automatically
        print("done critical section")
    ```

## FIFO lock ordering

Multiple workers competing for the same lock are granted access in FIFO order:

=== "Async"

    ```python
    import asyncio
    from dflockd.client import DistributedLock

    async def worker(worker_id: int):
        async with DistributedLock("foo", acquire_timeout_s=12) as lock:
            print(f"acquired  ({worker_id}): {lock.token}")
            await asyncio.sleep(1)
            print(f"released  ({worker_id}): {lock.token}")

    async def main():
        tasks = [worker(i) for i in range(9)]
        await asyncio.gather(*tasks)

    asyncio.run(main())
    ```

=== "Sync"

    ```python
    import threading
    import time
    from dflockd.sync_client import DistributedLock

    def worker(worker_id: int):
        with DistributedLock("foo", acquire_timeout_s=30) as lock:
            print(f"acquired  ({worker_id}): {lock.token}")
            time.sleep(1)
            print(f"released  ({worker_id}): {lock.token}")

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(9)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    ```

## Multi-server sharding

Distribute keys across multiple dflockd instances. Each key deterministically routes to the same server:

```python
from dflockd.sync_client import DistributedLock

servers = [("server1", 6388), ("server2", 6388), ("server3", 6388)]

with DistributedLock("my-key", servers=servers) as lock:
    print(f"token={lock.token} lease={lock.lease}")
```

## Custom sharding strategy

Override the default CRC-32 sharding with your own logic:

```python
from dflockd.sync_client import DistributedLock

def my_strategy(key: str, num_servers: int) -> int:
    """Route all keys to the first server."""
    return 0

servers = [("server1", 6388), ("server2", 6388)]

with DistributedLock("my-key", servers=servers, sharding_strategy=my_strategy) as lock:
    print(f"token={lock.token}")
```

## Raw TCP protocol

You can interact with dflockd from any language using its line-based TCP protocol:

```bash
# Acquire a lock with 10s timeout
printf 'l\nmy-key\n10\n' | nc localhost 6388
# Response: ok <token> <lease_ttl>

# Release (substitute your token)
printf 'r\nmy-key\n<token>\n' | nc localhost 6388
# Response: ok
```
