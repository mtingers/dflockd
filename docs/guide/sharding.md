# Sharding

## Overview

When running multiple dflockd instances, the client distributes keys across servers using a sharding strategy. Each key deterministically routes to the same server, ensuring all operations on a given key go to the same instance.

## Default strategy

The built-in `stable_hash_shard` uses `zlib.crc32` for deterministic hashing:

```python
def stable_hash_shard(key: str, num_servers: int) -> int:
    return zlib.crc32(key.encode("utf-8")) % num_servers
```

Unlike Python's built-in `hash()`, CRC-32 produces the same result across processes regardless of `PYTHONHASHSEED`, making it safe for distributed use.

## Multi-server setup

Pass a list of `(host, port)` tuples to the client:

```python
from dflockd.sync_client import DistributedLock

servers = [
    ("lock-server-1", 6388),
    ("lock-server-2", 6388),
    ("lock-server-3", 6388),
]

with DistributedLock("my-key", servers=servers) as lock:
    # "my-key" always routes to the same server
    pass
```

## Custom strategies

Provide any callable with the signature `(key: str, num_servers: int) -> int`:

```python
from dflockd.sync_client import DistributedLock

def region_shard(key: str, num_servers: int) -> int:
    """Route keys prefixed with 'eu-' to server 0, everything else hashed."""
    if key.startswith("eu-"):
        return 0
    import zlib
    return zlib.crc32(key.encode()) % num_servers

servers = [("eu-server", 6388), ("us-server-1", 6388), ("us-server-2", 6388)]

with DistributedLock("eu-job-1", servers=servers, sharding_strategy=region_shard) as lock:
    pass  # routes to eu-server
```

## Type signature

```python
from collections.abc import Callable

ShardingStrategy = Callable[[str, int], int]
```

The function receives the lock key and the number of servers, and must return a server index in `[0, num_servers)`.

!!! note
    Each dflockd instance is independent — there is no replication or consensus between servers. If a server goes down, locks assigned to that server become unavailable. For high availability, consider running behind a load balancer with health checks or using a consensus-based system.
