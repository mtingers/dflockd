# dflockd

A lightweight distributed lock server using a simple line-based TCP protocol with FIFO ordering, automatic lease expiry, and background renewal.

## Quick start

```bash
# Install
uv sync

# Run the server
uv run dflockd
```

The server listens on `0.0.0.0:6388` by default.

## Server configuration

All tuning is via environment variables:

| Variable | Default | Description |
|---|---|---|
| `HOST` | `0.0.0.0` | Bind address |
| `PORT` | `6388` | Bind port |
| `DEFAULT_LEASE_TTL_S` | `33` | Default lock lease duration (seconds) |
| `LEASE_SWEEP_INTERVAL_S` | `1` | How often to check for expired leases |
| `GC_LOOP_SLEEP` | `5` | How often to prune idle lock state |
| `GC_MAX_UNUSED_TIME` | `60` | Seconds before idle lock state is pruned |
| `MAX_LOCKS` | `256` | Maximum number of unique lock keys |

## CLI arguments

Settings can also be passed as command-line flags. Environment variables take precedence over CLI arguments.

```bash
uv run dflockd --port 7000 --max-locks 512
```

| Flag | Default | Env var override |
|---|---|---|
| `--host` | `0.0.0.0` | `DFLOCKD_HOST` |
| `--port` | `6388` | `DFLOCKD_PORT` |
| `--default-lease-ttl` | `33` | `DEFAULT_LEASE_TTL_S` |
| `--lease-sweep-interval` | `1` | `LEASE_SWEEP_INTERVAL_S` |
| `--gc-interval` | `5` | `GC_LOOP_SLEEP` |
| `--gc-max-idle` | `60` | `GC_MAX_UNUSED_TIME` |
| `--max-locks` | `1024` | `MAX_LOCKS` |
| `--read-timeout` | `23` | `DFLOCKD_READ_TIMEOUT_S` |

## Protocol

The wire protocol is line-based UTF-8 over TCP. Each request is exactly 3 lines: `command\nkey\narg\n`.

### Commands

**Lock (acquire)**
```
l
<key>
<timeout_s> [<lease_ttl_s>]
```
Response: `ok <token> <lease_ttl>\n` or `timeout\n`

**Renew**
```
n
<key>
<token> [<lease_ttl_s>]
```
Response: `ok <seconds_remaining>\n` or `error\n`

**Release**
```
r
<key>
<token>
```
Response: `ok\n` or `error\n`

### Behavior

- Locks are granted in strict FIFO order per key.
- Leases expire automatically if not renewed. On expiry, the lock passes to the next waiter.
- Connections that disconnect have their held locks released automatically.

## Client usage

### Async client

```python
import asyncio
from dflockd.client import DistributedLock

async def main():
    async with DistributedLock("my-key", acquire_timeout_s=10) as lock:
        print(lock.token, lock.lease)
        # critical section — lease auto-renews in background

asyncio.run(main())
```

### Sync client

```python
from dflockd.sync_client import DistributedLock

with DistributedLock("my-key", acquire_timeout_s=10) as lock:
    print(lock.token, lock.lease)
    # critical section — lease auto-renews in background thread
```

### Manual acquire/release

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

### Parameters

| Parameter | Default | Description |
|---|---|---|
| `key` | *(required)* | Lock name |
| `acquire_timeout_s` | `10` | Seconds to wait for lock acquisition |
| `lease_ttl_s` | `None` (server default) | Lease duration in seconds |
| `servers` | `[("127.0.0.1", 6388)]` | List of `(host, port)` tuples |
| `sharding_strategy` | `stable_hash_shard` | `Callable[[str, int], int]` — maps `(key, num_servers)` to server index |
| `renew_ratio` | `0.5` | Renew at `lease * ratio` seconds |

## Multi-server sharding

When running multiple dflockd instances, the client can distribute keys across servers using consistent hashing. Each key always routes to the same server.

```python
from dflockd.sync_client import DistributedLock

servers = [("server1", 6388), ("server2", 6388), ("server3", 6388)]

with DistributedLock("my-key", servers=servers) as lock:
    print(lock.token, lock.lease)
```

The default strategy uses `zlib.crc32` for stable, deterministic hashing. You can provide a custom strategy:

```python
from dflockd.sync_client import DistributedLock

def my_strategy(key: str, num_servers: int) -> int:
    """Route all keys to the first server."""
    return 0

with DistributedLock("my-key", servers=servers, sharding_strategy=my_strategy) as lock:
    pass
```
