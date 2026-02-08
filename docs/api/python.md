# Python API

## dflockd.client (async)

### DistributedLock

```python
@dataclass
class DistributedLock:
    key: str
    acquire_timeout_s: int = 10
    lease_ttl_s: int | None = None
    servers: list[tuple[str, int]] = [("127.0.0.1", 6388)]
    sharding_strategy: ShardingStrategy = stable_hash_shard
    renew_ratio: float = 0.5
```

**Methods:**

| Method | Returns | Description |
|---|---|---|
| `await acquire()` | `bool` | Acquire the lock. Returns `False` on timeout |
| `await release()` | `bool` | Release the lock and stop renewal |
| `await aclose()` | `None` | Close the connection and clean up |

**Context manager:**

```python
async with DistributedLock("key") as lock:
    ...  # lock.token, lock.lease available
```

Raises `TimeoutError` if the lock cannot be acquired.

### Low-level functions

#### acquire

```python
async def acquire(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    key: str,
    acquire_timeout_s: int,
    lease_ttl_s: int | None = None,
) -> tuple[str, int]
```

Send a lock request. Returns `(token, lease_ttl)`. Raises `TimeoutError` on timeout.

#### release

```python
async def release(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    key: str,
    token: str,
) -> None
```

Send a release request. Raises `RuntimeError` on failure.

#### renew

```python
async def renew(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    key: str,
    token: str,
    lease_ttl_s: int | None = None,
) -> int
```

Send a renew request. Returns seconds remaining, or `-1` if not reported by the server. Raises `RuntimeError` on failure.

---

## dflockd.sync_client

### DistributedLock

```python
@dataclass
class DistributedLock:
    key: str
    acquire_timeout_s: int = 10
    lease_ttl_s: int | None = None
    servers: list[tuple[str, int]] = [("127.0.0.1", 6388)]
    sharding_strategy: ShardingStrategy = stable_hash_shard
    renew_ratio: float = 0.5
```

**Methods:**

| Method | Returns | Description |
|---|---|---|
| `acquire()` | `bool` | Acquire the lock. Returns `False` on timeout |
| `release()` | `bool` | Release the lock and stop renewal |
| `close()` | `None` | Close the connection and clean up |

**Context manager:**

```python
with DistributedLock("key") as lock:
    ...  # lock.token, lock.lease available
```

### Low-level functions

#### acquire

```python
def acquire(
    sock: socket.socket,
    rfile: io.TextIOWrapper,
    key: str,
    acquire_timeout_s: int,
    lease_ttl_s: int | None = None,
) -> tuple[str, int]
```

#### release

```python
def release(
    sock: socket.socket,
    rfile: io.TextIOWrapper,
    key: str,
    token: str,
) -> None
```

#### renew

```python
def renew(
    sock: socket.socket,
    rfile: io.TextIOWrapper,
    key: str,
    token: str,
    lease_ttl_s: int | None = None,
) -> int
```

---

## dflockd.sharding

### ShardingStrategy

```python
ShardingStrategy = Callable[[str, int], int]
```

A callable that maps `(key, num_servers)` to a server index.

### stable_hash_shard

```python
def stable_hash_shard(key: str, num_servers: int) -> int
```

Default sharding strategy using `zlib.crc32`. Deterministic across processes regardless of `PYTHONHASHSEED`.

### DEFAULT_SERVERS

```python
DEFAULT_SERVERS: list[tuple[str, int]] = [("127.0.0.1", 6388)]
```
