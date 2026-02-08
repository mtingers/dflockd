import asyncio
import contextlib
import logging
from dataclasses import dataclass, field

from .sharding import DEFAULT_SERVERS, ShardingStrategy, stable_hash_shard

log = logging.getLogger("dflockd-client")


def _encode_lines(*lines: str) -> bytes:
    return ("".join(f"{ln}\n" for ln in lines)).encode("utf-8")


async def _readline(reader: asyncio.StreamReader) -> str:
    raw = await reader.readline()
    if raw == b"":
        raise ConnectionError("server closed connection")
    return raw.decode("utf-8").rstrip("\r\n")


async def acquire(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
    key: str, acquire_timeout_s: int, lease_ttl_s: int | None = None,
) -> tuple[str, int]:
    # l\nkey\n"<timeout> [<lease>]"\n
    arg = (
        str(acquire_timeout_s)
        if lease_ttl_s is None
        else f"{acquire_timeout_s} {lease_ttl_s}"
    )

    writer.write(_encode_lines("l", key, arg))
    await writer.drain()

    resp = await _readline(reader)
    if resp == "timeout":
        raise TimeoutError(f"timeout acquiring {key!r}")
    if not resp.startswith("ok "):
        raise RuntimeError(f"acquire failed: {resp!r}")

    # ok <token> <lease>
    parts = resp.split()
    if len(parts) < 2:
        raise RuntimeError(f"bad ok response: {resp!r}")
    token = parts[1]
    lease = int(parts[2]) if len(parts) >= 3 else 30
    return token, lease


async def renew(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
    key: str, token: str, lease_ttl_s: int | None = None,
) -> int:
    # n\nkey\n"<token> [<lease>]"\n
    arg = token if lease_ttl_s is None else f"{token} {lease_ttl_s}"
    writer.write(_encode_lines("n", key, arg))
    await writer.drain()

    resp = await _readline(reader)
    if not resp.startswith("ok"):
        raise RuntimeError(f"renew failed: {resp!r}")

    # ok <seconds_remaining> (optional)
    parts = resp.split()
    if len(parts) >= 2 and parts[1].isdigit():
        return int(parts[1])
    return -1


async def release(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, key: str, token: str) -> None:
    writer.write(_encode_lines("r", key, token))
    await writer.drain()

    resp = await _readline(reader)
    if resp != "ok":
        raise RuntimeError(f"release failed: {resp!r}")


@dataclass
class DistributedLock:
    key: str
    acquire_timeout_s: int = 10
    lease_ttl_s: int | None = None  # if None, server default
    servers: list[tuple[str, int]] = field(default_factory=lambda: list(DEFAULT_SERVERS))
    sharding_strategy: ShardingStrategy = stable_hash_shard
    renew_ratio: float = 0.5  # renew at lease * ratio

    _reader: asyncio.StreamReader | None = None
    _writer: asyncio.StreamWriter | None = None
    token: str | None = None
    lease: int = 0
    _renew_task: asyncio.Task | None = None
    _closed: bool = False

    def __post_init__(self):
        if not self.servers:
            raise ValueError("servers must be a non-empty list")

    def _pick_server(self) -> tuple[str, int]:
        idx = self.sharding_strategy(self.key, len(self.servers))
        return self.servers[idx % len(self.servers)]

    async def acquire(self) -> bool:
        self._closed = False
        host, port = self._pick_server()
        self._reader, self._writer = await asyncio.open_connection(host, port)
        try:
            self.token, self.lease = await acquire(
                self._reader,
                self._writer,
                self.key,
                self.acquire_timeout_s,
                self.lease_ttl_s,
            )
        except TimeoutError:
            await self.aclose()
            return False
        except BaseException:
            await self.aclose()
            raise
        # Start renew loop
        self._renew_task = asyncio.create_task(self._renew_loop())
        return True

    async def release(self) -> bool:
        try:
            if self._renew_task:
                self._renew_task.cancel()
                with contextlib.suppress(BaseException):
                    await self._renew_task

            if self._reader and self._writer and self.token:
                await release(self._reader, self._writer, self.key, self.token)
        finally:
            await self.aclose()
        return True

    async def __aenter__(self):
        self._closed = False
        host, port = self._pick_server()
        self._reader, self._writer = await asyncio.open_connection(host, port)
        try:
            self.token, self.lease = await acquire(
                self._reader,
                self._writer,
                self.key,
                self.acquire_timeout_s,
                self.lease_ttl_s,
            )
        except BaseException:
            await self.aclose()
            raise
        # Start renew loop
        self._renew_task = asyncio.create_task(self._renew_loop())
        return self

    async def _renew_loop(self):
        assert self._reader and self._writer and self.token
        interval = max(1.0, self.lease * self.renew_ratio)
        try:
            while True:
                await asyncio.sleep(interval)
                try:
                    await renew(
                        self._reader,
                        self._writer,
                        self.key,
                        self.token,
                        self.lease_ttl_s,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.error(
                        "lock lost (renew failed): key=%s token=%s",
                        self.key,
                        self.token,
                    )
                    self.token = None
                    return
        except asyncio.CancelledError:
            return

    async def __aexit__(self, exc_type, exc, tb):
        try:
            if self._renew_task:
                self._renew_task.cancel()
                with contextlib.suppress(BaseException):
                    await self._renew_task

            if self._reader and self._writer and self.token:
                await release(self._reader, self._writer, self.key, self.token)
        finally:
            await self.aclose()

    async def aclose(self):
        if self._closed:
            return
        self._closed = True
        if self._writer:
            self._writer.close()
            with contextlib.suppress(Exception):
                await self._writer.wait_closed()
        self._reader = None
        self._writer = None
        self.token = None
