import io
import logging
import socket
import threading
from dataclasses import dataclass, field

from .sharding import DEFAULT_SERVERS, ShardingStrategy, stable_hash_shard

log = logging.getLogger("dflockd-client")


def _encode_lines(*lines: str) -> bytes:
    return ("".join(f"{ln}\n" for ln in lines)).encode("utf-8")


def _readline(rfile: io.TextIOWrapper) -> str:
    raw = rfile.readline()
    if raw == "":
        raise ConnectionError("server closed connection")
    return raw.rstrip("\r\n")


def acquire(
    sock: socket.socket,
    rfile: io.TextIOWrapper,
    key: str,
    acquire_timeout_s: int,
    lease_ttl_s: int | None = None,
) -> tuple[str, int]:
    arg = (
        str(acquire_timeout_s)
        if lease_ttl_s is None
        else f"{acquire_timeout_s} {lease_ttl_s}"
    )

    sock.sendall(_encode_lines("l", key, arg))

    resp = _readline(rfile)
    if resp == "timeout":
        raise TimeoutError(f"timeout acquiring {key!r}")
    if not resp.startswith("ok "):
        raise RuntimeError(f"acquire failed: {resp!r}")

    parts = resp.split()
    if len(parts) < 2:
        raise RuntimeError(f"bad ok response: {resp!r}")
    token = parts[1]
    lease = int(parts[2]) if len(parts) >= 3 else 30
    return token, lease


def renew(
    sock: socket.socket,
    rfile: io.TextIOWrapper,
    key: str,
    token: str,
    lease_ttl_s: int | None = None,
) -> int:
    arg = token if lease_ttl_s is None else f"{token} {lease_ttl_s}"
    sock.sendall(_encode_lines("n", key, arg))

    resp = _readline(rfile)
    if not resp.startswith("ok"):
        raise RuntimeError(f"renew failed: {resp!r}")

    parts = resp.split()
    if len(parts) >= 2 and parts[1].isdigit():
        return int(parts[1])
    return -1


def enqueue(
    sock: socket.socket,
    rfile: io.TextIOWrapper,
    key: str,
    lease_ttl_s: int | None = None,
) -> tuple[str, str | None, int | None]:
    """
    Two-phase enqueue: join FIFO queue, return immediately.
    Returns (status, token, lease) where status is "acquired" or "queued".
    """
    arg = "" if lease_ttl_s is None else str(lease_ttl_s)
    sock.sendall(_encode_lines("e", key, arg))

    resp = _readline(rfile)
    if resp.startswith("acquired "):
        parts = resp.split()
        token = parts[1]
        lease = int(parts[2]) if len(parts) >= 3 else 30
        return ("acquired", token, lease)
    if resp == "queued":
        return ("queued", None, None)
    raise RuntimeError(f"enqueue failed: {resp!r}")


def wait(
    sock: socket.socket,
    rfile: io.TextIOWrapper,
    key: str,
    wait_timeout_s: int,
) -> tuple[str, int]:
    """
    Two-phase wait: block until lock is granted.
    Returns (token, lease). Raises TimeoutError on timeout.
    """
    sock.sendall(_encode_lines("w", key, str(wait_timeout_s)))

    resp = _readline(rfile)
    if resp == "timeout":
        raise TimeoutError(f"timeout waiting for {key!r}")
    if not resp.startswith("ok "):
        raise RuntimeError(f"wait failed: {resp!r}")

    parts = resp.split()
    token = parts[1]
    lease = int(parts[2]) if len(parts) >= 3 else 30
    return token, lease


def release(sock: socket.socket, rfile: io.TextIOWrapper, key: str, token: str) -> None:
    sock.sendall(_encode_lines("r", key, token))

    resp = _readline(rfile)
    if resp != "ok":
        raise RuntimeError(f"release failed: {resp!r}")


@dataclass
class DistributedLock:
    key: str
    acquire_timeout_s: int = 10
    lease_ttl_s: int | None = None
    servers: list[tuple[str, int]] = field(
        default_factory=lambda: list(DEFAULT_SERVERS)
    )
    sharding_strategy: ShardingStrategy = stable_hash_shard
    renew_ratio: float = 0.5

    _sock: socket.socket | None = field(default=None, repr=False)
    _rfile: io.TextIOWrapper | None = field(default=None, repr=False)
    token: str | None = None
    lease: int = 0
    _renew_thread: threading.Thread | None = field(default=None, repr=False)
    _stop_event: threading.Event = field(default_factory=threading.Event, repr=False)
    _closed: bool = False

    def __post_init__(self):
        if not self.servers:
            raise ValueError("servers must be a non-empty list")

    def _pick_server(self) -> tuple[str, int]:
        idx = self.sharding_strategy(self.key, len(self.servers))
        return self.servers[idx % len(self.servers)]

    def _connect(self):
        self._closed = False
        self._stop_event.clear()
        host, port = self._pick_server()
        self._sock = socket.create_connection((host, port))
        self._rfile = self._sock.makefile("r", encoding="utf-8")

    def _start_renew(self):
        self._renew_thread = threading.Thread(target=self._renew_loop, daemon=True)
        self._renew_thread.start()

    def _stop_renew(self):
        if self._renew_thread is not None:
            self._stop_event.set()
            self._renew_thread.join(timeout=5)
            self._renew_thread = None

    def acquire(self) -> bool:
        self._connect()
        sock, rfile = self._sock, self._rfile
        assert sock is not None and rfile is not None
        try:
            self.token, self.lease = acquire(
                sock,
                rfile,
                self.key,
                self.acquire_timeout_s,
                self.lease_ttl_s,
            )
        except TimeoutError:
            self.close()
            return False
        except BaseException:
            self.close()
            raise
        self._start_renew()
        return True

    def enqueue(self) -> str:
        """
        Two-phase step 1: connect and enqueue. Returns "acquired" or "queued".
        Starts renew loop on fast-path acquire.
        """
        self._connect()
        sock, rfile = self._sock, self._rfile
        assert sock is not None and rfile is not None
        try:
            status, tok, lease = enqueue(sock, rfile, self.key, self.lease_ttl_s)
        except BaseException:
            self.close()
            raise
        if status == "acquired":
            self.token = tok
            self.lease = lease or 0
            self._start_renew()
        return status

    def wait(self, timeout_s: int | None = None) -> bool:
        """
        Two-phase step 2: wait for lock grant. Returns True if granted, False on timeout.
        If already acquired (fast path from enqueue), returns immediately.
        """
        if self.token is not None:
            return True
        sock, rfile = self._sock, self._rfile
        if sock is None or rfile is None:
            raise RuntimeError("not connected; call enqueue() first")
        timeout = timeout_s if timeout_s is not None else self.acquire_timeout_s
        try:
            self.token, self.lease = wait(sock, rfile, self.key, timeout)
        except TimeoutError:
            self.close()
            return False
        except BaseException:
            self.close()
            raise
        self._start_renew()
        return True

    def release(self) -> bool:
        try:
            self._stop_renew()
            sock, rfile = self._sock, self._rfile
            if sock is not None and rfile is not None and self.token:
                release(sock, rfile, self.key, self.token)
        finally:
            self.close()
        return True

    def __enter__(self):
        self._connect()
        sock, rfile = self._sock, self._rfile
        assert sock is not None and rfile is not None
        try:
            self.token, self.lease = acquire(
                sock,
                rfile,
                self.key,
                self.acquire_timeout_s,
                self.lease_ttl_s,
            )
        except BaseException:
            self.close()
            raise
        self._start_renew()
        return self

    def _renew_loop(self):
        sock, rfile, token = self._sock, self._rfile, self.token
        assert sock is not None and rfile is not None and token is not None
        interval = max(1.0, self.lease * self.renew_ratio)
        while not self._stop_event.wait(interval):
            try:
                renew(sock, rfile, self.key, token, self.lease_ttl_s)
            except Exception:
                log.error(
                    "lock lost (renew failed): key=%s token=%s",
                    self.key,
                    self.token,
                )
                self.token = None
                return

    def __exit__(self, exc_type, exc, tb):
        try:
            self._stop_renew()
            sock, rfile = self._sock, self._rfile
            if sock is not None and rfile is not None and self.token:
                release(sock, rfile, self.key, self.token)
        finally:
            self.close()

    def close(self):
        if self._closed:
            return
        self._closed = True
        if self._rfile:
            try:
                self._rfile.close()
            except Exception:
                pass
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass
        self._rfile = None
        self._sock = None
        self.token = None
