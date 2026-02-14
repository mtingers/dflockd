import argparse
import asyncio
import contextlib
import logging
import os
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from enum import StrEnum


def getenv_int(key: str, default: int) -> int:
    val = os.environ.get(key)
    if val is None:
        return default
    try:
        return int(val)
    except ValueError:
        return default


HOST = os.environ.get("DFLOCKD_HOST", "0.0.0.0")
PORT = getenv_int("DFLOCKD_PORT", 6388)

# ---- Protocol limits / safety ----
MAX_LINE_BYTES = 256
READ_TIMEOUT_S = getenv_int("DFLOCKD_READ_TIMEOUT_S", 23)

# ---- Lease / GC tuning ----
DEFAULT_LEASE_TTL_S = getenv_int(
    "DFLOCKD_DEFAULT_LEASE_TTL_S", 33
)  # default lock lease duration
LEASE_SWEEP_INTERVAL_S = getenv_int(
    "DFLOCKD_LEASE_SWEEP_INTERVAL_S", 1
)  # how often to expire leases
GC_LOOP_SLEEP = getenv_int(
    "DFLOCKD_GC_LOOP_SLEEP", 5
)  # how often to prune unused lock states
GC_MAX_UNUSED_TIME = getenv_int(
    "DFLOCKD_GC_MAX_UNUSED_TIME", 60
)  # idle seconds before deleting unused lock state
#
# ---- Behavior toggles ----
# best-effort cleanup
AUTO_RELEASE_ON_DISCONNECT = os.environ.get(
    "DFLOCKD_AUTO_RELEASE_ON_DISCONNECT", "1"
).lower() in ("1", "yes", "true")
MAX_LOCKS = getenv_int(
    "DFLOCKD_MAX_LOCKS", 1024
)  # maximum number of unique locks to track (per Request.key)

# ---------------------------
log = logging.getLogger("dflockd")
tracking_lock = asyncio.Lock()


class MaxLocksError(RuntimeError):
    pass


class NotEnqueuedError(RuntimeError):
    pass


class Status(StrEnum):
    ok = "ok"
    acquired = "acquired"
    queued = "queued"
    error = "error"
    error_max_locks = "error_max_locks"
    timeout = "timeout"


@dataclass
class Request:
    cmd: str
    key: str
    acquire_timeout_s: int | None = None
    lease_ttl_s: int | None = None
    token: str | None = None


@dataclass
class Ack:
    status: Status
    token: str | None = None
    lease_ttl_s: int | None = None
    extra: str | None = None  # e.g., seconds remaining


@dataclass
class Waiter:
    fut: asyncio.Future
    conn_id: int
    lease_ttl_s: int
    enqueued_at: float


@dataclass
class LockState:
    owner_token: str | None = None
    owner_conn_id: int | None = None
    lease_expires_at: float = 0.0
    waiters: deque[Waiter] = field(default_factory=deque)
    last_activity: float = 0.0


_locks: dict[str, LockState] = {}

# Tracks which keys a connection currently owns (for disconnect cleanup)
_conn_owned: dict[int, set[str]] = {}


@dataclass
class EnqueuedState:
    waiter: Waiter | None
    token: str | None
    lease_ttl_s: int


# Tracks two-phase enqueue state: (conn_id, key) -> EnqueuedState
_conn_enqueued: dict[tuple[int, str], EnqueuedState] = {}


def _now() -> float:
    return time.monotonic()


def _new_token() -> str:
    return uuid.uuid4().hex


def _parse_int(s: str, what: str) -> int:
    try:
        return int(s)
    except ValueError as e:
        raise ProtocolError(4, f"invalid {what}: {s!r}") from e


class ProtocolError(Exception):
    def __init__(self, code: int, message: str):
        super().__init__(message)
        self.code = code


async def read_line(reader: asyncio.StreamReader) -> str:
    try:
        raw = await asyncio.wait_for(reader.readline(), timeout=READ_TIMEOUT_S)
        if raw == b"":
            raise ProtocolError(11, "client disconnected")
        if len(raw) > MAX_LINE_BYTES:
            raise ProtocolError(12, "line too long")
    except TimeoutError as err:
        raise ProtocolError(10, "read timeout") from err

    return raw.decode("utf-8", errors="strict").rstrip("\r\n")


async def read_request(reader: asyncio.StreamReader) -> Request:
    """
    Reads exactly 3 lines: command, key, arg
    Commands:
      l: arg = "<acquire_timeout_s> [<lease_ttl_s>]"
      r: arg = "<token>"
      n: arg = "<token> [<lease_ttl_s>]"
    """
    cmd = await read_line(reader)
    key = await read_line(reader)
    arg = await read_line(reader)

    if cmd not in ("l", "r", "n", "e", "w"):
        raise ProtocolError(3, f"invalid cmd {cmd!r}")
    if not key:
        raise ProtocolError(5, "empty key")

    parts = arg.split()
    if cmd == "l":
        if len(parts) not in (1, 2):
            raise ProtocolError(8, "lock arg must be: <timeout> [<lease_ttl>]")
        acquire_timeout_s = _parse_int(parts[0], "timeout")
        if acquire_timeout_s < 0:
            raise ProtocolError(6, "timeout must be >= 0")
        lease_ttl_s = (
            DEFAULT_LEASE_TTL_S
            if len(parts) == 1
            else _parse_int(parts[1], "lease_ttl")
        )
        if lease_ttl_s <= 0:
            raise ProtocolError(9, "lease_ttl must be > 0")
        return Request(
            cmd=cmd,
            key=key,
            acquire_timeout_s=acquire_timeout_s,
            lease_ttl_s=lease_ttl_s,
        )

    if cmd == "r":
        token = arg.strip()
        if not token:
            raise ProtocolError(7, "empty token")
        return Request(cmd=cmd, key=key, token=token)

    if cmd == "n":
        if len(parts) not in (1, 2):
            raise ProtocolError(8, "renew arg must be: <token> [<lease_ttl>]")
        token = parts[0].strip()
        if not token:
            raise ProtocolError(7, "empty token")
        lease_ttl_s = (
            DEFAULT_LEASE_TTL_S
            if len(parts) == 1
            else _parse_int(parts[1], "lease_ttl")
        )
        if lease_ttl_s <= 0:
            raise ProtocolError(9, "lease_ttl must be > 0")
        return Request(cmd=cmd, key=key, token=token, lease_ttl_s=lease_ttl_s)

    if cmd == "e":
        # e\n<key>\n[<lease_ttl_s>]\n  — 3rd line optional positive int
        stripped = arg.strip()
        if stripped == "":
            lease_ttl_s = DEFAULT_LEASE_TTL_S
        else:
            lease_ttl_s = _parse_int(stripped, "lease_ttl")
            if lease_ttl_s <= 0:
                raise ProtocolError(9, "lease_ttl must be > 0")
        return Request(cmd=cmd, key=key, lease_ttl_s=lease_ttl_s)

    # cmd == "w"
    stripped = arg.strip()
    if not stripped:
        raise ProtocolError(8, "wait arg must be: <timeout>")
    acquire_timeout_s = _parse_int(stripped, "timeout")
    if acquire_timeout_s < 0:
        raise ProtocolError(6, "timeout must be >= 0")
    return Request(cmd=cmd, key=key, acquire_timeout_s=acquire_timeout_s)


def format_response(ack: Ack) -> bytes:
    if ack.status in (Status.ok, Status.acquired):
        prefix = ack.status.value
        # Acquire/enqueue-acquired response: "<prefix> <token> <lease_ttl>"
        if ack.token is not None:
            lease = (
                ack.lease_ttl_s if ack.lease_ttl_s is not None else DEFAULT_LEASE_TTL_S
            )
            return f"{prefix} {ack.token} {lease}\n".encode()
        # Renew response: "ok <extra>"
        if ack.extra is not None:
            return f"{prefix} {ack.extra}\n".encode()
        return f"{prefix}\n".encode()
    return f"{ack.status.value}\n".encode()


def _conn_add_owned(conn_id: int, key: str) -> None:
    s = _conn_owned.get(conn_id)
    if s is None:
        s = set()
        _conn_owned[conn_id] = s
    s.add(key)


def _conn_remove_owned(conn_id: int | None, key: str) -> None:
    if conn_id is None:
        return
    s = _conn_owned.get(conn_id)
    if not s:
        return
    s.discard(key)
    if not s:
        _conn_owned.pop(conn_id, None)


async def fifo_acquire(
    key: str, acquire_timeout_s: int, lease_ttl_s: int, conn_id: int
) -> str | None:
    """
    Strict FIFO acquire per key. Returns token if acquired else None (timeout).
    Lease is set to now + lease_ttl_s.
    """
    loop = asyncio.get_running_loop()
    fut: asyncio.Future = loop.create_future()
    waiter = Waiter(
        fut=fut, conn_id=conn_id, lease_ttl_s=lease_ttl_s, enqueued_at=_now()
    )

    async with tracking_lock:
        if len(_locks) >= MAX_LOCKS:
            raise MaxLocksError(f"{len(_locks)} > {MAX_LOCKS}")
        st = _locks.get(key)
        if st is None:
            st = LockState(last_activity=_now())
            _locks[key] = st

        st.last_activity = _now()

        # Fast path: free and no waiters => immediate acquire
        if st.owner_token is None and not st.waiters:
            token = _new_token()
            st.owner_token = token
            st.owner_conn_id = conn_id
            st.lease_expires_at = _now() + lease_ttl_s
            st.last_activity = _now()
            _conn_add_owned(conn_id, key)
            return token

        # Otherwise enqueue FIFO
        st.waiters.append(waiter)

    try:
        token = await asyncio.wait_for(fut, timeout=float(acquire_timeout_s))
        async with tracking_lock:
            st = _locks.get(key)
            if st:
                st.last_activity = _now()
        return token

    except TimeoutError:
        # Remove ourselves from the queue if still present
        async with tracking_lock:
            st = _locks.get(key)
            if st:
                st.last_activity = _now()
                st.waiters = deque(w for w in st.waiters if w is not waiter)
        return None


async def fifo_enqueue(
    key: str, lease_ttl_s: int, conn_id: int
) -> tuple[Status, str | None, int | None]:
    """
    Phase 1 of two-phase acquire: enqueue into the FIFO queue and return immediately.
    Returns (acquired, token, lease) if lock was free, or (queued, None, None) if waiting.
    """
    eq_key = (conn_id, key)
    async with tracking_lock:
        if eq_key in _conn_enqueued:
            raise ProtocolError(8, "already enqueued for this key")

        if len(_locks) >= MAX_LOCKS and key not in _locks:
            raise MaxLocksError(f"{len(_locks)} >= {MAX_LOCKS}")

        st = _locks.get(key)
        if st is None:
            st = LockState(last_activity=_now())
            _locks[key] = st

        st.last_activity = _now()

        # Fast path: free and no waiters => immediate acquire
        if st.owner_token is None and not st.waiters:
            token = _new_token()
            st.owner_token = token
            st.owner_conn_id = conn_id
            st.lease_expires_at = _now() + lease_ttl_s
            st.last_activity = _now()
            _conn_add_owned(conn_id, key)
            _conn_enqueued[eq_key] = EnqueuedState(
                waiter=None, token=token, lease_ttl_s=lease_ttl_s
            )
            return (Status.acquired, token, lease_ttl_s)

        # Slow path: create waiter and enqueue
        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        waiter = Waiter(
            fut=fut, conn_id=conn_id, lease_ttl_s=lease_ttl_s, enqueued_at=_now()
        )
        st.waiters.append(waiter)
        _conn_enqueued[eq_key] = EnqueuedState(
            waiter=waiter, token=None, lease_ttl_s=lease_ttl_s
        )
        return (Status.queued, None, None)


async def fifo_wait(
    key: str, wait_timeout_s: int, conn_id: int
) -> tuple[str, int] | None:
    """
    Phase 2 of two-phase acquire: block until lock is granted or timeout.
    Returns (token, lease_ttl_s) on success, None on timeout.
    Resets lease to now + lease_ttl_s on success.
    """
    eq_key = (conn_id, key)

    async with tracking_lock:
        es = _conn_enqueued.get(eq_key)
        if es is None:
            raise NotEnqueuedError("not enqueued for this key")

    lease_ttl_s = es.lease_ttl_s

    # Fast path: already acquired during enqueue
    if es.token is not None:
        async with tracking_lock:
            _conn_enqueued.pop(eq_key, None)
            st = _locks.get(key)
            if st and st.owner_token == es.token:
                # Verify lock still held (lease may have expired)
                if st.lease_expires_at > 0 and _now() >= st.lease_expires_at:
                    # Lock was lost; clean up
                    return None
                # Reset lease
                st.lease_expires_at = _now() + lease_ttl_s
                st.last_activity = _now()
                return (es.token, lease_ttl_s)
            # Lock was lost (e.g. lease expired between enqueue and wait)
            return None

    # Slow path: waiter is pending
    waiter = es.waiter
    assert waiter is not None

    try:
        token = await asyncio.wait_for(waiter.fut, timeout=float(wait_timeout_s))
        async with tracking_lock:
            _conn_enqueued.pop(eq_key, None)
            st = _locks.get(key)
            if st:
                # Reset lease to full TTL from now
                st.lease_expires_at = _now() + lease_ttl_s
                st.last_activity = _now()
        return (token, lease_ttl_s)

    except TimeoutError:
        async with tracking_lock:
            _conn_enqueued.pop(eq_key, None)
            # Race check: future may have resolved between timeout and lock acquisition
            if waiter.fut.done() and not waiter.fut.cancelled():
                token = waiter.fut.result()
                st = _locks.get(key)
                if st:
                    st.lease_expires_at = _now() + lease_ttl_s
                    st.last_activity = _now()
                return (token, lease_ttl_s)
            # Remove from queue
            st = _locks.get(key)
            if st:
                st.last_activity = _now()
                st.waiters = deque(w for w in st.waiters if w is not waiter)
        return None


def _grant_next_waiter_locked(key: str, st: LockState) -> None:
    """
    Under tracking_lock: grant lock to next waiter in FIFO order if any,
    otherwise leave unlocked.
    """
    while st.waiters:
        w = st.waiters.popleft()
        if w.fut.cancelled() or w.fut.done():
            continue
        token = _new_token()
        st.owner_token = token
        st.owner_conn_id = w.conn_id
        st.lease_expires_at = _now() + w.lease_ttl_s
        st.last_activity = _now()
        _conn_add_owned(w.conn_id, key)
        w.fut.set_result(token)
        return

    # no waiters: unlock
    st.owner_token = None
    st.owner_conn_id = None
    st.lease_expires_at = 0.0
    st.last_activity = _now()


async def fifo_release(key: str, token: str) -> bool:
    """
    Release only if token matches. Transfers to next waiter FIFO if present.
    """
    async with tracking_lock:
        st = _locks.get(key)
        if st is None:
            return False

        st.last_activity = _now()

        if st.owner_token is None or st.owner_token != token:
            return False

        old_conn = st.owner_conn_id
        _conn_remove_owned(old_conn, key)

        # grant next or unlock
        _grant_next_waiter_locked(key, st)
        return True


async def fifo_renew(key: str, token: str, lease_ttl_s: int) -> int | None:
    """
    Renew lease if token matches current owner AND lease is not already expired.
    Reset semantics: expiry becomes now + lease_ttl_s.

    Returns seconds remaining after renew, or None on error.
    """
    async with tracking_lock:
        st = _locks.get(key)
        if st is None:
            return None

        cur = _now()
        st.last_activity = cur

        # Must currently own it
        if st.owner_token is None or st.owner_token != token:
            return None

        # NEW: If already expired, renew must error (do not resurrect)
        if st.lease_expires_at > 0 and cur >= st.lease_expires_at:
            # Optional but recommended: expire immediately & hand off FIFO
            log.warning(
                "renew rejected (already expired): key=%s owner_conn=%s",
                key,
                st.owner_conn_id,
            )

            _conn_remove_owned(st.owner_conn_id, key)
            st.owner_token = None
            st.owner_conn_id = None
            st.lease_expires_at = 0.0
            st.last_activity = cur
            _grant_next_waiter_locked(key, st)

            return None

        # Reset semantics: expiry becomes now + TTL
        st.lease_expires_at = cur + lease_ttl_s
        st.last_activity = cur

        remaining = int(max(0.0, st.lease_expires_at - _now()))
        return remaining


async def lease_expiry_loop():
    """
    Expires held locks whose lease has elapsed, then grants next waiter FIFO.
    """
    log.info("lease_expiry_loop: [starting]")
    while True:
        await asyncio.sleep(LEASE_SWEEP_INTERVAL_S)
        cur = _now()

        async with tracking_lock:
            for key, st in _locks.items():
                if st.owner_token is None:
                    continue
                if st.lease_expires_at <= 0:
                    continue
                if cur >= st.lease_expires_at:
                    log.warning(
                        "lease expired: key=%s owner_conn=%s", key, st.owner_conn_id
                    )

                    # remove ownership record
                    _conn_remove_owned(st.owner_conn_id, key)

                    # expire and transfer
                    st.owner_token = None
                    st.owner_conn_id = None
                    st.lease_expires_at = 0.0
                    st.last_activity = cur
                    _grant_next_waiter_locked(key, st)


async def lock_gc_loop():
    """
    Prune unused lock state:
      only if lock is UNHELD and has NO WAITERS and has been idle a while.
    """
    log.info("lock_gc_loop: [starting]")
    while True:
        await asyncio.sleep(GC_LOOP_SLEEP)
        cur = _now()
        expired_keys = []

        async with tracking_lock:
            for key, st in _locks.items():
                idle = cur - st.last_activity
                if (
                    idle > GC_MAX_UNUSED_TIME
                    and st.owner_token is None
                    and not st.waiters
                ):
                    expired_keys.append(key)

            for key in expired_keys:
                log.info("GC: pruning unused lock state: %s", key)
                _locks.pop(key, None)


async def cleanup_connection(conn_id: int):
    """
    Best-effort: if enabled, release locks currently owned by this conn_id
    and cancel any pending waiters from this connection.
    Also cleans up any two-phase enqueued state for this connection.
    NOTE: This assumes tokens are not transferred between connections.
    """
    if not AUTO_RELEASE_ON_DISCONNECT:
        return

    async with tracking_lock:
        # Clean up two-phase enqueued state for this connection
        enqueued_keys = [k for (cid, k) in _conn_enqueued if cid == conn_id]
        for key in enqueued_keys:
            es = _conn_enqueued.pop((conn_id, key), None)
            if es and es.waiter and not es.waiter.fut.done():
                es.waiter.fut.cancel()
                # Remove waiter from the lock's queue
                st = _locks.get(key)
                if st:
                    st.waiters = deque(w for w in st.waiters if w is not es.waiter)

        # Cancel pending waiters from this connection across all locks
        for key, st in _locks.items():
            remaining = deque()
            for w in st.waiters:
                if w.conn_id == conn_id:
                    if not w.fut.done():
                        w.fut.cancel()
                else:
                    remaining.append(w)
            st.waiters = remaining

        keys = list(_conn_owned.get(conn_id, set()))
        if not keys:
            _conn_owned.pop(conn_id, None)
            return

        for key in keys:
            st = _locks.get(key)
            if not st:
                _conn_remove_owned(conn_id, key)
                continue

            if st.owner_conn_id != conn_id:
                _conn_remove_owned(conn_id, key)
                continue

            log.warning(
                "disconnect cleanup: releasing key=%s owned by conn_id=%s", key, conn_id
            )
            _conn_remove_owned(conn_id, key)
            st.owner_token = None
            st.owner_conn_id = None
            st.lease_expires_at = 0.0
            st.last_activity = _now()
            _grant_next_waiter_locked(key, st)

        _conn_owned.pop(conn_id, None)


async def handle_request(req: Request, conn_id: int) -> Ack:
    log.info("request: conn=%s cmd=%s key=%s", conn_id, req.cmd, req.key)

    try:
        if req.cmd == "l":
            tok = await fifo_acquire(
                req.key,
                req.acquire_timeout_s or 0,
                req.lease_ttl_s or DEFAULT_LEASE_TTL_S,
                conn_id,
            )
            if tok is None:
                return Ack(Status.timeout)
            return Ack(Status.ok, token=tok, lease_ttl_s=req.lease_ttl_s)

        if req.cmd == "r":
            ok = await fifo_release(req.key, req.token or "")
            return Ack(Status.ok) if ok else Ack(Status.error)

        if req.cmd == "n":
            remaining = await fifo_renew(
                req.key, req.token or "", req.lease_ttl_s or DEFAULT_LEASE_TTL_S
            )
            if remaining is None:
                return Ack(Status.error)
            return Ack(Status.ok, extra=str(remaining))

        if req.cmd == "e":
            status, tok, lease = await fifo_enqueue(
                req.key,
                req.lease_ttl_s or DEFAULT_LEASE_TTL_S,
                conn_id,
            )
            return Ack(status, token=tok, lease_ttl_s=lease)

        if req.cmd == "w":
            result = await fifo_wait(
                req.key,
                req.acquire_timeout_s or 0,
                conn_id,
            )
            if result is None:
                return Ack(Status.timeout)
            tok, lease = result
            return Ack(Status.ok, token=tok, lease_ttl_s=lease)

        return Ack(Status.error)

    except ProtocolError as err:
        if err.code == 11:
            raise
        log.error("protocol error: code=%s %s", err.code, err)
        return Ack(Status.error)
    except MaxLocksError as err:
        log.error("max_locks error: %s", err)
        return Ack(Status.error_max_locks)
    except NotEnqueuedError:
        return Ack(Status.error)
    except Exception:
        log.exception("exception handling request")
        return Ack(Status.error)


async def handle_client(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    peer = writer.get_extra_info("peername")
    conn_id = id(writer)  # simple per-connection identifier
    log.info("client connected: %s conn_id=%s", peer, conn_id)

    try:
        while True:
            try:
                req = await read_request(reader)
                ack = await handle_request(req, conn_id)
                writer.write(format_response(ack))
                await writer.drain()

            except ProtocolError as e:
                if e.code == 11:
                    break
                log.warning("protocol error from %s: (%s) %s", peer, e.code, e)
                writer.write(format_response(Ack(Status.error)))
                await writer.drain()
                break

    except (ConnectionResetError, BrokenPipeError):
        log.info("client %s disconnected abruptly", peer)
    finally:
        # best-effort cleanup
        await cleanup_connection(conn_id)

        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass
        log.info("client closed: %s conn_id=%s", peer, conn_id)


async def main() -> None:
    server = await asyncio.start_server(handle_client, HOST, PORT)
    addrs = ", ".join(str(sock.getsockname()) for sock in (server.sockets or []))
    log.info("listening on %s", addrs)

    lease_task = asyncio.create_task(lease_expiry_loop())
    gc_task = asyncio.create_task(lock_gc_loop())

    try:
        async with server:
            await server.serve_forever()
    finally:
        lease_task.cancel()
        gc_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await lease_task
        with contextlib.suppress(asyncio.CancelledError):
            await gc_task


# (flag, env_var, global_name, type, default, help)
_CLI_CONFIG = [
    ("--host", "DFLOCKD_HOST", "HOST", str, "0.0.0.0", "Bind address"),
    ("--port", "DFLOCKD_PORT", "PORT", int, 6388, "Bind port"),
    (
        "--default-lease-ttl",
        "DEFAULT_LEASE_TTL_S",
        "DEFAULT_LEASE_TTL_S",
        int,
        33,
        "Default lock lease duration (seconds)",
    ),
    (
        "--lease-sweep-interval",
        "LEASE_SWEEP_INTERVAL_S",
        "LEASE_SWEEP_INTERVAL_S",
        int,
        1,
        "Lease expiry check interval (seconds)",
    ),
    (
        "--gc-interval",
        "GC_LOOP_SLEEP",
        "GC_LOOP_SLEEP",
        int,
        5,
        "Lock state GC interval (seconds)",
    ),
    (
        "--gc-max-idle",
        "GC_MAX_UNUSED_TIME",
        "GC_MAX_UNUSED_TIME",
        int,
        60,
        "Idle seconds before pruning lock state",
    ),
    (
        "--max-locks",
        "MAX_LOCKS",
        "MAX_LOCKS",
        int,
        1024,
        "Maximum number of unique lock keys",
    ),
    (
        "--read-timeout",
        "DFLOCKD_READ_TIMEOUT_S",
        "READ_TIMEOUT_S",
        int,
        23,
        "Client read timeout (seconds)",
    ),
    (
        "--auto-release-on-disconnect",
        "DFLOCKD_AUTO_RELEASE_ON_DISCONNECT",
        "AUTO_RELEASE_ON_DISCONNECT",
        bool,
        True,
        "Release locks when a client disconnects",
    ),
]


def cli():
    global HOST, PORT, DEFAULT_LEASE_TTL_S, LEASE_SWEEP_INTERVAL_S
    global GC_LOOP_SLEEP, GC_MAX_UNUSED_TIME, MAX_LOCKS, READ_TIMEOUT_S
    global AUTO_RELEASE_ON_DISCONNECT

    parser = argparse.ArgumentParser(description="dflockd — distributed lock server")
    for flag, _env, _glob, typ, default, helptext in _CLI_CONFIG:
        if typ is bool:
            parser.add_argument(
                flag,
                action=argparse.BooleanOptionalAction,
                default=default,
                help=helptext,
            )
        else:
            parser.add_argument(flag, type=typ, default=default, help=helptext)
    args = parser.parse_args()

    for flag, env_var, global_name, *_ in _CLI_CONFIG:
        if os.environ.get(env_var) is None:
            attr = flag.lstrip("-").replace("-", "_")
            globals()[global_name] = getattr(args, attr)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())


if __name__ == "__main__":
    cli()
