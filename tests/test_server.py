"""Unit tests for dflockd.server internals."""

import asyncio
import os
from unittest.mock import patch

import pytest

import dflockd.server as srv
from dflockd.server import (
    Ack,
    MaxLocksError,
    ProtocolError,
    Request,
    Status,
    _conn_add_owned,
    _conn_owned,
    _conn_remove_owned,
    _locks,
    _new_token,
    _now,
    _parse_int,
    cleanup_connection,
    fifo_acquire,
    fifo_release,
    fifo_renew,
    format_response,
    getenv_int,
    handle_request,
    read_line,
    read_request,
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _make_reader(*lines: str) -> asyncio.StreamReader:
    """Create a StreamReader pre-loaded with newline-terminated lines."""
    reader = asyncio.StreamReader()
    data = "".join(f"{ln}\n" for ln in lines).encode("utf-8")
    reader.feed_data(data)
    reader.feed_eof()
    return reader


# ---------------------------------------------------------------------------
# getenv_int
# ---------------------------------------------------------------------------


class TestGetenvInt:
    def test_returns_default_when_unset(self):
        assert getenv_int("__DFLOCKD_UNSET_KEY__", 42) == 42

    def test_returns_parsed_value(self):
        with patch.dict(os.environ, {"__DFLOCKD_TEST__": "7"}):
            assert getenv_int("__DFLOCKD_TEST__", 99) == 7

    def test_returns_default_on_bad_value(self):
        with patch.dict(os.environ, {"__DFLOCKD_TEST__": "abc"}):
            assert getenv_int("__DFLOCKD_TEST__", 99) == 99


# ---------------------------------------------------------------------------
# _parse_int / ProtocolError
# ---------------------------------------------------------------------------


class TestParseInt:
    def test_valid(self):
        assert _parse_int("10", "test") == 10

    def test_negative(self):
        assert _parse_int("-1", "test") == -1

    def test_invalid_raises(self):
        with pytest.raises(ProtocolError) as exc:
            _parse_int("abc", "timeout")
        assert exc.value.code == 4


# ---------------------------------------------------------------------------
# read_line
# ---------------------------------------------------------------------------


class TestReadLine:
    @pytest.mark.asyncio
    async def test_normal(self):
        reader = _make_reader("hello")
        assert await read_line(reader) == "hello"

    @pytest.mark.asyncio
    async def test_eof_raises(self):
        reader = asyncio.StreamReader()
        reader.feed_eof()
        with pytest.raises(ProtocolError) as exc:
            await read_line(reader)
        assert exc.value.code == 11

    @pytest.mark.asyncio
    async def test_too_long_raises(self):
        reader = _make_reader("x" * (srv.MAX_LINE_BYTES + 10))
        with pytest.raises(ProtocolError) as exc:
            await read_line(reader)
        assert exc.value.code == 12


# ---------------------------------------------------------------------------
# read_request
# ---------------------------------------------------------------------------


class TestReadRequest:
    @pytest.mark.asyncio
    async def test_lock_default_lease(self):
        reader = _make_reader("l", "mykey", "10")
        req = await read_request(reader)
        assert req.cmd == "l"
        assert req.key == "mykey"
        assert req.acquire_timeout_s == 10
        assert req.lease_ttl_s == srv.DEFAULT_LEASE_TTL_S

    @pytest.mark.asyncio
    async def test_lock_custom_lease(self):
        reader = _make_reader("l", "mykey", "10 20")
        req = await read_request(reader)
        assert req.lease_ttl_s == 20

    @pytest.mark.asyncio
    async def test_release(self):
        reader = _make_reader("r", "mykey", "abc123")
        req = await read_request(reader)
        assert req.cmd == "r"
        assert req.token == "abc123"

    @pytest.mark.asyncio
    async def test_renew_default_lease(self):
        reader = _make_reader("n", "mykey", "tok1")
        req = await read_request(reader)
        assert req.cmd == "n"
        assert req.token == "tok1"
        assert req.lease_ttl_s == srv.DEFAULT_LEASE_TTL_S

    @pytest.mark.asyncio
    async def test_renew_custom_lease(self):
        reader = _make_reader("n", "mykey", "tok1 15")
        req = await read_request(reader)
        assert req.lease_ttl_s == 15

    @pytest.mark.asyncio
    async def test_invalid_cmd(self):
        reader = _make_reader("x", "mykey", "arg")
        with pytest.raises(ProtocolError) as exc:
            await read_request(reader)
        assert exc.value.code == 3

    @pytest.mark.asyncio
    async def test_empty_key(self):
        reader = _make_reader("l", "", "10")
        with pytest.raises(ProtocolError) as exc:
            await read_request(reader)
        assert exc.value.code == 5

    @pytest.mark.asyncio
    async def test_negative_timeout(self):
        reader = _make_reader("l", "k", "-1")
        with pytest.raises(ProtocolError) as exc:
            await read_request(reader)
        assert exc.value.code == 6

    @pytest.mark.asyncio
    async def test_zero_lease_ttl(self):
        reader = _make_reader("l", "k", "10 0")
        with pytest.raises(ProtocolError) as exc:
            await read_request(reader)
        assert exc.value.code == 9

    @pytest.mark.asyncio
    async def test_empty_token_release(self):
        reader = _make_reader("r", "k", " ")
        with pytest.raises(ProtocolError) as exc:
            await read_request(reader)
        assert exc.value.code == 7

    @pytest.mark.asyncio
    async def test_lock_bad_arg_count(self):
        reader = _make_reader("l", "k", "1 2 3")
        with pytest.raises(ProtocolError) as exc:
            await read_request(reader)
        assert exc.value.code == 8


# ---------------------------------------------------------------------------
# format_response
# ---------------------------------------------------------------------------


class TestFormatResponse:
    def test_ok_with_token(self):
        ack = Ack(Status.ok, token="abc", lease_ttl_s=30)
        assert format_response(ack) == b"ok abc 30\n"

    def test_ok_with_token_default_lease(self):
        ack = Ack(Status.ok, token="abc", lease_ttl_s=None)
        assert format_response(ack) == f"ok abc {srv.DEFAULT_LEASE_TTL_S}\n".encode()

    def test_ok_with_extra(self):
        ack = Ack(Status.ok, extra="25")
        assert format_response(ack) == b"ok 25\n"

    def test_ok_bare(self):
        ack = Ack(Status.ok)
        assert format_response(ack) == b"ok\n"

    def test_error(self):
        ack = Ack(Status.error)
        assert format_response(ack) == b"error\n"

    def test_timeout(self):
        ack = Ack(Status.timeout)
        assert format_response(ack) == b"timeout\n"

    def test_error_max_locks(self):
        ack = Ack(Status.error_max_locks)
        assert format_response(ack) == b"error_max_locks\n"


# ---------------------------------------------------------------------------
# conn ownership tracking
# ---------------------------------------------------------------------------


class TestConnOwnership:
    def test_add_and_remove(self):
        _conn_add_owned(1, "a")
        _conn_add_owned(1, "b")
        assert _conn_owned[1] == {"a", "b"}

        _conn_remove_owned(1, "a")
        assert _conn_owned[1] == {"b"}

        _conn_remove_owned(1, "b")
        assert 1 not in _conn_owned

    def test_remove_nonexistent(self):
        _conn_remove_owned(999, "x")  # should not raise

    def test_remove_none_conn(self):
        _conn_remove_owned(None, "x")  # should not raise


# ---------------------------------------------------------------------------
# fifo_acquire / fifo_release / fifo_renew
# ---------------------------------------------------------------------------


class TestFifoAcquire:
    @pytest.mark.asyncio
    async def test_immediate_acquire(self):
        token = await fifo_acquire("k1", 5, 30, conn_id=1)
        assert token is not None
        assert _locks["k1"].owner_token == token
        assert _locks["k1"].owner_conn_id == 1

    @pytest.mark.asyncio
    async def test_acquire_timeout(self):
        # First acquire succeeds
        await fifo_acquire("k1", 5, 30, conn_id=1)
        # Second acquire should timeout quickly
        token = await fifo_acquire("k1", 0, 30, conn_id=2)
        assert token is None

    @pytest.mark.asyncio
    async def test_fifo_ordering(self):
        """Two waiters queued; they should be granted in order."""
        tok1 = await fifo_acquire("k1", 5, 30, conn_id=1)
        assert tok1 is not None

        # Start two waiters concurrently
        task2 = asyncio.create_task(fifo_acquire("k1", 5, 30, conn_id=2))
        task3 = asyncio.create_task(fifo_acquire("k1", 5, 30, conn_id=3))
        await asyncio.sleep(0.05)  # let them enqueue

        # Release: waiter 2 (first enqueued) should get it
        await fifo_release("k1", tok1)
        tok2 = await task2
        assert tok2 is not None
        assert _locks["k1"].owner_conn_id == 2

        # Release again: waiter 3 should get it
        await fifo_release("k1", tok2)
        tok3 = await task3
        assert tok3 is not None
        assert _locks["k1"].owner_conn_id == 3

    @pytest.mark.asyncio
    async def test_max_locks_error(self):
        old = srv.MAX_LOCKS
        srv.MAX_LOCKS = 1
        try:
            await fifo_acquire("k1", 5, 30, conn_id=1)
            with pytest.raises(MaxLocksError):
                await fifo_acquire("k2", 5, 30, conn_id=2)
        finally:
            srv.MAX_LOCKS = old


class TestFifoRelease:
    @pytest.mark.asyncio
    async def test_release_valid(self):
        token = await fifo_acquire("k1", 5, 30, conn_id=1)
        assert await fifo_release("k1", token) is True
        assert _locks["k1"].owner_token is None

    @pytest.mark.asyncio
    async def test_release_wrong_token(self):
        await fifo_acquire("k1", 5, 30, conn_id=1)
        assert await fifo_release("k1", "wrong") is False

    @pytest.mark.asyncio
    async def test_release_nonexistent_key(self):
        assert await fifo_release("nope", "tok") is False

    @pytest.mark.asyncio
    async def test_release_transfers_to_waiter(self):
        tok = await fifo_acquire("k1", 5, 30, conn_id=1)
        waiter_task = asyncio.create_task(fifo_acquire("k1", 5, 30, conn_id=2))
        await asyncio.sleep(0.05)

        await fifo_release("k1", tok)
        tok2 = await waiter_task
        assert tok2 is not None
        assert _locks["k1"].owner_conn_id == 2


class TestFifoRenew:
    @pytest.mark.asyncio
    async def test_renew_valid(self):
        token = await fifo_acquire("k1", 5, 30, conn_id=1)
        remaining = await fifo_renew("k1", token, 60)
        assert remaining is not None
        assert remaining > 0

    @pytest.mark.asyncio
    async def test_renew_wrong_token(self):
        await fifo_acquire("k1", 5, 30, conn_id=1)
        assert await fifo_renew("k1", "wrong", 30) is None

    @pytest.mark.asyncio
    async def test_renew_nonexistent_key(self):
        assert await fifo_renew("nope", "tok", 30) is None

    @pytest.mark.asyncio
    async def test_renew_expired_rejected(self):
        """Renew after lease has expired should fail."""
        token = await fifo_acquire("k1", 5, 1, conn_id=1)
        # Manually expire
        _locks["k1"].lease_expires_at = _now() - 1
        assert await fifo_renew("k1", token, 30) is None
        # Lock should have been released
        assert _locks["k1"].owner_token is None


# ---------------------------------------------------------------------------
# cleanup_connection
# ---------------------------------------------------------------------------


class TestCleanupConnection:
    @pytest.mark.asyncio
    async def test_releases_owned_locks(self):
        tok = await fifo_acquire("k1", 5, 30, conn_id=100)
        assert tok is not None
        assert _locks["k1"].owner_conn_id == 100

        await cleanup_connection(100)
        assert _locks["k1"].owner_token is None

    @pytest.mark.asyncio
    async def test_cancels_pending_waiters(self):
        await fifo_acquire("k1", 5, 30, conn_id=1)
        waiter = asyncio.create_task(fifo_acquire("k1", 10, 30, conn_id=2))
        await asyncio.sleep(0.05)

        await cleanup_connection(2)
        # The waiter future is cancelled, which propagates as CancelledError
        # or the waiter may return None if it was already dequeued
        try:
            result = await waiter
            assert result is None
        except asyncio.CancelledError:
            pass  # also acceptable

    @pytest.mark.asyncio
    async def test_transfers_to_next_waiter(self):
        await fifo_acquire("k1", 5, 30, conn_id=1)
        waiter_task = asyncio.create_task(fifo_acquire("k1", 5, 30, conn_id=2))
        await asyncio.sleep(0.05)

        # Disconnect conn_id=1 (owner) — should transfer to conn_id=2
        await cleanup_connection(1)
        tok2 = await waiter_task
        assert tok2 is not None
        assert _locks["k1"].owner_conn_id == 2

    @pytest.mark.asyncio
    async def test_noop_for_unknown_conn(self):
        await cleanup_connection(9999)  # should not raise


# ---------------------------------------------------------------------------
# handle_request
# ---------------------------------------------------------------------------


class TestHandleRequest:
    @pytest.mark.asyncio
    async def test_lock_acquire(self):
        req = Request(cmd="l", key="k1", acquire_timeout_s=5, lease_ttl_s=30)
        ack = await handle_request(req, conn_id=1)
        assert ack.status == Status.ok
        assert ack.token is not None

    @pytest.mark.asyncio
    async def test_lock_timeout(self):
        req1 = Request(cmd="l", key="k1", acquire_timeout_s=5, lease_ttl_s=30)
        await handle_request(req1, conn_id=1)

        req2 = Request(cmd="l", key="k1", acquire_timeout_s=0, lease_ttl_s=30)
        ack = await handle_request(req2, conn_id=2)
        assert ack.status == Status.timeout

    @pytest.mark.asyncio
    async def test_release(self):
        req = Request(cmd="l", key="k1", acquire_timeout_s=5, lease_ttl_s=30)
        ack = await handle_request(req, conn_id=1)

        req_r = Request(cmd="r", key="k1", token=ack.token)
        ack_r = await handle_request(req_r, conn_id=1)
        assert ack_r.status == Status.ok

    @pytest.mark.asyncio
    async def test_release_bad_token(self):
        req = Request(cmd="l", key="k1", acquire_timeout_s=5, lease_ttl_s=30)
        await handle_request(req, conn_id=1)

        req_r = Request(cmd="r", key="k1", token="bad")
        ack_r = await handle_request(req_r, conn_id=1)
        assert ack_r.status == Status.error

    @pytest.mark.asyncio
    async def test_renew(self):
        req = Request(cmd="l", key="k1", acquire_timeout_s=5, lease_ttl_s=30)
        ack = await handle_request(req, conn_id=1)

        req_n = Request(cmd="n", key="k1", token=ack.token, lease_ttl_s=60)
        ack_n = await handle_request(req_n, conn_id=1)
        assert ack_n.status == Status.ok
        assert ack_n.extra is not None

    @pytest.mark.asyncio
    async def test_max_locks(self):
        old = srv.MAX_LOCKS
        srv.MAX_LOCKS = 0
        try:
            req = Request(cmd="l", key="k1", acquire_timeout_s=5, lease_ttl_s=30)
            ack = await handle_request(req, conn_id=1)
            assert ack.status == Status.error_max_locks
        finally:
            srv.MAX_LOCKS = old


# ---------------------------------------------------------------------------
# lease_expiry_loop (functional test)
# ---------------------------------------------------------------------------


class TestLeaseExpiry:
    @pytest.mark.asyncio
    async def test_expired_lease_releases_lock(self):
        old_sweep = srv.LEASE_SWEEP_INTERVAL_S
        srv.LEASE_SWEEP_INTERVAL_S = 0.1
        try:
            token = await fifo_acquire("k1", 5, 1, conn_id=1)
            assert token is not None
            # Manually set expiry to past
            _locks["k1"].lease_expires_at = _now() - 1

            task = asyncio.create_task(srv.lease_expiry_loop())
            await asyncio.sleep(0.3)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

            assert _locks["k1"].owner_token is None
        finally:
            srv.LEASE_SWEEP_INTERVAL_S = old_sweep

    @pytest.mark.asyncio
    async def test_expired_lease_transfers_to_waiter(self):
        old_sweep = srv.LEASE_SWEEP_INTERVAL_S
        srv.LEASE_SWEEP_INTERVAL_S = 0.1
        try:
            await fifo_acquire("k1", 5, 1, conn_id=1)
            waiter_task = asyncio.create_task(fifo_acquire("k1", 5, 30, conn_id=2))
            await asyncio.sleep(0.05)

            # Expire the lease
            _locks["k1"].lease_expires_at = _now() - 1

            task = asyncio.create_task(srv.lease_expiry_loop())
            tok2 = await asyncio.wait_for(waiter_task, timeout=2)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

            assert tok2 is not None
            assert _locks["k1"].owner_conn_id == 2
        finally:
            srv.LEASE_SWEEP_INTERVAL_S = old_sweep


# ---------------------------------------------------------------------------
# lock_gc_loop (functional test)
# ---------------------------------------------------------------------------


class TestLockGC:
    @pytest.mark.asyncio
    async def test_prunes_idle_unlocked_state(self):
        old_sleep = srv.GC_LOOP_SLEEP
        old_unused = srv.GC_MAX_UNUSED_TIME
        srv.GC_LOOP_SLEEP = 0.1
        srv.GC_MAX_UNUSED_TIME = 0
        try:
            # Create and release a lock so the state is idle
            tok = await fifo_acquire("k1", 5, 30, conn_id=1)
            await fifo_release("k1", tok)
            # Force last_activity far in the past
            _locks["k1"].last_activity = _now() - 100

            task = asyncio.create_task(srv.lock_gc_loop())
            await asyncio.sleep(0.3)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

            assert "k1" not in _locks
        finally:
            srv.GC_LOOP_SLEEP = old_sleep
            srv.GC_MAX_UNUSED_TIME = old_unused

    @pytest.mark.asyncio
    async def test_does_not_prune_held_lock(self):
        old_sleep = srv.GC_LOOP_SLEEP
        old_unused = srv.GC_MAX_UNUSED_TIME
        srv.GC_LOOP_SLEEP = 0.1
        srv.GC_MAX_UNUSED_TIME = 0
        try:
            await fifo_acquire("k1", 5, 30, conn_id=1)
            _locks["k1"].last_activity = _now() - 100

            task = asyncio.create_task(srv.lock_gc_loop())
            await asyncio.sleep(0.3)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

            # Still present because lock is held
            assert "k1" in _locks
        finally:
            srv.GC_LOOP_SLEEP = old_sleep
            srv.GC_MAX_UNUSED_TIME = old_unused


# ---------------------------------------------------------------------------
# _new_token
# ---------------------------------------------------------------------------


class TestNewToken:
    def test_unique(self):
        tokens = {_new_token() for _ in range(100)}
        assert len(tokens) == 100

    def test_hex_string(self):
        tok = _new_token()
        assert isinstance(tok, str)
        int(tok, 16)  # should not raise
