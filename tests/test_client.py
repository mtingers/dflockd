"""Integration tests: async client and sync client against a real server."""

import asyncio
import socket
import threading
import time

import pytest

import dflockd.client as aclient
import dflockd.server as srv
import dflockd.sync_client as sclient

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


async def _open(port: int):
    return await asyncio.open_connection("127.0.0.1", port)


def _sync_connect(port: int) -> tuple[socket.socket, ...]:
    sock = socket.create_connection(("127.0.0.1", port))
    rfile = sock.makefile("r", encoding="utf-8")
    return sock, rfile


# ===========================================================================
# Async client (dflockd.client) — low-level functions
# ===========================================================================


class TestAsyncAcquireRelease:
    @pytest.mark.asyncio
    async def test_acquire_and_release(self, server_port):
        reader, writer = await _open(server_port)
        try:
            token, lease = await aclient.acquire(reader, writer, "k1", 5)
            assert isinstance(token, str) and len(token) > 0
            assert lease > 0
            await aclient.release(reader, writer, "k1", token)
        finally:
            writer.close()
            await writer.wait_closed()

    @pytest.mark.asyncio
    async def test_acquire_timeout(self, server_port):
        r1, w1 = await _open(server_port)
        r2, w2 = await _open(server_port)
        try:
            await aclient.acquire(r1, w1, "k1", 5)
            with pytest.raises(TimeoutError):
                await aclient.acquire(r2, w2, "k1", 0)
        finally:
            w1.close()
            w2.close()

    @pytest.mark.asyncio
    async def test_release_bad_token(self, server_port):
        reader, writer = await _open(server_port)
        try:
            await aclient.acquire(reader, writer, "k1", 5)
            with pytest.raises(RuntimeError, match="release failed"):
                await aclient.release(reader, writer, "k1", "badtoken")
        finally:
            writer.close()


class TestAsyncRenew:
    @pytest.mark.asyncio
    async def test_renew(self, server_port):
        reader, writer = await _open(server_port)
        try:
            token, _lease = await aclient.acquire(
                reader, writer, "k1", 5, lease_ttl_s=10
            )
            remaining = await aclient.renew(reader, writer, "k1", token, lease_ttl_s=20)
            assert remaining >= 0
        finally:
            writer.close()

    @pytest.mark.asyncio
    async def test_renew_bad_token(self, server_port):
        reader, writer = await _open(server_port)
        try:
            await aclient.acquire(reader, writer, "k1", 5)
            with pytest.raises(RuntimeError, match="renew failed"):
                await aclient.renew(reader, writer, "k1", "badtoken")
        finally:
            writer.close()


# ===========================================================================
# Async DistributedLock context manager
# ===========================================================================


class TestAsyncDistributedLock:
    @pytest.mark.asyncio
    async def test_context_manager(self, server_port):
        lock = aclient.DistributedLock(
            key="k1",
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[("127.0.0.1", server_port)],
            renew_ratio=0.3,
        )
        async with lock as lk:
            assert lk.token is not None
        assert lock.token is None

    @pytest.mark.asyncio
    async def test_acquire_release_methods(self, server_port):
        lock = aclient.DistributedLock(
            key="k1",
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[("127.0.0.1", server_port)],
        )
        ok = await lock.acquire()
        assert ok is True
        assert lock.token is not None
        await lock.release()
        assert lock.token is None

    @pytest.mark.asyncio
    async def test_acquire_timeout_returns_false(self, server_port):
        lock1 = aclient.DistributedLock(
            key="k1",
            acquire_timeout_s=5,
            lease_ttl_s=30,
            servers=[("127.0.0.1", server_port)],
        )
        lock2 = aclient.DistributedLock(
            key="k1",
            acquire_timeout_s=0,
            lease_ttl_s=30,
            servers=[("127.0.0.1", server_port)],
        )
        await lock1.acquire()
        try:
            ok = await lock2.acquire()
            assert ok is False
        finally:
            await lock1.release()

    @pytest.mark.asyncio
    async def test_mutual_exclusion(self, server_port):
        """Two async locks on the same key; second must wait."""
        results: list[int] = []

        async def worker(n: int, timeout: int):
            lock = aclient.DistributedLock(
                key="mutex",
                acquire_timeout_s=timeout,
                lease_ttl_s=5,
                servers=[("127.0.0.1", server_port)],
            )
            async with lock:
                results.append(n)
                await asyncio.sleep(0.1)

        t1 = asyncio.create_task(worker(1, 5))
        await asyncio.sleep(0.02)
        t2 = asyncio.create_task(worker(2, 5))
        await asyncio.gather(t1, t2)
        assert results == [1, 2]


# ===========================================================================
# Sync client (dflockd.sync_client) — low-level functions
# ===========================================================================


class TestSyncAcquireRelease:
    @pytest.mark.asyncio
    async def test_acquire_and_release(self, server_port):
        """Run sync client in a thread against the async server."""

        def _work():
            sock, rfile = _sync_connect(server_port)
            try:
                token, lease = sclient.acquire(sock, rfile, "k1", 5)
                assert isinstance(token, str) and len(token) > 0
                assert lease > 0
                sclient.release(sock, rfile, "k1", token)
            finally:
                rfile.close()
                sock.close()

        await asyncio.to_thread(_work)

    @pytest.mark.asyncio
    async def test_renew(self, server_port):
        def _work():
            sock, rfile = _sync_connect(server_port)
            try:
                token, _ = sclient.acquire(sock, rfile, "k1", 5, lease_ttl_s=10)
                remaining = sclient.renew(sock, rfile, "k1", token, lease_ttl_s=20)
                assert remaining >= 0
            finally:
                rfile.close()
                sock.close()

        await asyncio.to_thread(_work)


# ===========================================================================
# Sync DistributedLock context manager
# ===========================================================================


class TestSyncDistributedLock:
    @pytest.mark.asyncio
    async def test_context_manager(self, server_port):
        def _work():
            lock = sclient.DistributedLock(
                key="k1",
                acquire_timeout_s=5,
                lease_ttl_s=5,
                servers=[("127.0.0.1", server_port)],
                renew_ratio=0.3,
            )
            with lock as lk:
                assert lk.token is not None
            assert lock.token is None

        await asyncio.to_thread(_work)

    @pytest.mark.asyncio
    async def test_acquire_release_methods(self, server_port):
        def _work():
            lock = sclient.DistributedLock(
                key="k1",
                acquire_timeout_s=5,
                lease_ttl_s=5,
                servers=[("127.0.0.1", server_port)],
            )
            ok = lock.acquire()
            assert ok is True
            assert lock.token is not None
            lock.release()
            assert lock.token is None

        await asyncio.to_thread(_work)

    @pytest.mark.asyncio
    async def test_mutual_exclusion_threads(self, server_port):
        """Two sync locks from different threads; verify serial execution."""
        results: list[int] = []
        lock_obj = threading.Lock()

        def _worker(n, timeout):
            lock = sclient.DistributedLock(
                key="mutex",
                acquire_timeout_s=timeout,
                lease_ttl_s=5,
                servers=[("127.0.0.1", server_port)],
            )
            with lock:
                with lock_obj:
                    results.append(n)
                time.sleep(0.1)

        def _run():
            t1 = threading.Thread(target=_worker, args=(1, 5))
            t2 = threading.Thread(target=_worker, args=(2, 5))
            t1.start()
            time.sleep(0.02)
            t2.start()
            t1.join(timeout=10)
            t2.join(timeout=10)

        await asyncio.to_thread(_run)
        assert results == [1, 2]


# ===========================================================================
# Disconnect / lease-expiry integration
# ===========================================================================


class TestDisconnectBehavior:
    @pytest.mark.asyncio
    async def test_disconnect_releases_lock(self, server_port):
        """When a client disconnects, the server releases its lock."""
        r1, w1 = await _open(server_port)
        token1, _ = await aclient.acquire(r1, w1, "k1", 5, lease_ttl_s=30)

        # Disconnect abruptly
        w1.close()
        await asyncio.sleep(0.2)

        # Now a second client should be able to acquire immediately
        r2, w2 = await _open(server_port)
        try:
            token2, _ = await aclient.acquire(r2, w2, "k1", 1, lease_ttl_s=30)
            assert token2 is not None
        finally:
            w2.close()

    @pytest.mark.asyncio
    async def test_lease_expiry_allows_reacquire(self, server_port):
        """Acquire with a very short lease; after expiry another client gets it."""
        old_sweep = srv.LEASE_SWEEP_INTERVAL_S
        srv.LEASE_SWEEP_INTERVAL_S = 0.1
        try:
            r1, w1 = await _open(server_port)
            # Acquire with 1-second lease; don't renew
            token1, _ = await aclient.acquire(r1, w1, "k1", 5, lease_ttl_s=1)

            # Wait for lease to expire
            await asyncio.sleep(1.5)

            r2, w2 = await _open(server_port)
            try:
                token2, _ = await aclient.acquire(r2, w2, "k1", 2, lease_ttl_s=30)
                assert token2 is not None
                assert token2 != token1
            finally:
                w2.close()
        finally:
            w1.close()
            srv.LEASE_SWEEP_INTERVAL_S = old_sweep


# ===========================================================================
# Sharding
# ===========================================================================


# ===========================================================================
# Async two-phase: enqueue + wait
# ===========================================================================


class TestAsyncTwoPhase:
    @pytest.mark.asyncio
    async def test_low_level_enqueue_wait_release(self, server_port):
        """Low-level: enqueue + wait + release on a free lock."""
        reader, writer = await _open(server_port)
        try:
            status, token, lease = await aclient.enqueue(reader, writer, "k1")
            assert status == "acquired"
            assert token is not None
            assert lease > 0

            # wait should return immediately (already acquired)
            tok, ttl = await aclient.wait(reader, writer, "k1", 5)
            assert tok == token

            await aclient.release(reader, writer, "k1", tok)
        finally:
            writer.close()
            await writer.wait_closed()

    @pytest.mark.asyncio
    async def test_low_level_queued_then_wait(self, server_port):
        """Low-level: conn1 holds, conn2 enqueues (queued), conn1 releases, conn2 waits."""
        r1, w1 = await _open(server_port)
        r2, w2 = await _open(server_port)
        try:
            # conn1 acquires
            tok1, _ = await aclient.acquire(r1, w1, "k1", 5)

            # conn2 enqueues — should be queued
            status, _, _ = await aclient.enqueue(r2, w2, "k1")
            assert status == "queued"

            # Release conn1 in background, then conn2 waits
            async def _release_soon():
                await asyncio.sleep(0.1)
                await aclient.release(r1, w1, "k1", tok1)

            release_task = asyncio.create_task(_release_soon())
            tok2, lease2 = await aclient.wait(r2, w2, "k1", 5)
            await release_task

            assert tok2 is not None
            assert lease2 > 0
            await aclient.release(r2, w2, "k1", tok2)
        finally:
            w1.close()
            w2.close()

    @pytest.mark.asyncio
    async def test_distributed_lock_two_phase(self, server_port):
        """DistributedLock.enqueue() + wait() + release() flow."""
        lock = aclient.DistributedLock(
            key="k1",
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[("127.0.0.1", server_port)],
        )
        status = await lock.enqueue()
        assert status == "acquired"
        assert lock.token is not None

        ok = await lock.wait()
        assert ok is True

        await lock.release()
        assert lock.token is None

    @pytest.mark.asyncio
    async def test_distributed_lock_two_phase_contention(self, server_port):
        """Two DistributedLock instances: lock1 holds, lock2 does enqueue+wait."""
        lock1 = aclient.DistributedLock(
            key="k1",
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[("127.0.0.1", server_port)],
        )
        lock2 = aclient.DistributedLock(
            key="k1",
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[("127.0.0.1", server_port)],
        )

        await lock1.acquire()

        status = await lock2.enqueue()
        assert status == "queued"

        async def _release_soon():
            await asyncio.sleep(0.1)
            await lock1.release()

        release_task = asyncio.create_task(_release_soon())
        ok = await lock2.wait()
        await release_task

        assert ok is True
        assert lock2.token is not None
        await lock2.release()


class TestAsyncSharding:
    def test_empty_servers_raises(self):
        with pytest.raises(ValueError, match="non-empty"):
            aclient.DistributedLock(key="k", servers=[])

    def test_default_servers(self):
        lock = aclient.DistributedLock(key="k")
        assert lock.servers == [("127.0.0.1", 6388)]

    @pytest.mark.asyncio
    async def test_custom_strategy(self, server_port):
        lock = aclient.DistributedLock(
            key="k1",
            acquire_timeout_s=5,
            servers=[("127.0.0.1", server_port)],
            sharding_strategy=lambda key, n: 0,
        )
        async with lock as lk:
            assert lk.token is not None
        assert lock.token is None
