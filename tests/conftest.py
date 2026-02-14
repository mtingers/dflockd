import asyncio

import pytest
import pytest_asyncio

import dflockd.server as srv


@pytest.fixture(autouse=True)
def _reset_server_state():
    """Clear global lock state between every test."""
    srv._locks.clear()
    srv._conn_owned.clear()
    srv._conn_enqueued.clear()
    yield
    srv._locks.clear()
    srv._conn_owned.clear()
    srv._conn_enqueued.clear()


@pytest_asyncio.fixture()
async def server_port():
    """Start an actual server on a random OS-assigned port; yield the port number."""
    # Use short TTLs for faster tests
    old_ttl = srv.DEFAULT_LEASE_TTL_S
    old_sweep = srv.LEASE_SWEEP_INTERVAL_S
    srv.DEFAULT_LEASE_TTL_S = 5
    srv.LEASE_SWEEP_INTERVAL_S = 1

    started = asyncio.Event()
    port_holder: list[int] = []

    async def _run():
        server = await asyncio.start_server(srv.handle_client, "127.0.0.1", 0)
        sock = server.sockets[0]
        port_holder.append(sock.getsockname()[1])
        lease_task = asyncio.create_task(srv.lease_expiry_loop())
        gc_task = asyncio.create_task(srv.lock_gc_loop())
        started.set()
        try:
            async with server:
                await server.serve_forever()
        finally:
            lease_task.cancel()
            gc_task.cancel()

    task = asyncio.create_task(_run())
    await started.wait()
    yield port_holder[0]
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    srv.DEFAULT_LEASE_TTL_S = old_ttl
    srv.LEASE_SWEEP_INTERVAL_S = old_sweep
