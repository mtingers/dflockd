import asyncio

from dflockd.client import DistributedLock


async def demo():
    """Acquire a lock, hold it for 45 s (renewing automatically), then release."""
    async with DistributedLock("foo", acquire_timeout_s=10, lease_ttl_s=20) as lock:
        print(f"acquired key={lock.key} token={lock.token} lease={lock.lease}")
        await asyncio.sleep(45)  # will keep renewing
        print("done critical section")


async def _demo_lock_ordering(worker_id: int):
    print(f"_demo_lock_ordering[start]: {worker_id}")
    async with DistributedLock("foo", acquire_timeout_s=12) as lock:
        print(f"acquired  token({worker_id}): {lock.token}")
        await asyncio.sleep(1)
        print(f"released  token({worker_id}): {lock.token}")


async def demo_lock_ordering():
    """Launch several workers that all compete for the same lock (FIFO order)."""
    num_tasks = 9
    tasks = [_demo_lock_ordering(i) for i in range(num_tasks)]
    print(f"launched {num_tasks} workers with shared lock. gathering...")
    await asyncio.gather(*tasks)
    print("all workers finished")


async def demo_multi_server():
    """Demonstrate multi-server sharding: different keys route to different servers."""
    servers = [("127.0.0.1", 6388), ("127.0.0.1", 6389)]
    for key in ("job-a", "job-b", "job-c"):
        async with DistributedLock(key, acquire_timeout_s=10, servers=servers) as lock:
            print(f"key={lock.key} token={lock.token} lease={lock.lease}")


if __name__ == "__main__":
    # asyncio.run(demo())
    asyncio.run(demo_lock_ordering())
