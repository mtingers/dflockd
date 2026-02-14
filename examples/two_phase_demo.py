"""Two-phase lock acquisition demo.

Shows how to split lock acquisition into enqueue + wait so you can
notify an external system between joining the queue and blocking.

Usage:
    uv run python examples/two_phase_demo.py
"""

import asyncio

from dflockd.client import DistributedLock


def notify_external_system(worker_id: int, status: str):
    """Simulate notifying an external system (e.g. webhook, queue, database)."""
    print(f"  worker {worker_id}: notified external system (status={status})")


async def demo_two_phase():
    """Basic two-phase flow: enqueue, notify, wait, work, release."""
    lock = DistributedLock("two-phase-key", acquire_timeout_s=10, lease_ttl_s=20)

    print("step 1: enqueue")
    status = await lock.enqueue()
    print(f"  enqueue returned: {status}")

    print("step 2: notify external system")
    notify_external_system(0, status)

    print("step 3: wait for lock")
    granted = await lock.wait(timeout_s=10)
    print(f"  wait returned: granted={granted}, token={lock.token}")

    if granted:
        print("step 4: critical section")
        await asyncio.sleep(1)
        print("step 5: release")
        await lock.release()

    print("done\n")


async def _two_phase_worker(worker_id: int, hold_time: float = 1.0):
    """A worker that uses two-phase acquisition."""
    lock = DistributedLock(
        "shared-key", acquire_timeout_s=30, lease_ttl_s=20
    )

    print(f"worker {worker_id}: enqueueing")
    status = await lock.enqueue()
    print(f"worker {worker_id}: enqueue returned {status}")

    notify_external_system(worker_id, status)

    print(f"worker {worker_id}: waiting for lock")
    granted = await lock.wait(timeout_s=30)
    if not granted:
        print(f"worker {worker_id}: timed out")
        return

    print(f"worker {worker_id}: acquired token={lock.token}")
    await asyncio.sleep(hold_time)
    print(f"worker {worker_id}: releasing")
    await lock.release()


async def demo_contention():
    """Multiple workers competing with two-phase acquisition (FIFO order)."""
    num_workers = 4
    print(f"launching {num_workers} two-phase workers with shared lock...\n")
    tasks = [_two_phase_worker(i, hold_time=0.5) for i in range(num_workers)]
    await asyncio.gather(*tasks)
    print("\nall workers finished")


if __name__ == "__main__":
    asyncio.run(demo_two_phase())
    # asyncio.run(demo_contention())
