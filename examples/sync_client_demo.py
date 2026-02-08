import threading
import time

from dflockd.sync_client import DistributedLock


def demo():
    """Acquire a lock, hold it for a few seconds (renewing automatically), then release."""
    with DistributedLock("foo", acquire_timeout_s=10, lease_ttl_s=20) as lock:
        print(f"acquired key={lock.key} token={lock.token} lease={lock.lease}")
        time.sleep(5)
        print("done critical section")


def _demo_lock_ordering(worker_id: int):
    print(f"_demo_lock_ordering[start]: {worker_id}")
    with DistributedLock("foo", acquire_timeout_s=30) as lock:
        print(f"acquired  token({worker_id}): {lock.token}")
        time.sleep(1)
        print(f"released  token({worker_id}): {lock.token}")


def demo_lock_ordering():
    """Launch several threads that all compete for the same lock (FIFO order)."""
    num_threads = 9
    threads = [
        threading.Thread(target=_demo_lock_ordering, args=(i,))
        for i in range(num_threads)
    ]
    print(f"launching {num_threads} threads with shared lock...")
    for t in threads:
        t.start()
        # race: sleep since we're not tracking if thread is running yet
        time.sleep(0.01)
    for t in threads:
        t.join()
    print("all threads finished")


def demo_multi_server():
    """Demonstrate multi-server sharding: different keys route to different servers."""
    servers = [("127.0.0.1", 6388), ("127.0.0.1", 6389)]
    for key in ("job-a", "job-b", "job-c"):
        with DistributedLock(key, acquire_timeout_s=10, servers=servers) as lock:
            print(f"key={lock.key} token={lock.token} lease={lock.lease}")


if __name__ == "__main__":
    # demo()
    demo_lock_ordering()
