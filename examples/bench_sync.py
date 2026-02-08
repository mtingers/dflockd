"""
Sync benchmark: N concurrent threads each acquire/release a shared lock
repeatedly and report latency statistics.

Usage:
    uv run python examples/bench_sync.py [--workers 10] [--rounds 50] [--key bench] \
        [--servers host1:port1,host2:port2]
"""

import argparse
import statistics
import threading
import time

from dflockd.sync_client import DistributedLock


def parse_servers(raw: str) -> list[tuple[str, int]]:
    servers = []
    for entry in raw.split(","):
        host, port_str = entry.rsplit(":", 1)
        servers.append((host, int(port_str)))
    return servers


def worker(
    key: str,
    rounds: int,
    timeout_s: int,
    servers: list[tuple[str, int]],
    results: list[list[float]],
    idx: int,
) -> None:
    latencies: list[float] = []
    for _ in range(rounds):
        t0 = time.perf_counter()
        with DistributedLock(key, acquire_timeout_s=timeout_s, lease_ttl_s=10, servers=servers):
            pass  # acquire + immediate release
        latencies.append(time.perf_counter() - t0)
    results[idx] = latencies


def run(workers: int, rounds: int, key: str, timeout_s: int, servers: list[tuple[str, int]]) -> None:
    print(f"bench_sync: {workers} workers x {rounds} rounds (key={key!r})")
    print()

    results: list[list[float]] = [[] for _ in range(workers)]
    threads = [
        threading.Thread(target=worker, args=(key, rounds, timeout_s, servers, results, i))
        for i in range(workers)
    ]

    t_start = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    wall = time.perf_counter() - t_start

    all_latencies: list[float] = []
    for lats in results:
        all_latencies.extend(lats)

    total_ops = len(all_latencies)
    mean = statistics.mean(all_latencies)
    mn = min(all_latencies)
    mx = max(all_latencies)
    p50 = statistics.median(all_latencies)
    stdev = statistics.stdev(all_latencies) if total_ops > 1 else 0.0

    print(f"  total ops : {total_ops}")
    print(f"  wall time : {wall:.3f}s")
    print(f"  throughput: {total_ops / wall:.1f} ops/s")
    print()
    print(f"  mean      : {mean * 1000:.3f} ms")
    print(f"  min       : {mn * 1000:.3f} ms")
    print(f"  max       : {mx * 1000:.3f} ms")
    print(f"  p50       : {p50 * 1000:.3f} ms")
    print(f"  stdev     : {stdev * 1000:.3f} ms")


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync dflockd benchmark")
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--rounds", type=int, default=50)
    parser.add_argument("--key", default="bench")
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--servers", default="127.0.0.1:6388",
                        help="Comma-separated host:port pairs")
    args = parser.parse_args()
    servers = parse_servers(args.servers)
    run(args.workers, args.rounds, args.key, args.timeout, servers)


if __name__ == "__main__":
    main()
