"""Unit tests for the sharding module."""

from dflockd.sharding import DEFAULT_SERVERS, stable_hash_shard


class TestStableHashShard:
    def test_deterministic(self):
        """Same key always maps to the same index."""
        idx1 = stable_hash_shard("mykey", 5)
        idx2 = stable_hash_shard("mykey", 5)
        assert idx1 == idx2

    def test_in_range(self):
        """Result is always in [0, num_servers)."""
        for key in ("a", "b", "foo", "bar/baz", "x" * 1000):
            for n in (1, 2, 3, 7, 16, 100):
                idx = stable_hash_shard(key, n)
                assert 0 <= idx < n, f"key={key!r} n={n} idx={idx}"

    def test_single_server(self):
        """With one server, every key maps to index 0."""
        for key in ("a", "b", "c", "hello", "world"):
            assert stable_hash_shard(key, 1) == 0

    def test_reasonable_distribution(self):
        """Keys should spread across buckets, not all land in one."""
        num_servers = 4
        counts = [0] * num_servers
        for i in range(1000):
            idx = stable_hash_shard(f"key-{i}", num_servers)
            counts[idx] += 1
        # Each bucket should get at least 10% of keys (expect ~25%)
        for c in counts:
            assert c > 100, f"poor distribution: {counts}"


class TestDefaults:
    def test_default_servers(self):
        assert DEFAULT_SERVERS == [("127.0.0.1", 6388)]
