# Changelog

## 0.3.0

- Add multi-server sharding with `stable_hash_shard` (CRC-32)
- Add custom sharding strategy support
- Add `renew_ratio` parameter to control renewal frequency

## 0.2.0

- Add sync client (`dflockd.sync_client`)
- Add background lease renewal for both async and sync clients
- Add `DistributedLock` context manager interface

## 0.1.0

- Initial release
- Async lock server with FIFO ordering
- Automatic lease expiry and garbage collection
- Disconnect cleanup
- Async client (`dflockd.client`)
