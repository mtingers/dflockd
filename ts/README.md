# dflockd TypeScript client

TypeScript client for the dflockd distributed lock daemon.

## Setup

```bash
cd ts/
npm install
npm run build
```

## Usage

Start the server first (from the project root):

```bash
uv run dflockd
```

### `withLock` (recommended)

The simplest way to use a lock. Acquires, runs your callback, and releases
automatically — even if the callback throws.

```ts
import { DistributedLock } from "./client.js";

const lock = new DistributedLock({ key: "my-resource" });

await lock.withLock(async () => {
  // critical section — lock is held here
});
// lock is released
```

### Manual `acquire` / `release`

```ts
import { DistributedLock } from "./client.js";

const lock = new DistributedLock({
  key: "my-resource",
  acquireTimeoutS: 10, // wait up to 10 s (default)
  leaseTtlS: 20,       // server-side lease duration
});

const ok = await lock.acquire(); // true on success, false on timeout
if (!ok) {
  console.error("could not acquire lock");
  process.exit(1);
}

try {
  // critical section — lock is held and auto-renewed
} finally {
  await lock.release();
}
```

### Options

| Option           | Type     | Default       | Description                                      |
|------------------|----------|---------------|--------------------------------------------------|
| `key`            | `string` | *(required)*  | Lock name                                        |
| `acquireTimeoutS`| `number` | `10`          | Seconds to wait for the lock before giving up    |
| `leaseTtlS`      | `number` | server default| Server-side lease duration in seconds             |
| `host`           | `string` | `127.0.0.1`   | Server host                                      |
| `port`           | `number` | `6388`        | Server port                                      |
| `renewRatio`     | `number` | `0.5`         | Renew at `lease * ratio` seconds (e.g. 50% of TTL)|

### Error handling

```ts
import { DistributedLock, AcquireTimeoutError, LockError } from "./client.js";

try {
  await lock.withLock(async () => { /* ... */ });
} catch (err) {
  if (err instanceof AcquireTimeoutError) {
    // lock could not be acquired within acquireTimeoutS
  } else if (err instanceof LockError) {
    // protocol-level error (bad token, server disconnect, etc.)
  }
}
```

### Low-level functions

For cases where you manage the socket yourself:

```ts
import * as net from "net";
import { acquire, renew, release } from "./client.js";

const sock = net.createConnection({ host: "127.0.0.1", port: 6388 });

const { token, lease } = await acquire(sock, "my-key", 10);
const remaining = await renew(sock, "my-key", token, 60);
await release(sock, "my-key", token);

sock.destroy();
```

## Running the example

```bash
# build first
npm run build

# run one of the three demos:
node dist/example.js single    # acquire one lock, hold 5 s, release
node dist/example.js withlock  # same thing using withLock()
node dist/example.js ordering  # 9 concurrent workers, FIFO ordering
```
