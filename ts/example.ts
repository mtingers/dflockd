import { DistributedLock } from "./client.js";

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ---------------------------------------------------------------------------
// demo: acquire a single lock, hold it, release
// ---------------------------------------------------------------------------

async function demo(): Promise<void> {
  const lock = new DistributedLock({
    key: "foo",
    acquireTimeoutS: 10,
    leaseTtlS: 20,
  });

  await lock.acquire();
  console.log(`acquired key=${lock.key} token=${lock.token} lease=${lock.lease}`);

  await sleep(500); // lock auto-renews in the background
  console.log("done critical section");

  await lock.release();
}

// ---------------------------------------------------------------------------
// demo: withLock helper (acquire → run callback → release)
// ---------------------------------------------------------------------------

async function demoWithLock(): Promise<void> {
  const lock = new DistributedLock({
    key: "foo",
    acquireTimeoutS: 10,
    leaseTtlS: 20,
  });

  await lock.withLock(async () => {
    console.log(`acquired key=${lock.key} token=${lock.token} lease=${lock.lease}`);
    await sleep(500);
    console.log("done critical section");
  });
}

// ---------------------------------------------------------------------------
// demo: FIFO ordering — N workers competing for the same lock
// ---------------------------------------------------------------------------

async function demoWorker(workerId: number): Promise<void> {
  console.log(`worker[start]: ${workerId}`);
  const lock = new DistributedLock({
    key: "foo",
    acquireTimeoutS: 30,
  });

  await lock.withLock(async () => {
    console.log(`acquired  token(${workerId}): ${lock.token}`);
    await sleep(100);
    console.log(`released  token(${workerId}): ${lock.token}`);
  });
}

async function demoLockOrdering(): Promise<void> {
  const numWorkers = 9;
  console.log(`launching ${numWorkers} workers with shared lock...`);
  await Promise.all(
    Array.from({ length: numWorkers }, (_, i) => demoWorker(i)),
  );
  console.log("all workers finished");
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  const arg = process.argv[2] ?? "ordering";

  switch (arg) {
    case "single":
      await demo();
      break;
    case "withlock":
      await demoWithLock();
      break;
    case "ordering":
      await demoLockOrdering();
      break;
    default:
      console.error(`usage: node example.js [single|withlock|ordering]`);
      process.exit(1);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
