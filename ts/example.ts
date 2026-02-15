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
// demo: two-phase lock (enqueue → notify → wait → work → release)
// ---------------------------------------------------------------------------

function notifyExternalSystem(workerId: number, status: string): void {
  console.log(`  worker ${workerId}: notified external system (status=${status})`);
}

async function demoTwoPhase(): Promise<void> {
  const lock = new DistributedLock({
    key: "two-phase-key",
    acquireTimeoutS: 10,
    leaseTtlS: 20,
  });

  console.log("step 1: enqueue");
  const status = await lock.enqueue();
  console.log(`  enqueue returned: ${status}`);

  console.log("step 2: notify external system");
  notifyExternalSystem(0, status);

  console.log("step 3: wait for lock");
  const granted = await lock.wait(10);
  console.log(`  wait returned: granted=${granted}, token=${lock.token}`);

  if (granted) {
    console.log("step 4: critical section");
    await sleep(1000);
    console.log("step 5: release");
    await lock.release();
  }

  console.log("done");
}

// ---------------------------------------------------------------------------
// demo: two-phase contention — N workers with enqueue/wait
// ---------------------------------------------------------------------------

async function twoPhaseWorker(workerId: number, holdTimeMs: number): Promise<void> {
  const lock = new DistributedLock({
    key: "shared-key",
    acquireTimeoutS: 30,
    leaseTtlS: 20,
  });

  console.log(`worker ${workerId}: enqueueing`);
  const status = await lock.enqueue();
  console.log(`worker ${workerId}: enqueue returned ${status}`);

  notifyExternalSystem(workerId, status);

  console.log(`worker ${workerId}: waiting for lock`);
  const granted = await lock.wait(30);
  if (!granted) {
    console.log(`worker ${workerId}: timed out`);
    return;
  }

  console.log(`worker ${workerId}: acquired token=${lock.token}`);
  await sleep(holdTimeMs);
  console.log(`worker ${workerId}: releasing`);
  await lock.release();
}

async function demoTwoPhaseContention(): Promise<void> {
  const numWorkers = 4;
  console.log(`launching ${numWorkers} two-phase workers with shared lock...\n`);
  await Promise.all(
    Array.from({ length: numWorkers }, (_, i) => twoPhaseWorker(i, 500)),
  );
  console.log("\nall workers finished");
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
    case "twophase":
      await demoTwoPhase();
      break;
    case "twophase-contention":
      await demoTwoPhaseContention();
      break;
    default:
      console.error(`usage: node example.js [single|withlock|ordering|twophase|twophase-contention]`);
      process.exit(1);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
