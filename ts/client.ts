import * as net from "net";

const DEFAULT_HOST = "127.0.0.1";
const DEFAULT_PORT = 6388;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

export class AcquireTimeoutError extends Error {
  constructor(key: string) {
    super(`timeout acquiring '${key}'`);
    this.name = "AcquireTimeoutError";
  }
}

export class LockError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "LockError";
  }
}

// ---------------------------------------------------------------------------
// Low-level helpers
// ---------------------------------------------------------------------------

function encodeLines(...lines: string[]): Buffer {
  return Buffer.from(lines.map((l) => l + "\n").join(""), "utf-8");
}

/**
 * Read one newline-terminated line from the socket.
 * Resolves with the line (without trailing \r\n).
 * Rejects if the connection closes before a full line arrives.
 */
function readline(sock: net.Socket): Promise<string> {
  return new Promise((resolve, reject) => {
    let buf = "";

    const onData = (chunk: Buffer) => {
      buf += chunk.toString("utf-8");
      const idx = buf.indexOf("\n");
      if (idx !== -1) {
        cleanup();
        resolve(buf.slice(0, idx).replace(/\r$/, ""));
      }
    };

    const onError = (err: Error) => {
      cleanup();
      reject(err);
    };

    const onClose = () => {
      cleanup();
      reject(new LockError("server closed connection"));
    };

    const cleanup = () => {
      sock.removeListener("data", onData);
      sock.removeListener("error", onError);
      sock.removeListener("close", onClose);
    };

    sock.on("data", onData);
    sock.on("error", onError);
    sock.on("close", onClose);
  });
}

function connect(host: string, port: number): Promise<net.Socket> {
  return new Promise((resolve, reject) => {
    const sock = net.createConnection({ host, port }, () => resolve(sock));
    sock.on("error", reject);
  });
}

// ---------------------------------------------------------------------------
// Protocol functions
// ---------------------------------------------------------------------------

export async function acquire(
  sock: net.Socket,
  key: string,
  acquireTimeoutS: number,
  leaseTtlS?: number,
): Promise<{ token: string; lease: number }> {
  const arg =
    leaseTtlS == null
      ? String(acquireTimeoutS)
      : `${acquireTimeoutS} ${leaseTtlS}`;

  sock.write(encodeLines("l", key, arg));

  const resp = await readline(sock);
  if (resp === "timeout") {
    throw new AcquireTimeoutError(key);
  }
  if (!resp.startsWith("ok ")) {
    throw new LockError(`acquire failed: '${resp}'`);
  }

  const parts = resp.split(" ");
  if (parts.length < 2) {
    throw new LockError(`bad ok response: '${resp}'`);
  }
  const token = parts[1];
  const lease = parts.length >= 3 ? parseInt(parts[2], 10) : 30;
  return { token, lease };
}

export async function renew(
  sock: net.Socket,
  key: string,
  token: string,
  leaseTtlS?: number,
): Promise<number> {
  const arg = leaseTtlS == null ? token : `${token} ${leaseTtlS}`;
  sock.write(encodeLines("n", key, arg));

  const resp = await readline(sock);
  if (!resp.startsWith("ok")) {
    throw new LockError(`renew failed: '${resp}'`);
  }

  const parts = resp.split(" ");
  if (parts.length >= 2 && /^\d+$/.test(parts[1])) {
    return parseInt(parts[1], 10);
  }
  return -1;
}

export async function enqueue(
  sock: net.Socket,
  key: string,
  leaseTtlS?: number,
): Promise<{ status: "acquired" | "queued"; token: string | null; lease: number | null }> {
  const arg = leaseTtlS == null ? "" : String(leaseTtlS);
  sock.write(encodeLines("e", key, arg));

  const resp = await readline(sock);
  if (resp.startsWith("acquired ")) {
    const parts = resp.split(" ");
    const token = parts[1];
    const lease = parts.length >= 3 ? parseInt(parts[2], 10) : 33;
    return { status: "acquired", token, lease };
  }
  if (resp === "queued") {
    return { status: "queued", token: null, lease: null };
  }
  throw new LockError(`enqueue failed: '${resp}'`);
}

export async function waitForLock(
  sock: net.Socket,
  key: string,
  waitTimeoutS: number,
): Promise<{ token: string; lease: number }> {
  sock.write(encodeLines("w", key, String(waitTimeoutS)));

  const resp = await readline(sock);
  if (resp === "timeout") {
    throw new AcquireTimeoutError(key);
  }
  if (!resp.startsWith("ok ")) {
    throw new LockError(`wait failed: '${resp}'`);
  }

  const parts = resp.split(" ");
  const token = parts[1];
  const lease = parts.length >= 3 ? parseInt(parts[2], 10) : 33;
  return { token, lease };
}

export async function release(
  sock: net.Socket,
  key: string,
  token: string,
): Promise<void> {
  sock.write(encodeLines("r", key, token));

  const resp = await readline(sock);
  if (resp !== "ok") {
    throw new LockError(`release failed: '${resp}'`);
  }
}

// ---------------------------------------------------------------------------
// DistributedLock
// ---------------------------------------------------------------------------

export interface DistributedLockOptions {
  key: string;
  acquireTimeoutS?: number;
  leaseTtlS?: number;
  host?: string;
  port?: number;
  renewRatio?: number;
}

export class DistributedLock {
  readonly key: string;
  readonly acquireTimeoutS: number;
  readonly leaseTtlS: number | undefined;
  readonly host: string;
  readonly port: number;
  readonly renewRatio: number;

  token: string | null = null;
  lease: number = 0;

  private sock: net.Socket | null = null;
  private renewTimer: ReturnType<typeof setTimeout> | null = null;
  private closed = false;

  constructor(opts: DistributedLockOptions) {
    this.key = opts.key;
    this.acquireTimeoutS = opts.acquireTimeoutS ?? 10;
    this.leaseTtlS = opts.leaseTtlS;
    this.host = opts.host ?? DEFAULT_HOST;
    this.port = opts.port ?? DEFAULT_PORT;
    this.renewRatio = opts.renewRatio ?? 0.5;
  }

  /** Acquire the lock. Returns `true` on success, `false` on timeout. */
  async acquire(): Promise<boolean> {
    this.closed = false;
    this.sock = await connect(this.host, this.port);
    try {
      const result = await acquire(
        this.sock,
        this.key,
        this.acquireTimeoutS,
        this.leaseTtlS,
      );
      this.token = result.token;
      this.lease = result.lease;
    } catch (err) {
      await this.close();
      if (err instanceof AcquireTimeoutError) return false;
      throw err;
    }
    this.startRenew();
    return true;
  }

  /** Release the lock and close the connection. */
  async release(): Promise<void> {
    try {
      this.stopRenew();
      if (this.sock && this.token) {
        await release(this.sock, this.key, this.token);
      }
    } finally {
      await this.close();
    }
  }

  /**
   * Two-phase step 1: connect and join the FIFO queue.
   * Returns `"acquired"` (fast-path, lock is already held) or `"queued"`.
   * If acquired immediately, the renew loop starts automatically.
   */
  async enqueue(): Promise<"acquired" | "queued"> {
    this.closed = false;
    this.sock = await connect(this.host, this.port);
    try {
      const result = await enqueue(this.sock, this.key, this.leaseTtlS);
      if (result.status === "acquired") {
        this.token = result.token;
        this.lease = result.lease ?? 0;
        this.startRenew();
      }
      return result.status;
    } catch (err) {
      await this.close();
      throw err;
    }
  }

  /**
   * Two-phase step 2: block until the lock is granted.
   * Returns `true` if granted, `false` on timeout.
   * If already acquired during `enqueue()`, returns `true` immediately.
   */
  async wait(timeoutS?: number): Promise<boolean> {
    if (this.token !== null) {
      // Already acquired during enqueue (fast path)
      return true;
    }
    if (!this.sock) {
      throw new LockError("not connected; call enqueue() first");
    }
    const timeout = timeoutS ?? this.acquireTimeoutS;
    try {
      const result = await waitForLock(this.sock, this.key, timeout);
      this.token = result.token;
      this.lease = result.lease;
    } catch (err) {
      await this.close();
      if (err instanceof AcquireTimeoutError) return false;
      throw err;
    }
    this.startRenew();
    return true;
  }

  /**
   * Run `fn` while holding the lock, then release automatically.
   *
   * ```ts
   * const lock = new DistributedLock({ key: "my-resource" });
   * await lock.withLock(async () => {
   *   // critical section
   * });
   * ```
   */
  async withLock<T>(fn: () => T | Promise<T>): Promise<T> {
    const ok = await this.acquire();
    if (!ok) {
      throw new AcquireTimeoutError(this.key);
    }
    try {
      return await fn();
    } finally {
      await this.release();
    }
  }

  /** Close the underlying socket (idempotent). */
  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.stopRenew();
    if (this.sock) {
      this.sock.destroy();
      this.sock = null;
    }
    this.token = null;
  }

  // -- internals --

  private startRenew(): void {
    const interval = Math.max(1, this.lease * this.renewRatio) * 1000;
    const loop = async () => {
      if (!this.sock || !this.token) return;
      try {
        await renew(this.sock, this.key, this.token, this.leaseTtlS);
      } catch {
        console.error(
          `lock lost (renew failed): key=${this.key} token=${this.token}`,
        );
        this.token = null;
        return;
      }
      this.renewTimer = setTimeout(loop, interval);
    };
    this.renewTimer = setTimeout(loop, interval);
  }

  private stopRenew(): void {
    if (this.renewTimer != null) {
      clearTimeout(this.renewTimer);
      this.renewTimer = null;
    }
  }
}
