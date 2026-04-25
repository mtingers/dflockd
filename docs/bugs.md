# Historical Bug Audit

Findings from an earlier read-only audit, kept for traceability. This is not the current list of known bugs. Entries under "Fixed in current code" describe the pre-fix behavior that was audited and then corrected. Line references are historical and may no longer match the current files exactly.

Each entry has a confidence level — **high** means "reading the code makes the bug obvious", **medium** means "the bug exists but requires an unusual trigger", **speculative** means "plausible but I didn't reproduce it."

Not included: design tradeoffs, style nits, cosmetic comment errors. Those are discussed but not tracked as bugs.

---

## Fixed in current code

The confirmed bugs below have been fixed in the current codebase with dedicated commits and (where practical) regression tests. Kept here for history and to make the audit-to-fix trail visible.

## High severity

### 1. Auth token env var overrides CLI flag, contradicting documented precedence

**Confidence:** high
**Where:** `internal/config/config.go:87-113` (`loadAuthToken`) vs `README.md:53` and the rest of `Load`.

Every other setting resolves via "explicit flag > env var > default" (`resolveInt`/`resolveString`). `loadAuthToken` short-circuits with `os.Getenv("DFLOCKD_AUTH_TOKEN")` first and only falls back to the flag if the env var is empty. Either the code or the docs is wrong — pick one and align.

Resolution: `loadAuthToken` now uses CLI-first precedence and the docs state the exact source order.

### 2. `stopRenew` can hang indefinitely on an unresponsive server

**Confidence:** high
**Where:** `client/client.go:856-868` (Lock) and `client/client.go:1257-1269` (Semaphore).

`stopRenew` cancels the renewal goroutine's ctx, then `<-done` waits for it. But the goroutine can be blocked inside `Renew` → `sendRecv` → network I/O, and ctx cancellation doesn't unblock that. `Release` and `Close` both call `stopRenew` before closing the conn, so they inherit the hang.

Original fix options:
- Close the conn first, then `stopRenew` (Renew I/O errors out, goroutine exits). Requires reordering Release.
- Add a timeout to `stopRenew`'s `<-done` wait and force-close on timeout.

### 3. Accept loop busy-spins on persistent non-cancellation errors

**Confidence:** high
**Where:** `internal/server/server.go:104-114`.

On any `Accept` error, if `ctx` isn't done, the loop logs and `continue`s immediately. If the error is persistent (FD exhaustion from `ulimit`, etc.), this pegs a core and floods logs. Standard pattern is to back off:

```go
if ne, ok := err.(net.Error); ok && ne.Temporary() {
    time.Sleep(backoff)
    continue
}
```

### 4. Asymmetric `connID == 0` guard in `connRemoveOwned` vs `connAddOwned`

**Confidence:** high
**Where:** `internal/lock/lock.go:210` vs `:194`.

`connRemoveOwned` no-ops when `connID == 0`, but `connAddOwned` happily adds. Today `connSeq` starts at 1 so this is dormant. Any future code path that passes 0 (HTTP bridge, test helper, etc.) gets silent memory accumulation under the zero key with no cleanup. Either guard both sides or neither — and document the choice.

---

## Medium severity

### 5. Grant-then-cancel race returns a lock to a context-cancelled caller

**Confidence:** high (code reads unambiguously)
**Where:** `internal/lock/lock.go:383-407` (Acquire), duplicated at `:543-569` (Wait).

In the timeout branch, the race-check re-selects on `w.ch` and returns the granted token if one arrived. It does this even when `ctx.Err() != nil`. The caller treated `ctx.Done()` as "abandon" but now owns a lock they don't know they hold. The lock leaks until lease expiry. The `if ctx.Err() != nil { return "", ctx.Err() }` below the race-check is dead in the race-win branch.

Resolution: if `ctx.Err() != nil` after the race-check win, the just-granted token is released before returning the context error.

### 6. `Signal` holds `m.mu.RLock` across `CancelConn` (blocking syscall)

**Confidence:** high
**Where:** `internal/signal/signal.go:335-338`, `:366-369`, and `deliverToGroup` at `:294-298`.

When a listener's `WriteCh` is full, `Signal` calls `CancelConn()` → `conn.Close()` (syscall) under the read lock. Concurrent `Listen`/`Unlisten` (which need the write lock) stall until the close returns. Not a deadlock but a real throughput stall under slow-consumer conditions.

Resolution: doomed listeners are collected into a local slice and cancelled after releasing the manager lock.

### 7. Client-side silent signal drop

**Confidence:** high (design, but undocumented)
**Where:** `client/client.go:1394-1397`.

When `sigCh` (buffer 64) is full, `readLoop` drops signals via `select { ... default }`. User's `Signals()` channel never reflects the loss. Server-side slow-consumer eviction is documented; client-side silent drop is not.

Resolution: the drop policy is documented and `DroppedSignals() uint64` exposes a monotonic counter.

### 8. `resourceTotal` check-then-increment is not atomic; `MaxLocks` can be exceeded

**Confidence:** high
**Where:** `internal/lock/lock.go:292-311` (`getOrCreateLocked`).

```go
if lm.resourceCount() >= lm.cfg.MaxLocks {
    return nil, ErrMaxLocks
}
// ... between these two lines, another shard's goroutine can also pass the check ...
lm.resourceTotal.Add(1)
```

Under high concurrency across shards, the total overshoots `MaxLocks`. Soft cap, but advertised as a hard cap.

Fix: use `CompareAndSwap` loop on the counter to implement a true cap, or accept the overshoot and document it.

### 9. Dead code: `ack == nil` check after `handleRequest`

**Confidence:** high
**Where:** `internal/server/server.go:286-288`.

`handleRequest` has no `return nil` path — every `case` returns a non-nil `*protocol.Ack`. The `if ack == nil { break }` in the handler loop is unreachable. Either remove it, or if the intent is to support handlers that terminate the connection silently, add an explicit contract and use it.

---

## Found in re-analysis of the HTTP API code

### 10. SSE payload escaping uses Go's `%q` verb, producing invalid JSON for control bytes

**Confidence:** high
**Where:** `internal/httpapi/sse.go` (original version — now fixed).

The SSE handler built the `data:` field with `fmt.Fprintf(w, `...`, %q, %q)`. Go's `%q` verb uses Go-syntax string literals, which escape control bytes as `\xNN`, `\a`, `\v`, etc. — none of which are valid JSON escape sequences. Any signal payload containing a byte that Go escapes with `\x` yields malformed JSON in the `data:` field, which no JSON parser can consume.

**Fix:** use `json.Marshal` on each field individually and assemble the frame from the resulting byte slices. Ensures valid JSON regardless of payload content. Regression test: `TestSSE_PayloadWithControlCharProducesValidJSON`.

## Speculative / needs repro

### 11. `recover()` in `grantNextWaiterLocked` masks panics from post-send state mutation

**Confidence:** speculative
**Where:** `internal/lock/lock.go:243-255`.

The `defer recover()` is there to survive sending on a closed `w.ch`, but it also swallows panics from `st.Holders[token] = ...` and `sh.connAddOwned(...)`. If those ever panicked (nil map, OOM-style), the client would receive a token with no server-side record. Hard to trigger given the code paths that initialize `Holders`, but worth either logging the recovered panic or narrowing the `recover` scope to just the send.

### 12. `removeWaiterFromState` doesn't call `compactWaiters`

**Confidence:** speculative (minor)
**Where:** `internal/lock/lock.go:155-164`.

Timeouts that remove waiters without granting leave `WaiterHead` unchanged while the slice shrinks. The compaction only runs in `grantNextWaiterLocked`. If a key sees many timeouts and few grants, a bit of dead head space accumulates. Bounded and small, but one-liner fix (`st.compactWaiters()` at function end).

---

## Notes

- Zero-dependency policy is being honored — `go.sum` is empty. Good.
- No CI target for `go vet` / `staticcheck` / `golangci-lint` is visible. Adding one would catch #9 (dead code) and likely a few others.
- The old `connMu` lock-ordering note is obsolete. Per-connection indexes now live inside each shard and are protected by that shard's mutex.
