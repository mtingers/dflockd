# Complexity reduction plan

Goal: make production code small enough that behavior is easy to test locally. Target every production function at 5 non-blank lines or less, and drive cyclomatic complexity toward 1-3 per function. Tests can be longer when they read better as scenarios, but test helpers should still be small.

## Progress

- [x] Phase 0: measurement guardrails.
- [x] Phase 1: config split.
- [x] Phase 2: HTTP handler lifecycle.
- [x] Phase 3: client lock/semaphore state machines.
- [x] Phase 4: LockManager transitions.
- [x] Phase 5: server lifecycle loops.
- [x] Phase 6: protocol helpers.
- [x] Phase 7: clean test complexity.

Latest verification after Phase 7:

- `go test ./...` passes.
- `go test -race ./...` passes.
- `go vet ./...` passes.
- `go run ./tools/complexity -prod -summary`: production has 647 functions, 220 over 5 lines, 94 over 10 lines, 26 over 20 lines, and 11 with complexity >= 10.
- `go run ./tools/complexity -tests -top 30`: tests have 229 functions, 148 over 5 lines, 63 over 10 lines, 15 over 20 lines, and 0 with complexity >= 10.
- Server package status: all named server functions report 5 non-blank lines or less except the two select primitives `waitForTimer` and `channelReady`, both at 6 lines and C3.
- Protocol package status: all named production protocol functions report 5 non-blank lines or less.
- Test cleanup status: HTTP scenario helpers, TCP test server helpers, config env helpers, and lock test manager helpers are now below 10 lines. Longer remaining tests are scenario bodies that still read directly as workflows.

## Baseline Inventory

Initial rough active-code scan, using non-blank function lines and a simple cyclomatic count for `if`, `for`, `case`, `default`, `&&`, and `||`:

- Production: 261 functions; 202 over 5 lines; 144 over 10 lines; 74 over 20 lines; 28 with complexity >= 10.
- Tests: 155 functions; 149 over 5 lines; 95 over 10 lines; 31 over 20 lines; 2 with complexity >= 10.
- Highest production line counts:
  - `internal/config/config.go:62 Load`: 167 lines, C11.
  - `cmd/bench/main.go:38 main`: 99 lines, C11.
  - `internal/httpapi/server.go:41 Run`: 81 lines, C12.
  - `internal/server/server.go:88 serve`: 74 lines, C13.
  - `internal/config/config.go:239 Validate`: 72 lines, C29.
  - `cmd/bench/main.go:196 worker`: 65 lines, C15.
  - `internal/server/conn.go:21 ServeConn`: 63 lines, C14.
  - `client/lock.go:693 Semaphore.Enqueue`: 62 lines, C15.
  - `client/lock.go:627 Semaphore.Acquire`: 62 lines, C13.
  - `client/lock.go:381 Lock.Acquire`: 61 lines, C12.
- Highest production complexity:
  - `internal/config/config.go:239 Validate`: C29.
  - `cmd/bench/main.go:196 worker`: C15.
  - `client/lock.go:693 Semaphore.Enqueue`: C15.
  - `internal/lock/lock.go:507 CleanupConnection`: C15.
  - `internal/server/conn.go:21 ServeConn`: C14.
  - `client/lock.go:447 Lock.Enqueue`: C14.
  - `client/client.go:442 doEnqueue`: C14.
  - `internal/server/server.go:88 serve`: C13.
  - `internal/protocol/protocol.go:318 FormatResponse`: C13.

## Findings

1. `internal/config` is the largest single complexity hotspot.
   `Load` mixes flag definition, env resolution, auth-token loading, config construction, and validation. `Validate` is a long flat list of independent checks, but it still produces the highest complexity score in the repo. These functions are easy to split without changing public behavior.

2. `client/lock.go` has duplicated state machines for `Lock` and `Semaphore`.
   `Acquire`, `Enqueue`, and `Wait` repeat connect, context watching, abandoned-grant cleanup, state installation, and renew-loop startup. The duplication hides the real protocol differences and makes each path hard to test in isolation.

3. HTTP handlers repeat the same request lifecycle.
   Most handlers manually do `sessionOrGone`, decode, validate, `BeginRequest`, LockManager call, and render. This creates many 20-40 line functions even when the endpoint behavior is simple. The shared lifecycle should be expressed once, with per-endpoint functions only doing parse/validate and operation dispatch.

4. `internal/lock` still has dense state transitions.
   `Wait`, `CleanupConnection`, `Renew`, `Acquire`, and `Enqueue` combine shard locking, state mutation, lease checks, bookkeeping, and grant propagation. The code is much smaller after the refactor, but the hard-to-test logic is still embedded inside long functions.

5. Server loops are operationally correct but structurally broad.
   `server.serve`, `ServeConn`, `watchPeerClose`, and HTTP `Run` combine lifecycle, listener setup, backoff, connection accounting, shutdown, and request loops. These are prime candidates for small pure helpers plus thin orchestration functions.

6. Protocol parsing is close to extractable tables.
   `parseAcquire`, `parseEnqueue`, `FormatResponse`, and `readLine` are relatively bounded, but each still carries several decisions. The command grammar can be made more table-driven so each parser handles one shape.

7. Tests are long mostly because scenarios are inlined.
   The production target should come first. After production is split, tests should be moved toward scenario helpers and table rows. The highest-priority test cleanup is `internal/httpapi/httpapi_test.go:696 TestHTTP_BadJsonDoesNotKeepSessionAlive` at 190 lines, C24.

## Constraints

- Do not perform behavior-changing refactors without characterization tests around the target function.
- Preserve wire formats, exported API names, error sentinels, and HTTP status/error codes.
- Prefer pure helpers over new shared mutable abstractions.
- Avoid moving logic across package boundaries unless it reduces actual coupling.
- Keep `old/` untouched.

## Plan

### Phase 0: Add measurement guardrails

1. Add a small AST-based complexity tool or test helper that reports function line count and cyclomatic complexity for active Go files.
2. Add a make target such as `make complexity` that excludes `old/` and can fail on configurable thresholds.
3. Initial thresholds should be non-blocking report mode. After each phase, lower limits until production code reaches `max-lines=5` and `max-cyclo=3`, with explicit temporary exemptions checked into the report.

### Phase 1: Extract obvious straight-line config helpers

1. Split `Load` into `defineFlags`, `resolveConfig`, `resolveDurations`, `resolveHTTPConfig`, and `applyDerivedDefaults`.
2. Convert `Validate` into a list of named validators, each returning one error or nil.
3. Add focused tests for each validator group before replacing the current monolith.
4. Target: no function in `internal/config` over 10 lines after this phase; then do a second pass to reach 5.

### Phase 2: Shrink HTTP handler lifecycle

1. Introduce a small handler runner that performs `sessionOrGone`, decode, validation, `BeginRequest`, and common `session_gone` rendering.
2. Keep endpoint-specific parse/validate functions pure and <=5 lines.
3. Keep LockManager calls in tiny operation functions, one per endpoint.
4. Split response rendering into `renderToken`, `renderTimeout`, `renderLockErr`, and `cleanupCanceledEnqueue`.
5. Target: every `handle*` and `do*` in `internal/httpapi/handlers.go` <=5 lines.

### Phase 3: Normalize client lock/semaphore state machines

1. Define a small internal operation descriptor for lock vs semaphore protocol functions.
2. Extract connect/auth, context-watch, timeout mapping, abandoned-grant cleanup, state installation, and renew-loop startup into independent helpers.
3. Replace duplicated `Lock` and `Semaphore` methods with thin wrappers over shared flow helpers.
4. Add tests for the shared flow helpers using fake protocol functions before deleting duplicated branches.
5. Target: public `Lock`/`Semaphore` methods <=5 lines; helpers <=5 lines with cyclo <=3.

### Phase 4: Split LockManager transitions

1. Split `Wait` into `loadEnqueuedState`, `consumePreGrantedToken`, `waitQueuedGrant`, and `finishWait`.
2. Split `CleanupConnection` into `cleanupEnqueuedForConn`, `cleanupPendingWaitersForConn`, and `releaseOwnedForConn`.
3. Split `Renew` into `lookupHolder`, `rejectExpiredRenew`, and `extendLease`.
4. Add table tests for each extracted state transition with direct shard/resource fixtures.
5. Target: state transition helpers pure or single-lock scoped, each <=5 lines where possible.

### Phase 5: Decompose server lifecycle loops

1. Split `server.serve` into `startBackgroundLoops`, `acceptOne`, `rejectIfOverLimit`, `startConn`, and `handleAcceptError`.
2. Split `ServeConn` into auth, read, optional peer-close watcher, dispatch, and write steps.
3. Split `watchPeerClose` into poll setup, peek decision, timeout classification, and stop handling.
4. Preserve existing integration tests and add focused tests for accept-error/backoff and peer-close decision helpers.

### Phase 6: Table-drive protocol helpers

1. Convert response formatting to a status table plus tiny grant/plain helpers.
2. Split command parsing into one pure function per arg shape: timeout-only, token-only, token-plus-lease, timeout-limit-lease, limit-lease.
3. Add table tests for every command shape and malformed-argument case.
4. Target: parsing helpers <=5 lines; command dispatch <=5 lines by table lookup.

### Phase 7: Clean test complexity

1. Extract HTTP scenario builders for session creation, acquire, enqueue, wait, release, and idle-sweep polling.
2. Split long scenario tests into arrange/act/assert helpers where that reduces branching.
3. Keep test names descriptive and avoid hiding protocol-important steps behind overly generic helpers.
4. Target: test helpers <=10 lines first, then bring scenario tests down opportunistically.

## Suggested order of work

1. Measurement guardrails.
2. `internal/config`, because it is low-risk and highest complexity.
3. HTTP handlers, because recent lifecycle fixes need preservation and better shape.
4. Client state-machine duplication.
5. LockManager transitions.
6. Server loops and protocol helpers.
7. Tests.

## Definition of done

- `go test ./...`, `go test -race ./...`, and `go vet ./...` pass after each phase.
- Active production functions report <=5 non-blank lines, or have a documented temporary exemption with a removal phase.
- Active production functions report cyclomatic complexity <=3, except dispatch tables with documented justification.
- No behavior, wire format, exported API, status code, or error sentinel changes without an explicit compatibility note.
