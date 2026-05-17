# dflockd — one-shot audit

Audit of the project against the nine-phase one-shot framework
(Discovery → Glossary → Plan → Interfaces → RED → GREEN → Security →
Docs → Final report) at **service-tier** depth in **refactor mode**.
Run on branch `raft-replication`. Companion artifacts:
[`MILESTONES.md`](MILESTONES.md), [`GLOSSARY.md`](GLOSSARY.md),
[`PLAN.md`](PLAN.md), [`PRODUCTION_READINESS.md`](PRODUCTION_READINESS.md).

Each phase records: **State**, **Audit findings**, **Fixes this
session**, **Follow-ups**.

---

## Phase 1 — Discovery

**State.** Refactor mode: not a greenfield grilling. Discovery comes
from README, PLAN.md, PRODUCTION_READINESS.md, CHANGELOG, and the code.

**One-paragraph project summary.**

> **dflockd** is a single-binary Go daemon implementing a distributed
> FIFO lock + semaphore server with leases and fencing tokens.
> Users: backend / platform / SRE engineers who need cross-process
> mutual exclusion without standing up Redis or ZooKeeper. Run shape:
> a single OS process (binary or container) on Linux/macOS, optionally
> assembled into an N-node Raft-replicated cluster (`--raft-dir
> --node-id --raft-addr --cluster-peers`). Wire protocols: a
> line-based TCP protocol (canonical), an HTTP REST API (optional,
> auth-opt-in), both backed by the same in-memory `LockManager` so a
> TCP and an HTTP caller on the same key share a single FIFO queue.
> Scale: ~13k ops/s single-client, ~90k ops/s at 200 concurrent
> workers on an M1 (single-node, localhost, unique keys); cluster
> throughput is bounded by Raft commit. Data flow: opaque keys + 32-
> hex tokens; no user content stored; FSM state is in memory + WAL +
> snapshot on disk. Worst failure mode: **incorrect grants** (a
> partitioned node hands out a lock the cluster has already given
> away) — addressed by the Raft safety properties (election
> restriction, same-term commit, durable HardState, fencing tokens).
> External integrations: none mandatory — `go.sum` is empty, no
> runtime dependencies; optional fence-state file (`flock(2)`),
> optional mTLS files. Constraints: single-binary, stdlib-only,
> backward-compatible defaults (single-node binary is byte-identical
> to v2.1.x). Success criteria: cluster mode reaches GA after the six
> follow-ons listed in PRODUCTION_READINESS.md; single-node already
> shipped (v2.1.1). Out of scope: pub/sub (v1 had it, v2 dropped it),
> persistence of granted-lock state beyond Raft, multi-region
> consensus, key-value storage of arbitrary payloads.

**Scale & calibration confirm.**
- Project type: **Service** (with library aspect via `client/`).
- Source: ~26,929 lines of Go; ~9,982 of those are tests
  (~37% test-to-source by line).
- Commits since project inception: 201 (`git log --oneline | wc -l`).
- Phases apply at full depth; no phase is N/A by virtue of project
  type. Individual sub-items may be N/A and will be marked as such.

**Audit findings (Phase 1).**
- ✅ Problem space, users, run shape, scale, worst failure mode are
  all documented across README + PLAN.md + PRODUCTION_READINESS.md.
- 🟡 Discovery artifacts are scattered (README has "who/what/how-to-
  run", PLAN.md has "design + acceptance", PRODUCTION_READINESS.md
  has "what got built + what's left"). For a service-tier project
  this is fine — but no single document has the one-paragraph "what
  is this thing" view that a new joiner could read in 30 seconds.

**Fixes this session.**
- Created `MILESTONES.md` (the audit checklist).
- Created `ONE_SHOT_AUDIT.md` (this file) carrying the one-paragraph
  summary above as the canonical "what is this thing" anchor.

**Follow-ups.** None for this phase.

---

## Phase 2 — Glossary

**State.** No `GLOSSARY.md` existed before this session. Domain
vocabulary was spread across README, PLAN.md, comments in
`internal/lock/lock.go` and `internal/raft/doc.go`. Some terms drift:

- `Node` alone means either `raft.Node` or `cluster.Node` depending on
  package context.
- "Lock state" appears in pre-v2 callers; the unified type is
  `ResourceState` (lock = semaphore with `Limit=1`).
- "Fence" / "fence token" / "fencing token" all appear; the README's
  canonical spelling is *fencing token*.
- "Cluster member" / "voter" / "peer" are synonyms today and will
  diverge if non-voting learners are added later (PLAN.md §1).

**Audit findings (Phase 2).**
- ❌ Gap (fixed): `GLOSSARY.md` was missing. Created with ~30 terms
  covering core domain (lock/semaphore/resource/holder/waiter/lease/
  fencing token/salt), wire protocols (TCP, HTTP, session,
  `error_not_leader`), cluster + Raft (term, log entry, commit index,
  WAL, HardState, snapshot, FSM, FSM determinism, apply path,
  membership change, leadership transfer, ReadIndex/Barrier, mTLS,
  PreVote), and ops (`--raft-dir`, `--fence-state-file`, sweep loops,
  `/metrics`, `stats`, CRC32 sharding).
- ✅ Drifting terms pinned in the glossary's "Known to drift if not
  pinned" section — code/docs can be unified incrementally.

**Fixes this session.**
- Created `GLOSSARY.md` (above).

**Follow-ups.**
- (Low priority) Sweep code comments to align on the pinned terms —
  particularly `"fence token"` → `"fencing token"`, ambiguous
  `"node"` → `"raft.Node"` / `"cluster.Node"`.

---

## Phase 3 — Plan

**State.** `PLAN.md` is a thorough 800-line design doc covering the
whole cluster lift (sections 1–10, 16 phases). It's marked a "living
document" with §10 declared the source of truth for progress.

**Audit findings (Phase 3).**

- ❌ Drift in `PLAN.md` §10 (the *declared* source of truth for
  progress): the checklist showed Phases 5–15 all unchecked. That was
  factually wrong — CHANGELOG and PRODUCTION_READINESS.md both show
  Phases 5–9 and 14–15 fully delivered (with Phase 15 plus a
  post-review hardening pass plus mTLS / graceful leader transfer /
  cluster observability / HTTP-in-cluster follow-ons), and Phases
  10/11/12/13 partly delivered.
- ❌ `PLAN.md` §9 Open Questions left four design decisions
  "to confirm during implementation" — all four are answered by the
  code (both bootstrap shapes shipped; JSON command codec; redirect
  not forwarding; applied-state stats reads) but the doc didn't
  record the resolutions.
- ✅ The §6 phase narratives accurately describe what was built
  (spot-checked across `internal/raft`, `internal/cluster`,
  `internal/lock`, `internal/server`, `internal/httpapi`, `client/`).
- ✅ The §7 production-readiness checklist matches what
  `PRODUCTION_READINESS.md` actually validated.

**Fixes this session.**
- Updated `PLAN.md` §9: each Open Question now has a **Resolved**
  bullet recording the decision and a short pointer to the
  implementing code (or wire shape).
- Updated `PLAN.md` §10: every phase is now marked ✅ / 🟡 / ❌ with a
  one-line note. Partial phases point to the matching
  PRODUCTION_READINESS.md "Recommended next work" item so a reader
  has one canonical place to track the deferred sub-deliverables.

**Follow-ups.**
- (Tracked separately as the four PRODUCTION_READINESS.md "Recommended
  next work" items: admin endpoints, counter-style metrics,
  `cmd/cluster-soak`, cluster-aware Go client.) Not in scope for this
  audit per the user's "audit + fix gaps in alignment, not implement
  new features" framing.

---

## Phase 4 — Interfaces & schemas

**State.** Go module, stdlib-only (`go.sum` is empty). Two
machine-readable contracts: the OpenAPI spec
(`internal/httpapi/openapi.json`, mirrored to `docs/openapi.json` via
`make openapi-sync`) and the TCP wire format
(`docs/architecture/protocol.md`). Eight internal packages plus a
`client` package, all Go. PRODUCTION_READINESS.md claims "Every
exported symbol in `internal/raft` / `internal/cluster` has a doc
comment".

**Audit findings (Phase 4).**

- ✅ `go vet ./...`, `go build ./...`, `gofmt -l .` all clean.
- ✅ `internal/httpapi/openapi.json` == `docs/openapi.json` (the
  Makefile's `openapi-sync` target is up-to-date).
- ❌ Doc-comment coverage (was a real gap): a scripted check
  (`/tmp/docscheck2.sh`) found **74 exported symbols** across `client`,
  `internal/raft`, `internal/cluster`, `internal/server`,
  `internal/httpapi`, `internal/protocol` missing a directly-attached
  doc comment. The PRODUCTION_READINESS.md "every exported symbol has
  a doc comment" claim was wrong.
  - The biggest concrete gap was `client/lock.go`: all eight public
    `Lock.{Acquire,Enqueue,Wait,Release}` and
    `Semaphore.{Acquire,Enqueue,Wait,Release}` methods — i.e. **the
    Go API users actually call** — had no doc comments.
  - The `internal/raft/transport.go` section dividers (`---`-style
    block comments) explained each pair of request/response types but
    didn't attach (per `go doc` convention, a blank line between
    comment and decl breaks the link). Same pattern in
    `storage_file.go` and `storage_mem.go`'s `// --- Storage
    interface ---` blocks: the methods themselves had no per-method
    docs.
  - Five `Cluster*` exported methods on `internal/server.Server`
    (`ClusterAcquire/Enqueue/Wait/Release/Renew`) — the
    package-boundary cluster surface — were undocumented.

**Fixes this session.**
- Added doc comments to **64** previously-undocumented exported
  symbols (74 → 10):
  - `client/lock.go`: 8 public `Lock`/`Semaphore` methods.
  - `client/cluster.go`: `NotLeaderError.Error`.
  - `internal/server/cluster_ops.go`: 5 `Cluster*` methods.
  - `internal/cluster/command.go`: `Kind.String`, `DecodeSalt`.
  - `internal/cluster/fsm.go`: `fsmSnapshot.Persist`, `Release`, plus
    type comment.
  - `internal/raft/transport.go`: 8 RPC request/response type
    comments (`RequestVoteReq/Resp`, `AppendEntriesReq/Resp`,
    `InstallSnapshotReq/Resp`, `TimeoutNowReq/Resp`).
  - `internal/raft/tcptransport.go`: `TCPTransport` type comment +
    `LocalID/SetHandler/AddPeer/RemovePeer/Close` per-method
    "implements raft.Transport" docs.
  - `internal/raft/transport_mem.go`: `MemTransport`'s 5
    interface-impl methods.
  - `internal/raft/storage_file.go`: `FileStorage`'s 12 interface-impl
    methods.
  - `internal/raft/storage_mem.go`: `MemStorage`'s 12 interface-impl
    methods.
  - `internal/raft/fsm.go`: `noopFSM`'s 3 + `noopFSMSnapshot`'s 2.
  - `internal/raft/types.go`: `EntryType.String`.
  - `internal/protocol/protocol.go`: `ProtocolError.Error`.
- 7 remaining "missing" results are methods on **unexported** types
  whose only callers are inside the package
  (`role.String`, `noopWriter.Write` in `internal/raft/node.go`;
  `statusRecorder.{WriteHeader,Write,Unwrap}` in
  `internal/httpapi/metrics.go`; `sessionShutdown.{Start,Wait}` in
  `internal/httpapi/server.go`). Per Go convention these don't need
  doc comments — the receiver can't be referenced from outside the
  package, so the method names being capitalised is incidental
  (forced by the standard-library interface signatures they satisfy
  — `http.ResponseWriter`, `io.Writer`, `fmt.Stringer`).
- `go vet`, `go build`, `gofmt -l` continue to be clean.

**Follow-ups.**
- (Cosmetic) The `/tmp/docscheck2.sh` script can't distinguish
  exported-receiver from unexported-receiver — a more precise check
  would need to track type declarations. Out of scope for this audit;
  worth adding a `make doccheck` target later if doc coverage is
  worth enforcing in CI.

---

## Phase 5 — RED tests

*Pending audit.*

---

## Phase 6 — GREEN implementation

*Pending audit.*

---

## Phase 7 — Security pass

*Pending audit.*

---

## Phase 8 — Documentation pass

*Pending audit.*

---

## Phase 9 — Final report

*Pending audit.*
