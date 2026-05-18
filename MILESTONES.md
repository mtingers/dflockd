# Milestones — one-shot audit of dflockd

Auditing the project against the nine-phase one-shot framework
(Discovery → Glossary → Plan → Interfaces → RED → GREEN → Security →
Docs → Final report) at **service-tier** depth, in **refactor mode**.
Calibration: dflockd is a daemon / service (single-node + N-node Raft
cluster), so all nine phases apply at full depth. Backward-compat
posture for this audit: per `/one-shot` answer, *anything goes if
better*, but the existing single-node byte-identical invariant remains
desirable.

Tick rules: `✅ pass` (already aligned), `🟡 partial` (aligned with
caveats — see audit note), `❌ gap` (was a gap; fixed inline this
session OR noted as follow-up), `N/A` (does not apply; reason
documented).

Deliverables of the audit:
- `MILESTONES.md` *(this file — running checklist)*
- `GLOSSARY.md` *(Phase 2 artifact — created this session)*
- `ONE_SHOT_AUDIT.md` *(Phase 9 artifact — per-phase findings,
  evidence, fixes, and remaining gaps)*
- Inline fixes to docs/code/tests for small gaps (committed per phase)

## Discovery & Planning
- [x] **Phase 1 — Discovery**: project scope captured as a one-paragraph summary *(✅ — see [ONE_SHOT_AUDIT.md §Phase 1](ONE_SHOT_AUDIT.md#phase-1--discovery))*
- [x] **Phase 2 — Glossary**: `GLOSSARY.md` present and matches code usage *(✅ — created [GLOSSARY.md](GLOSSARY.md); follow-up: align code comments on pinned terms)*
- [x] **Phase 3 — Plan**: `PLAN.md` matches what shipped; deltas noted *(✅ — checklist + Open Questions reconciled in `PLAN.md` §§9–10; see [ONE_SHOT_AUDIT.md §Phase 3](ONE_SHOT_AUDIT.md#phase-3--plan))*

## Build
- [x] **Phase 4 — Interfaces & schemas**: exported surfaces documented; OpenAPI + protocol stable; types/contracts crisp *(✅ — `go vet`/`go build`/`gofmt`/OpenAPI all clean; 64 missing doc comments added across `client` + `internal/{raft,cluster,server,protocol}`; see [ONE_SHOT_AUDIT.md §Phase 4](ONE_SHOT_AUDIT.md#phase-4--interfaces--schemas))*
- [x] **Phase 5 — RED tests**: coverage targets met (≥80% service-tier); test pyramid honest; characterization tests where refactor mode applies *(🟡 — 5 of 8 packages over 80% (cluster moved 78.2→84.3 this session via new FSM-adapter + membership tests); 3 still below (client 73.9, server 69.7, httpapi 69.2) with documented follow-ups, mostly failure-only branches that need fault injection; see [ONE_SHOT_AUDIT.md §Phase 5](ONE_SHOT_AUDIT.md#phase-5--red-tests))*
- [x] **Phase 6 — GREEN implementation**: `go test -race ./...` clean; complexity ceilings (cyclo ≤10, funlen ≤40) verified; any breaches documented *(✅ — every package race-clean in isolation; whole-tree flake is CPU-contention, fixed with `-p=2`; refactored `cmd/bench` `worker`/`httpWorker`/`main` to bring 0 functions over the bar; see [ONE_SHOT_AUDIT.md §Phase 6](ONE_SHOT_AUDIT.md#phase-6--green-implementation))*

## Hardening
- [x] **Phase 7 — Security pass**: trust-boundary checklist walked; gaps fixed or recorded *(✅ — `govulncheck` clean; full checklist green except a 500-response detail leak in HTTP session-create which is now fixed; see [ONE_SHOT_AUDIT.md §Phase 7](ONE_SHOT_AUDIT.md#phase-7--security-pass))*
- [x] **Phase 8 — Documentation pass**: missing docs written; README install/run verified on a fresh checkout; drift reconciled (CHANGELOG, OpenAPI, docs site, comments) *(✅ — `make build` + `make docs-build` clean; mkdocs nav updated to include the missing cluster docs; `docs/server.md` gains a Cluster section; `CHANGELOG.md` Known-Limitations list de-staled; see [ONE_SHOT_AUDIT.md §Phase 8](ONE_SHOT_AUDIT.md#phase-8--documentation-pass))*

## Handoff
- [x] **Phase 9 — `ONE_SHOT_AUDIT.md`** written; `MILESTONES.md` fully resolved; commits per phase on `raft-replication` *(✅ — final 6-section report in [ONE_SHOT_AUDIT.md §Phase 9](ONE_SHOT_AUDIT.md#phase-9--final-report); 9 commits on `raft-replication` (`cba9463`..this commit))*

---

# Production-hardening pass (2026-05-17)

Second one-shot pass on `raft-replication`, this time **build** mode: the
audit declared the cluster mode "alpha"; this pass aims to graduate it to
"GA-eligible for static-bootstrap workloads that don't depend on
FIFO-across-leader-failover," closing the audit's deferred items that are
safely shippable in a single session. Calibration: **service tier**,
refactor mode (extending an existing tree).

Deliverables this pass:
- `MILESTONES.md` (this file — second checklist below)
- `GLOSSARY.md` (extended)
- `PRODUCTION_READINESS.md` (rewritten — see Phase 9)
- New code: counter metrics, admin add/remove voter endpoints
  (HTTP + TCP), public ReadIndex/Barrier API, constant-time token
  compare, CORS middleware test, race-flake-resistant `make test-race`.
- Documented deferrals (with workarounds): dynamic-join InstallSnapshot,
  FIFO across leader failover (stable client ref), multi-node
  fault-injection soak harness.

Tick rules: `✅ shipped`, `🟡 partial`, `❌ deferred with workaround`,
`N/A`.

## Discovery & Planning
- [x] **Phase 1 — Discovery**: deficits restated; scope cut from the 8-item gap list ✅
- [x] **Phase 2 — Glossary**: new terms pinned (admin endpoint, voter, ReadIndex public API, counter metric, stable client ref) ✅
- [x] **Phase 3 — Plan**: ship-now vs deferred matrix, with reasons ✅

## Build
- [x] **Phase 4 — Interfaces & schemas**: HTTP routes, TCP commands, Go client methods, counter store API — stubs only ✅
- [x] **Phase 5 — RED tests**: failing tests for every new interface ✅
- [x] **Phase 6 — GREEN implementation**: tests green; cyclo ≤10, funlen ≤40 preserved ✅

## Hardening
- [x] **Phase 7 — Security pass**: admin auth, audit logs, follower-redirect, no info leak; constant-time compare on tokens ✅
- [x] **Phase 8 — Documentation pass**: README + operations/cluster.md + OpenAPI + CHANGELOG reconciled ✅

## Handoff
- [x] **Phase 9 — `PRODUCTION_READINESS.md` rewritten**: declares GA-eligibility for the supported workloads; remaining gaps named ✅

---

# Production-hardening pass 2 (2026-05-17)

Third one-shot pass on `raft-replication`. Pass 1 (the audit) said
"alpha". Pass 2 graduated it to beta — GA-eligible for static-bootstrap
workloads that don't depend on FIFO across leader failover. This pass
closes three more of the deferred follow-ons to widen the
GA envelope without taking on FSM-determinism-touching work in a single
session. Calibration: **service tier**, refactor mode.

Scope (ship now):
- **Cluster-aware Go client** (`client.Cluster`) — transparent leader cache + automatic redirect/retry on `*NotLeaderError`. Closes PR-1 item 4.
- **Soak harness** (`cmd/cluster-soak`) — in-process 3-node cluster + sustained writes + injected leader-kills, asserts no fence-token regression and no lost-grant violations. Closes PR-1 item 1.
- **Fuzz targets for cluster codecs** — `internal/raft` frame codec and `internal/cluster` Command codec. Closes PR-1 item 5.

Deferred (with workarounds, documented):
- **FIFO across leader failover** (PR-1 item 2 / PLAN.md §4.7) — stable client refs in the FSM. Single-session-sized on its own; touches FSM determinism; defer.
- **Dynamic-join with InstallSnapshot to empty node** (PR-1 item 3) — affects boot sequence and recovery semantics; defer; workaround is pre-seed snapshot then `AddVoter`.

Tick rules: `✅ shipped`, `🟡 partial`, `❌ deferred with workaround`,
`N/A`.

## Discovery & Planning
- [x] **Phase 1 — Discovery (non-interactive)**: scope cut from PR-1 deferred items ✅
- [x] **Phase 2 — Glossary delta**: new terms — cluster-aware client, leader cache, retry budget, soak harness ✅
- [x] **Phase 3 — Plan + ship-vs-defer matrix**: per-item reason ✅

## Build
- [x] **Phase 4 — Interfaces & schemas**: `client.Cluster` API, soak harness CLI, fuzz target signatures ✅
- [x] **Phase 5 — RED tests**: failing tests for client leader-cache / redirect / budget; fuzz seed corpus ✅
- [x] **Phase 6 — GREEN implementation**: tests green; cyclo ≤10, funlen ≤40 preserved ✅

## Hardening
- [x] **Phase 7 — Security pass**: leader-hint clamp added; redirect-to-attacker-addr now bounded; retry budget DoS-resistant ✅
- [x] **Phase 8 — Documentation pass**: client + soak docs in operations/cluster.md; CHANGELOG entries ✅

## Handoff
- [x] **Phase 9 — `PRODUCTION_READINESS.md` updated**: PR-1 items 1/4/5 closed; envelope tightened ✅
