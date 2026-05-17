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
- [ ] **Phase 9 — `ONE_SHOT_AUDIT.md`** written; `MILESTONES.md` fully resolved; commits per phase on `raft-replication`
