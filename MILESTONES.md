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
- [ ] **Phase 2 — Glossary**: `GLOSSARY.md` present and matches code usage
- [ ] **Phase 3 — Plan**: `PLAN.md` matches what shipped; deltas noted

## Build
- [ ] **Phase 4 — Interfaces & schemas**: exported surfaces documented; OpenAPI + protocol stable; types/contracts crisp
- [ ] **Phase 5 — RED tests**: coverage targets met (≥80% service-tier); test pyramid honest; characterization tests where refactor mode applies
- [ ] **Phase 6 — GREEN implementation**: `go test -race ./...` clean; complexity ceilings (cyclo ≤12, funlen ≤40) verified; any breaches documented

## Hardening
- [ ] **Phase 7 — Security pass**: trust-boundary checklist walked; gaps fixed or recorded
- [ ] **Phase 8 — Documentation pass**: missing docs written; README install/run verified on a fresh checkout; drift reconciled (CHANGELOG, OpenAPI, docs site, comments)

## Handoff
- [ ] **Phase 9 — `ONE_SHOT_AUDIT.md`** written; `MILESTONES.md` fully resolved; commits per phase on `raft-replication`
