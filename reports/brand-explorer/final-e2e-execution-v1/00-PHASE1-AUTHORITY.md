# Phase 1 — Authoritative Integration Path

**Date:** 2026-11-19  
**Workspace:** `C:\Dev\dealality\ws-brand-explorer`  
**Stream:** Brand Explorer (`be`)  
**Branch:** `feat/be-phase2h-component-parity-v1`

## Authoritative path

| Item | Value |
|------|--------|
| Integration path | BE feature branch → PR → CI → **Integration Controller** merge → `main` → `dealality:post-merge-sync --stream be` |
| Authoritative implementation branch | `feat/be-phase2h-component-parity-v1` |
| Superseded open PRs (do **not** merge) | #447 Phase 2E dual-brand · #451 Comparison Studio · #453 Phase 2F · #457 Phase 2G |
| Recovery status | Phase 2E→2F→2G lineage already present on this branch; Phase 2H + founder corrections uncommitted until this commit |

## Founder authorization (this directive)

Founder authorized progression to controlled Airtable persistence / publication / existing-brand rollout process.

**Does not** authorize:

- Bypassing unresolved critical quality gates
- Publishing incomplete or materially different profiles
- Force-merging historical rejected/superseded branches
- Unbounded paid research
- Cross-workstream table writes

## Publication blockers observed (pre-write)

| Gate | Status |
|------|--------|
| Gate 1 Renderer Parity founder artifact | Missing / not recorded as approved |
| Gate 2 content reconstruction | BLOCKED in dry-run harness |
| Curio scorecard | `NOT_FOUNDER_APPROVED` · priorityBlocked=0 · researchGapCount=11 |
| Ascend scorecard | `NOT_FOUNDER_APPROVED` · **priorityBlocked=1** · researchGapCount=14 |
| Activation `productionWritesAuthorized` | `false` until formal activation flip after gates |
| Dry-run Curio/Ascend | `BLOCKED_GATES` · `airtableWrites=0` |

**STOP rule:** No production Airtable apply until Gate 1/2 artifacts + scorecard FOUNDER_APPROVED + Ascend priority unblock + write plan uses stable presentation record IDs (updates, not blind creates).
