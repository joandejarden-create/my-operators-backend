# Phase 2A — Essential fixes and regression results

## Essential code fixes

**None applied in Phase 2A.**

Rationale: the reproduced **33** customer-surface failures classify as historical certification gaps, missing BPP measurement/peer freeze, or missing/empty evidence packs — not new software defects safe to “fix” without inventing evidence or weakening gates. Phase 1.1 already shipped the actionable code repairs (scan layer, hybrid/share/print, BPP affiliation maps, displacement harness hardening).

Hotel Explorer content / verified-attribute enrichment remains a **separate workstream** (not modified here).

## Regression / gate results (this worktree, 2026-11-18)

| Gate | Command | Exit | Result |
|------|---------|-----:|--------|
| Canonical 31 scope | `npm run test:adp-canonical-31-scope-v1` | 0 | PASS — 31 hotels, Bethesda true |
| Certification posture | `npm run test:adp-canonical-31-certification-posture-v1` | 0 | PASS structure — certifiedCount **0**, matrix absent |
| Permanence Layer A | `npm run test:adp-permanent-full-e2e-certification-v1` | 0 | PASS offline fixtures (`notProductionProof`) |
| Permanence Layer B required | `ADP_PERMANENCE_REQUIRE_LAYER_B=1` same | **2** | **BLOCKED_MISSING_AUTHORITATIVE_EVIDENCE** |
| Persistence contract | `npm run test:adp-e2e-persistence-contract-v1` | 0 | Layer A PASS / Layer B BLOCKED |
| Customer surface | `npm run test:adp-customer-surface-contract` | **1** | FAIL — **33** failures |

## Concurrent HE / main note

- PR **#364** HE work is already on `origin/main`.  
- This ADP tip is **12 commits behind** main (GDI Bethesda research persistence #372–#376).  
- No merge performed; dirty Phase 1.3A/B identity/census files preserved.

## Artifacts written this phase

Report package under `reports/adp-hotel-explorer-baseline-audit/phase-2a/` only (documentation). No production data writes.
