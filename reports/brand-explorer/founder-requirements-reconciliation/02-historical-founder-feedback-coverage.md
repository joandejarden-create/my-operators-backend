# Historical founder feedback coverage (2C → reconciliation)

**Purpose:** Prove recovery was not limited to the Lessons Registry alone.  
**Master register:** `01-master-founder-requirements-register.json`

## Sources reviewed

| Phase | Primary docs / evidence |
|---|---|
| 2C | `docs/brand-explorer/BE_PHASE2C_CURIO_RECONSTRUCTION_V1.md`, `reports/brand-explorer/phase2c-curio-reconstruction/` |
| 2D | `docs/brand-explorer/BE_PHASE2D_CURIO_QUALITY_RECOVERY.md` |
| 2E | `docs/brand-explorer/BE_PHASE2E_DUAL_BRAND_VALIDATION.md` |
| 2F | `BE_PHASE2F_FOUNDER_REVIEW_PACKAGE.md`, `BE_PHASE2F_VISUAL_QUALITY_RCA.md`, PR #453 founder-rejected |
| 2G | `BE_PHASE2G_FOUNDER_ACCEPTANCE_PACKAGE.md`, `BE_PHASE2G_REPEATED_CONTENT_RCA.md`, PR #457 lineage |
| 2H | `BE_PHASE2H_CONTENT_DEPTH_RCA.md`, `BE_PHASE2H_FOUNDER_REVIEW_PACKAGE.md` |
| 2H-A | `BE_PHASE2H_A_GATE1_RENDERER_PARITY_FOUNDER_REVIEW.md`, `reports/brand-explorer/phase2h-a-renderer-parity/` |
| Permanent standards | founder prompt + `configs/brand-explorer-permanent-content-standards-contract-v1.json` |
| Architecture corrections | founder prompt 2026-11-19 + architecture contract/inventory/fixtures |
| Reconciliation | this folder — dispositions for 50 checklist + 12 additional |

## Themes recovered beyond the Lessons Registry

1. Independent reconstruction without Presentation-as-source (2C) → `BE-REQ-056`
2. Atelier mount validation historically NOT_VALIDATED (2C) → Gate1/2H-A path
3. Visual score ≠ approval; Ascend imagery blocker (2F) → `BE-REQ-037–041`
4. PR #453 rejected despite green CI (2G) → `BE-REQ-054`
5. Clear-without-refill depth regression (2H RCA) → `BE-REQ-055`
6. Gate1 identical payload / shared renderer (2H-A) → `BE-REQ-045–050`, `BE-REQ-059`
7. Dual Guest/Owner buying (architecture) → `BE-REQ-016–018` (**contract only**)
8. Lifecycle 6 vs 7 conflict → `BE-CONFLICT-LIFECYCLE-6-VS-7`
9. Advisory usefulness over completion scores → `BE-REQ-051`, `BE-REQ-061`

## Exhaustive recovery merge (post-audit)

Historical recovery audit ([Recovered requirements](0c1696e8-0ab9-4ba6-b861-b42e9369d8f2)) added process/comparison/SoT instructions beyond the 50-item content checklist into the master register as `BE-REQ-063`…`BE-REQ-082` (with `recoveryId` ADD-2C/2D/2E/2F/2G/2H/2H-A/STD crosswalk).

Confirmed again: **no** production `valueOwners.lifecycle.7`; Stabilization is Opening Path stage 5 only; PR #453 remains founder-rejected.

## Explicit non-claims

- Reviewing docs ≠ product implementation.
- Registry ENCODED ≠ VERIFIED.
- Gate1 browser pack ≠ FOUNDER_APPROVED.
