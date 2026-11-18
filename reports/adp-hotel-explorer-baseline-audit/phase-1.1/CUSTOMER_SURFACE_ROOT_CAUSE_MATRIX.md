# Phase 1.1 — Customer-surface failure root-cause matrix

**Before:** 36 failures  
**After Phase 1.1 fixes:** 33 failures  
**Machine:** `CUSTOMER_SURFACE_ROOT_CAUSE_MATRIX.json` + `adp-customer-surface-contract-v1-latest.json`

## Fixed in Phase 1.1 (implementation defects)

| Gate | Fix |
|------|-----|
| `EXECUTIVE_SCAN_LAYER_PRESENT` | Attach scan layer when published packages lack local runtime periods |
| `ADP_EXECUTIVE_LEFT_CONSTRAINT_PRIMARY_ISSUE_PARITY` | Align constraint family/headline to V3 `primaryIssueId` (JW + Westin Monterrey) |
| `HYBRID_LAYOUT` / `SHARE` / `PRINT` | Cascaded pass once scan + left-constraint pass |
| BPP `AFFILIATION_MISSING` (8 hotels) | Static portfolio mappings added (fail-closed; does not invent READY) |
| Displacement harness crash | Null-period guard; published DSS-first when runtime missing |

## Remaining (classified — fail-closed preserved)

| Gate | Count | Root cause | Phase 1.1 action |
|------|------:|------------|------------------|
| `TERRITORY_SUBJECT_RATES` | 5 | Historical compatibility vs locked Bethesda expectations | **Deferred** — no constant flip / no grandfather |
| `BPP` | 24 | `MEASUREMENT_NOT_ATTEMPTED` / `PEER_SET_NOT_FROZEN` / smoke `UNKNOWN` | Mappings done; measurement deferred |
| Evidence link trio | 3 | Empty evidence packs / missing runtime (427 defects) | **Deferred** — no invented evidence |
| `DISPLACEMENT_COUNT_EVIDENCE_EXACT_PARITY` | 1 | Fixture drift (`adp_smoke_test_synthetic`) + missing runtimes | Harness hardened; residual remain |

## Doctrine held

- Strict fail-closed retained
- No assertion bypass
- No new commercial periods grandfathered into legacy
- No production certification mutation
