# Bethesda Gold-Standard Replay V1 — Founder Report

**Priority:** P0 diagnostic  
**Spend:** $0 · Agent OFF · Surfe/Findymail OFF · Airtable OFF  
**Pointers:** STAGED/CURRENT **unchanged** (checksums match freeze)  
**localhost:8080:** not modified · preview on **8091**

## Verdict

**Current canonical Full E2E + FQO does not reproduce the approved Bethesda 67.**

| | |
|--|--:|
| Gold opportunities | 67 |
| Replay STAGED candidate | 20 |
| Technical regressions | 46 |
| FQO raw pass on gold package | 0 |
| FQO pass after evidence rehydration probe | 14 |

Dominant root causes: FQO evidence-URL gate vs presentation package shape; historical compatibility gaps; over-strict HOW channel rule; PASS→package shrink (28→20); NYC-hardcoded competitive incumbent.

## Deliverables map

1. `01_VERIFIED_BETHESDA_BASELINE.md` + `BETHESDA_GOLD_STANDARD_67.json`  
2. `02_FULL_E2E_CALL_GRAPH.md`  
3. `03_EXECUTION_TRACE.md`  
4. `04_…MATRIX_SUMMARY.md` + `BETHESDA_67_REPLAY_PARITY_MATRIX.json`  
5. `05_FQO_CURRENT_VS_APPROVED_RULES.md`  
6. `06_NAMED_CONTACT_AND_HOW_RETENTION.md`  
7. `07_TYPE_AND_SALES_PILL_PARITY.md`  
8. `08_COMMERCIAL_RECOMMENDATION_QUALITY.md`  
9. `09_ISOLATED_VISUAL_COMPARISON.md` + screenshot/HTML  
10. `10_TECHNICAL_REGRESSIONS_AND_ROOT_CAUSES.md`  
11. `11_MINIMAL_REMEDIATION_PLAN.md`  
12. `12_WYNDHAM_WATERSTONE_READINESS.md`  
13. `13_PR_AND_TEST_EVIDENCE.md`  
14. `14_SIX_LEISURE_CATEGORIES_INTEGRATION_AUDIT.md` + matrix JSON — six LDI categories vs canonical E2E (0 fully executed; 5 partial; 1 defined-not-executed)

## PR

- **#596** https://github.com/joandejarden-create/my-operators-backend/pull/596  
- Tip: diagnostic branch `feat/gdi-bethesda-gold-standard-replay-v1`  
- Status: **OPEN** — Integration Controller owns merge  

## STOP

**Await founder acceptance** of Bethesda parity findings and remediation plan.  
**Do not** begin Wyndham/Waterstone full reprocessing.  
**Do not** deploy fixes or activate replacement STAGED in this wave.

`AWAITING_INTEGRATION_CONTROLLER`
