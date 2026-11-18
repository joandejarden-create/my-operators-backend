# Phase 1.1 — E2E persistence gate results

**Command:** `npm run test:adp-e2e-persistence-contract-v1`  
**Artifact:** `reports/dealality-adp-airtable-persistence-historical-traceability-v1/PERSISTENCE_TEST_RESULTS.json`

## Classification of prior failures

| Failure | Root cause | Resolution |
|---------|------------|------------|
| `PERSIST_WEEK0_COMPLETE` | Stale field `persistenceLifecycle` vs impl `persistenceState` | Alias + sync in `persistAdpHistoricalPeriodV1` |
| `PERSISTENCE_DISTINCT_FROM_PUBLICATION` | Missing `publicationLifecycle` | Return `NOT_PUBLISHED` always (persist ≠ publish) |
| `DEMOTION_PLANNED` / `SINGLE_CURRENT_AFTER_WEEK1` | Demotion plan not wired into mock persist | Wire `buildCurrentVersionDemotionPlan` + `applyDemotionPlanToMockStore` |
| Measured-dates ENOENT crash | Missing authoritative evidence file | Fail-closed as **BLOCKED** Layer B (no fabrication) |

## Results (this environment)

| Layer | Status |
|-------|--------|
| **A (CI / synthetic mock)** | **PASS** (23 checks) |
| **B (durable measured-dates)** | **BLOCKED_MISSING_AUTHORITATIVE_EVIDENCE** (5 checks) |

Ordinary PR CI exits on Layer A only. Layer B remains documented and separate.

## Safeguards preserved

- No Airtable production writes
- No fabricated `PRODUCTION_DURABLE_PUBLISHED_VS_MEASURED.json`
- Publication policy untouched (`publishPerformed: false`)
- Historical week0 rows preserved after week1 demotion
