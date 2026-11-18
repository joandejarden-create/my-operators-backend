# Persistence retry and read-back certification

**Scope:** Planner + adapter contracts — **not** a live production apply in this phase

## Production write guard

- `mutation: false`, `productionWritesAuthorized: false`
- Guard code: `AIRTABLE_PRODUCTION_WRITES_NOT_AUTHORIZED`
- `assertNoUnauthorizedProductionWrite` throws unless explicit founder authorization flags are set

## Partial failure / retry behavior (planner-certified)

| Scenario | Expected behavior | Test |
|----------|-------------------|------|
| CREATE candidate before Airtable row exists | Decision **CREATE** | Assertion 13 |
| Same ID appears after partial apply | Decision **UNCHANGED** (no duplicate CREATE) | Assertion 13 |
| Identical dry-run inputs rerun | Identical plan | Assertions 11–12, manifest `idempotencyRecheck` |

## Read-back (Bethesda pilot reference)

PR #367 completed four live writes with successful read-back for Demand Generators and Private Event Venue update. Rock Creek read-back **confirmed** the structured/payload ID split — motivating this reconciliation phase.

## What is not certified here

End-to-end production persistence for YOTEL, Radisson, or Guadalajara — dry-run only. Deterministic tests do not substitute for founder-approved live apply + read-back on a named write set.

## Next pilot read-back checklist

1. Pre-apply manifest with exact record IDs and field patches
2. Apply under allowlist only
3. Post-apply read-back per op
4. Idempotent re-plan → all **UNCHANGED** for applied IDs
5. No unintended GDO creates