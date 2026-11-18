# Production activation & selective backfill proposal

**Status:** awaiting founder approval — no production writes in this PR.

## Authorize first

1. Confirm base `appa2cE7FTRmIbB32` + existing table IDs in contract match live schema.  
2. Review Bethesda dry-run write set for processed-20 (not 165 raw).  
3. Approve narrow apply path: Demand Generators + Group Demand Opportunities + PE venues for **hotel-qualified / retainGlobally** identities only.  
4. Keep Research Runs as summary links to Full E2E ledger IDs (no full ledger duplication).

## Do not authorize yet

- Backfill of all 165 Bethesda raw Exa candidates  
- Automatic CURRENT publication package updates  
- Cross-hotel feedback sync  
- Creation of new “GDI Global Demand Intelligence” tables  

## Pilot write set (proposed)

| Source | Scope | Tables |
|---|---|---|
| Bethesda processed watchlist / NEW_UNIQUE | org + opportunity | Demand Generators, Group Demand Opportunities |
| Bethesda preferred-lodging venues (non-EXCLUDE) | venue identity | Private Event Venues |
| Credible out-of-market retainGlobally orgs | org only | Demand Generators |
| Canonical Full E2E run summary | run metadata | GDI Research Runs |

## Verification

- Idempotent upsert by identity field  
- Read-back confirmation per record  
- Outcome ledger: CREATED / UPDATED / UNCHANGED / DUPLICATE_LINKED / DEFERRED / FAILED  
- Re-run same batch → no duplicate creates  
- Publication CURRENT pointers unchanged  

## Rollback / recovery

- Failed rows remain visible with `FAILED` + error  
- Safe retry uses same identity keys  
- No delete-and-recreate of customer-published inventory  
