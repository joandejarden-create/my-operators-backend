# Founder report — Full E2E Airtable persistence & Bethesda reconciliation

## Verdict

Canonical Full E2E now has a **fail-closed dry-run persistence contract** that routes research objects to the **existing** AI Demand Positioning tables (`appa2cE7FTRmIbB32`). Production Airtable writes remain blocked until you authorize a selective apply set.

Campaign-reported `airtableWrites=0` for Bethesda live Exa run `gdi_e2e_14seg_mwladic1_ff9898` does **not** mean the base is empty — prior schema audit shows substantial Bethesda coverage (e.g. 86 Group Demand Opportunities).

## Bethesda reconciliation (fixture)

| Metric | Value |
|---|---|
| Run | `gdi_e2e_14seg_mwladic1_ff9898` |
| Raw Exa candidates | 165 |
| Processed in chain | 20 |
| Campaign Airtable writes | 0 |
| Prior GDO (Bethesda) | 86 |
| Prior DG signals | 23 |
| Prior hotel generator fit | 20 |
| Prior research targets | 53 |
| Dry-run planned writes (processed 20) | 7 (Demand Generators + GDO + PE venue — see JSON) |
| Deferred audit-only among processed | 16 EXCLUDE / raw rows |
| Backfill 165 raw? | **No** |

Reference hotels (local `opportunities.json` counts) are included in the reconciliation JSON for YOTEL Geneva, Wyndham San Diego Bayside, Castillo Son Vida, and Radisson Blu Toronto.

## What was implemented

- Existing-table routing inventory + explicit non-Airtable destinations  
- Idempotent dry-run planner + read-back contract  
- Chain checkpoint (`airtablePersistenceDryRun`) with `airtableWrites=0`  
- Global-intel mapping redirected to prefer Demand Generators (deprecated competing-table proposal)  
- Deterministic gate: `npm run test:gdi-full-e2e-airtable-persistence-reconciliation-v1`

## Approvals still required

1. Production write enablement on existing adapters  
2. Narrow pilot write set (not 165 raw)  
3. Any historical selective backfill  

Until then: valuable research remains in FS / ledgers / prior Airtable rows; the new path plans and verifies destinations without mutating production.
