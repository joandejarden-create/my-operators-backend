# HIF Track C — US Universe Expansion Report

**As of:** 2026-11-21T17:01:18Z  
**Publication:** CUSTOMER_BLOCKED  
**Merge owner:** Integration Controller  
**Branch:** `feat/hif-v6-track-c-us-universe-expansion`

## 1. Verified HPC country distribution (live)

| Country | Count |
|---------|------:|
| Brazil | 6,179 |
| Mexico | 3,772 |
| United States | **30** (was 15) |
| United Kingdom | 1 |
| Canada | 1 |
| **HPC total scanned (C1)** | **19,462** |
| CALA total | 19,268 |

Live recount after HPC creates confirmed MX/BR/UK/CA unchanged.

## 2–4. US hotels

- **Before research (HPC):** 15
- **After research (HPC, live):** 30
- **Distinct new US hotels identified (discovery pool):** 83
- **New HPC records persisted:** 15 (authorized CREATE_READY pathway)
- **Discovery Inventory creates:** 89
- **Track A handoff findings created:** 87

## 5. Existing enriched

- Discovery↔HPC links after create: 15

## 6. SEC→HPC match

- Live CREATE_READY collision recheck: 18 hotels — **0 already in HPC** (all verified absent)
- CREATE_READY applied: 15
- Held as UNMATCHED_VERIFIED (missing official brand URL in row): 3 Comfort properties — inventory only
- Deferred CMBS/Aimbridge rows: research-required / create-candidate (next resumable wave)

## 7. Brand / operator coverage

- Brands spanned: Marriott family (Fairfield, Residence Inn, SpringHill, Courtyard), IHG (Candlewood), Choice (Comfort — inventory), Hyatt/Hilton/voco/Delta via Aimbridge disclosures
- Operators only when source-disclosed (Aimbridge). Brand ≠ operator.

## 8. UK / Canada

- UK discovery seeds persisted: 4 (Aimbridge EMEA public)
- Canada identity strengthen: 2 (Yorkville Royal Sonesta; Sonesta ES Thornhill)
- HPC UK=1 · CA=1 (unchanged this wave)

## 9. CALA preservation

- Mexico 3,772 / Brazil 6,179 unchanged
- CALA total preserved: 19,268
- No CALA overwrites or deletes

## 10. Coverage gaps

- US HPC still thin vs branded/managed US supply — continue resumable waves
- UK/Canada HPC nearly empty
- Sparse non-CALA countries remain (<20 records)

## 11. Persistence ledger

- `reports/sec-edgar-hif-track-c-universe-expansion/PERSISTENCE_LEDGER_APPLY.json`
- Mode: APPLY_WITH_HPC_CREATE
- Read-back included for inventory creates; HPC creates via `HPC_AUTHORIZED_CREATE_REUSE_V1`

## 12. Cost / tests / branch

- Research cost USD: **0**
- Unit test: `npm run test:hif-track-c-universe-expansion`
- Orchestrator: `npm run hif-track-c-wave-orchestrator -- --apply --apply-hpc-create`
