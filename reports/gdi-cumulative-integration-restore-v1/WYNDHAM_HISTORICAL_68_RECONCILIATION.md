# Wyndham historical “68 opportunities” reconciliation

**Hotel:** Wyndham San Diego Bayside `recnPB3Q02NO8Ma1o`  
**Date:** 2026-11-27  
**NO_PAID_RESEARCH · NO_AIRTABLE_WRITES · NO_STAGED_ACTIVATE · NO_CURRENT_PUBLISH**

## Verdict on “68”

**No historical Wyndham San Diego Bayside publication package with opportunity count = 68 can be substantiated.**

The durable **68** inventory in this product stream belongs to **Bethesda Marriott** (`recLuxvwwxID7U2B8`):

| Hotel | STAGED | CURRENT | Source |
|-------|-------:|--------:|--------|
| Bethesda Marriott | **68** | **67** | Live `:8080` API + `HOTEL_DATA_READINESS_AUDIT.json` |
| Wyndham San Diego Bayside | **11** (CLEAN11) | **0** | Live STAGED pointer `…_3283ee92` |

Founder expectation of “68 for Wyndham” is therefore a **cross-hotel count conflation** with Bethesda STAGED, not a missing Wyndham STAGED activation of 68 rows.

## Wyndham bundled publication packages (complete)

| publicationVersion | lifecycle | opportunityCount | Pointer? | Notes |
|--------------------|-----------|-----------------:|----------|-------|
| `…_20261125_3283ee92` | PRODUCTION_STAGED | **11** | **STAGED (active)** | CLEAN11 freeze |
| `…_20261125_fb10174d` | CUSTOMER_RENDERED | **59** | no | Intermediate candidate |
| `…_20261125_12746a8d` | CUSTOMER_RENDERED | **89** | no | Hotel-wide finalization candidate — **not activated** |
| `…_20261125_bd1e0ff3` | CUSTOMER_RENDERED | 18 | no | |
| `…_20261127_06822449` | CUSTOMER_RENDERED | 18 | no | |
| `…_20261125_1a2e074d` | CUSTOMER_RENDERED | 9 | no | |
| `…_20261125_692abac8` | CUSTOMER_RENDERED | 9 | no | |
| CURRENT.json | — | — | **absent** | No customer CURRENT |

Closest historical magnitudes to “68” for Wyndham: **59** and **89** — both **non-activated** CUSTOMER_RENDERED candidates superseded by the CLEAN11 STAGED freeze. They are **not** the locked accepted qualified write-set.

## Correct complete research / commercial inventory (governed)

| Classification | Count | Founder-visible in GDI (this restore) | Customer CURRENT |
|----------------|------:|----------------------------------------|------------------|
| Original discovery corpus | 161 | Not as qualified | No |
| Historical non-activated packages | 59 / 89 | Historical evidence only | No |
| CLEAN11 STAGED (qualified staged) | **11** | Inventory mode **Staged** | No CURRENT |
| Locked proposed qualified write-set | **26** | Inventory mode **Proposed** | Not applied |
| Of which already in STAGED | 11 | Both Staged + Proposed | |
| Additional proposed (not in STAGED) | 15 | **Proposed** only | |
| HOLD (not qualified) | **2** | Inventory mode **HOLD** | No |
| WHO/HOW continuation candidates | **25** | Inventory mode **Continuations** | Not qualified |
| CURRENT | **0** | Published empty/unavailable | N/A |

**Locked 26 opportunity IDs** — see `08-PROPOSED_WRITE_SET_QA.json` and Proposed mode List/Tiles (exact ID parity with locked artifact).

## Why UI showed 11 (not loss of 68 Wyndham rows)

1. Active STAGED pointer is CLEAN11-only (by freeze / non-activation of 26).  
2. Proposed 26 was previously reviewable only via admin JSON/HTML — not Opportunities List/Tiles.  
3. Separately, `:8080` had briefly been owned by a non-GDI checkout (0 of 0) — restored earlier.

## Restoration policy

- **Appropriate now:** Founder review of locked **26** + HOLD **2** + Continuations **25** inside normal GDI Opportunities (List/Tiles/drawer) without activating STAGED or publishing CURRENT.  
- **Not appropriate:** Fabricating a Wyndham 68 package, reactivating 89/59 candidates as qualified, or merging discovery 161 into customer counts.
