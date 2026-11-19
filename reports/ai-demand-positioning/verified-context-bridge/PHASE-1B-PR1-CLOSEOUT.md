# ADP 2.0 Phase 1B / PR-1 — Verified Context Bridge Closeout

**Date:** 2026-11-19  
**Scope:** Diagnostic only — no Airtable writes, no customer RG cutover, no paid research  
**Artifacts:** `phase-1b-dual-run-*-latest.json`  
**Tests:** `npm run test:adp-verified-context-bridge-v1`

## Pilot results

### Bethesda Marriott (`adp_bethesda_marriott` / `recLuxvwwxID7U2B8`)

| Metric | Value |
|---|---|
| Selected attributes | 15 (unchanged) |
| Fixture RG denominator | 15 |
| Bridged-eligible denominator | 12 |
| Fixture gapScore | 80 |
| Bridged-eligible gapScore | 75 |
| Minimum-12 eligible | **MEETS** (12) |
| Needs verification | `fitness_center_24hr`, `peloton`, `outdoor_pool` |
| Undefined | none |
| Period used | `adp_period_adp_bethesda_marriott_20261002142154_62428a` |
| Artifact warning | **DENOMINATOR_ARTIFACT** — lower bridged score is not AI improvement |

Suggested additions (not auto-applied): `large_ballroom` (verified support), plus pending `executive_lounge`, `fitness_center`, `event_space_outdoor`, `business_center`.

### Spice Island Beach Resort (`adp_spice_island_beach_resort` / `recKRJjcPnb4tVDDS`)

| Metric | Value |
|---|---|
| Selected attributes | 24 (unchanged) |
| Fixture / bridged denominators | 24 / 24 |
| Fixture / bridged gapScore | 100 / 100 |
| Minimum-12 eligible | **MEETS** (24) |
| Needs verification | none |
| Semantic overlaps | beachfront/oceanfront; spa/full_service_spa; meeting/event; luxury/five_diamond |
| Period used | none found in local runtime (RG computed on empty/missing obs path safely) |

Suggested additions: `heated_outdoor_pool`, `wellness` (pending verification).

### YOTEL Geneva Lake (regression)

| Metric | Value |
|---|---|
| Selected | 8 |
| Undefined (SELECTED_UNDEFINED) | `airport_shuttle`, `park_and_fly` |
| Fixture denom (defined only) | 6 |
| Bridged-eligible | 3 |
| Minimum-12 | **BELOW** (shortfall 9) — diagnostic only |

Y1 defs not enabled for customer RG (D2 conditional).

## Historical compatibility

- Customer-facing Reality Gap path unchanged  
- No historical report regeneration  
- No methodology weight changes  
- Bridged path labeled `RG_BRIDGED_ELIGIBLE_DIAGNOSTIC_V1`

## Airtable selection persistence

Proposal: Option B — Configuration header + Attribute Lines  
Doc: `docs/ai-demand-positioning/adp-strategic-attribute-selection-airtable-persistence-proposal-v1.md`  
**No writes.** Awaiting founder approval before PR-3.

## Persistence classification

`PERSISTENCE_DESIGNED` for strategic selection Airtable SoT  
Phase 1B diagnostics themselves are code + FS report artifacts (not FULL_E2E_VALIDATED)
