# Stable Identity, Deduplication, and Weekly Change

## Identity (CONFIRMED observations)

- Opportunity IDs in packages are stable strings (e.g. `gdi_opp_yotel_fe2e_icpic_20270914`, `rx_*` prefixes) — multiple minting schemes coexist.  
- Dedup modules exist (e.g. Apify cross-provider dedup, scaling doNotRediscoverAsNetNew).  
- Global discovery date ≠ hotel-newness (weekly refresh contract aware).

## Dedup risks (LIKELY)

- Multi-provider near-duplicates (name variants, URL variants).  
- Rewritten AI titles minting false NEW.  
- Campaign re-runs without identity join.  
- Over-aggressive dedup suppressing distinct demand occurrences.

## Weekly Change — taxonomy mismatch (CONFIRMED)

**UI** (`dealality-gdi-ui.js` weekly filter):

- NEW, UPDATED, REACTIVATED, HIGH_PRIORITY (+ All)

**Backend weekly refresh contract** (`weekly-refresh-contract-v1.js` CHANGE_CLASS):

- NEW_PROSPECT, MATERIAL_UPDATE, CONTACT_ENRICHED, OCCASION_CONCLUDED_OR_STATUS_CHANGED, NO_MATERIAL_CHANGE, NEEDS_REVIEW, RESEARCH_BLOCKED

YOTEL published opportunities: **empty `weeklyDeltaState`** (all 42) — filter cannot classify freshness from package alone.

Automated publication remains OFF until explicitly enabled (contract header).

## Required tests before weekly automation

1. Same source reprocess ≠ duplicate IDs.  
2. Package rebuild ≠ false NEW.  
3. Cosmetic prose ≠ MATERIAL_UPDATE.  
4. Evidence updates preserve history.  
5. Hotel feedback survives refresh.  
6. Failed research ≠ overwrite CURRENT.  
7. No customer publish without approval.

## Required result

Do **not** enable scheduled weekly runs until UI/backend taxonomy is unified and packages emit weeklyDelta fields from a single contract.
