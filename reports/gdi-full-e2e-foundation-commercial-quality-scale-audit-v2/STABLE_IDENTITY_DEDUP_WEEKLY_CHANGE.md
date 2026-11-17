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

## Weekly Change — dual contracts + empty package deltas (CONFIRMED)

**UI** (`dealality-gdi-ui.js` weekly filter):

- NEW, UPDATED, REACTIVATED (+ All)
- **HIGH_PRIORITY** is hitchhiked into the same control (priority filter, not a weekly freshness state)

**Backend weekly delta states** (`weekly-delta.js` → `WEEKLY_DELTA_STATE`):

- NEW, UPDATED, REACTIVATED, UNCHANGED, CLOSED_DOWNGRADED, NOT_IN_CURRENT

**Backend weekly refresh CHANGE_CLASS** (`weekly-refresh-contract-v1.js`) — research orchestration taxonomy:

- NEW_PROSPECT, MATERIAL_UPDATE, CONTACT_ENRICHED, OCCASION_CONCLUDED_OR_STATUS_CHANGED, NO_MATERIAL_CHANGE, NEEDS_REVIEW, RESEARCH_BLOCKED

**Package observation:** YOTEL / Radisson / Bethesda CURRENT packages omit `weeklyDeltaState` on essentially all rows → New/Updated/Reactivated filters empty on package-served UI.

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
