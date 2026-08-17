# Production Census — Market Geography Completion

**Status:** `production_census_market_geography_completion_applied_clean`  
**Write target:** Deal Capture Platform → Hotel Property Census (`tbl9aY5ijiuIzzWam`)  
**Generated:** 2026-08-06

## Schema

Created on Hotel Property Census only (did not exist before):

| Field | Type | Airtable ID |
|-------|------|-------------|
| Continent | singleSelect | `fldN4YbhOgN286JWP` |
| Sub-Continent | singleSelect | `fld5sOW7bpFEL3m0R` |
| Market | singleLineText | `fldB6DdmONl0pcsu1` |
| Submarket | singleLineText | `fldPANZ60nzvxbGOQ` |

Legacy combined field `Market / Submarket` left untouched (no dual-write).

## Clean Core standard update

**Required for Clean Core** (when geo fields exist): Continent, Sub-Continent (plus existing identity fields).

**Helpful / not required for Clean Core v1:** Market, Submarket, State / Region, Address, Lat/Long, Phone, Rooms.

## Controlled run

- Records scanned: **907**
- High Continent proposals: **903**
- High Sub-Continent proposals: **903**
- High Market proposals: **461**
- High Submarket proposals: **38**
- Steward cases: **0**
- Airtable writes (controlled): no

## Mission apply

- Updates applied: **903**
- Inserts: **0**
- Run: `reports/research-engine-v2/autopilot/2026-08-06T21-46-35_CALA-clean-census-v1-mission`

## Before → After

| Metric | Before | After |
|--------|-------:|------:|
| Total records | 907 | 907 |
| Clean Core | 865 | 865 |
| Below Clean Core | 42 | 42 |
| Continent complete | 0 | **903** |
| Sub-Continent complete | 0 | **903** |
| Market complete | 0 | **461** |
| Submarket complete | 0 | **38** |

## Fields written

Continent, Sub-Continent, Market, Submarket, Enrichment Status, Enrichment Priority, Last Reviewed Date

## Not written

Address, Latitude, Longitude, Phone, Rooms / Keys, owner/operator/dates, Recent Momentum, Company Validated, Brand Verified, Brand Status. Brand Setup / Brand Explorer / old Census / VIC untouched.

## Source gaps

- **~4** Continent blank — Country missing or unmapped
- **~446** Market blank — City not in recognized market map (`market_source_needed`)
- Submarket remains optional; filled only at High (token / city-is-submarket rules)

## Examples

| Property | After |
|----------|--------|
| DoubleTree by Hilton Punta Cana Downtown | Continent=North America, Sub-Continent=Caribbean, Market=Punta Cana |
| HS HOTSSON Hotel CDMX Condesa Sur | Market=Mexico City, Submarket=Condesa |
| Tropicana Los Cabos (Tapestry) | Market=Los Cabos, Submarket=San José del Cabo |

## Autopilot wiring

- Queue: `market_geography_completion` (after city_state_normalization, before key_field_completion)
- clean-census-v1 mission Phase 1 includes this queue
- Ensure script: `npm run ensure:hotel-property-census-market-geography-fields`

## Validation

- `npm run test:census-autopilot` — pass
- `npm run dealality:batch-learning-audit` — pass
