# Census Missing Field Source Strategy Controller v1

**Status:** `production_census_v4_full_build_resumed_after_railway_discovery_fix_complete` + `partial_source_remaining` + `partial_network_remaining`
**Objective:** `census-missing-field-source-strategy-controller-v1`
**Write target:** Hotel Property Census (`tbl9aY5ijiuIzzWam`) only

## Purpose

After Railway-safe discovery completes (including partial network timeouts), hand off to field completion — do not loop discovery forever on timed-out units.

## Discovery resume (V4)

| Metric | Value |
|--------|-------|
| Discovered (cache) | 1245 |
| Units completed | 234 |
| Timed out units | 1 (Hilton / Mexico) |
| Failed units | 0 |
| Resume | unit cache + checkpoint |
| Force refresh | 0 |

## First live field pass (Railway `765fc2e7`)

- Scanned **2561** · updated **276** (address **5** + market **269**)
- Batch size **20** · runtime ceiling **30m** (avoids prior 10m hard-kill mid-DFS)
- Continues automatically each supervisor cycle

## Priority fields

1. Identity / dedupe
2. Address
3. State / Region
4. Market
5. Submarket
6. Latitude / Longitude via Mapbox after validated address
7. Hotel Website / Official Property URL
8. Phone (Medium internal + provenance)
9. Rooms / Keys (approved underlying sources)

## Held (candidate reports only)

Owner, operator, developer, opening/renovation/affiliation dates, Recent Momentum, Company Validated, Brand Verified, Brand Status, release fields. Direct DataForSEO coordinates held.

## CLI

```bash
npm run census:missing-field-source-strategy
npm run census:missing-field-source-strategy-apply
```
