# Production Census — Marriott CALA Discovery Adapter

**Status:** `production_census_marriott_discovery_adapter_ready_for_insert_review`  
**Airtable writes:** false · **HQV required for discovery:** false · **Webhound:** not used  
**Target:** Deal Capture Platform → Hotel Property Census (`tbl9aY5ijiuIzzWam`)

## What shipped

- New module: `lib/research-engine-v2/census-autopilot-marriott-discovery-adapter.js`
- Wired into Autopilot `source_discovery` (`census-autopilot-source-discovery.js` v2)
- `CALA_DISCOVERY_ADAPTER_COVERAGE` marks Marriott **supported** for Mexico, Dominican Republic, Costa Rica, Colombia, Panama (and other CALA countries with known sitemap slugs)
- Deprecated `*.sitemap-hotels.xml` blocked; HQV never consulted for listing discovery

## Source pattern

| Item | Value |
| --- | --- |
| Directory | `https://www.marriott.com/en-us/hotel-sitemap/{country}-hotel-sitemap` |
| Property URL | `/en-us/hotels/{MARSHA5}-{slug}/overview` |
| Identity | `ind_marriott_{cc}_{marsha}` (cr ≠ co) |
| Parent | Marriott |

## Controlled runs (no apply)

### 1. Marriott Mexico smoke
`2026-08-05T23-45-16_CALA-source-discovery`

- Discovered **301** · Existing exact **301** · New **0** · Inserts **0**
- MARSHA / URL coverage **100%**

### 2. Marriott CALA (priority five)
`2026-08-05T23-45-32_CALA-source-discovery`

| Country | Hotels |
| --- | ---: |
| Mexico | 301 |
| Dominican Republic | 25 |
| Costa Rica | 25 |
| Colombia | 30 |
| Panama | 17 |
| **Total** | **398** |

- Existing exact **301** (Mexico already in Census)
- New candidates **93** (non-Mexico)
- Steward **4** · Duplicate risk **0**
- **Estimated inserts if applied: 65** (High identity only)
- Approval bundle: `…/2026-08-05T23-45-32_CALA-source-discovery/approval-bundle.json`

### 3. Active Brand Setup (all ready adapters)
`2026-08-05T23-47-10_CALA-source-discovery`

- Families: Hilton + Choice + Marriott (+ VIC evidence)
- Discovered **290** · Existing **262** · New **28** · **Inserts 28**
- Marriott filtered to Active/Live Brand Setup brands

## Acceptance

| Check | Result |
| --- | --- |
| Marriott wired into Autopilot | Yes |
| CALA + Mexico + Active Setup runs | Yes |
| Approval-bundle-bound inserts | Yes (not applied) |
| HQV not required | Yes |
| Deprecated XML excluded | Yes |
| No Airtable / BE / Brand Setup writes | Yes |
| Tests | `npm run test:census-autopilot` pass |

## Recommended next

1. Founder review insert approval bundle(s) — do **not** apply until confirms.
2. Build non-Mexico Hilton/Choice country adapters.
3. Wire IHG destination directory into Autopilot.

## Change impact

- **Impact:** Medium (read-path discovery; insert proposals only)
- **Rollback:** Set Marriott `ready: false` in `CALA_DISCOVERY_ADAPTER_COVERAGE` or revert source-discovery Marriott branch
- **Regression:** Re-test Mexico Hilton/Choice discovery; Marriott Mexico should remain 301/301 match
