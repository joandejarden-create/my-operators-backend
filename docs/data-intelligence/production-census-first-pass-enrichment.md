# Production Census First Pass Enrichment

**Status:** `production_census_first_pass_applied_ready_for_next_enrichment_lane`  
**Contract:** `production-census-first-pass-enrichment-v1`  
**Generated:** 2026-08-05T13:56:50.000Z  
**Apply executed:** true  
**Base:** Deal Capture Platform (`appCCU…foLk`)  
**Table:** Hotel Property Census (`tbl9aY5ijiuIzzWam`)

## 1. Executive summary

First-pass production enrichment filled safe geography/Radar/amenity fields from VIC official directory and property claims. Blocked owner/operator/rooms/date values were researched into a queue only — never written.

| Metric | Value |
| --- | ---: |
| Total scanned | 666 |
| Active-brand mapped | 425 |
| Eligible | 425 |
| Blocked (not active / held) | 241 |
| Coordinate updates | 132 |
| Radar updates | 666 |
| Amenity updates | 215 |
| Description updates | 0 |
| Market / Submarket updates | 217 |
| Blocked research queue | 298 |
| Airtable updates written | 666 |

## 2. Active-brand Census scope

Mapped via Brand Explorer Active/Live **62** baseline (slug, name, aliases).

| Classification | Count |
| --- | ---: |
| exact_match | 390 |
| alias_match | 35 |
| not_in_active_universe | 237 |
| blocked_brand_unconfirmed | 4 |

Held / Brand-Unconfirmed excluded from content enrichment. Brands outside Active/Live (e.g. Holiday Inn core, City Express Plus/Junior, Fairfield, Staybridge, Iberostar) → Radar **Internal Only**, no safe-enrichment writes.

## 3. Coordinate coverage audit

| State | Count |
| --- | ---: |
| Before (with coords) | 0 |
| After (with coords) | 132 |
| Zero-zero | 0 |
| Shared campus pins (Medium confidence) | 2 clusters |

Sources: Hilton directory `localization.coordinate` + Choice hotel-card `geoLocation`. Marriott VIC freeze claims are null → deferred. IHG freeze has nearly no lat/lng.

## 4. Proposed / applied coordinate updates

132 Latitude/Longitude pairs written with evidence URL + Medium/High confidence. No city centroids, no 0,0, no fabricated geocodes.

## 5. Radar readiness classification counts

| Radar Display Status | Count |
| --- | ---: |
| Public Map Eligible | 132 |
| Public List Eligible | 293 |
| Internal Only | 237 |
| Hold | 4 |

All 6 Radar/public fields populated on all 666 rows.

## 6–9. Safe enrichment applied

| Lane | Count |
| --- | ---: |
| Descriptions | 0 (no VIC grounded description text) |
| Amenities Source Text + tags | 215 |
| Property Type | 220 |
| Asset Context | 205 |
| Strategic flags | 215 |
| Market / Submarket (Dealality) | 217 |

Market/Submarket written only when Dealality market or corridor inference is strong (no bare weak `Other`).

## 10. Blocked field research queue

**298** items queued (not written): Rooms / Keys, Opening Date, and related blocked claims from VIC where values exist — especially IHG rooms.

## 11. Source support summary

Writes require VIC `field_claims` with evidence URL and Medium/High confidence from Official Brand Directory / Official Property Page. Dealality Market·Submarket uses Census city → Dealality geography config (not STR).

## 12. Webhound usage

Sidecar session (source-discovery only; no Airtable writes):

- **Title:** Marriott Mexico hotel page coordinate extraction patterns
- **URL:** https://webhound.ai/session/bbaa85f9-3d19-4b05-a53c-4bc4e44fde02
- **Budget:** $5
- **Purpose:** Improve Marriott Mexico property-page lat/lng crawler patterns for the next coordinate lane
- **Not used for:** production writes, owner/operator/rooms/dates, Brand Explorer, Recent Momentum

## 13. Forbidden fields untouched

Owner Name, Developer, Operator / Management Company, Rooms / Keys, Opening Date, Renovation Date, Affiliation Start Date, Company Validated, Brand Verified, Recent Momentum, Brand Explorer public fields — **zero writes**.

## 14. Brand Explorer safety result

| Gate | Result |
| --- | --- |
| active_universe_sot | PASS |
| global_active_semantic | PASS |
| pvql_quiet | PASS |
| momentum_evidence | PASS |
| mandatory_release_gates | PASS |

Active universe **62** · semantic expected **0/0/0** · all_pass **true**

## 15. Next recommended lane

1. Incorporate Webhound Marriott coordinate extraction patterns into `marriott-mexico-discovery` / first-pass claim loader  
2. IHG property-page coordinate pass (official hoteldetail)  
3. Official-page description Source Text + grounded AI Summary scrape  
4. Steward review of blocked rooms/date/owner queue (separate approval lane)

## Commands

```bash
npm run research-engine-v2:production-census-first-pass-enrichment -- --dry-run

ALLOW_PRODUCTION_CENSUS_FIRST_PASS=1 \
CONFIRM_NO_BRAND_EXPLORER_WRITES=1 \
CONFIRM_NO_OWNER_OPERATOR_WRITES=1 \
CONFIRM_NO_ROOM_DATE_WRITES=1 \
npm run research-engine-v2:production-census-first-pass-enrichment -- --apply \
  --confirm-first-pass-census-enrichment \
  --confirm-source-supported-coordinates-only \
  --confirm-no-city-centroid-coordinates \
  --confirm-no-zero-zero-coordinates \
  --confirm-official-public-sources-only \
  --confirm-no-brand-explorer-writes \
  --confirm-no-owner-operator-writes \
  --confirm-no-room-date-writes \
  --confirm-no-recent-momentum \
  --confirm-held-records-blocked
```

## Reports

- `reports/research-engine-v2/production-census-first-pass-enrichment-dry-run.md`
- `reports/research-engine-v2/production-census-first-pass-enrichment-dry-run.json`
- `reports/research-engine-v2/production-census-first-pass-enrichment-apply.md`
- `reports/research-engine-v2/production-census-first-pass-enrichment-apply.json`

## Change impact

- **Classification:** High (Airtable Census writes; Radar readiness; enrichment governance)
- **Rollback:** Re-clear Radar + enrichment fields for this freeze via steward script, or restore from pre-apply Airtable snapshot if available. Brand Explorer untouched.
- **Modules/pages:** Hotel Property Census only; Brand Explorer untouched

## Data contract snapshot

- **Table:** Hotel Property Census
- **Field map:** `MAP_FIRST_PASS` in `lib/research-engine-v2/production-census-first-pass-enrichment.js`
- **Allowed writes:** Lat/Lng, 6 Radar/public fields, description/amenity/property-type/asset/market/flags, Enrichment Status/Priority, Last Reviewed Date
- **Select options:** v1.1 / v1.1.2 schema (Property Type, Asset Context, Radar enums, Enrichment Status)
- **Linked records:** none written
