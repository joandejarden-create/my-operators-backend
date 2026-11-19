# Hotel News & Articles ← Market Alerts (assessment + delivery)

**Date:** 2026-11-19  
**PR:** #463  
**Status:** Implemented (read path + matching + Brand/Operator UI); association apply gated

## 1. Market Alerts schema (canonical)

| Item | Value |
| --- | --- |
| Base | `AIRTABLE_BASE_ID` (`appvtnDurnMSjINP6`) |
| Table | `MarketAlerts` (`AIRTABLE_TABLE_MARKET_ALERTS`) |
| Article key | `Dedupe ID` = SHA-256 of canonical Source URL (`lib/market-alerts-dedupe.js`) |
| Core fields | Title, Summary, Source Name, Source URL, Published At, Category, Region Group, Priority, Tags |
| Property text | Intelligence field `Hotel / Project` (+ Brand/Operator/Owner Involved) |
| HPC link today | **No linked-record field.** Association via Tags `hpc:<HPC recId>` (designed optional text field `HPC Hotel Record IDs`) |
| HPC base | Hotel Property Census lives on `AIRTABLE_BASE_ID_ALT` — cross-base string IDs in Tags are intentional |

APIs: `GET /api/market-alerts`, RSS sync `api/run-market-alerts-rss-sync.js`, news fetch `api/market-alerts-news.js`.

## 2. Matching

Module: `lib/hotel-intelligence/hotel-news-from-market-alerts-v1.js`

Visible in HE only: `HOTEL_SPECIFIC`, `MULTI_HOTEL_SPECIFIC`  
Excluded: `ORGANIZATION_CONTEXT_ONLY`, `AMBIGUOUS_MATCH`, `UNRELATED`

## 3. Persistence / association

- Reuse Market Alerts rows; never write news into HPC/HI.
- Idempotent Tag planner: `planHpcHotelAssociation` / `planMarketAlertsHotelAssociations`
- Apply only when `MARKET_ALERTS_HOTEL_LINK_APPLY=1` (default dry-run / no write)

## 4. HE read path

`GET /api/hotel-explorer/hotels/:hotelId/news-articles` — GET-only, cached, fail-soft.

## 5. Live validation notes

- Bethesda Marriott: no property-specific Market Alerts rows found → subsection **hidden** (expected).
- Example property-specific MA rows exist (e.g. Loews Nashville, Plaza Hotel) for positive matching demos.
- Mullen / Marbeth: no MA hits in probe.

## 6. Remaining blockers

- No production Tag apply until Market Alerts owner + founder authorize `MARKET_ALERTS_HOTEL_LINK_APPLY`.
- Optional `HPC Hotel Record IDs` field not created (design-only).
- Sources & Evidence deep-link to MA record IDs deferred to association maturity (reference by URL/dedupe, not duplicated HI evidence rows).
