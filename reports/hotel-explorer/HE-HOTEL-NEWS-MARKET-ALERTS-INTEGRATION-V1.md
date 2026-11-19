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

## 6. Closeout status (2026-11-19)

See `HE-MARKET-ALERTS-INTEGRATION-CLOSEOUT-V1.md` + HE-QL-023.

- Sources & Evidence: **wired** via GET `evidenceCitations` (no HI Evidence copy).
- Association apply still gated (`MARKET_ALERTS_HOTEL_LINK_APPLY`); `HPC Hotel Record IDs` still design-only (field absent).
- Bethesda property-specific MA coverage: discovery gap recorded (`HE-BETHESDA-MARKET-ALERTS-DISCOVERY-GAP-V1.md`).

