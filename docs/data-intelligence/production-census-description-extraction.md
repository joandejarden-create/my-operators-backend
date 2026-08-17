# Production Census Description Extraction

**Status:** `production_census_description_extraction_dry_run_ready_for_founder_review`  
**Generated:** 2026-08-05

## Why this lane

Geocode provider/storage decision is **not** ready:

- `MAPBOX_ACCESS_TOKEN` unset
- `MAPBOX_PERMANENT_GEOCODING` unset
- `GOOGLE_GEOCODE_STORAGE_TERMS_REVIEWED` unset

Census population continues via official-page description extraction so geocoding does not block all enrichment.

## Dry-run snapshot (fetch-limit 90)

| Metric | Value |
| --- | ---: |
| Records scanned | 666 |
| Eligible (active brand, not held, property URL, missing desc) | 425 |
| Pages fetched | 90 |
| Pages ok | 84 (all IHG) |
| Pages blocked | 6 (Hilton edge) |
| Fetch deferred | 335 |
| Updates if applied | 84 |
| Descriptions proposed | 84 |
| Amenities proposed | 0 (already filled on those rows) |
| Geocode 34 | still blocked |

### Extraction methods observed

- `json_ld_hotel_description` (preferred when substantive)
- `official_page_amenities_factual_assembly` (when narrative missing but same-page amenities exist)
- `html_paragraph` (secondary)

Booking-boilerplate meta/OG (“Best Price Guarantee…”) is **rejected**.

### Family posture

| Family | Fetch | Notes |
| --- | --- | --- |
| IHG | Works | hoteldetail HTML + JSON-LD |
| Hilton | Blocked | Akamai / page reference shell |
| Choice | Deferred / blocked | Edge denial on plain fetch |
| Marriott | Deferred / blocked | Akamai on overview HTML |

## Rules

- Official/public property pages only (`Official Property URL` preferred)
- AI Summary only when grounded in Source Text
- No owner/operator/rooms/dates / Recent Momentum / Brand Explorer
- Blank beats fake completeness
- Do not mark held records public eligible

## Commands

```bash
npm run research-engine-v2:production-census-description-extraction -- --dry-run
npm run research-engine-v2:production-census-next-lane -- --dry-run
npm run test:production-census-description-extractor
```

## Apply (founder only after review)

```bash
ALLOW_PRODUCTION_CENSUS_DESCRIPTION_EXTRACTION=1 \
CONFIRM_NO_BRAND_EXPLORER_WRITES=1 \
CONFIRM_NO_OWNER_OPERATOR_WRITES=1 \
CONFIRM_NO_ROOM_DATE_WRITES=1 \
npm run research-engine-v2:production-census-description-extraction -- --apply \
  --confirm-census-description-extraction \
  --confirm-official-public-sources-only \
  --confirm-no-brand-explorer-writes \
  --confirm-no-owner-operator-writes \
  --confirm-no-room-date-writes \
  --confirm-no-recent-momentum \
  --confirm-held-records-blocked \
  --confirm-no-fake-completeness \
  --confirm-ai-summary-grounded-in-source-text
```

> Apply path is intentionally dry-run-first in this ship; confirm flags are defined for the next step.

## Reports

- `reports/research-engine-v2/production-census-description-extraction-dry-run.{md,json}`
- `docs/data-intelligence/production-census-next-lane.md`

## Change impact

**Medium** (read-path dry-run + extractor code). No Airtable writes in this run.  
**High** when apply is later approved.
