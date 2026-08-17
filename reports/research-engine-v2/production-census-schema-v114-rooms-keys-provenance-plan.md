# Production Census Schema v1.1.4 — Rooms / Keys Provenance Plan

**Status:** plan_only_no_schema_create  
**Generated:** 2026-08-05  

## Existing (live)

- Rooms / Keys  
- Rooms Confidence  
- Rooms Source URL  

## Proposed creates

### 1. Rooms Source Type

- Type: `singleSelect`  
- Options:
  - `official_property_page`
  - `official_brand_directory`
  - `official_hotel_website`
  - `official_press_release`
  - `official_development_page`
  - `trusted_secondary_source`
  - `steward_review`

### 2. Rooms Reviewed Date

- Type: `date`

### 3. Rooms Notes

- Type: `multilineText`

## Recommend: Hold on Rooms Confidence

If Airtable allows adding a select option safely, **add `Hold`** to existing **Rooms Confidence**.

Until then Autopilot maps Hold → `Insufficient` on any write path that must use live options.

Do **not** apply option/field creates unless:

```bash
npm run census:autopilot -- --mode schema-apply --confirm-schema-v114-rooms-provenance
```

…and founder explicitly approves (this build task does not create fields).

## Naming note

Prefer short names aligned with live `Rooms Confidence` / `Rooms Source URL`. Optional later rename to `Rooms / Keys *` is deferred.

## Why

Rooms/Keys is an early Autopilot queue. Provenance fields make High-confidence applies auditable and keep Medium/Hold in steward review.
