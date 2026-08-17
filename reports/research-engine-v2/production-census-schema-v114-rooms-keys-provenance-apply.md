# Production Census Schema v1.1.4 — Rooms / Keys Provenance Apply

**Status:** `production_census_schema_v114_rooms_keys_provenance_applied_ready_for_controlled_autopilot`  
**Mode:** apply  
**Generated:** 2026-08-05T19:56:18.219Z  
**Apply executed:** true

## Executive summary

- Fields created this task (session): **3** (108 → 111)
- Hold option on Rooms Confidence: **added** (typecast seed + restore; Meta API choices PATCH unsupported)
- Census records: **666** (unchanged)
- New provenance populated: **no** (expected)
- Existing cell value drift: **0**
- Validation pass: **true**
- Brand Explorer writes: **false**
- Brand Setup writes: **false**
- Record population writes: **false**

## Fields created (session)

| Field | Type | Field ID |
| --- | --- | --- |
| Rooms Source Type | singleSelect | `fld1REDWd9zlQBswU` |
| Rooms Reviewed Date | date | `fldOzFFDqynSm7OaH` |
| Rooms Notes | multilineText | `fld8OIYN43wPYgPDM` |

Rooms Source Type options: `official_property_page`, `official_brand_directory`, `official_hotel_website`, `official_press_release`, `official_development_page`, `trusted_secondary_source`, `steward_review`.

## Rooms Confidence Hold

Meta API rejects `options.choices` PATCH (422). Hold created via typecast seed on blank record `rec02wJ8dk7HtjPjx`, then restored to null. Drift check = 0.

Choices now: Exact, High, Medium, Low, Insufficient, Unknown, **Hold**.

## Rename recommendation (not applied)

Optional later rename `Rooms Confidence` / `Rooms Source URL` → `Rooms / Keys*` for naming parity — deferred.

## Next

Run Autopilot controlled: `npm run census:autopilot -- --region CALA --scope active-brand-setup --mode controlled --strategy fastest-safe --run-until-complete --batch-size 250`
