# Tripadvisor Priority CALA Profile Wave V1 — Interim Status

**Updated:** 2026-08-12 (overnight run in progress)

## Executive summary

| Item | Status |
|------|--------|
| First-batch validation (Mexico 25) | **PASS** |
| Airtable write mode | **NULL_FILL_ENABLED** |
| First-batch hotels updated | **25** |
| First-batch executed writes | **153** |
| Schema changes | **0** |
| Brand / Owner / Operator / Rooms authoritative writes | **0** |
| Overnight runner | **RUNNING** (server-side Apify + local token) |
| Budget ceiling | **$25** |
| Spend after first-batch | **~$0.12** |

## Important policy notes applied

- **Hotel Class / Segment** is NOT filled from Tripadvisor (stars ≠ chain-scale options Luxury/Upper Upscale/…). Stars go to Hotel Intelligence / evidence only.
- **Email** and **Postal Code**: no census columns → evidence only.
- **Rooms / Keys**: candidate / evidence only.
- Writes only into **KEEP_CORE / KEEP_SUPPORTING** retained fields; never overwrite non-null.

## First-batch fields written (Mexico)

From manifest (178 proposed → 153 executed after live apply/read-back):

- Official Property URL, Phone, Address, Latitude, Longitude
- City / State / Region (sparse)
- Property Type
- Amenities - Structured Tags (normalized tags into empty multiline only)

## Resume

```bash
ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES=1 \
ENABLE_CENSUS_FIELD_ENRICHMENT=1 \
ENABLE_COORDINATE_WRITES=1 \
CONFIRM_TRIPADVISOR_OVERNIGHT_WRITES=1 \
APIFY_OVERNIGHT_BUDGET_USD=25 \
node scripts/tripadvisor-priority-cala-profile-wave.mjs --mode=resume
```

Artifacts:

- `data/hotel-intelligence/tripadvisor-priority-cala-profile-wave-v1/checkpoint.json`
- `reports/hotel-intelligence/tripadvisor-priority-cala-profile-wave-v1/`
