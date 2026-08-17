# Production Census Write — Dry-Run

**Status:** `production_census_write_dry_run_pass`
**Dry-run pass:** true
**Base:** `appCCU…foLk`
**Env OK for apply:** true

## Counts

```json
{
  "vic_total": 666,
  "data_eligible": 580,
  "census_create": 666,
  "census_update": 0,
  "affiliations_create": 666,
  "evidence_create": 666,
  "steward_create": 4,
  "held_brand_unconfirmed": 4,
  "by_affiliation": {
    "Soft-Branded / Collection": 89,
    "Branded": 569,
    "Future / Pipeline": 4,
    "Brand-Unconfirmed": 4
  }
}
```

## Existing

```json
{
  "census_rows_for_freeze": 0,
  "upsert_mode": false
}
```

## Field write summary

```json
{
  "will_write": [
    "Property Name",
    "Canonical Property Name",
    "Property Identity Key",
    "Family / Source Family",
    "Country",
    "City",
    "VIC Freeze Hash",
    "Data Eligible",
    "Identity Confidence",
    "Production Use Status",
    "Current Brand",
    "Brand Family",
    "Affiliation Status",
    "Future Opening Flag",
    "Brand Confidence",
    "Steward Review Status",
    "Source Type",
    "Source Confidence",
    "Official Property URL",
    "Source URL",
    "Discovery Date",
    "Affiliation As-Of Date"
  ],
  "will_not_write": [
    "Rooms",
    "Owner",
    "Operator",
    "Opening Date",
    "Affiliation Start Date",
    "Latitude/Longitude when missing",
    "Brand Explorer Presentation/Basics/Status/CV/Verified/Momentum"
  ]
}
```
