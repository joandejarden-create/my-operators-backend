# Production Census First Pass — Apply

**Status:** `production_census_first_pass_applied_ready_for_next_enrichment_lane`  
**Generated:** 2026-08-05T13:25:37.439Z  
**Apply executed:** true

## Summary

- Updates attempted: **666**
- Updates written: **666**
- Airtable errors: **0**

## Post-apply validation

```json
{
  "record_count": 666,
  "expected_record_count": 666,
  "zero_zero": 0,
  "held_public_eligible": 0,
  "brand_unconfirmed_public_map": 0,
  "owner_filled": 0,
  "operator_filled": 0,
  "rooms_filled": 0,
  "opening_filled": 0,
  "renovation_filled": 0,
  "affiliation_start_filled": 0,
  "coords_filled": 132,
  "radar_populated": 666,
  "amenities_filled": 215,
  "pass": true
}
```

## Forbidden fields untouched

```json
{
  "fields": [
    "Owner Name",
    "Developer Name",
    "Developer",
    "Operator / Management Company",
    "Rooms / Keys",
    "Opening Date",
    "Renovation / Conversion Date",
    "Renovation Date",
    "Affiliation Start Date",
    "Company Validated",
    "Brand Verified",
    "Recent Momentum"
  ],
  "proposed_writes": [],
  "ok": true
}
```

## Next recommended lane

Marriott/IHG property-level coordinate sourcing + official-page description scrape (blocked owner/operator/rooms/dates remain queued)
