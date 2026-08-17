# Production Census Schema v1.1.3 — Provenance Fields Dry Run

**Status:** `production_census_schema_v113_dry_run_pass`  
**Generated:** 2026-08-05T15:21:26.545Z

- Field count before: **101**
- Expected after: **108**
- To add: **7**
- Already exist: **0**
- Census records: **666**

## Fields to add

```json
[
  {
    "name": "Address Confidence",
    "type": "singleSelect",
    "options": [
      "High",
      "Medium",
      "Low",
      "Hold"
    ]
  },
  {
    "name": "Address Source URL",
    "type": "url",
    "options": null
  },
  {
    "name": "Coordinate Source Type",
    "type": "singleSelect",
    "options": [
      "official_coordinates",
      "official_address_geocode",
      "existing_source",
      "structured_data_extraction",
      "embedded_map_extraction",
      "blocked_low_confidence",
      "blocked_no_official_address",
      "steward_review"
    ]
  },
  {
    "name": "Coordinate Confidence",
    "type": "singleSelect",
    "options": [
      "High",
      "Medium",
      "Low",
      "Hold"
    ]
  },
  {
    "name": "Geocode Provider",
    "type": "singleSelect",
    "options": [
      "Mapbox",
      "Google",
      "Official Page",
      "Existing Source",
      "Manual Review",
      "None"
    ]
  },
  {
    "name": "Geocode Method",
    "type": "singleSelect",
    "options": [
      "official_coordinates",
      "official_address_geocode",
      "structured_data_extraction",
      "embedded_map_extraction",
      "manual_review",
      "none"
    ]
  },
  {
    "name": "Geocode Reviewed Date",
    "type": "date",
    "options": null
  }
]
```

## Already existing

```json
[]
```
