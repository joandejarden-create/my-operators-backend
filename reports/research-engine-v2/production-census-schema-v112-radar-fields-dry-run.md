# Production Census Schema v1.1.2 Radar Fields — Dry Run

**Status:** `production_census_schema_v112_dry_run_pass`
**Generated:** 2026-08-05T12:35:35.028Z

- Field count before: 95
- Expected after: 101
- To add: 6
- Already exist: 0
- Census records: 666

## Fields to add

```json
[
  {
    "name": "Radar Display Status",
    "type": "singleSelect",
    "options": [
      "Public Map Eligible",
      "Public List Eligible",
      "Internal Only",
      "Hold"
    ]
  },
  {
    "name": "Radar Display Reason",
    "type": "multilineText",
    "options": null
  },
  {
    "name": "Radar Geography Status",
    "type": "singleSelect",
    "options": [
      "Coordinates Available",
      "City-Level Only",
      "Address Available No Coordinates",
      "Geography Insufficient",
      "Hold"
    ]
  },
  {
    "name": "Public Census Eligibility",
    "type": "singleSelect",
    "options": [
      "Eligible",
      "Eligible With Limits",
      "Not Eligible",
      "Hold"
    ]
  },
  {
    "name": "Public Display Confidence",
    "type": "singleSelect",
    "options": [
      "High",
      "Medium",
      "Low",
      "Hold"
    ]
  },
  {
    "name": "Public Display Review Status",
    "type": "singleSelect",
    "options": [
      "Auto-Classified",
      "Needs Review",
      "Approved",
      "Hold"
    ]
  }
]
```
