# Production Census Schema v1.1 — Dry-Run

**Status:** `production_census_schema_v11_dry_run_pass`
**Dry-run pass:** true
**Fields to add:** 62
**Already existed:** 0
**Census freeze rows:** 666

## Fields to add

| Field | Type |
| --- | --- |
| Hotel Description - Source Text | multilineText |
| Hotel Description - AI Summary | multilineText |
| Short Property Summary | multilineText |
| Property Positioning | multilineText |
| Hotel Class / Segment | singleSelect |
| Property Type | singleSelect |
| Asset Context | singleSelect |
| Market / Submarket | singleLineText |
| Amenities - Source Text | multilineText |
| Amenities - Structured Tags | multilineText |
| F&B Flag | checkbox |
| Meeting Space Flag | checkbox |
| Fitness Flag | checkbox |
| Pool Flag | checkbox |
| Resort Amenities Flag | checkbox |
| Extended Stay Amenity Flag | checkbox |
| Parking Flag | checkbox |
| Airport Shuttle Flag | checkbox |
| Spa Flag | checkbox |
| Beach / Waterfront Flag | checkbox |
| Branded Residences Flag | checkbox |
| Mixed-Use Flag | checkbox |
| Rooms / Keys | number |
| Rooms Source URL | url |
| Rooms Confidence | singleSelect |
| Building / Asset Notes | multilineText |
| Opening Date | date |
| Opening Date Source URL | url |
| Renovation / Conversion Status | singleLineText |
| Renovation / Conversion Date | date |
| Renovation / Conversion Source URL | url |
| Owner Name | singleLineText |
| Owner Type | singleSelect |
| Owner Source URL | url |
| Owner Confidence | singleSelect |
| Developer Name | singleLineText |
| Developer Source URL | url |
| Developer Confidence | singleSelect |
| Ownership Review Status | singleSelect |
| Operator / Management Company | singleLineText |
| Operator Type | singleSelect |
| Management Model | singleSelect |
| Operator Source URL | url |
| Operator Confidence | singleSelect |
| Operator Review Status | singleSelect |
| Possible Operator Target | checkbox |
| Independent Hotel Flag | checkbox |
| Independent Classification | singleSelect |
| Brand-Unassigned Reason | singleLineText |
| Possible Soft-Brand Candidate | checkbox |
| Possible Brand Conversion Candidate | checkbox |
| Possible Owner Outreach Target | checkbox |
| Possible Financing Target | checkbox |
| Possible Dealality Opportunity | checkbox |
| Data Confidence Tier | singleSelect |
| Relationship Confidence | singleSelect |
| Last Verified Date | date |
| Next Review Needed | date |
| Enrichment Status | singleSelect |
| Enrichment Priority | singleSelect |
| Human Review Required | checkbox |
| Notes for Steward | multilineText |

## Safe backfill proposed

```json
{
  "enrichment_status_not_started": 666,
  "human_review_required_true": 4,
  "data_confidence_tier_from_identity": 666,
  "enrichment_priority_derived": 666,
  "independent_flag_only_if_independent": 0,
  "possible_soft_brand_candidate": 89,
  "possible_brand_conversion_candidate": 4,
  "will_not_backfill": [
    "Hotel Description",
    "Amenities",
    "Owner",
    "Developer",
    "Operator / Management Company",
    "Rooms / Keys",
    "Opening Date",
    "Renovation Date",
    "Management Model"
  ]
}
```
