# Mexico VIC → Production Census Dry-Run

**Dry-run only — execute: false**
**Freeze hash:** `c1cb244a95d7311b4ab2cf31d4988685879ef492f4f6420710633267d0effda3`
**Freeze match:** true
**Total VIC records:** 666
**Data eligible:** 580

## Counts

| Metric | Count |
| --- | ---: |
| Records to create | 666 |
| Records to update | 0 |
| Skipped | 0 |
| Held | 4 |
| Missing required fields | 0 |
| Duplicate risks | 0 |
| Brand-unconfirmed (ids) | 4 |
| Independent / non-brand-ready samples | 0 |

### By affiliation status

| Status | Count |
| --- | ---: |
| Branded | 569 |
| Soft-Branded / Collection | 89 |
| Brand-Unconfirmed | 4 |
| Independent | 0 |
| Formerly Branded | 0 |
| Future / Pipeline | 4 |
| Unknown | 0 |

## Fields to be written

- Property Name
- Canonical Property Name
- Property Identity Key
- Family / Source Family
- Country
- State / Region
- City
- Address
- Official Property URL
- Source URL
- Source Type
- Source Confidence
- Discovery Date
- VIC Freeze Hash
- Data Eligible
- Identity Confidence
- Production Use Status
- Current Brand
- Brand Family
- Brand Explorer Slug if mapped
- Affiliation Status
- Affiliation As-Of Date
- Future Opening Flag
- Brand Confidence
- Steward Review Status
- Latitude
- Longitude
- Phone

## Fields explicitly not written

- Rooms
- Owner
- Operator
- Opening Date
- Affiliation Start Date (unless source-supported — VIC policy: never fabricate)
- Brand Explorer Presentation Title/Body/Slot Key
- Brand Basics
- Brand Status
- release fields
- Company Validated
- Brand Verified
- Recent Momentum
- any public-rendered Brand Explorer field
- coordinates when missing (never 0,0)

## Production safety status

```json
{
  "production_writes_executed": false,
  "execute": false,
  "freeze_artifacts_modified": false,
  "brand_explorer_modified": false,
  "schema_ready_for_write": true,
  "founder_approval_required": true,
  "production_census_write_may_proceed": false,
  "reason_blocked": "Schema present — awaiting founder approval of census record dry-run (no execute yet)"
}
```

## Held sample (first 10)

```json
[
  {
    "independent_record_id": "ind_marriott_mx_slwak",
    "reason": "marriott_steward_manual_review_required",
    "affiliation_status": "Brand-Unconfirmed",
    "steward_review_status": "steward_manual_review_required"
  },
  {
    "independent_record_id": "ind_marriott_mx_mtyjd",
    "reason": "marriott_steward_manual_review_required",
    "affiliation_status": "Brand-Unconfirmed",
    "steward_review_status": "steward_manual_review_required"
  },
  {
    "independent_record_id": "ind_marriott_mx_pbcde",
    "reason": "marriott_steward_exclude_from_brand_completion",
    "affiliation_status": "Brand-Unconfirmed",
    "steward_review_status": "exclude_from_brand_completion"
  },
  {
    "independent_record_id": "ind_marriott_mx_gdlcc",
    "reason": "marriott_steward_exclude_from_brand_completion",
    "affiliation_status": "Brand-Unconfirmed",
    "steward_review_status": "exclude_from_brand_completion"
  }
]
```

## Decision

- Production Census write may proceed: **false**
- Reason: Schema present — awaiting founder approval of census record dry-run (no execute yet)
