# Production Census Airtable Schema Plan

**Status:** `production_census_dry_run_ready_for_founder_approval`
**Version:** production-census-and-be-patch-plan-v1
**Operator Explorer:** paused

## Decision

- Production Airtable Census foundation first
- Brand Explorer production updates controlled (no bulk public patch)
- Operator Explorer remains paused

## Recommended tables

1. **Hotel Property Census** — Primary hotel/property identity
1. **Hotel Property Brand Affiliations** — Current / historical / future / soft-brand / brand-unconfirmed / independent states
1. **Hotel Property Source Evidence** — Source lineage, URL, type, discovery date, confidence, freeze hash
1. **Hotel Property Steward Review** — Holds, ambiguity, duplicate risk, brand-unconfirmed, manual decisions

## Required fields

### Identity

- Property Name
- Canonical Property Name
- Property Identity Key
- Family / Source Family
- Country
- State / Region
- City
- Address
- Latitude
- Longitude
- Phone
- Official Property URL
- Source URL
- Source Type
- Source Confidence
- Discovery Date
- VIC Freeze Hash
- Data Eligible
- Identity Confidence
- Production Use Status

### Affiliation

- Current Brand
- Brand Family
- Brand Explorer Slug if mapped
- Affiliation Status
- Affiliation As-Of Date
- Affiliation Start Date
- Prior Brand
- Future Opening Flag
- Brand Confidence
- Steward Review Status

### Affiliation statuses

- Branded
- Soft-Branded / Collection
- Brand-Unconfirmed
- Independent
- Formerly Branded
- Future / Pipeline
- Unknown

## Existing production schema gaps

- **Verified Independent Hotel Census:** field_model_incomplete_vs_hotel_property_census (Do not silently remap VIC freeze into this stub; prefer new Hotel Property Census tables)
- **Hotel Census:** legacy_str_not_write_target — Keep read-only; never receive VIC production writes

## Do tables already exist?

- Proposed four tables exist: **yes**
- Schema must be created manually: **no**
- Recommended base: platform (AIRTABLE_BASE_ID_ALT) — isolate from MVP Brand Explorer rendering

## Write rules

- Allowed: create/update records in dedicated Census tables only (after schema + founder approval)
- Mark: `Production Use Status = Census Only / Not Owner-Facing`
- Forbidden now: Brand Setup - Brand Explorer Presentation; Brand Setup - Brand Basics; Brand Status / release fields; Company Validated; Brand Verified; Recent Momentum; Hotel Census (legacy STR)

## Safe vs unsafe fields

**Safe (when schema + approval):**

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

**Unsafe / held:**

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

## Probe summary

```json
{
  "platform": "appCCU…",
  "prod_mvp": "appvtn…",
  "proposed": {
    "Hotel Property Census": {
      "exists": true,
      "platform": {
        "id": "tbl9aY5ijiuIzzWam",
        "field_count": 33
      },
      "prod_mvp": null
    },
    "Hotel Property Brand Affiliations": {
      "exists": true,
      "platform": {
        "id": "tbll7n0xgmYywyrTd",
        "field_count": 15
      },
      "prod_mvp": null
    },
    "Hotel Property Source Evidence": {
      "exists": true,
      "platform": {
        "id": "tblfhosu44nMbvSbS",
        "field_count": 10
      },
      "prod_mvp": null
    },
    "Hotel Property Steward Review": {
      "exists": true,
      "platform": {
        "id": "tbluxLjGTuKGRO2iM",
        "field_count": 15
      },
      "prod_mvp": null
    }
  }
}
```


## Scope

Schema plan only. No Airtable table creation or writes from this command.
