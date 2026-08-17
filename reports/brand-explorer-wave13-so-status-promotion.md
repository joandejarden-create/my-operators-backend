# Brand Explorer Wave 13 — SO/ Status Promotion

Version: `wave13-so-status-promotion-v1` · Generated: 2026-07-28T07:15:19.216Z
Mode: **APPLY** · writePerformed: **true**

Target Brand Status: **Active**
Active universe before: **45** (expected 45)
Active universe after: **46** (expected 46)

## Scope

- Promote (1): `so-hotels-and-resorts` (`recTJdPlr4mDs9app`)
- Untouched: active 45 · House of Originals · Morgans Originals · Radisson Collection

## Founder acceptance

- founder_accepts_cleanly_unavailable_steward_posture: **true**
- promotion_recommendation: **approve_for_status_promotion_and_public_release**

## Preflight

- Protected 45 baseline: **PASS**
- SO/ Under Review → Active: **PASS**
- Preflight OK: **true**

## Planned patch (Brand Status only)

| Slug | From | To | Needs write |
| --- | --- | --- | --- |
| so-hotels-and-resorts | Under Review | Active | true |

## Apply results

```json
[
  {
    "slug": "so-hotels-and-resorts",
    "recordId": "recTJdPlr4mDs9app",
    "applied": true,
    "writePerformed": true,
    "table": "Brand Setup - Brand Basics",
    "fieldMapping": {
      "brandStatus": "Brand Status"
    },
    "sanitizedPayloadPreview": {
      "Brand Status": "Active"
    },
    "response": {
      "id": "recTJdPlr4mDs9app",
      "fieldsPatched": [
        "Brand Status"
      ]
    }
  }
]
```

## Guardrails

- soOnly: true
- brandStatusOnly: true
- noContentWrites: true
- noImageWrites: true
- noReleaseFieldWrites: true
- noCompanyValidationChanges: true
- noSourceLibraryStatusChanges: true
- noRegistryApprovalChanges: true
- noActive45Writes: true
- noHouseOfOriginalsWrites: true
- noMorgansOriginalsWrites: true
- noRadissonCollectionChanges: true
- excludedSlugs: the-house-of-originals, morgans-originals, radisson-collection
- neverWriteFields: Company Validated, Company Validation Date, Source Library status, Registry approval/status, Source Library Status, Registry Approval, Registry Status
- priorPartialActiveCount: 45

Ready: `wave13_so_status_promotion_complete_ready_for_public_release`
