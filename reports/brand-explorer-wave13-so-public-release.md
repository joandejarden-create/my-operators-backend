# Brand Explorer Wave 13 — SO/ Public Release

Version: `wave13-so-public-release-v1` · Generated: 2026-07-28T07:15:57.514Z
Mode: **APPLY** · writePerformed: **true**

Active universe: **46** (expected 46)
Ready: `wave13_so_status_and_release_fields_applied_universe_46_pvql_blocked_on_section_pattern_parity`

**Post-release note:** SO/ is Active + release fields set + `shouldRenderFullProfile=true` (presentation Brand Name alias). PVQL public-full-only still fails SO/ `tab_factory_audit` / `section_pattern_parity` — content micro-cleanup required before 46 baseline freeze.

## Scope

- Release (1): `so-hotels-and-resorts`
- Untouched: active 45 · House · Morgans · Radisson Collection

## Founder acceptance

- founder_accepts_cleanly_unavailable_steward_posture: **true**

## Planned release fields

- `Active Profile Approved`
- `Ready for Active Profile`
- `Active Profile Approved Date`
- `Founder Visual Review Pass`

## Brand readiness

| Slug | Status | Active/Live | Needs release write |
| --- | --- | --- | --- |
| so-hotels-and-resorts | Active | true | true |

## Apply outcome

```json
{
  "applied": true,
  "results": [
    {
      "slug": "so-hotels-and-resorts",
      "recordId": "recTJdPlr4mDs9app",
      "applied": true,
      "writePerformed": true,
      "table": "Brand Setup - Brand Basics",
      "fieldMapping": {
        "activeProfileApproved": "Active Profile Approved",
        "readyForActiveProfile": "Ready for Active Profile",
        "activeProfileApprovedDate": "Active Profile Approved Date",
        "founderVisualReviewPass": "Founder Visual Review Pass"
      },
      "sanitizedPayloadPreview": {
        "Active Profile Approved": true,
        "Ready for Active Profile": true,
        "Active Profile Approved Date": "2026-07-28",
        "Founder Visual Review Pass": true
      },
      "response": {
        "id": "recTJdPlr4mDs9app",
        "fieldsPatched": [
          "Active Profile Approved",
          "Ready for Active Profile",
          "Active Profile Approved Date",
          "Founder Visual Review Pass"
        ]
      }
    }
  ],
  "intentionalRegistry": {
    "updated": true,
    "beforeCount": 35,
    "afterCount": 36,
    "soAdded": true,
    "soAlreadyInRegistry": false,
    "path": "data/brand-explorer-public-restore-intentional.json",
    "note": "SO/ added for intentional public restore; no content/CV/Source/Registry-approval writes."
  }
}
```

## Intentional restore registry

- Before count: 35
- After count: 36
- SO/ added: **true**

## Baseline freeze posture

- Active universe is 46 after SO/ release. Protected 45 freeze remains historical. Next task: freeze protected 46 Active/Live public-full baseline.

## Guardrails

- soOnly: true
- releaseFieldsOnly: true
- noBrandStatusWrites: true
- noContentRewrites: true
- noImageWrites: true
- noCompanyValidationChanges: true
- noSourceLibraryStatusChanges: true
- noRegistryApprovalChanges: true
- noActive45Writes: true
- noHouseOfOriginalsWrites: true
- noMorgansOriginalsWrites: true
- noRadissonCollectionChanges: true
- excludedSlugs: the-house-of-originals, morgans-originals, radisson-collection
- neverWriteFields: Company Validated, Company Validation Date, Source Library status, Registry approval/status, Source Library Status, Registry Approval, Registry Status
