# Brand Explorer Wave 15 — Status Promotion

Version: `wave15-status-promotion-v1` · Generated: 2026-08-04T23:31:53.224Z
Mode: **APPLY** · writePerformed: **true**

Target Brand Status: **Active**
Active universe before: **54** (expected 54)
Active universe after: **62** (expected 62)

## Scope

- Promote (8, all — no held slug in cohort): `hilton-hotels-and-resorts`, `homewood-suites-by-hilton`, `home2-suites-by-hilton`, `tru-by-hilton`, `doubletree-by-hilton`, `hampton-by-hilton`, `hilton-garden-inn`, `spark-by-hilton`
- Four Points Flex by Sheraton: held **outside** the Wave 15 cohort (verified Under Review, not written)
- Excluded: House of Originals · Morgans Originals · Radisson Collection

## Preflight

- Protected 54 baseline: **PASS**
- Founder eight approvals: **PASS**
- Four Points Flex verified held (Under Review, not in active universe): **PASS** (Under Review)
- Status gate (Under Review → Active): **PASS**

## Planned patches (Brand Status only)

| Slug | Record | From | To | Needs write |
| --- | --- | --- | --- | --- |
| hilton-hotels-and-resorts | `recWubG3rhiS1BaWi` | Under Review | Active | true |
| homewood-suites-by-hilton | `recZjYI4nYflGHFNR` | Under Review | Active | true |
| home2-suites-by-hilton | `reccZ4zV6wMav7a2i` | Under Review | Active | true |
| tru-by-hilton | `recJLiMTv4W8VgO9L` | Under Review | Active | true |
| doubletree-by-hilton | `rechVYWQ5ikRnr99B` | Under Review | Active | true |
| hampton-by-hilton | `rectRvOWQPaL6FkzZ` | Under Review | Active | true |
| hilton-garden-inn | `recrvdAjRlXxPvPPF` | Under Review | Active | true |
| spark-by-hilton | `recfv66er4Ch2vJDO` | Under Review | Active | true |

## Apply results

```json
[
  {
    "slug": "hilton-hotels-and-resorts",
    "recordId": "recWubG3rhiS1BaWi",
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
      "id": "recWubG3rhiS1BaWi",
      "fieldsPatched": [
        "Brand Status"
      ]
    }
  },
  {
    "slug": "homewood-suites-by-hilton",
    "recordId": "recZjYI4nYflGHFNR",
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
      "id": "recZjYI4nYflGHFNR",
      "fieldsPatched": [
        "Brand Status"
      ]
    }
  },
  {
    "slug": "home2-suites-by-hilton",
    "recordId": "reccZ4zV6wMav7a2i",
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
      "id": "reccZ4zV6wMav7a2i",
      "fieldsPatched": [
        "Brand Status"
      ]
    }
  },
  {
    "slug": "tru-by-hilton",
    "recordId": "recJLiMTv4W8VgO9L",
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
      "id": "recJLiMTv4W8VgO9L",
      "fieldsPatched": [
        "Brand Status"
      ]
    }
  },
  {
    "slug": "doubletree-by-hilton",
    "recordId": "rechVYWQ5ikRnr99B",
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
      "id": "rechVYWQ5ikRnr99B",
      "fieldsPatched": [
        "Brand Status"
      ]
    }
  },
  {
    "slug": "hampton-by-hilton",
    "recordId": "rectRvOWQPaL6FkzZ",
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
      "id": "rectRvOWQPaL6FkzZ",
      "fieldsPatched": [
        "Brand Status"
      ]
    }
  },
  {
    "slug": "hilton-garden-inn",
    "recordId": "recrvdAjRlXxPvPPF",
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
      "id": "recrvdAjRlXxPvPPF",
      "fieldsPatched": [
        "Brand Status"
      ]
    }
  },
  {
    "slug": "spark-by-hilton",
    "recordId": "recfv66er4Ch2vJDO",
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
      "id": "recfv66er4Ch2vJDO",
      "fieldsPatched": [
        "Brand Status"
      ]
    }
  }
]
```

## Guardrails

- targetBrandsOnly: true
- allEightApprovedNoHeldSlugInCohort: true
- flexHeldOutsideCohort: true
- flexUntouched: true
- singleFieldPayload: true
- companyValidatedWrites: false
- sourceLibraryWrites: false
- registryWrites: false
- contentWrites: false
- imageWrites: false
- releaseFieldWrites: false
- protected54Untouched: true
- protected54ReadOnlyValidation: true
- houseOfOriginalsUntouched: true
- morgansOriginalsUntouched: true
- radissonCollectionUntouched: true
- neverWriteFields: Company Validated, Company Validation Date, Source Library status, Registry approval/status, Source Library Status, Registry Approval, Registry Status, Brand Status
- baselineConvention: frozen_54_were_Active
- writeThrottleMs: 280
