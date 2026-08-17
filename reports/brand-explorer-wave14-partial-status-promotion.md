# Brand Explorer Wave 14 — Partial Status Promotion

Version: `wave14-partial-status-promotion-v1` · Generated: 2026-07-28T22:32:57.222Z
Mode: **APPLY** · writePerformed: **true**

Target Brand Status: **Active**
Active universe before: **46** (expected 46)
Active universe after: **54** (expected 54)

## Scope

- Promote (8): `marriott-hotels`, `sheraton`, `westin`, `residence-inn-by-marriott`, `springhill-suites-by-marriott`, `towneplace-suites-by-marriott`, `aloft-hotels`, `studiores`
- Held: `four-points-flex-by-sheraton` (no Brand Status write)
- Excluded: House of Originals · Morgans Originals · Radisson Collection

## Preflight

- Protected 46 baseline: **PASS**
- Founder eight-only approvals: **PASS**
- Four Points Flex held Under Review: **PASS** (Under Review)
- Status gate (Under Review → Active): **PASS**
- `--approved-only`: **yes**

## Planned patches (Brand Status only)

| Slug | Record | From | To | Needs write |
| --- | --- | --- | --- | --- |
| marriott-hotels | `recn59UtkyyoYwzSz` | Under Review | Active | true |
| sheraton | `recg8HjT5Bky7NXeV` | Under Review | Active | true |
| westin | `recIPuBC50fv13zRR` | Under Review | Active | true |
| residence-inn-by-marriott | `rec9Ufbpa0GxJGzt8` | Under Review | Active | true |
| springhill-suites-by-marriott | `recBzdGfkMUN9fYsv` | Under Review | Active | true |
| towneplace-suites-by-marriott | `recUPiPDivkhNUogr` | Under Review | Active | true |
| aloft-hotels | `recJ1GZQpttX7qHgw` | Under Review | Active | true |
| studiores | `recDM0LAD8jVRA2x3` | Under Review | Active | true |

## Apply results

```json
[
  {
    "slug": "marriott-hotels",
    "recordId": "recn59UtkyyoYwzSz",
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
      "id": "recn59UtkyyoYwzSz",
      "fieldsPatched": [
        "Brand Status"
      ]
    }
  },
  {
    "slug": "sheraton",
    "recordId": "recg8HjT5Bky7NXeV",
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
      "id": "recg8HjT5Bky7NXeV",
      "fieldsPatched": [
        "Brand Status"
      ]
    }
  },
  {
    "slug": "westin",
    "recordId": "recIPuBC50fv13zRR",
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
      "id": "recIPuBC50fv13zRR",
      "fieldsPatched": [
        "Brand Status"
      ]
    }
  },
  {
    "slug": "residence-inn-by-marriott",
    "recordId": "rec9Ufbpa0GxJGzt8",
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
      "id": "rec9Ufbpa0GxJGzt8",
      "fieldsPatched": [
        "Brand Status"
      ]
    }
  },
  {
    "slug": "springhill-suites-by-marriott",
    "recordId": "recBzdGfkMUN9fYsv",
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
      "id": "recBzdGfkMUN9fYsv",
      "fieldsPatched": [
        "Brand Status"
      ]
    }
  },
  {
    "slug": "towneplace-suites-by-marriott",
    "recordId": "recUPiPDivkhNUogr",
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
      "id": "recUPiPDivkhNUogr",
      "fieldsPatched": [
        "Brand Status"
      ]
    }
  },
  {
    "slug": "aloft-hotels",
    "recordId": "recJ1GZQpttX7qHgw",
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
      "id": "recJ1GZQpttX7qHgw",
      "fieldsPatched": [
        "Brand Status"
      ]
    }
  },
  {
    "slug": "studiores",
    "recordId": "recDM0LAD8jVRA2x3",
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
      "id": "recDM0LAD8jVRA2x3",
      "fieldsPatched": [
        "Brand Status"
      ]
    }
  }
]
```

## Guardrails

- targetBrandsOnly: true
- eightApprovedOnly: true
- flexHeld: true
- singleFieldPayload: true
- companyValidatedWrites: false
- sourceLibraryWrites: false
- registryWrites: false
- contentWrites: false
- imageWrites: false
- releaseFieldWrites: false
- protected46Untouched: true
- houseOfOriginalsUntouched: true
- morgansOriginalsUntouched: true
- radissonCollectionUntouched: true
- neverWriteFields: Company Validated, Company Validation Date, Source Library status, Registry approval/status, Source Library Status, Registry Approval, Registry Status, Brand Status
- baselineConvention: frozen_46_were_Active
