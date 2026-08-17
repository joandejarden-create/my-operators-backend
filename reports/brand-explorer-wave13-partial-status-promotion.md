# Brand Explorer Wave 13 — Partial Status Promotion

Version: `wave13-partial-status-promotion-v1` · Generated: 2026-07-27T18:17:16.293Z
Mode: **APPLY** · writePerformed: **true**

Target Brand Status: **Active**
Active universe before: **39** (expected 39)
Active universe after: **45** (expected 45)

## Scope

- Promote (6): `mama-shelter`, `mercure`, `ibis`, `novotel`, `pullman`, `fairmont-hotels-and-resorts`
- Held: `so-hotels-and-resorts` (no Brand Status write)
- Excluded: House of Originals · Morgans Originals · Radisson Collection

## Preflight

- Protected 39 baseline: **PASS**
- Founder six-only approvals: **PASS**
- SO/ held Under Review: **PASS** (Under Review)
- Status gate (Under Review → Active): **PASS**
- `--approved-only`: **yes**

## Planned patches (Brand Status only)

| Slug | Record | From | To | Needs write |
| --- | --- | --- | --- | --- |
| mama-shelter | `recXCZCK05XXYX7Q8` | Under Review | Active | true |
| mercure | `recevrLJ3m6rIug3S` | Under Review | Active | true |
| ibis | `reclFXbpZ5XzLWbGP` | Under Review | Active | true |
| novotel | `recQE2lSSSSyuUrMQ` | Under Review | Active | true |
| pullman | `recFW9kfqKfOjv7Z1` | Under Review | Active | true |
| fairmont-hotels-and-resorts | `recJhPaDVU3YUDQUt` | Under Review | Active | true |

## Apply results

```json
[
  {
    "slug": "mama-shelter",
    "recordId": "recXCZCK05XXYX7Q8",
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
      "id": "recXCZCK05XXYX7Q8",
      "fieldsPatched": [
        "Brand Status"
      ]
    }
  },
  {
    "slug": "mercure",
    "recordId": "recevrLJ3m6rIug3S",
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
      "id": "recevrLJ3m6rIug3S",
      "fieldsPatched": [
        "Brand Status"
      ]
    }
  },
  {
    "slug": "ibis",
    "recordId": "reclFXbpZ5XzLWbGP",
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
      "id": "reclFXbpZ5XzLWbGP",
      "fieldsPatched": [
        "Brand Status"
      ]
    }
  },
  {
    "slug": "novotel",
    "recordId": "recQE2lSSSSyuUrMQ",
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
      "id": "recQE2lSSSSyuUrMQ",
      "fieldsPatched": [
        "Brand Status"
      ]
    }
  },
  {
    "slug": "pullman",
    "recordId": "recFW9kfqKfOjv7Z1",
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
      "id": "recFW9kfqKfOjv7Z1",
      "fieldsPatched": [
        "Brand Status"
      ]
    }
  },
  {
    "slug": "fairmont-hotels-and-resorts",
    "recordId": "recJhPaDVU3YUDQUt",
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
      "id": "recJhPaDVU3YUDQUt",
      "fieldsPatched": [
        "Brand Status"
      ]
    }
  }
]
```

## Guardrails

- targetBrandsOnly: true
- sixApprovedOnly: true
- soHeld: true
- singleFieldPayload: true
- companyValidatedWrites: false
- sourceLibraryWrites: false
- registryWrites: false
- contentWrites: false
- imageWrites: false
- releaseFieldWrites: false
- protected39Untouched: true
- houseOfOriginalsUntouched: true
- morgansOriginalsUntouched: true
- radissonCollectionUntouched: true
- neverWriteFields: Company Validated, Company Validation Date, Source Library status, Registry approval/status, Source Library Status, Registry Approval, Registry Status
- baselineConvention: frozen_39_were_Active

## Post-release validation (appended)

- Generated: 2026-07-27T19:07:34.453Z
- Active universe after Stage 10: **45**
- PVQL public-full-only: **FAIL** (six Wave 13 `tab_factory_audit` / section_pattern_parity)
- 24-tab quality: **do_not_freeze_remediation_required** (39 approve / 6 remediation)
- Ready statement (writes): `wave13_six_brand_partial_release_complete_so_held`
- Full acceptance (incl. PVQL+quality): **false** until section-pattern cleanup
