# Wave 01 Apply Results

```json
{
  "mode": "apply",
  "webhound": "Deferred — supplemental enrichment incomplete",
  "startedAt": "2026-08-10T15:50:16.791Z",
  "views": {
    "apiCreateSupported": false,
    "fieldsSynced": [
      {
        "name": "OE Explorer Publishable",
        "id": "fldtrhlTixyANK7Ut"
      },
      {
        "name": "OE Strong Profile",
        "id": "fldf8jKezLVmbZGKz"
      },
      {
        "name": "OE Fit Data Ready",
        "id": "fld1PPzj3qReRaG6g"
      },
      {
        "name": "OE Enrichment Class",
        "id": "fldHjkRYggWqklvl5"
      }
    ],
    "recipes": [
      {
        "name": "OE — All",
        "filter": "(none)",
        "expected": 46
      },
      {
        "name": "OE — Production",
        "filter": "{Record Purpose} = \"Production\"",
        "expected": 24
      },
      {
        "name": "OE — Research",
        "filter": "{Record Purpose} = \"Research\"",
        "expected": 13
      },
      {
        "name": "OE — Test Fixtures",
        "filter": "{Record Purpose} = \"Test Fixture\"",
        "expected": 9
      },
      {
        "name": "OE — Explorer Publishable",
        "filter": "{OE Explorer Publishable}",
        "expected": "dynamic"
      },
      {
        "name": "OE — Needs Enrichment",
        "filter": "OR({OE Enrichment Class}=\"Production Needs Enrichment\",{OE Enrichment Class}=\"Research Needs Enrichment\",{OE Enrichment Class}=\"Research Content Complete Gated\")",
        "expected": "dynamic"
      },
      {
        "name": "OE — Strong Profiles",
        "filter": "{OE Strong Profile}",
        "expected": "dynamic"
      },
      {
        "name": "OE — Fit Data Ready",
        "filter": "{OE Fit Data Ready}",
        "expected": "dynamic"
      }
    ],
    "apiCreateAttempt": {
      "ok": false,
      "status": 422,
      "error": {
        "type": "INVALID_REQUEST_UNKNOWN",
        "message": "Invalid request: parameter validation failed. Check your request data."
      }
    }
  },
  "baseline": {
    "totalMasters": 46,
    "production": 24,
    "research": 13,
    "testFixtures": 9,
    "realOperators": 37,
    "explorerContentComplete": 17,
    "explorerPublishable": 9,
    "strongProfiles": 5,
    "contentCompleteButLifecycleGated": 8,
    "fitDataReady": 4,
    "fitConditional": 15,
    "fitResearchRequired": 27,
    "calibrationMembership": 27,
    "brandManagedMembership": 15
  },
  "after": {
    "totalMasters": 46,
    "production": 24,
    "research": 13,
    "testFixtures": 9,
    "realOperators": 37,
    "explorerContentComplete": 27,
    "explorerPublishable": 19,
    "strongProfiles": 5,
    "contentCompleteButLifecycleGated": 8,
    "fitDataReady": 4,
    "fitConditional": 24,
    "fitResearchRequired": 18,
    "calibrationMembership": 27,
    "brandManagedMembership": 15
  },
  "wave": {
    "operators": [
      "Marriott International (Managed)",
      "Hilton (Managed)",
      "Accor (Managed)",
      "IHG Hotels & Resorts (Managed)",
      "Minor Hotels (Managed)",
      "Atlantica Hotels International (AHI)",
      "Grupo Iberostar",
      "Tafer Hotels & Resorts",
      "Royalton Hotels & Resorts",
      "Grupo Presidente"
    ]
  },
  "assignments": {
    "created": 27,
    "held": 1,
    "failed": []
  },
  "presence": {
    "created": 14
  },
  "sources": {
    "created": 25,
    "reused": 2
  },
  "brandRel": {
    "created": 0
  },
  "mastersPatched": 46,
  "holdouts": [
    {
      "operator": "Tafer Hotels & Resorts",
      "property": "Grand Fiesta Americana Coral Beach Cancun",
      "reason": "Possible Posadas vs Tafer counterparty ambiguity"
    }
  ]
}
```

Backup: `backups/operator-explorer/wave-01/2026-08-10T15-50-16/`
