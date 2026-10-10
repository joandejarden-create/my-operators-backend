# Bethesda Marriott — Regression Case

Certification status: **QA_REVIEW_REQUIRED**
Result: **PASS**

- ✅ CERTIFIED_OR_REVIEW_NOT_HARD_FAIL_IDENTITY
- ✅ SCENARIO_MANIFEST_PRESENT
- ✅ PROVIDER_COMPLETENESS
- ✅ RAW_RECOMPUTE
- ✅ SOURCE_CLASSIFICATION

## Detail
```json
{
  "propertyId": "adp_bethesda_marriott",
  "status": "QA_REVIEW_REQUIRED",
  "pass": true,
  "findings": [
    {
      "test": "CERTIFIED_OR_REVIEW_NOT_HARD_FAIL_IDENTITY",
      "pass": true,
      "detail": {
        "status": "QA_REVIEW_REQUIRED",
        "hard": []
      }
    },
    {
      "test": "SCENARIO_MANIFEST_PRESENT",
      "pass": true,
      "detail": {
        "scenarioCount": 78
      }
    },
    {
      "test": "PROVIDER_COMPLETENESS",
      "pass": true,
      "detail": {
        "expected": 252,
        "successful": 252,
        "failed": 0
      }
    },
    {
      "test": "RAW_RECOMPUTE",
      "pass": true,
      "detail": {
        "stored": {
          "aiConsideration": null,
          "scenarioPresence": null,
          "ownedSourceShare": null
        },
        "recomputed": {
          "aiConsideration": 42.9,
          "scenarioPresence": 61.5
        }
      }
    },
    {
      "test": "SOURCE_CLASSIFICATION",
      "pass": true,
      "detail": {
        "topSourceSupportingThisProperty": {
          "domain": "marriott.com",
          "count": 115,
          "taxonomy": "PROPERTY_OWNED",
          "ownershipType": "PROPERTY_OWNED",
          "brand": "Marriott Hotels"
        },
        "topOwnedBrandSource": {
          "domain": "marriott.com",
          "count": 115,
          "taxonomy": "PROPERTY_OWNED",
          "ownershipType": "PROPERTY_OWNED",
          "brand": "Marriott Hotels"
        },
        "topExternalPropertySource": {
          "domain": "hotels.com",
          "count": 87,
          "taxonomy": "PROPERTY_SPECIFIC_EXTERNAL",
          "ownershipType": "GENERAL_MARKET",
          "brand": null
        },
        "topCompetitiveUniverseSource": {
          "domain": "hyatt.com",
          "count": 24,
          "taxonomy": "COMPETITOR_OWNED",
          "ownershipType": "COMPETITOR_OWNED",
          "brand": "Hyatt"
        }
      }
    }
  ]
}
```
