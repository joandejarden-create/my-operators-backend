# Hilton New York Times Square — Regression Case

Result: **PASS**

- ✅ IDENTITY_MISSING_ALIAS_CANARY
- ✅ IDENTITY_CANARY_CURRENT
- ✅ SCENARIO_MISMATCH_50_VS_65
- ✅ MARRIOTT_COMPETITOR_UNIVERSE_SOURCE
- ✅ HILTON_BRAND_OWNED
- ✅ COMPETITOR_SOURCE_MISLABEL_CAUGHT

## Detail
```json
{
  "propertyId": "adp_hilton_times_square",
  "pass": true,
  "findings": [
    {
      "test": "IDENTITY_MISSING_ALIAS_CANARY",
      "pass": true,
      "detail": {
        "ok": true,
        "propertyId": "adp_hilton_times_square",
        "criticalAlias": "Hilton Times Square",
        "recognizedWithAlias": true,
        "recognizedWithoutAlias": false,
        "missingAliasWouldFailCanary": true
      }
    },
    {
      "test": "IDENTITY_CANARY_CURRENT",
      "pass": true,
      "detail": {
        "failed": []
      }
    },
    {
      "test": "SCENARIO_MISMATCH_50_VS_65",
      "pass": true,
      "detail": {
        "outcome": "COMMON_SET_COMPARABLE",
        "reasons": [
          "scenario_count_mismatch",
          "scenario_id_set_mismatch",
          "legacy_uncertified_metadata"
        ]
      }
    },
    {
      "test": "MARRIOTT_COMPETITOR_UNIVERSE_SOURCE",
      "pass": true,
      "detail": {
        "domain": "marriott.com",
        "ownershipType": "COMPETITOR_OWNED",
        "brand": "Marriott",
        "portfolio": "Marriott",
        "propertyId": null
      }
    },
    {
      "test": "HILTON_BRAND_OWNED",
      "pass": true,
      "detail": {
        "domain": "hilton.com",
        "ownershipType": "BRAND_OWNED",
        "brand": "Hilton",
        "portfolio": "Hilton",
        "propertyId": "adp_hilton_times_square"
      }
    },
    {
      "test": "COMPETITOR_SOURCE_MISLABEL_CAUGHT",
      "pass": true,
      "detail": [
        {
          "code": "COMPETITOR_SOURCE_MISLABELED_AS_PROPERTY_TOP_SOURCE",
          "domain": "marriott.com",
          "taxonomy": "COMPETITOR_OWNED"
        }
      ]
    }
  ]
}
```
