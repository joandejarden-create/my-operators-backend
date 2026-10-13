# Renaissance New York Times Square — Regression Case

Result: **PASS**

- ✅ SCENARIO_UNIVERSE_VERSIONED
- ✅ PROP_RTS_TRACKED_OR_DOCUMENTED
- ✅ IDENTITY_ALIASES
- ✅ OWNED_DOMAIN_CLASSIFICATION
- ✅ COMPARABILITY_VS_HILTON

## Detail
```json
{
  "propertyId": "adp_renaissance_times_square",
  "pass": true,
  "findings": [
    {
      "test": "SCENARIO_UNIVERSE_VERSIONED",
      "pass": true,
      "detail": {
        "scenarioUniverseId": "su_f8fbc39f3cf16385",
        "scenarioCount": 80
      }
    },
    {
      "test": "PROP_RTS_TRACKED_OR_DOCUMENTED",
      "pass": true,
      "detail": {
        "propRtsCount": 15,
        "sample": [
          "prop_rts_01",
          "prop_rts_02",
          "prop_rts_03",
          "prop_rts_04",
          "prop_rts_05"
        ]
      }
    },
    {
      "test": "IDENTITY_ALIASES",
      "pass": true,
      "detail": {
        "failed": []
      }
    },
    {
      "test": "OWNED_DOMAIN_CLASSIFICATION",
      "pass": true,
      "detail": {
        "domain": "marriott.com",
        "ownershipType": "BRAND_OWNED",
        "brand": "Marriott",
        "portfolio": "Marriott",
        "propertyId": "adp_renaissance_times_square"
      }
    },
    {
      "test": "COMPARABILITY_VS_HILTON",
      "pass": true,
      "detail": {
        "outcome": "DIRECTIONAL_ONLY",
        "commonCount": 50,
        "formal": false
      }
    }
  ]
}
```
