# Brand AI Benchmark Cohort Remediation V1

**Final:** BRAND_AI_BENCHMARK_COHORT_REMEDIATION_PASS
**Next:** BRAND_SCENARIO_BENCHMARK_VALIDATION

## Architecture

- BENCHMARK_UNIVERSE: benchmark_eligible_brands_v1 (27 brands)
- SCENARIO_SPECIFIC: PASS
- FULL_SET_FALLBACK_REMOVED: YES
- UNION_GRAIN_REMOVED: YES
- COMMON_GRAIN_METHOD: PAIRWISE
- WHY: Stored Brand AI prompts are OPEN_ENDED and shared. Pairwise Presence on scenario measurement grains equals a scenario-level shared denominator without collapsing to grains where every peer was mentioned. Global all-cohort positive-mention intersection would be too sparse. Pairwise preserves evidence and stays comparable.

## Headline / aggregation

- HEADLINE_INDEX_AGGREGATION: DEFERRED
- MEDIAN_VS_MEAN: DEFERRED
- CUSTOMER_INDEX_STATUS: INTERNAL_REVIEW_ONLY

## Scenario index readiness

- VALID: 45
- LIMITED: 24
- SUPPRESSED: 78

## Autograph

```json
{
  "SOFT_BRAND_AFFILIATION": {
    "CORE": [
      "Tribute Portfolio",
      "Curio Collection by Hilton",
      "Tapestry Collection by Hilton",
      "Ascend Hotel Collection",
      "Vignette Collection"
    ],
    "COMMON_DATA": [
      "Tribute Portfolio",
      "Curio Collection by Hilton",
      "Tapestry Collection by Hilton",
      "Ascend Hotel Collection",
      "Vignette Collection"
    ],
    "BENCHMARK_ELIGIBLE": true,
    "CURIO_INCLUDED": "YES",
    "VIGNETTE_INCLUDED": "YES",
    "SCENARIO_INDEX": 129,
    "STATUS": "VALID"
  },
  "CONVERSION": {
    "CORE": [
      "Tribute Portfolio",
      "Curio Collection by Hilton",
      "Tapestry Collection by Hilton",
      "Ascend Hotel Collection",
      "Vignette Collection"
    ],
    "COMMON_DATA": [
      "Tribute Portfolio",
      "Curio Collection by Hilton",
      "Tapestry Collection by Hilton",
      "Ascend Hotel Collection",
      "Vignette Collection"
    ],
    "BENCHMARK_ELIGIBLE": true,
    "CURIO_INCLUDED": "YES",
    "VIGNETTE_INCLUDED": "YES",
    "SCENARIO_INDEX": 177,
    "STATUS": "VALID",
    "scenarioId": "scenario_conversion_suitability_v1",
    "independentConversionStatus": "SUPPRESSED_INSUFFICIENT_DATA"
  }
}
```

## Hotel Indigo lifestyle

```json
{
  "LIFESTYLE": {
    "CORE": [
      "AC Hotels by Marriott",
      "Canopy by Hilton",
      "Tempo by Hilton",
      "Kimpton Hotels",
      "Voco Hotels"
    ],
    "KIMPTON": "CORE",
    "CANOPY": "CORE",
    "VOCO": "CORE",
    "TEMPO": "CORE",
    "AC": "CORE",
    "DESIGN": "SECONDARY",
    "RADISSON_RED": "SECONDARY",
    "SCENARIO_INDEX": 190,
    "STATUS": "VALID"
  }
}
```
