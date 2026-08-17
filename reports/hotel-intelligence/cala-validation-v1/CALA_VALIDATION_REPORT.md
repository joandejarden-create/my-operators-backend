# Dealality Hotel Intelligence — CALA Validation Report

**Marker:** `DEALALITY_HOTEL_INTELLIGENCE_CALA_VALIDATION_COMPLETE`
**Generated:** 2026-08-09T23:59:46.387Z

## 1. Safety

```
Airtable writes: 0
Migrations: 0
Schema changes: 0
Brand Explorer writes: 0
Secrets exposed: no
```

## 2. Live Census Baseline

- Universe records: **5956**
- CALA records: **5956**
- CALA rooms: 191 present / 5765 missing
- CALA brand: 2100 present / 3856 missing
- CALA coords: 836 present / 5120 missing
- CALA website: 3945 present / 2011 missing

Top countries: Mexico (2181), Colombia (967), Costa Rica (748), Dominican Republic (654), Brazil (494), Panama (325), Argentina (129), Jamaica (78), Chile (65), Peru (57)

## 3. Validation Sample

- Size: **400** (target 400)
- Seed: `hotel-intelligence-cala-validation-v1`
- Countries: {"Antigua and Barbuda":5,"Argentina":15,"Aruba":13,"Bahamas":9,"Barbados":15,"Belize":12,"Brazil":19,"British Virgin Islands":3,"Cayman Islands":7,"Chile":15,"Colombia":24,"Costa Rica":22,"Curaçao":5,"Dominica":2,"Dominican Republic":22,"Ecuador":15,"El Salvador":10,"Grenada":4,"Guatemala":13,"Honduras":12,"Jamaica":15,"Mexico":57,"Nicaragua":7,"Panama":21,"Peru":15,"Puerto Rico":15,"Saint Kitts and Nevis":3,"Saint Lucia":3,"Trinidad and Tobago":7,"Uruguay":15}
- Quality buckets: {"missing_coords+missing_rooms":233,"missing_rooms":38,"sparse":104,"complete":11,"missing_coords":14}

## 4. Resolution Results

| Class | Count | % |
| --- | ---: | ---: |
| exact | 338 | 84.5% |
| strong | 27 | 6.8% |
| probable | 12 | 3% |
| ambiguous | 23 | 5.8% |
| no_match | 0 | 0% |
| error | 0 | 0% |

- Exact+Strong rate: **91.3%**
- Self-match rate: **100%**
- Probable: 3% · Review: 8.8% · No match: 0%

## 5. Duplicate Risk

| Risk | Count |
| --- | ---: |
| safe_unique | 377 |
| possible_duplicate | 0 |
| probable_duplicate | 0 |
| ambiguous_identity | 23 |

## 6. Enrichment Yield

_External candidate metrics are measured on the HBX enrich subset when provider available; present/missing before cover the full sample._

| Field | Present Before | Missing Before | Candidate Found | High Confidence | Conflict | Still Missing |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| official_name | 400 | 0 | 0 | 0 | 0 | 0 |
| address_line_1 | 101 | 299 | 0 | 0 | 0 | 299 |
| city | 325 | 75 | 0 | 0 | 0 | 75 |
| country | 400 | 0 | 0 | 0 | 0 | 0 |
| latitude | 49 | 351 | 0 | 0 | 0 | 351 |
| longitude | 49 | 351 | 0 | 0 | 0 | 351 |
| room_count | 25 | 375 | 0 | 0 | 0 | 375 |
| brand_name | 296 | 104 | 0 | 0 | 0 | 104 |
| parent_company_name | 305 | 95 | 0 | 0 | 0 | 95 |
| website | 362 | 38 | 0 | 0 | 0 | 38 |
| phone | 105 | 295 | 0 | 0 | 0 | 295 |
| status | 400 | 0 | 0 | 0 | 0 | 0 |

## 7. Room Count

```
{
  "sample_records": 400,
  "room_count_present_before": 25,
  "room_count_missing_before": 375,
  "room_count_found_for_missing": 0,
  "room_count_high_confidence": 0,
  "room_count_conflicts": 0,
  "room_count_unresolved": 375,
  "conflict_magnitude": {
    "1-2": 0,
    "3-5": 0,
    "6-10": 0,
    ">10": 0
  },
  "conflicts": [],
  "note": "HBX unavailable — room recovery from external provider not measured; resolution_validation complete"
}
```

## 8. Brand / Parent

```
{
  "brand_present_before": 296,
  "brand_missing_before": 104,
  "brand_candidate_found": 0,
  "brand_exact_agreement": 0,
  "brand_normalization_only": 0,
  "brand_conflicts": 0,
  "parent_company_candidate_found": 0,
  "unresolved_source_labels": []
}
```

## 9. Confidence

```
{
  "identity": {
    "0.95-1.00": 338,
    "0.85-0.94": 27,
    "0.70-0.84": 12,
    "0.50-0.69": 23,
    "<0.50": 0,
    "unknown": 0
  },
  "room_count": {
    "0.95-1.00": 0,
    "0.85-0.94": 0,
    "0.70-0.84": 0,
    "0.50-0.69": 0,
    "<0.50": 0,
    "unknown": 0
  },
  "brand": {
    "0.95-1.00": 0,
    "0.85-0.94": 0,
    "0.70-0.84": 0,
    "0.50-0.69": 0,
    "<0.50": 0,
    "unknown": 0
  },
  "coordinates": {
    "0.95-1.00": 0,
    "0.85-0.94": 0,
    "0.70-0.84": 0,
    "0.50-0.69": 0,
    "<0.50": 0,
    "unknown": 0
  }
}
```

## 10. Review Queue

Issue counts: {"identity_ambiguous":1,"possible_duplicate":12}

Top examples: see `10-review-queue.json` (13 listed).

## 11. Hotelbeds

```
{
  "status": {
    "provider": "hotelbeds",
    "status": "quota_exhausted",
    "retryable": true,
    "message": "TEST_DAILY_QUOTA_EXHAUSTED",
    "http_status": 403
  },
  "available": false,
  "calls": 0,
  "quota_events": 0,
  "failed_calls": 0,
  "enrich_targets": 0,
  "incremental_value": {
    "note": "Provider unavailable/quota — external enrichment validation limited; resolution_validation still valid"
  }
}
```

## 12. Performance

```
{
  "sample_size": 400,
  "total_runtime_ms": 83060,
  "total_runtime_min": 1.38,
  "records_per_minute": 288.9,
  "provider_calls": 0,
  "hbx_calls": 0,
  "quota_events": 0,
  "failed_calls": 0,
  "retry_events": 0,
  "avg_resolve_latency_ms": 163,
  "avg_enrich_latency_ms": null,
  "scale_notes": {
    "1000": "~3 min at observed resolve throughput (enrich extra)",
    "10000": "~35 min resolve-only at observed rate",
    "50000": "~173 min resolve-only; requires batching + provider budgets"
  }
}
```

## 13. 10,000-Hotel Projection

```
{
  "label": "extrapolation_from_cala_validation_sample",
  "sample_size": 400,
  "target": 10000,
  "exact_strong": 9125,
  "probable": 300,
  "review_required": 875,
  "ambiguous": 575,
  "missing_rooms_in_sample": 375,
  "projected_missing_rooms": 9375,
  "room_recovery_note": "HBX unavailable — room recovery not extrapolated from external provider",
  "brand_gaps": 2600,
  "human_review_cases": 875,
  "caveat": "Identity rates may scale; external enrichment yield is capped by HBX quota/coverage and must not be linearly extrapolated from TEST quota runs."
}
```

## 14. Go / No-Go Recommendation

**GO_WITH_PROVIDER_EXPANSION_FIRST**

Identity layer is usable, but external enrichment (esp. rooms) blocked by provider availability; expand/entitle sources before auto-accept design.

## 15. Highest-Value Next Step

Entitle LIVE Hotelbeds Content API (or add one licensed rooms/geo source) and re-run the same CALA sample enrich slice — identity is ahead of external fill yield.

## Spot checks (summary)

Exact: 5, Strong: 5, Probable: 5, Ambiguous: 5, Room conflicts: 0

Artifacts under `reports/hotel-intelligence/cala-validation-v1/`.
