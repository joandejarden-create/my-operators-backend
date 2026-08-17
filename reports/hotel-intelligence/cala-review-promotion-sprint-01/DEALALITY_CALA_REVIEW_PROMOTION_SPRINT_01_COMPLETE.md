# DEALALITY_CALA_REVIEW_PROMOTION_SPRINT_01_COMPLETE

## 1. Safety

```text
ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES=0
ENABLE_HBX_CENSUS_WRITES=0
Production writes: 0
Census writes: 0
Automatic merges: 0
Automatic imports: 0
```

## 2. Review Queue Baseline

| Cohort | Count |
| --- | ---: |
| REVIEW_BEFORE | 832 |
| TRACK_B_REVIEW | 354 |
| BRAZIL_REVIEW | 478 |
| Merged READY before | 348 |

## 3. Review-Wall Diagnosis

| Reason | Count | % | Avg conf | Avg gap to 0.90 |
| --- | ---: | ---: | ---: | ---: |
| ADDRESS_MISSING | 832 | 100% | 0.862 | 0.038 |
| COORDINATES_MISSING | 832 | 100% | 0.862 | 0.038 |
| DUPLICATE_RISK | 589 | 70.8% | 0.861 | 0.039 |
| CITY_CONFIDENCE | 472 | 56.7% | 0.863 | 0.037 |
| IDENTITY_CONFIDENCE | 139 | 16.7% | 0.827 | 0.073 |
| LOCALITY_AMBIGUITY | 75 | 9% | 0.849 | 0.051 |
| NAME_AMBIGUITY | 23 | 2.8% | 0.799 | 0.103 |

Note: ADDRESS/COORDINATES are universal gaps on this Cvent-sourced cohort but are **not** Tier A gates. The actual READY blockers were city_confidence & soft-duplicate pressure (fixed in `city-infer-v2` + confidence).

## 4. Confidence Distribution (before)

- 0.88–0.899: **49**
- 0.85–0.879: **607**
- 0.80–0.849: **158**
- <0.80: **18**

## 5. Territory Normalization Audit

Version: `discovery-factory-city-infer-v2`

Fixes (smallest reusable rules — no manual score bumps):
- Expanded CITY_CANONICAL for Track B + Brazil secondary markets
- CITY_IMPLIES_COUNTRY conflict guard (cross-island Cvent pollution)
- ISLAND_PRIMARY_LOCALITY for bare territory labels
- soft_duplicate_pressure only on near-dupe scores ≥0.55

Systematic issues:
- Unknown URL slug city_confidence 0.78 blocked Tier A (needed ≥0.85)
- soft_duplicate_pressure fired on all NEW with 3 weak pool rows
- Cvent cross-island city pollution (e.g. Willemstad on Bonaire, Castries on Martinique)

## 6. Track B Promotion Results

| Country | Before | Promoted | Still REVIEW | Avg conf before → after |
| --- | ---: | ---: | ---: | --- |
| Turks and Caicos | 57 | 40 | 12 | 0.834 → 0.925 |
| Bonaire | 48 | 14 | 34 | 0.86 → 0.881 |
| Martinique | 49 | 13 | 36 | 0.846 → 0.867 |
| U.S. Virgin Islands | 50 | 48 | 2 | 0.853 → 0.958 |
| Anguilla | 25 | 7 | 18 | 0.84 → 0.93 |
| Montserrat | 50 | 2 | 48 | 0.845 → 0.899 |
| Guadeloupe | 31 | 17 | 13 | 0.846 → 0.923 |
| Saint Lucia | 44 | 36 | 8 | 0.85 → 0.951 |

**Track B totals:** promoted **177** / 354 (50%); still REVIEW **171**; rejected **6**

## 7. Geographic Coverage Impact

| Metric | Value |
| --- | --- |
| ZERO_COVERAGE_COUNTRIES_LIVE | 22 |
| ZERO_COVERAGE_COUNTRIES_AFTER_DEDUPED_READY | 15 |
| Moving above 20% | Martinique, Turks and Caicos Islands, Bonaire, U.S. Virgin Islands, Guadeloupe, Anguilla, Saint Lucia |
| Moving above 50% | — |

## 8. Brazil Promotion Results

- Promoted: **167** / 478 (34.9%)
- Still REVIEW: **311**
- Avg confidence: 0.873 → 0.963
- SerpApi calls: 0 (key absent — skipped)

## 9. Provider Usage

```text
GIATA Drive calls: 17
GIATA matches: 1
SerpApi calls: 0
SerpApi useful: 0
Promotions existing evidence only: 343
Promotions requiring GIATA: 1
Promotions requiring SerpApi: 0
```

## 10. Promotion Efficiency

```text
REVIEW_BEFORE = 832
PROMOTED_TO_READY = 344
STILL_REVIEW = 482
AMBIGUOUS = 0
REJECTED = 6
promotion_rate = 41.3%
READY per external API call = 0.059 (almost all promotions needed no API)
```

## 11. Quality Audit

See `promotion-summary.json` → `quality_audit`. Spot-check island-primary and the single GIATA-path promotion. Residual Track B REVIEW is concentrated in cross-island Cvent pollution (Bonaire/Martinique/Montserrat) where GIATA Open Content overlap was thin.

## 12. READY Queue

```text
Before: 348
New promotions: 344
Duplicates removed: 3
After: 689
```

## 13. Projected Census

```text
LIVE: 5956
PROJECTED: 6645
Remaining to 10K: 3355
Remaining to 12.5K: 5855
Remaining to 15K: 8355
```

## 14. Updated Coverage Dashboard

Regenerated under `reports/hotel-intelligence/cala-coverage-dashboard-v1/` (live + projected-with-all-deduped-ready).

| | LIVE | PROJECTED_WITH_ALL_DEDUPED_READY |
| --- | ---: | ---: |
| Hotels | 5956 | 6645 |
| Overall coverage | 36.8% | 39.5% |
| Zero coverage countries | 22 | 15 |
| <20% | 38 | 31 |
| <50% | 47 | 47 |
| <80% | 50 | 50 |
| ≥95% | 2 | 2 |

## 15. Review Bottleneck Verdict

- `FIXABLE_EVIDENCE_GAP` — primary (aliases + soft-dup false positives)
- `GEOGRAPHY_NORMALIZATION_PROBLEM` — Track B cross-island Cvent pollution
- `CONFIDENCE_MODEL_WORKING_AS_DESIGNED` — bar stayed ≥0.90; no threshold cut
- `NORMAL_EXPECTED_REVIEW` — residual ~482 after legitimate promotion

Not concluded: `CONFIDENCE_MODEL_TOO_CONSERVATIVE` (evidence did not support lowering the bar).

## 16. Recommended Operating Mode

```text
DISCOVER_AND_PROMOTE
```

Evidence-first promotion converted 41% of REVIEW with almost no API spend. Next sprints should discover and promote in the same loop.

## 17. SPRINT 02 (recommendation only — not executed)

**TRACK A:** Brazil ×600

**TRACK B:**
- Saint Barthélemy ×50
- Paraguay ×80
- Bahamas ×80
- Belize ×80
- Dominica ×23
- Saint Martin ×40

Expected raw ~953, READY ~362, REVIEW ~496

## 18. Final Verdict

```text
READY_FOR_IMPORT_REVIEW
```

Do NOT import. Do NOT execute Sprint 02.
