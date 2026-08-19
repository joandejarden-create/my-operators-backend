# Scenario Benchmark Validation V1

> **Result:** `BRAND_AI_SCENARIO_BENCHMARK_VALIDATION_PARTIAL`  
> **Next:** `BRAND_SCENARIO_BENCHMARK_REMEDIATION`  
> **Report:** `reports/ai-visibility/scenario-benchmark-validation-v1.json`  
> **Gate:** `npm run test:scenario-benchmark-validation-v1` · `npm run scenario-benchmark-validation:run`

Independent recompute of the 45 VALID scenario-level AI Presence Index candidates from stored observations. No provider calls. No UI. Headline index and mean/median remain **DEFERRED**.

## Certified (measurement layer)

| Check | Result |
|-------|--------|
| EXACT_MATCH vs remediation | 45 / 45 |
| MATERIAL_MISMATCH | 0 |
| Pairwise common grain | PASS |
| Denominator mismatches | 0 |
| UNION grain usage | 0 |
| INCORRECT commercial peers | 0 |
| Mandatory CORE failures | 0 |
| Reciprocal sanity | YES |
| Cross-scenario differentiation | PASS |
| Min common grains | KEEP 8 |

Math, pairwise denominators, and commercial INCORRECT=0 are certified. Customer-grade scenario indices are **not**.

## Not certified for customer use

Leave-one-peer-out and CORE-only vs CORE+SECONDARY show the working median-of-all-eligible aggregation is **fragile** on most of the 45:

- Many VALID candidates move >14 index points when one peer is dropped.
- CORE-only vs all-eligible often differs by ≥15 points (secondary peers, plus weak CORE names such as AC / Voco on lifestyle, pull the median).
- Extreme indices (Indigo lifestyle 211, Kimpton 222, Autograph conversion 177, Voco 30) are true pairwise Presence ratios on 22–24 grains — but the **benchmark denominator** is composition-sensitive.

Do not ship a customer scenario index until CORE-only vs all-eligible policy is remediating those movements. Do **not** treat this as a mean-vs-median decision; that stays deferred.

## Coverage wave (not executed)

The proposed 7 calls ($4.74, 1 provider) map only to **Independent UU conversion** (`p_*_independent_affiliation_*`). They do **not** cover Market entry / geographic relevance. They do **not** cover Distribution & Loyalty (`PLANNED_NO_PROMPTS`). Recommendation: **DO_NOT_RUN** until a governed Distribution prompt exists and a multi-provider design is specified.

## Product note (no UI)

Owner-intent scenario indices (soft-brand vs conversion vs lifestyle vs owner-flex) remain commercially more useful than one headline number. Autograph 129 vs 177 (flexibility suppressed) is the proof. That is a product concept, not a customer-ready ship.

## Regression

Brand classifier / QM / UI / longitudinal unchanged except this validation artifact. Operator universe remains 9. Customer payload still hides full peer matrices.
