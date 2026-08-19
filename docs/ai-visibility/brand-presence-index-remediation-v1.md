# Brand Presence Index Remediation V1

> **Result:** `BRAND_AI_BENCHMARK_COHORT_REMEDIATION_PASS`  
> **Next:** `BRAND_SCENARIO_BENCHMARK_VALIDATION` completed as **PARTIAL** — see `docs/ai-visibility/scenario-benchmark-validation-v1.md`.  
> **Report:** `reports/ai-visibility/benchmark-cohort-remediation-v1.json`

## What changed

Scenario-specific commercial peers + pairwise intersection grains replaced the failed V1 construction (one primary tag + UNION denominator + full-set fallback).

V1 experimental headline figures (Autograph 321, Curio 228, Indigo 209, Ascend 92) remain **INTERNAL_ONLY / NOT CERTIFIED**. They are not the V2 scenario candidates.

## What did not change

- Peer sets v2, v3, v4, v5 (no v6)
- Customer-visible 19-brand dropdown
- Seven internal benchmark additions (relevance is now per scenario)
- Operator Presence (9 operators)
- Customer UI / Presence Index card
- Provider calls ($0)
- Recommendation / Preference metrics
- Headline index aggregation
- Mean vs median certification

## Architecture

| Item | Value |
|------|--------|
| BENCHMARK_UNIVERSE | `benchmark_eligible_brands_v1` (27) |
| SCENARIO_SPECIFIC | PASS |
| FULL_SET_FALLBACK_REMOVED | YES |
| UNION_GRAIN_REMOVED | YES |
| COMMON_GRAIN_METHOD | PAIRWISE |
| HEADLINE_INDEX_AGGREGATION | DEFERRED |
| CUSTOMER_INDEX_STATUS | INTERNAL_REVIEW_ONLY |

## Autograph (internal candidates)

Soft-brand affiliation: CORE includes Curio + Vignette. Scenario index candidate **129**, status VALID.

Conversion suitability (stored grains exist): CORE includes Curio + Vignette. Scenario index candidate **177**, status VALID.

Independent UU conversion: commercially eligible, **no stored scenario grains** → `MEASUREMENT_COVERAGE_GAP` / SUPPRESSED.

## Curio

Soft-brand: Autograph YES, Vignette YES. Scenario index candidate **125**, VALID.

## Hotel Indigo lifestyle

CORE: Kimpton, Canopy, Tempo, Voco, AC. Design + Radisson RED SECONDARY. Scenario index candidate **211**, VALID.

## Ascend

Owner-flex CORE includes Radisson Individuals, Vignette, Handwritten, Trademark, Preferred. Soft-collection CORE includes Autograph + Curio. Internal indices only.

## Westin

FULL_SET_FALLBACK = NO. Commercial peers: Radisson Blu, Radisson, DoubleTree, AC (secondary). LIMITED_SAMPLE on chain-scale (4 peers) — suppress is better than a 20-brand mix.

## Coverage wave (not executed)

Some CORE scenarios (notably independent conversion, market-entry, distribution/loyalty) have zero stored grains. Smallest incremental shared wave = existing monitoring-eligible prompt rows for those scenarios × **1 provider**, not × brand. `EXECUTED: NO`.

## Commands

```bash
npm run benchmark-cohort-remediation:run
npm run test:benchmark-cohort-remediation-v1
```
