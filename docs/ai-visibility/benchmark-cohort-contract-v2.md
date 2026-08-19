# Benchmark Cohort Contract V2

> **Status:** Internal remediation — scenario-level construction certified for review.  
> **Headline AI Presence Index:** DEFERRED  
> **Customer certification:** OFF  
> **Module:** `lib/ai-visibility/competitive-moat/`

## Construction order

1. Owner intent / scenario (governed `scenarioId`)
2. Commercially relevant peer universe
3. Governed eligibility (CORE / SECONDARY; CONDITIONAL only when the condition is met)
4. Common measurement intersection (pairwise scenario grains)
5. Minimum quality gates (`BENCHMARK_COHORT_VALIDITY_V2`)
6. Benchmark calculation (working candidate only)

**BENCHMARK RELEVANCE FIRST. MEASUREMENT COMPARABILITY SECOND. SAMPLE SIZE THIRD.**

## Prohibited

| Behavior | Status |
|----------|--------|
| UNION-grain Presence denominators | PROHIBITED |
| Full-set / “use all peers if N is small” fallback | PROHIBITED |
| Headline index = median of scenario indices | DEFERRED |
| Mean vs median vs trimmed mean certification | DEFERRED |
| Customer-certified index | OFF |
| Recommendation / Preference / Win-Loss | BLOCKED |

## Universes (do not equate)

| Universe | Meaning |
|----------|---------|
| CUSTOMER_VISIBLE_MONITORED_BRANDS | 19 showcase portfolio brands |
| INTERNAL_BENCHMARK_BRANDS | 7 founder-approved additions + MGallery from peer v2 |
| BENCHMARK_ELIGIBLE_BRANDS | `benchmark_eligible_brands_v1` (27) — may serve as a peer in **some** scenario |

Eligibility ≠ comparability. Peer sets **v2–v5 are frozen**; comparability lives in scenario rules, not a new giant peer-set list.

## Common-grain method

**PAIRWISE** on stored OPEN_ENDED responses filtered to the scenario.

- Grain: scenario × prompt family × promptId × provider × geo × language × prompt version
- Subject and each peer scored on the **same scenario measurement grains**
- Do not use union of positive-mention keys
- Do not require every peer to co-occur on the same grain (global positive intersection is too sparse)

`PAIRWISE_COMMON_GRAIN_APPROACH: PASS`

## Validity V2 states

`VALID` · `LIMITED_SAMPLE` · `LIMITED_CORE_PEERS_MISSING` · `LIMITED_COMMON_GRAIN_COVERAGE` · `LIMITED_PROVIDER_COVERAGE` · `SUPPRESSED_SEMANTIC_MISMATCH` · `SUPPRESSED_INSUFFICIENT_DATA`

Gates: min 3 calculation peers (5 for VALID), min 2 CORE, CORE coverage **both** ≥50% **and** named mandatory cores (e.g. Autograph must include Curio + Vignette on soft-brand), ≥8 common grains. Single-provider allowed internally, not as a headline.

## Scenario index candidate (internal)

```
SCENARIO_AI_PRESENCE_INDEX = subject comparable Presence ÷ scenario competitive benchmark Presence × 100
```

Working aggregation = median of pairwise peer Presence values. **Not certified.**

## Customer

May eventually see: benchmark, safe label, selected observed competitors.  
Must not see: full membership, full peer Presence, scenario matrix, comparability rules.

## Commands

```bash
npm run benchmark-cohort-remediation:run
npm run test:benchmark-cohort-remediation-v1
```
