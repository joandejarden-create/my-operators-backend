# Bethesda ADP client baseline display — QA (2026-10-02)

## Exact string location (before fix)
`public/js/ai-demand-positioning/ai-demand-positioning.js` → `renderTrends` empty branch
(was: “No official baseline monitoring period is available yet.”)
now: “Official baseline not yet established.” (State 1 only)

## Root cause
**A + B + E (combined):**
1. Published Bethesda report had **no `trends[]`** baked.
2. Runtime period still had Day-0 unpublished flags: `customerVisible=false`, `customerTrendEligible=false`, `certified=false`, `publicationBlocked=true` (from `bethesda-marriott-baseline-period-001-v1.js` founder-review gate).
3. `attachTrendsIfMissing` therefore could not rebuild an eligible series → client empty-state.
4. Live rebuild from current attributes would have **drifted** Scenario Presence 81 → 65.4 — forbidden. Fix bakes Trends from **published frozen executiveMetrics / realityGap only**.

## Bethesda baseline
| Field | Value |
|---|---|
| Baseline run found | YES |
| baselineId | BETHESDA_ADP_BASELINE_V1 |
| periodId | adp_period_adp_bethesda_marriott_20260909091016_9f3a60 |
| Baseline date | 2026-10-01 |
| Baseline metrics found | YES |
| Official later measurement | NO |
| Baseline changed | NO |

## Oct 1 baseline KPI values (frozen published)

| KPI | Oct 1 baseline | Source / run | Displayed in client after fix |
|---|---:|---|---|
| AI Consideration Rate | **42.1%** | published EM · period `…9f3a60` | YES |
| AI Scenario Presence (Demand Query Coverage) | **81%** | published EM / demandCapture | YES |
| Demand Capture Rate | **81%** | published demandCapture.overallRate | YES |
| Top-3 Appearance Rate | **76.7%** | published EM rankMetrics | YES |
| #1 Appearance Rate | **33.3%** | published EM rankMetrics | YES |
| Property Reality Coverage | **26.7%** | published realityGap 4/15 | YES (Trends) |
| Competitor-Present Scenarios | **49** | published EM | YES |
| Headline ADP score (Demand Capture) | **81%** | manifest / demandCapture | YES |

Note: Product labels “AI Recommendation Rate” map to **AI Consideration Rate** in current ADP UI; “Demand Query Coverage” maps to **AI Scenario Presence / Demand Capture (81%)**.

## Client State 2 copy
- Baseline values + **October 1, 2026**
- Monitoring **Active**
- Next formal remeasurement **November 2026**
- Does **not** invent a “current” from unofficial runs

## Production deploy required
**YES** — ship UI JS + published report trends bake + runtime visibility flags.
