# FOUNDER REPORT — YOTEL Process Parity + Multilingual Runtime

## Verdict

**AC Coruña: PARTIAL_PARITY** · **Radisson Santo Domingo: PARTIAL_PARITY**

Shared orchestration and multilingual query generation are now wired.
The remaining live gap vs YOTEL is **empty demand-campaign stores** on AC/RAD (YOTEL has 10), so second-generation / official-list decomposition does not yet fire on those hotels.

## Downstream (unchanged / healthy)

- Ready thresholds unchanged · AC Ready 0 · RAD Ready 0
- Valid Watches preserved · Pursuits 5
- No Apify · No forced volume · No pursuit churn

## Upstream proof

| Capability | Shared? | YOTEL invoked? | AC invoked? | RAD invoked? |
|------------|---------|----------------|-------------|--------------|
| Multilingual query generator | YES | YES | YES (generator) | YES (generator) |
| ES discovery | YES | n/a | YES | YES |
| GL discovery | YES | n/a | YES | n/a |
| Campaign second-gen decomp | YES | YES | NO (0 campaigns) | NO (0 campaigns) |
| Ready/Watch gates | YES | YES | YES | YES |
| Pursuit handoff | YES | YES | YES | YES |

## Controlled discovery

Equal-budget **query lanes** (EN / native / combined) generated for all 10 Bases.
Live SERP apply **not** run (no unlimited discovery). Incremental Ready/Watch from local language = **0** this pass (by design).

## Top gaps

1. **Process:** AC/RAD need market-agnostic demand-generator / campaign seeding (same store YOTEL uses) before second-gen yield matches YOTEL.
2. **Language:** Generator + parsers are wired; incremental **evidence yield** still needs a bounded SERP research pass using the new lanes (separate authorize).

## Classification

- AC missing after repair: DEMAND_GENERATOR_CAMPAIGNS_EMPTY, SECOND_GEN_NOT_TRIGGERED_NO_CAMPAIGNS, OFFICIAL_LIST_DECOMP_AWAITING_CAMPAIGNS
- RAD missing after repair: DEMAND_GENERATOR_CAMPAIGNS_EMPTY, SECOND_GEN_NOT_TRIGGERED_NO_CAMPAIGNS, OFFICIAL_LIST_DECOMP_AWAITING_CAMPAIGNS
