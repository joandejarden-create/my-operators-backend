# GDI Agent Test 001 — 20-Opportunity Controlled Experiment

**Experiment ID:** `gdi_20opp_adaptive_investigation_experiment_v1`  
**Version:** `2026-11-19.1`  
**Constraint:** `$0` paid external providers · no production / publication writes

## Purpose

Determine whether **bounded adaptive investigation** (reuse of existing ORS supervisor + adaptive public research controller + commercial evidence recovery + evidence challenger) materially improves opportunity quality vs the current GDI evaluation path.

This is **not** a second GDI platform and does **not** introduce a multi-agent framework.

## Arms

| Arm | What runs |
|-----|-----------|
| **Baseline (current GDI)** | `evaluateOpportunity` + `evaluateSharedCommercialEligibility` + evidence challenger on frozen opportunity snapshots |
| **Agentic (enhanced)** | Same gates, plus `recoverCommercialEvidence` → adaptive preflight → `selectAdaptiveNextAction` → challenger override → eligibility |

## Execution fidelity labels

Outputs are tagged as:

- `fresh_deterministic_evaluation_on_frozen_snapshot`
- `fresh_deterministic_adaptive_planning_on_frozen_snapshot`
- Historical publication fields on snapshots are **held out** and never claimed as newly executed baselines
- Gated live public-fetch protocol: **NOT executed** without founder authorization (cap `$20`)

## Cohort

Exactly 20 immutable opportunity IDs from local `data/group-demand-intelligence/hotels/*/opportunities.json`, quota:

- 4 corporate meetings/training/expansion
- 4 associations/event rotation
- 3 commercial projects/workforce
- 3 medical/pharma/research
- 2 sports/university/other group demand
- 2 previously rejected/unsupported
- 2 cross-hotel / already-booked-elsewhere

Preferred hotels: Bethesda Marriott, Wyndham San Diego Bayside, YOTEL Geneva Lake, Radisson Blu Toronto, W Rome.  
Substitutes documented when Mayflower / Falls Church / Nobu lack local opportunity stores.

Manifest + frozen snapshots:

- `fixtures/gdi-experiments/gdi-20opp-adaptive-investigation-v1/`
- `reports/gdi-20opp-adaptive-investigation-experiment-v1/`

## Commands

```bash
npm run gdi:20opp-adaptive-investigation-experiment-v1
npm run test:gdi-20opp-adaptive-investigation-experiment-v1
```

## Decision rule (honest)

Offline `$0` runs **cannot** produce `GO` for full live-agent validation.  
`GO` requires authorized retrieval plus eligibility/contact lift.  
Typical offline outcome: **ITERATE** with a founder-gated ≤`$20` public-fetch subset as next phase.

## Safety

- Paid Exa / SerpAPI / PDL / Surfe / Firecrawl / Parallel denied
- No Airtable writes, no customer publication
- Challenger blocks speculative sales-ready promotion
