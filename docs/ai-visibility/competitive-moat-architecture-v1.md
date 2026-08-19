# Dealality Competitive Moat Architecture V1

> **Status:** Architecture complete — offline validation only  
> **Principle:** SHOW THE BENCHMARK · HIDE THE BENCHMARK ENGINE  
> **Supporting:** TRANSPARENT INSIGHT · PROPRIETARY MECHANICS

## Purpose

Protect and structure Dealality's proprietary AI intelligence without redesigning Brand AI V1 or building new unvalidated signals. This phase defines how validated measurement compounds into a defensible benchmark system.

## Four Layers

| Layer | Name | Customer sees | Dealality keeps |
|-------|------|---------------|-----------------|
| 1 | Proprietary Measurement Corpus | Intent name, business meaning | Prompt families, mutations, raw responses, model metadata |
| 2 | Proprietary Benchmark Engine | — | Cohort logic, aggregation, eligibility, normalization |
| 3 | Controlled Customer Benchmark | Index, gap, limited competitors | Full cohort matrix, methodology |
| 4 | Customer Intelligence | Findings, evidence summary | Classifier rules, weights, full ledger |

## Existing Assets (extended, not replaced)

| Asset | Location |
|-------|----------|
| Brand scenario registry | `fixtures/ai-visibility/scenario-registry-v1.json` |
| Operator scenario registry | `lib/ai-visibility/operator-intelligence/scenarios.js` |
| Observed Demand provenance | `lib/ai-visibility/observed-demand-activation.js` |
| Brand longitudinal ledger | `lib/ai-visibility/brand-longitudinal/idempotency.js` |
| Competitive gap (production-safe) | `lib/ai-visibility/competitive-gap-intelligence.js` |
| Access depth / entitlements | `lib/ai-visibility/access-depth.js`, `entitlements.js` |

## Canonical Intent Bridge

Internal layer above Brand + Operator registries — **no parallel customer taxonomy**.

- Module: `lib/ai-visibility/competitive-moat/canonical-intent.js`
- Maps `scenarioId` → `canonicalIntentId` with governed metadata
- Customer exposure: **CONTROLLED** (intent name + business meaning only)

## Observation Ledger

Append-only measurement history extending brand longitudinal architecture.

- Schema: `lib/ai-visibility/competitive-moat/observation-ledger-schema.js`
- Dataset classes: `DEMO_VALIDATION`, `PILOT`, `PRODUCTION_CLIENT`
- **No unvalidated inference fields** (Winner, Recommended, Displaced, etc.)

## Observed Competitive Set V1

Derived from **Presence + validated gap evidence only**.

- Not: won against us, beat us, displaced us, preferred over us
- Module: `lib/ai-visibility/competitive-moat/observed-competitive-set.js`
- Customer limit: 3–5 named observed competitors

## Blocked Signals (do not rename)

Recommendation Rate, Win Rate, AI Preference Index, Displacement, First Choice, Questions Won, Operator Decision Share of Voice — all remain blocked until separately certified.

Registry: `lib/ai-visibility/competitive-moat/blocked-signals.js`

## Index Status

| Index | Status |
|-------|--------|
| AI Presence Index (APIx) | **V1 candidate — READY_FOR_INTERNAL_VALIDATION** |
| AI Visibility Index (AVI) | Not selected — broader than Presence |
| AI Consideration Index | BLOCKED |
| AI Preference Index | BLOCKED |
| AI Representation Index | RESEARCH_DESIGN_ONLY |
| Proprietary Raw Score | NOT_REQUIRED_YET |

**Naming decision:** Use **AI Presence Index** — semantically precise for validated Presence-only V1. Avoid "API" alone (conflicts with blocked Preference Index).

## Brand Longitudinal Integration

Preserve without rewrite:

- 2026-08-14 baseline
- 2026-08-18 period `aiv_brand_longitudinal_period_20260818_6579d2`
- Do not mix DEMO_VALIDATION with future PRODUCTION_CLIENT datasets

## Operator Integration

Operator uses same infrastructure after Presence validation passes.

- Operator benchmark/index: **READY_FOR_INTERNAL_VALIDATION** (Presence PRODUCTION_VALIDATED)
- Brand and Operator cohorts **never mixed**

## Commands

```bash
npm run test:competitive-moat-architecture-v1
npm run competitive-moat:audit
```

## Related Docs

- `docs/ai-visibility/benchmark-engine-v1.md`
- `docs/ai-visibility/competitive-data-access-policy-v1.md`
