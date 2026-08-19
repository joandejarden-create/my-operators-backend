# Competitive Data Access Policy V1

> **Policy version:** `competitive_data_access_v1`  
> **Module:** `lib/ai-visibility/competitive-moat/access-redaction.js`

## Core Rule

**Server/API must construct customer-safe payloads.** Do not rely on CSS, hidden components, or frontend filtering.

## Permission Classes

| Class | Access |
|-------|--------|
| INTERNAL_ADMIN | Full corpus, benchmark matrix, cohort definitions, raw responses |
| CUSTOMER_ENTITY | Own detailed intelligence + benchmark summary |
| CUSTOMER_EXECUTIVE | Same as entity (future tiering) |
| CUSTOMER_ANALYST | Same as entity (future tiering) |

Reuse existing entitlement system: `lib/ai-visibility/entitlements.js`, `access-depth.js`.

## Customer Allowlist

Explicit fields permitted in customer benchmark payloads:

- `subjectEntityId`, `subjectName`, `entityType`
- `indexName`, `indexValue`, `benchmarkParity`, `relativeGapPct`
- `gapToLeaderIndexPoints` (when eligible)
- `benchmarkLabel`, `benchmarkSampleBand`, `benchmarkStatus`
- `topObservedCompetitors` (max 3–5)
- `intentStrengths`, `intentWeaknesses`
- `measurementPeriod`, `comparisonPeriod`, `evidenceSummary`
- `provider`, `accessDepth`, `payloadVersion`

## Blocked from Customer

| Resource | Status |
|----------|--------|
| Full competitor matrix | BLOCKED |
| All competitor raw Presence rates | BLOCKED |
| Complete prompt library | BLOCKED |
| Exact prompt wording | BLOCKED |
| Mutation rules | BLOCKED |
| Prompt-generation metadata | BLOCKED |
| Benchmark membership details | BLOCKED (safe summary only) |
| Scenario peer matrix / comparability rules | BLOCKED (internal) |
| Classifier thresholds | BLOCKED |
| Methodology weights | BLOCKED |
| Raw observation ledger | BLOCKED |
| Research-only outputs | BLOCKED |
| Recommendation / win-loss fields | BLOCKED |

## Internal-Only Fields

`benchmarkMembers`, `allCompetitorScores`, `rawScore`, `promptTextFullCorpus`, `mutationRule`, `classifierThreshold`, `normalizationRule`, `fullObservationLedger`, `cohortSelectionRules`, `methodologyWeights`

## API Versioning

- Do **not** break current Brand APIs
- Additive endpoints preferred
- Future Competitive / Peer Analysis fields (`ownerIntentBenchmarks`, selected core-peer names) are **prepared but not activated** (`CUSTOMER_INDEX_RENDERING = OFF`). Live customer payloads must not include full peer Presence values or cohort rules.
  - Customer pilot: `GET /api/ai-visibility/brand/:brandId/benchmark`
  - Internal diagnostics: `GET /api/ai-visibility/brand/:brandId/benchmark/diagnostics` (runbook admin)
  - Operator (future): `/api/ai-visibility/operator/:operatorId/benchmark`

## Dataset Separation

| Class | Use |
|-------|-----|
| DEMO_VALIDATION | Internal audits, founder validation |
| PILOT | Controlled pilot customers |
| PRODUCTION_CLIENT | Customer-owned production history |

Customer does not gain unrestricted rights to Dealality benchmark corpus because their observations contribute to it.

## Privacy / Suppression

Suppress benchmark when:
- One competitor effectively constitutes entire benchmark
- Cohort membership would reveal confidential entity data
- Sample size too small
- Customer access rules prohibit

## Leak Testing

Automated inspection: `auditPayloadForMethodologyLeaks()`, `auditCustomerPayloadForBlockedSignals()`

Gate: `npm run test:competitive-moat-architecture-v1`

## Info Icon Contracts

Customer-safe copy in `lib/ai-visibility/competitive-moat/info-contracts.js`:

- Owner Intent
- Observed Competitive Set
- Emerging Competitor (longitudinal gate required)
- Historical Intelligence
- AI Presence Index
- Competitive Benchmark
- Gap to Leader

## Source Intelligence

Preserve: Cited, Recurring cited, Associated, Co-occurring  
Block: influence, caused, drove (unless future causal methodology exists)

## Customer vs Dealality Data

| Type | Owner |
|------|-------|
| CUSTOMER_ENTITY_DATA | Customer's detailed intelligence |
| DEALALITY_BENCHMARK_DATA | Dealality proprietary aggregated corpus |

This document is architecture/product control — not legal drafting.
