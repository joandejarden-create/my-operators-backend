# Decision & Outcome — Airtable persistence architecture

**Status:** Live on canonical intelligence base  
**Date:** 2026-09-17 (cutover from Deal Capture MVP)

## Bases

| Role | Base ID | Notes |
|------|---------|-------|
| **Canonical (current)** | `appa2cE7FTRmIbB32` | Decision/Outcome + GDI Opportunities (+ ADP published reports) |
| **Legacy mistaken writes** | `appvtnDurnMSjINP6` | Deal Capture MVP — retained as LEGACY_MIGRATED_SOURCE; **no new writes** |

Do **not** change global `AIRTABLE_BASE_ID` (still MVP for Deals, Brand Setup, Users, etc.).

## Config

```
AIRTABLE_INTELLIGENCE_BASE_ID=appa2cE7FTRmIbB32
AIRTABLE_DECISION_OUTCOME_BASE_ID=appa2cE7FTRmIbB32
AIRTABLE_GDI_BASE_ID=appa2cE7FTRmIbB32
```

Resolver: `lib/decision-outcomes/airtable-base.js`  
Guard: `WRONG_CANONICAL_AIRTABLE_BASE` if resolved base is MVP (unless `DECISION_OUTCOMES_ALLOW_MVP_BASE=1`).

## Tables (canonical base)

| Table | Id |
|-------|-----|
| Decisions | `tblulPWvEmd3iiDhJ` |
| Decision Events | `tblLL1CuKpvI43DtB` |
| Group Demand Opportunities | `tblRuReslJMwsfRQj` |

## Primary source of truth

| Layer | Role |
|-------|------|
| **Airtable (appa2cE7…)** | Canonical durable persistence |
| **Filesystem** | Audit mirror after successful Airtable writes |

## Write / read flow

Unchanged service adapter stack; only base resolution changed:

```
GDI / ADP → DecisionOutcomeService → persistence.js → airtable-store.js → appa2cE7…
GDI research/UI → opportunity-persistence → airtable-opportunity-store → appa2cE7…
```

## Migration

`scripts/migrate-canonical-airtable-mvp-to-intelligence-base.mjs`  
Copied Decisions / Events / Opportunities from MVP → intelligence base; **did not delete** MVP rows.

## Verify

`node scripts/verify-canonical-airtable-cutover.mjs` — confirms new writes on target and absent from MVP.
