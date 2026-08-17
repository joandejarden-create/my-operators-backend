# Competitive Gap Engine (P0C)

> **Status:** Production-ready (Presence-first). Association gaps limited to validated attributes.

## Gap classes

| Class | P0C status |
|-------|------------|
| `PEER_PRESENT_BRAND_MISSING` | Production eligible |
| `PERSISTENT_SCENARIO_GAP` | Production eligible |
| `VALIDATED_ASSOCIATION_GAP` | Production eligible — **DISTRIBUTION only** |
| `AI_PERCEPTION_VS_DEALALITY_FACT_GAP` | Structure only (P0D hook) |

## Association eligibility

Central gate: `lib/ai-visibility/gaps/association-eligibility.js`

- `isAssociationAttributeProductionEligible("DISTRIBUTION")` → `true`
- All other attributes → blocked from production gap classification

## Priority matrix

`COMMERCIAL_PRIORITY × GAP_PERSISTENCE` → `MONITOR | REVIEW | PRIORITY | HIGH_PRIORITY`

No numeric confidence scores.

## Storage

File store only: `data/ai-visibility/gaps/`

No Airtable Opportunity writes in P0C.

## Commands

```bash
npm run ai-visibility:competitive-gap
npm run test:ai-visibility-competitive-gap
```

## Certified layer

Gap engine reads existing evidence/observations only. Does not modify Presence, Questions Missing, All Providers, or Citation contracts.
