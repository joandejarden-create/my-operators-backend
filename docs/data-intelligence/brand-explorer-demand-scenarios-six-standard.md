# Brand Explorer — Permanent Six Demand Scenarios Standard

**Lesson:** `BE-QL-DEMAND-SCENARIOS-SIX-001`  
**Contract:** `BE_FACTORY_PERMANENT_DEMAND_SCENARIOS_SIX_V1`  
**Slot:** `commercial.demand`

## Rule

Every Brand Explorer profile must contain **exactly six** distinct Demand Scenario Fit assessments (min = max = 6).

Fit labels (only):

- Strong Fit  
- Moderate Fit  
- Limited Fit  
- Not a Fit  

Banned: Moderate–strong, Very strong, Medium-high, Weak-moderate (and synonyms).

## Module

`lib/brand-explorer/be-factory-permanent-demand-scenarios-six-v1.js`

- Canonical taxonomy (`CANONICAL_DEMAND_SCENARIO_TAXONOMY`)
- Exact-six validation
- Scored selection when CURRENT/candidate has >6 (never silent truncate)
- Assembler / Factory / publication gate integration

## Commands

```bash
npm run test:be-factory-permanent-demand-scenarios-six-v1
npm run be-factory-demand-scenarios-six-normalize          # dry-run
npm run be-factory-demand-scenarios-six-normalize -- --apply
```

## Publication

Do **not** auto-overwrite published Curio / Ascend Airtable Presentation. Stage exact-six corrections for founder exact-candidate approval first.
