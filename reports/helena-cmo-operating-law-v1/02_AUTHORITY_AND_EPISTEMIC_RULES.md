# 02 — Authority and Epistemic Rules

## Authority hierarchy (machine)

```
CURRENT_FOUNDER_LOCK
> CURRENT_COMMERCIAL_EVIDENCE
> CURRENT_PRODUCT_REALITY
> CURRENT_APPROVED_MARKETING_DECISIONS
> HISTORICAL_STRATEGY
> ANALYTICAL_RECOMMENDATION
> HYPOTHESIS
```

Module: `lib/helena-cmo/operating-law/authority.js`

## Epistemic types

| Type | Meaning |
|---|---|
| FACT | Verified / established |
| OBSERVATION | Observed signal |
| HYPOTHESIS | Unproven inference |
| RECOMMENDATION | Proposed action/strategy |
| FOUNDER_DECISION | Joan-locked decision |

`DECISION` (Phase 3A) aliases to `FOUNDER_DECISION`.

## Confidence

HIGH · MEDIUM · LOW

## Forbidden conversions

- HYPOTHESIS → FACT  
- RECOMMENDATION → FOUNDER_DECISION  

Modules: `lib/helena-cmo/epistemic.js`, `lib/helena-cmo/operating-law/epistemic.js`
