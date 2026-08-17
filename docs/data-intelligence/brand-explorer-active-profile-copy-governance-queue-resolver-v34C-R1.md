# Brand Explorer Active Profile Copy Governance Queue Resolver v34C-R1

Resolves `founderReviewQueue` items from v34C through **brand config** — not suburban-specific writers.

## Module

`lib/partner-intelligence/brand-explorer-active-profile-copy-governance-queue-resolver.js`

Runs automatically at the end of `buildCopyGovernancePlan()`.

## Config extensions (`copy-governance-config.js`)

Per brand:

- `founderQueueResolutions` — strategy per slot: `rewrite`, `hide`, `founder_manual_review`
- Expanded `slotRewrites` for queued slots

## Resolution strategies

| Strategy | Behavior |
|----------|----------|
| `rewrite` | Apply brand-specific slot package; validate safety + specificity |
| `hide` | Set `External Display Status` = Do Not Display |
| `founder_manual_review` | Remain in queue |

## Suburban v34C-R1 queue (8 rows)

| Slot | Resolution |
|------|------------|
| `economics.opening.financials` | Brand-specific rewrite |
| `overview.proof.3` | Brand-specific rewrite |
| `overview.proof.4` | Brand-specific rewrite |
| `overview.proof_operator` | Brand-specific rewrite |
| `insight.similar` | Hide (IHG competitor comparison) |
| `loyalty.kpi.mix` | Brand-specific rewrite (hide fallback) |
| `valueOwners.watchouts` | Brand-specific rewrite |
| `loyalty.proof` | Brand-specific rewrite (slot before regex) |

## Reports

- `reports/brand-explorer-active-profile-factory-suburban-studios-copy-governance.md`
- `reports/brand-explorer-active-profile-factory-suburban-studios-founder-queue-audit.md`

## Apply guardrails

Copy apply blocks: Image fields, Company Validated, Summary URL, Brand Asset Registry.
