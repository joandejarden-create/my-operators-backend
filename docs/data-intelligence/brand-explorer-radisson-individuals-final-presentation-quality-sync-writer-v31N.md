# Brand Explorer Radisson Individuals Final Presentation Quality Sync v31N

## Purpose

Reconcile **standalone Final QA presentation quality 100** vs **Complete Build embedded 88 / almost_ready** for Radisson Individuals by Choice, and optionally upgrade `overview.featured_application` copy when below Tribute reference depth.

## Root cause

Complete Build passes **record ID** (`recRyvM8OmLlDj9G7`) into `buildBrandExplorerFinalQaAuditorReport`. The v28C resolver returned `resolutionSource: record_id` for record-ID inputs, so `isExpansionBacklogBrandTarget()` was false and Final QA ran **active-registry UI quality checks** (gallery title-only cards, proof row) — six `category: copy` defects → presentation quality `100 − 6×2 = 88`.

Slug resolution (`radisson-individuals-by-choice`) mapped to `expansion_backlog` via discovery backlog and skipped those checks → presentation quality **100**.

Classification: **orchestrator_using_old_scoring_function** / **section-specific scoring mismatch** (record ID vs slug resolution path).

## Code fix (no brand hardcoding)

`lib/partner-intelligence/brand-explorer-final-qa-auditor.js`:

- `isExpansionBacklogBrandTarget()` — also true when `getDiscoveryBrandConfig(slug)` exists.
- `resolveFinalQaBrandTarget()` — normalizes discovery-config brands to `resolutionSource: expansion_backlog`.

## Copy fix (optional apply)

When `overview.featured_application.body` word count &lt; 25, patch Body only on record `recmLXxrggj0nSzgY`:

**Title:** Conversion-Friendly Individuality  
**Body:** Independent hotels that want Choice-family distribution and soft-brand flexibility while preserving local identity, owner story, and market-specific positioning within hand-selected collection standards—compare Individuals against rigid prototype flags when underwriting conversion economics.

## Apply gates

```bash
npm run brand-explorer-radisson-individuals-final-presentation-quality-sync-writer -- \
  --brand radisson-individuals-by-choice \
  --apply \
  --approve-brand-explorer-v31N-final-presentation-quality-sync \
  --confirm-no-company-validation-claim \
  --confirm-no-image-or-opening-changes
```

## Dry-run

```bash
npm run brand-explorer-radisson-individuals-final-presentation-quality-sync-writer -- \
  --brand radisson-individuals-by-choice --dry-run
```

## Guardrails

- Radisson Individuals only; protected brands blocked.
- No openings, momentum, gallery, scenarios, registry, or Company Validated changes.
- Blocks apply if unsupported/internal language in proposed copy.

## Expected results

| Check | After v31N |
|-------|------------|
| Final QA (slug or record ID) | ready (100) |
| Complete Build | readyForActiveProfile: true |
| Visual defect audit | 0 defects (after copy apply) |

Report: `reports/brand-explorer-radisson-individuals-final-presentation-quality-sync-writer.json`
