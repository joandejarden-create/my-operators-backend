# Brand Explorer WoodSpring Presentation Cleanup + Required Section Backfill v33B

**Brand:** WoodSpring Suites (`woodspring-suites`, `recsOd51NzRPYsMko`)  
**Default:** dry-run / read-only

## Purpose

After v33A source stewardship and registry candidates, v33B clears presentation/content gates:

- Internal / FDD / Item 19 language removal
- Missing section creates (`overview.featured_application`, portfolio context, demand scenario, value scenarios, portfolio mix chips)
- `overview.scenario.3` wrong-brand quarantine (no image field changes)
- Copy cleanup for loyalty, standards, economics, geographic footprint

**Does not** build openings/momentum (v33C), approve images (v33D), or modify Source Library.

## Apply gates

```bash
npm run brand-explorer-woodspring-presentation-cleanup-backfill-writer -- \
  --brand woodspring-suites \
  --apply \
  --approve-brand-explorer-v33B-woodspring-presentation-cleanup-backfill \
  --confirm-no-company-validation-claim \
  --confirm-no-image-field-changes \
  --confirm-no-image-approval \
  --confirm-no-source-library-changes \
  --confirm-woodspring-only
```

## Sequence

| Writer | Purpose |
|--------|---------|
| **v33B** (this) | Presentation cleanup + required section backfill |
| **v33C** | Openings / momentum build |
| **v33D** | Image recognition / registry approval |
| **v33E** | Final fact stewardship + active profile |

## Change impact

**High** — Presentation row patches/creates and visibility quarantine. Rollback via report row IDs.

## Post-run validation

```bash
npm run brand-explorer-woodspring-presentation-cleanup-backfill-writer -- --brand woodspring-suites --dry-run
npm run brand-explorer-final-qa-auditor -- --brand woodspring-suites --dry-run
npm run brand-explorer-complete-build -- --brand woodspring-suites --dry-run --target-quality active-profile
npm run brand-explorer-visual-display-defect-audit -- --brand woodspring-suites --dry-run
```
