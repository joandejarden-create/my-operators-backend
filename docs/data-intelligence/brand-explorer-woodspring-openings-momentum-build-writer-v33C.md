# Brand Explorer WoodSpring Openings + Momentum Build v33C

**Brand:** WoodSpring Suites (`woodspring-suites`, `recsOd51NzRPYsMko`)  
**Default:** dry-run / read-only

## Purpose

After v33B presentation cleanup, v33C creates missing `footprint.openings` and `footprint.momentum` rows from the five approved WoodSpring Source Library records—without image fields, source stewardship changes, or registry approvals.

## Apply gates

```bash
npm run brand-explorer-woodspring-openings-momentum-build-writer -- \
  --brand woodspring-suites \
  --apply \
  --approve-brand-explorer-v33C-woodspring-openings-momentum-build \
  --confirm-no-company-validation-claim \
  --confirm-no-image-field-changes \
  --confirm-no-source-library-changes \
  --confirm-no-registry-approval-changes \
  --confirm-woodspring-only
```

## Sequence

| Writer | Purpose |
|--------|---------|
| **v33C** (this) | Openings + momentum row creates |
| **v33D** | Image recognition / registry approval |
| **v33E** | Final fact stewardship + active profile |

## Change impact

**High** — Presentation row creates only. Openings ship without images (v33D scope). Rollback via report record IDs after apply.

## Post-run validation

```bash
npm run brand-explorer-woodspring-openings-momentum-build-writer -- --brand woodspring-suites --dry-run
npm run brand-explorer-final-qa-auditor -- --brand woodspring-suites --dry-run
npm run brand-explorer-complete-build -- --brand woodspring-suites --dry-run --target-quality active-profile
npm run brand-explorer-visual-display-defect-audit -- --brand woodspring-suites --dry-run
```
