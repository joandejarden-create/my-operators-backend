# Brand Explorer WoodSpring Source Stewardship + Profile Build Readiness Writer v33A

**Batch:** v33A  
**Brand:** WoodSpring Suites (`woodspring-suites`, `recsOd51NzRPYsMko`)  
**Default:** dry-run / read-only

## Purpose

Everhome is `readyForActiveProfile`. v33A begins the WoodSpring activation path after v32B source capture:

1. Audit resolver / discovery config (`recordId` null in code config)
2. Steward the five v32B Source Library rows (Approved for Explorer Use)
3. Inventory presentation rows by section
4. Audit image preservation (no image field writes)
5. Propose Brand Asset Registry **candidate** creates (Pending Review / Candidate Only)
6. Produce required-section build plan and v33B–v33E sequence

**Does not touch** presentation copy, image fields, image approvals, or Company Validated.

## Apply gates

```bash
npm run brand-explorer-woodspring-source-registry-readiness-writer -- \
  --brand woodspring-suites \
  --apply \
  --approve-brand-explorer-v33A-woodspring-source-registry-readiness \
  --confirm-no-company-validation-claim \
  --confirm-no-image-field-changes \
  --confirm-no-image-approval \
  --confirm-no-presentation-copy-changes \
  --confirm-woodspring-only
```

## Recommended sequence

| Writer | Purpose |
|--------|---------|
| **v33A** (this) | Source stewardship + registry candidates + build plan |
| **v33B** | Presentation cleanup + required section backfill |
| **v33C** | Openings / momentum build |
| **v33D** | Existing image recognition / registry approval |
| **v33E** | Final fact stewardship + active profile finalization |

## Change impact

**High** — Source Library stewardship + Brand Asset Registry creates (metadata only). Rollback via report row lists.

## Post-run validation

```bash
npm run brand-explorer-woodspring-source-registry-readiness-writer -- --brand woodspring-suites --dry-run
npm run brand-explorer-final-qa-auditor -- --brand woodspring-suites --dry-run
npm run brand-explorer-complete-build -- --brand woodspring-suites --dry-run --target-quality active-profile
npm run brand-explorer-visual-display-defect-audit -- --brand woodspring-suites --dry-run
npm run test:partner-intelligence-publish-readiness
npm run test:partner-intelligence-profile-governance-publish
```
