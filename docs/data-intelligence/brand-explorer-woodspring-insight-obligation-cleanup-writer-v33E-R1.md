# Brand Explorer WoodSpring Insight / Obligation Card Cleanup v33E-R1

WoodSpring-only writer that patches `overview.proof.1`–`overview.proof.6` and `overview.proof_operator` with founder-ready extended-stay copy, and pairs with a UI fix in `brand-explorer-atelier-from-api.js` so the overview proof grid reads presentation slots instead of hardcoded Brand Basics fallbacks.

## Scope

- Overview proof-point cards (Global Open Footprint / Pipeline Depth / loyalty estimates / operator copy)
- Presentation Title + Body only
- WoodSpring Suites (`woodspring-suites`, `recsOd51NzRPYsMko`)

## Out of scope

- Images, registry, source library, openings, momentum, gallery
- Company Validated / Company Validation Date
- Everhome Suites, Suburban Studios

## Run

```bash
npm run brand-explorer-woodspring-insight-obligation-cleanup-writer -- --brand woodspring-suites --dry-run
```

## Apply gates

```bash
npm run brand-explorer-woodspring-insight-obligation-cleanup-writer -- \
  --brand woodspring-suites \
  --apply \
  --approve-brand-explorer-v33E-R1-woodspring-insight-obligation-cleanup \
  --confirm-no-company-validation-claim \
  --confirm-no-image-field-changes \
  --confirm-no-registry-approval-changes \
  --confirm-no-source-library-changes \
  --confirm-no-openings-or-momentum-changes \
  --confirm-woodspring-only
```
