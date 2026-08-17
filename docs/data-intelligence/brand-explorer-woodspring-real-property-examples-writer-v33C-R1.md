# Brand Explorer WoodSpring Real Property Examples v33C-R1

Replaces generic `footprint.openings` platform cards with U.S. WoodSpring property examples and aligns registry-backed images.

## Scope

- WoodSpring `footprint.openings` rows only
- Brand Asset Registry rows linked to selected property examples
- Optional UI section hint for WoodSpring (`Curated U.S. examples · Not a full directory`)

## Out of scope

- Momentum, gallery, overview proof cards, standard detail
- Company Validated / Company Validation Date
- Summary URL / View Summary URL
- Non-target brands

## Property selection

1. CALA WoodSpring property examples if present in approved extract
2. U.S. WoodSpring property examples from official Choice property URLs
3. Fewer than 3 cards if fewer than 3 safe distinct images exist

Current build uses 3 U.S. examples (Orlando, Charlotte, Raleigh) and hides the generic platform card.

## Dry-run

```bash
npm run brand-explorer-woodspring-real-property-examples-writer -- --brand woodspring-suites --dry-run
```

## Apply (all gates required)

```bash
npm run brand-explorer-woodspring-real-property-examples-writer -- --brand woodspring-suites --apply --approve-brand-explorer-v33C-R1-woodspring-real-property-examples --founder-approved-woodspring-property-example-images --confirm-official-source-images-only --confirm-no-company-validation-claim --confirm-no-summary-url-field --confirm-no-momentum-gallery-proof-standard-changes --confirm-woodspring-only
```

## Post-apply QA

```bash
npm run brand-explorer-final-qa-auditor -- --brand woodspring-suites --dry-run
npm run brand-explorer-complete-build -- --brand woodspring-suites --dry-run --target-quality active-profile
npm run brand-explorer-visual-display-defect-audit -- --brand woodspring-suites --dry-run
```
