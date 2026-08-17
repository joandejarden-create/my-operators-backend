# Brand Explorer WoodSpring Property-Specific Hotel Images v33C-R2

Corrects WoodSpring `footprint.openings` property example cards and visible gallery rows that use generic brand, lifestyle, or logo imagery. Property cards remain visible only when durable hotel/property photography is available from official Choice hoteldam CDN.

## Scope

- WoodSpring `footprint.openings` property example rows
- WoodSpring opening-related Brand Asset Registry rows
- WoodSpring visible gallery rows with logo / lifestyle / non-hotel graphics

## Out of scope

- Momentum, proof cards, standard detail, portfolio/context rows
- Company Validated / Company Validation Date
- Summary URL / View Summary URL
- Source Library approval statuses
- Everhome, Suburban, non-target brands

## Image policy

1. Property example cards require hotel/property photography (prefer property-specific hoteldam).
2. If no property-specific hotel image exists → hide card (`Do Not Display`).
3. Do not use Choice logo, WoodSpring logo-only, pet/lifestyle, or generic brand assets.
4. Fewer than 3 visible cards is acceptable; do not pad with generic images.

## Dry-run

```bash
npm run brand-explorer-woodspring-property-specific-images-writer -- --brand woodspring-suites --dry-run
```

## Apply (all gates required)

```bash
npm run brand-explorer-woodspring-property-specific-images-writer -- --brand woodspring-suites --apply --approve-brand-explorer-v33C-R2-woodspring-property-specific-images --founder-approved-woodspring-property-specific-hotel-images --confirm-official-source-images-only --confirm-no-company-validation-claim --confirm-no-summary-url-field --confirm-no-momentum-proof-standard-changes --confirm-woodspring-only
```

## Post-apply QA

```bash
npm run brand-explorer-final-qa-auditor -- --brand woodspring-suites --dry-run
npm run brand-explorer-complete-build -- --brand woodspring-suites --dry-run --target-quality active-profile
npm run brand-explorer-visual-display-defect-audit -- --brand woodspring-suites --dry-run
npm run test:partner-intelligence-publish-readiness
npm run test:partner-intelligence-profile-governance-publish
```
