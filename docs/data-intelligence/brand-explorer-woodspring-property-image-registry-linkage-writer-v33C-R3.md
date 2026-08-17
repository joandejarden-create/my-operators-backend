# Brand Explorer WoodSpring Property Image Registry Linkage v33C-R3

Reconciles WoodSpring `footprint.openings` property examples and `materials.gallery.1–3` presentation rows to canonical approved Brand Asset Registry rows so Final QA / Visual Defect / Complete Build recognize imagery as approved (without changing any image fields).

## Scope

- 3 visible `footprint.openings` rows (Orlando, Charlotte, Raleigh)
- 3 visible gallery rows (`materials.gallery.1–3`)
- Brand Asset Registry rows created by v33C-R2 + any older rows that could be matched by QA
- QA recognition logic for registry matching (narrow patch only)

## Out of scope (must not change)

- Image fields, attachments, image URLs
- Opening copy, gallery titles
- Momentum, proof cards, standard detail
- Source Library approvals
- Company Validated / Company Validation Date
- Summary URL / View Summary URL
- Non-target brands

## Why this writer exists

Airtable materializes external image URLs as attachments (`airtableusercontent.com/...`). QA matching previously relied on `imageUrl` ↔ registry `sourceUrl`, which fails after materialization. v33C-R3 patches the match logic to also use the **durable property source URL embedded in the opening body**, and writes the explicit `Brand Asset Registry` link back onto the relevant presentation rows (when available).

## Dry-run

```bash
npm run brand-explorer-woodspring-property-image-registry-linkage-writer -- --brand woodspring-suites --dry-run
```

## Apply (all gates required)

```bash
npm run brand-explorer-woodspring-property-image-registry-linkage-writer -- --brand woodspring-suites --apply --approve-brand-explorer-v33C-R3-woodspring-property-image-registry-linkage --founder-approved-woodspring-property-specific-hotel-images --confirm-no-image-field-changes --confirm-no-company-validation-claim --confirm-no-source-library-changes --confirm-no-summary-url-field --confirm-no-momentum-proof-standard-changes --confirm-woodspring-only
```

## Post-apply QA

```bash
npm run brand-explorer-final-qa-auditor -- --brand woodspring-suites --dry-run
npm run brand-explorer-complete-build -- --brand woodspring-suites --dry-run --target-quality active-profile
npm run brand-explorer-visual-display-defect-audit -- --brand woodspring-suites --dry-run
npm run test:partner-intelligence-publish-readiness
npm run test:partner-intelligence-profile-governance-publish
```

