# Brand Explorer Everhome Openings Description Cleanup Writer v32E-R1

**Batch:** v32E-R1  
**Brand:** Everhome Suites (`everhome-suites`, `recqkkrsevi4r9ibj`)  
**Default:** dry-run / read-only

## Purpose

v32E only cleaned one opening teaser. v32E-R1 audits **all** `footprint.openings` rows and rewrites source-metadata style teaser copy while preserving:

- Chips / labels
- Location, meta, and scenario paragraphs (`parseFootprintOpeningParas` structure)
- Source URLs (trailing Body URL)
- Images and visibility

## Out of scope

- Momentum, gallery, scenario slots
- Image / registry / Source Library approval fields
- Company Validated
- WoodSpring, Suburban, non-Everhome brands

## Apply gates

```bash
npm run brand-explorer-everhome-openings-description-cleanup-writer -- \
  --brand everhome-suites \
  --apply \
  --approve-brand-explorer-v32E-R1-everhome-openings-description-cleanup \
  --confirm-no-company-validation-claim \
  --confirm-no-image-field-changes \
  --confirm-no-source-url-changes \
  --confirm-no-opening-label-changes \
  --confirm-no-visibility-changes \
  --confirm-everhome-only
```

## Change impact

**Medium** — presentation `Body` teaser paragraph only on `footprint.openings`. Rollback via report `openingsDescriptionsBeforeAfter`.

## Post-run validation

```bash
npm run brand-explorer-everhome-openings-description-cleanup-writer -- --brand everhome-suites --dry-run
npm run brand-explorer-final-qa-auditor -- --brand everhome-suites --dry-run
npm run brand-explorer-complete-build -- --brand everhome-suites --dry-run --target-quality active-profile
npm run brand-explorer-visual-display-defect-audit -- --brand everhome-suites --dry-run
npm run test:partner-intelligence-publish-readiness
npm run test:partner-intelligence-profile-governance-publish
```
