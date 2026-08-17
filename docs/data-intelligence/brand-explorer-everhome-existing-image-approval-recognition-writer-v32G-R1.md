# Brand Explorer Everhome Existing Image Approval Recognition Writer v32G-R1

**Batch:** v32G-R1  
**Brand:** Everhome Suites (`everhome-suites`, `recqkkrsevi4r9ibj`)  
**Default:** dry-run / read-only

## Purpose

v32G incorrectly treated 13 working Everhome Explorer images as pending founder manual approval. The founder has confirmed those images are correct and loaded appropriately.

v32G-R1 **aligns Brand Asset Registry governance** to recognize founder-confirmed working images — without reloading, replacing, or writing presentation `Image` fields.

## What changes

- Registry `Asset Status`, `Explorer Use Permission`, `Usage Review Status`
- `Visual Slot Validation Status` / notes, slot metadata, source page URL alignment
- Per-row canonical registry links via `Source Notes`
- Optional registry **creates** when a working image has no suitable registry row
- Duplicate superseded notes only (no deletes)

## What does not change

- Presentation `Image` / attachment fields
- Opening / momentum copy, labels, chips, source URLs
- Source Library approvals
- `Company Validated` / `Company Validation Date`
- WoodSpring, Suburban, or other brands

## Apply gates

```bash
npm run brand-explorer-everhome-existing-image-approval-recognition-writer -- \
  --brand everhome-suites \
  --apply \
  --approve-brand-explorer-v32G-R1-everhome-existing-image-approval-recognition \
  --founder-confirmed-current-everhome-images-approved \
  --confirm-preserve-working-images \
  --confirm-no-image-field-changes \
  --confirm-no-company-validation-claim \
  --confirm-everhome-only
```

## Change impact

**High** — registry approval/governance writes only. Rollback via report patch/create list; presentation images untouched.

## Post-run validation

```bash
npm run brand-explorer-everhome-existing-image-approval-recognition-writer -- --brand everhome-suites --dry-run
npm run brand-explorer-final-qa-auditor -- --brand everhome-suites --dry-run
npm run brand-explorer-complete-build -- --brand everhome-suites --dry-run --target-quality active-profile
npm run brand-explorer-visual-display-defect-audit -- --brand everhome-suites --dry-run
npm run test:partner-intelligence-publish-readiness
npm run test:partner-intelligence-profile-governance-publish
```
