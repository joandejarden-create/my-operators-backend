# Brand Explorer Everhome Image Governance Recognition Writer v32F

**Batch:** v32F  
**Brand:** Everhome Suites (`everhome-suites`, `recqkkrsevi4r9ibj`)  
**Default:** dry-run / read-only

## Purpose

After v32C registry creation, v32D internal-language cleanup, and v32E openings/momentum copy parity:

1. **Audit** working presentation images without reloading or replacing them.
2. **Recognize/link** Brand Asset Registry metadata to existing working images.
3. **Materialize** Image fields only when founder-approved assets exist and the presentation row is missing an image.
4. **Proper Case** cleanup for `footprint.momentum` headings (Title field only).

## Out of scope

- Reloading, replacing, clearing, or re-fetching working images
- Auto-approving registry assets or Source Library rows
- Company Validated / Company Validation Date changes
- Openings label/chip/source link changes
- Momentum body or source URL changes
- WoodSpring, Suburban, or non-Everhome brands

## Apply gates

```bash
npm run brand-explorer-everhome-image-governance-recognition-writer -- \
  --brand everhome-suites \
  --apply \
  --approve-brand-explorer-v32F-everhome-image-governance-recognition \
  --confirm-founder-approved-assets-only \
  --confirm-preserve-working-images \
  --confirm-no-company-validation-claim \
  --confirm-no-opening-label-changes \
  --confirm-everhome-only
```

## Data contract

| Area | Table | Fields touched |
|------|-------|----------------|
| Registry recognition | Brand Asset Registry | `Source Notes`, `Recommended Explorer Slot`, `Source Page URL`, `Source URL` (missing only), `Review Notes` (superseded duplicate annotation) |
| Momentum headings | Brand Explorer Presentation | `Title` only on `footprint.momentum` |
| Image materialization | Brand Explorer Presentation | `Image` only when founder-approved + row missing image |

## Change impact

**High** — registry metadata writes and presentation Title patches. Rollback: revert Airtable patches from report `registryPatches` / `momentumHeadingBeforeAfter`.

## Post-run validation

```bash
npm run brand-explorer-everhome-image-governance-recognition-writer -- --brand everhome-suites --dry-run
npm run brand-explorer-final-qa-auditor -- --brand everhome-suites --dry-run
npm run brand-explorer-complete-build -- --brand everhome-suites --dry-run --target-quality active-profile
npm run brand-explorer-visual-display-defect-audit -- --brand everhome-suites --dry-run
npm run test:partner-intelligence-publish-readiness
npm run test:partner-intelligence-profile-governance-publish
```

## Next writer

**v32G** — Everhome per-brand activation after founder registry image approvals.
