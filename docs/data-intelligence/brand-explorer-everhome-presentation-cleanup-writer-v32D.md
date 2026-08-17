# Brand Explorer Everhome Presentation Cleanup Writer v32D

Copy-only presentation repairs for **Everhome Suites** (`recqkkrsevi4r9ibj`). Removes internal-language from owner-facing copy, backfills thin sections, and reports image/registry governance — **without** changing images, visibility, or approvals.

## Commands

```bash
npm run brand-explorer-everhome-presentation-cleanup-writer -- \
  --brand everhome-suites --dry-run
```

Apply:

```bash
npm run brand-explorer-everhome-presentation-cleanup-writer -- \
  --brand everhome-suites \
  --apply \
  --approve-brand-explorer-v32D-everhome-presentation-cleanup \
  --confirm-no-company-validation-claim \
  --confirm-no-image-field-changes \
  --confirm-no-image-or-registry-approval-changes \
  --confirm-no-visibility-changes \
  --confirm-everhome-only
```

## Guardrails

- **PATCH fields:** `Title`, `Body`, `Slot Key`, `Sort Order`, `Brand` link only
- **Never writes:** `Image`, `External Display Status`, `Company Validated`
- **Preserves:** working Explorer images, openings/momentum visibility, registry/source approvals
- **Momentum/openings:** internal-language cleanup only; full rebuild deferred to **v32E**

## Internal-language replacements

FDD / Item 19 / franchise disclosure → owner diligence / commercial model review language. Census / metadata / source-capture → neutral owner-facing phrasing.

## Backfill packages

Thin slots receive Tribute-style owner-facing copy: `overview.featured_application`, scenarios, loyalty, economics, standards, portfolio context, demand scenario, gallery captions.

## Post-run verification

```bash
npm run brand-explorer-final-qa-auditor -- --brand everhome-suites --dry-run
npm run brand-explorer-complete-build -- --brand everhome-suites --dry-run --target-quality active-profile
npm run brand-explorer-visual-display-defect-audit -- --brand everhome-suites --dry-run
```

**Next writer:** v32E — Everhome openings/momentum rebuild
