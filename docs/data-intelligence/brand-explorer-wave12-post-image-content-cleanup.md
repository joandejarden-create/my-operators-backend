# Wave 12 Post-Image Content Cleanup

Stage 6 of the Wave 12 factory applies targeted non-image cleanup after Stage 5 image materialization.

## Scope

- Thin owner-facing scenarios / proofs / lifecycle / opening steps
- Stub chips (`conversion-friendly`, `neighborhood focus`)
- Recent Momentum dated cards, CALA-first Sort Order, International Reference labels
- Openings region labels where needed

## Forbidden

- Brand Status, release fields, Company Validated, Source Library, Registry
- Protected 27 brands, Radisson Collection, non-target brands
- Image materialization / broad rewrites

## Commands

```bash
npm run brand-explorer-wave12-factory -- --stage post-image-content-cleanup --dry-run
npm run brand-explorer-wave12-factory -- --stage post-image-content-cleanup --apply \
  --approve-wave12-post-image-content-cleanup \
  --confirm-target-brands-only \
  --confirm-targeted-field-fixes-only \
  --confirm-recent-momentum-and-openings-quality \
  --confirm-cala-first-priority \
  --confirm-international-reference-labels-where-needed \
  --confirm-no-company-validation-changes \
  --confirm-no-source-library-status-changes \
  --confirm-no-registry-approval-changes \
  --confirm-no-brand-status-changes \
  --confirm-no-release-field-writes \
  --confirm-no-protected-27-brand-changes \
  --confirm-no-radisson-collection-changes \
  --confirm-no-image-writes-except-caption-only-if-flagged \
  --confirm-no-broad-rewrites \
  --confirm-no-raw-urls
```

Protected baseline remains 27.

## Status

Stage 6 applied and acceptance gates passed (2026-07-24). See `reports/brand-explorer-wave12-post-image-content-cleanup-acceptance.md`.

