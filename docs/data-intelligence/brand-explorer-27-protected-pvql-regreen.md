# Brand Explorer — Protected 27 PVQL Re-Green

Version: `27-protected-pvql-regreen-v1`

## Purpose

Clear PVQL `generic_copy_scan` + `tab_factory_audit` on three protected public-full brands before Wave 12 Stage 3 resumes.

## Targets

- Preferred Hotels & Resorts (`preferred-hotels-and-resorts`)
- Radisson Individuals by Choice (`radisson-individuals-by-choice`)
- Small Luxury Hotels of the World (`small-luxury-hotels-of-the-world`)

## Exact fix

Brand Basics multi-select **Target Guest Segments**: remove `Luxury / Discerning` when `Leisure` is also selected, so rendered Audience no longer matches golden `generic_audience_prose`.

## Commands

```bash
npm run brand-explorer-27-protected-pvql-regreen -- --brands preferred-hotels-and-resorts,radisson-individuals-by-choice,small-luxury-hotels-of-the-world --dry-run

npm run brand-explorer-27-protected-pvql-regreen -- --brands preferred-hotels-and-resorts,radisson-individuals-by-choice,small-luxury-hotels-of-the-world --apply \
  --approve-protected-27-pvql-regreen \
  --confirm-target-brands-only \
  --confirm-targeted-field-fixes-only \
  --confirm-no-company-validation-changes \
  --confirm-no-source-library-status-changes \
  --confirm-no-registry-approval-changes \
  --confirm-no-brand-status-changes \
  --confirm-no-release-field-changes \
  --confirm-no-image-writes \
  --confirm-no-wave12-brand-changes \
  --confirm-no-broad-rewrites \
  --confirm-no-raw-urls \
  --confirm-no-generic-copy
```

## Forbidden

CV / Source Library / Registry / Brand Status / release / images / Wave 12 / unrelated protected brands / Presentation broad rewrites.

## Acceptance

1. Three target brands pass PVQL — **done**
2. `test:brand-explorer-public-visibility-quality-lock -- --public-full-only` passes — **done** (`overallPass=true`, publicFull=27)
3. Tightened `test:brand-explorer-27-active-public-full-baseline` requires fresh/clean PVQL — **done**
4. Wave 12 may resume at Stage 3 — **cleared** (preflight `protectedBaselineClean=true`)

