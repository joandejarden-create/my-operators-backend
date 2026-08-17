# Wave 13 — Post-Image Content Cleanup

Stage 6 cleans residual SO/ positioning copy and Fairmont San Francisco openings visibility,
and documents protected-39 Bunkhouse PVQL re-green diagnosis.

## Outcomes

- Protected 39 live PVQL re-greened to **39/39** (Bunkhouse was stale `remediation_locked`; no Bunkhouse writes).
- SO/ Basics `recTJdPlr4mDs9app`: Brand Positioning + Guest Psychographics Description rewritten to fashion-led luxury lifestyle (cleared `too_thin` + economy-oriented copy).
- Fairmont San Francisco openings `recQXp6Y3EkfaC9hG`: **Do Not Display** (idempotent).
- Steward gaps (`snapshot.*`, `footprint.primary_regions`) intentionally not invented.
- No Brand Status / release / CV / Source Library / Registry / image writes.
- Ready: `wave13_post_image_cleanup_ready_for_founder_review`

## Commands

```bash
npm run brand-explorer-wave13-factory -- --stage post-image-content-cleanup --dry-run
npm run brand-explorer-wave13-factory -- --stage post-image-content-cleanup --apply \
  --approve-wave13-post-image-content-cleanup \
  --confirm-seven-brand-stage6-scope \
  --confirm-target-brands-only \
  --confirm-targeted-field-fixes-only \
  --confirm-so-live-basics-record-recTJdPlr4mDs9app \
  --confirm-so-steward-data-not-invented \
  --confirm-fairmont-san-francisco-do-not-display-only \
  --confirm-protected-39-live-pvql-regreen \
  --confirm-no-company-validation-changes \
  --confirm-no-source-library-status-changes \
  --confirm-no-registry-approval-changes \
  --confirm-no-brand-status-changes \
  --confirm-no-release-field-writes \
  --confirm-no-image-writes \
  --confirm-no-house-of-originals-writes \
  --confirm-no-morgans-originals-writes \
  --confirm-no-radisson-collection-changes \
  --confirm-no-broad-rewrites \
  --confirm-no-adr \
  --confirm-no-revpar \
  --confirm-no-fee-stack \
  --confirm-no-raw-urls
```

## Reports

- `reports/brand-explorer-wave13-post-image-cleanup.json`
- `reports/brand-explorer-wave13-post-image-cleanup.md`
- `reports/brand-explorer-wave13-post-image-cleanup-so-hotels-and-resorts.md`
- `reports/brand-explorer-wave13-post-image-cleanup-fairmont-hotels-and-resorts.md`
- `reports/brand-explorer-39-bunkhouse-remediation-lock-resolution.json`
- `reports/brand-explorer-39-bunkhouse-remediation-lock-resolution.md`

## Ready statement

`wave13_post_image_cleanup_ready_for_founder_review`
