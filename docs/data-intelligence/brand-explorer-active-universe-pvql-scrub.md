# Active Universe PVQL Scrub

Targeted owner-facing Presentation scrub for the **16** `public_full_failing_pvql` brands in the Active/Live universe.

## Scope

- Only the 16 public-full targets
- Presentation Title / Body / Case Summary / tags only
- Preserves Recent Momentum & openings trailing announcement URLs

## Out of scope

- Everhome
- restored_pending_validation (Quality / Radisson / Blu / RED)
- active_but_unconfigured (BW Premier / Signature / Preferred)
- Draft / Under Review (Radisson Collection / Tapestry)

## Run

```bash
npm run brand-explorer-active-universe-pvql-scrub -- --dry-run
npm run brand-explorer-active-universe-pvql-scrub -- --apply \
  --approve-active-universe-pvql-scrub \
  --confirm-visible-owner-facing-fields-only \
  --confirm-no-company-validation-changes \
  --confirm-no-source-library-status-changes \
  --confirm-no-registry-approval-changes \
  --confirm-no-brand-status-changes \
  --confirm-no-release-field-changes \
  --confirm-no-public-restore-changes \
  --confirm-no-image-writes \
  --confirm-targeted-pvql-fields-only \
  --confirm-no-raw-urls \
  --confirm-no-forbidden-owner-facing-language
```

## Validation

```bash
npm run test:brand-explorer-public-visibility-quality-lock -- --public-full-only
```

Latest: see `reports/brand-explorer-active-universe-pvql-scrub.json`

