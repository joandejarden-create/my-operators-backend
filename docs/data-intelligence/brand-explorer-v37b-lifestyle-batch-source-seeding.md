# Brand Explorer v37B — Lifestyle Batch Source Seeding

## Purpose
Register Hotel Indigo factory config, validate MGallery config, seed Source Library rows only, and produce batch readiness for draft-apply eligibility.

## Command
```bash
npm run brand-explorer-v37b-lifestyle-batch-source-seeding -- --brands hotel-indigo,mgallery-collection --dry-run
```

## Apply gates (Source Library only)
- `--approve-brand-explorer-v37B-lifestyle-batch-source-seeding`
- `--confirm-source-library-only`
- `--confirm-no-company-validation-claim`
- `--confirm-no-presentation-row-changes`
- `--confirm-no-registry-changes`
- `--confirm-no-image-field-changes`
- `--confirm-no-active-profile-approval`
- `--confirm-hotel-indigo-mgallery-only`

## Outputs
- `reports/brand-explorer-v37b-lifestyle-batch-source-seeding.json`
- `reports/brand-explorer-v37b-lifestyle-batch-source-seeding.md`
- `reports/brand-explorer-v37b-hotel-indigo-config.md`
- `reports/brand-explorer-v37b-hotel-indigo-image-review.md`
- `reports/brand-explorer-v37b-mgallery-image-review.md`