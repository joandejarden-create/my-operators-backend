# Wave 12 — Image / Visual Materialization

Stage 5 materializes gallery, scenario, and openings images for the 12 Wave 12 factory-preview brands.

## Commands

```bash
npm run brand-explorer-wave12-factory -- --stage image-materialization --dry-run
npm run brand-explorer-wave12-factory -- --stage image-materialization --apply \
  --approve-wave12-image-materialization \
  --confirm-target-brands-only \
  --confirm-no-company-validation-changes \
  --confirm-no-source-library-status-changes \
  --confirm-no-registry-approval-changes \
  --confirm-no-brand-status-changes \
  --confirm-no-release-field-writes \
  --confirm-no-protected-27-brand-changes \
  --confirm-image-uniqueness \
  --confirm-image-role-match \
  --confirm-cala-first-openings-priority \
  --confirm-international-reference-labels-where-needed \
  --confirm-no-logo-only-filler \
  --confirm-no-wrong-brand-images
```

## Guardrails

- Target brands only (Wave 12 / factory preview)
- No Brand Status / release / CV / Source Library / Registry writes
- No protected 27 image or content writes
- CALA-first openings; International Reference labels when non-CALA
- Image uniqueness + role-match required

## Fixtures

- `fixtures/wave12-{slug}-gallery-pool.json` (from `scripts/harvest-wave12-image-pools.mjs`)

## Reports

- `reports/brand-explorer-wave12-image-materialization.{json,md}`
- `reports/brand-explorer-wave12-image-materialization-{slug}.md`
