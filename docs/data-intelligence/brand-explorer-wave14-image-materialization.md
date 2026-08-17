# Wave 14 — Image / Visual Materialization

Stage 5 materializes gallery, scenario, and openings images for the nine Wave 14 Marriott factory-preview brands.

## Commands

```bash
npm run brand-explorer-wave14-factory -- --stage image-materialization --dry-run
npm run brand-explorer-wave14-factory -- --stage image-materialization --apply \
  --approve-wave14-image-materialization \
  --confirm-nine-brand-stage5-scope \
  --confirm-target-brands-only \
  --confirm-no-protected-46-brand-changes \
  --confirm-no-accor-wave13-active-brand-writes \
  --confirm-no-house-of-originals-writes \
  --confirm-no-morgans-originals-writes \
  --confirm-no-radisson-collection-changes \
  --confirm-no-company-validation-changes \
  --confirm-no-source-library-status-changes \
  --confirm-no-registry-approval-changes \
  --confirm-no-brand-status-changes \
  --confirm-no-release-field-writes \
  --confirm-no-content-rewrites \
  --confirm-image-uniqueness \
  --confirm-image-role-match \
  --confirm-no-wrong-brand-images \
  --confirm-no-sibling-brand-images \
  --confirm-marriott-brand-family-separated \
  --confirm-four-points-flex-not-four-points \
  --confirm-studiores-not-residence-inn-or-towneplace \
  --confirm-cala-first-openings-priority \
  --confirm-international-reference-labels-where-needed \
  --confirm-property-url-matches-required-for-named-gallery \
  --confirm-cleanly-unavailable-for-unsupported-property-images
```

## Guardrails

- Target brands only (nine Marriott Wave 14)
- No Accor Wave 13 Active writes
- No protected 46 / House of Originals / Morgans / Radisson Collection writes
- No Brand Status / release / CV / Source / Registry writes
- No content body rewrites
- Four Points Flex ≠ Four Points by Sheraton
- StudioRes ≠ Residence Inn / TownePlace / Element / Apartments
- CALA-first openings where supported; International Reference otherwise
- Cleanly unavailable / hold for unsupported Flex property URLs

## Apply result (2026-07-28)

- Ready: **9/9** · Patches: **103**
- Scenario images: **3/3** for all nine (clears Stage 4 `overview.scenario.*` missing-image fails)
- Gallery: **6/6** for eight brands; Flex **4/6** with documented shortfall
- Openings: **3/3** for eight brands; Flex **0/3** openings held (Do Not Display)
- Flex hygiene: `scripts/wave14-flex-hold-hygiene.mjs`

## Fixtures

- `fixtures/wave14-{slug}-gallery-pool.json` (from `scripts/harvest-wave14-image-pools.mjs`)
- Flex curated seed: `WAVE14_CURATED_POOL_SEED_BY_SLUG` in `brand-explorer-wave14-image-supplemental.js`

## Reports

- `reports/brand-explorer-wave14-image-materialization.{json,md}`
- `reports/brand-explorer-wave14-image-materialization-{slug}.md`
- `reports/brand-explorer-wave14-active-baseline-watch-note.md`

Ready: `wave14_image_materialization_ready_for_post_image_cleanup`
