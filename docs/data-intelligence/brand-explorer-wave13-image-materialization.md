# Wave 13 — Image / Visual Materialization

Stage 5 materializes gallery (6), scenario (3), and openings/property (3) images for the **seven** Wave 13 Stage 4–approved brands only.

**Status:** Applied. Ready statement: `wave13_image_materialization_ready_for_post_image_cleanup`.

## Scope

- `mama-shelter`
- `mercure`
- `ibis`
- `novotel`
- `pullman`
- `so-hotels-and-resorts`
- `fairmont-hotels-and-resorts`

Excluded: The House of Originals, Morgans Originals, Radisson Collection, protected 39, non-targets.

## Commands

```bash
npm run brand-explorer-wave13-factory -- --stage image-materialization --dry-run
npm run brand-explorer-wave13-factory -- --stage image-materialization --apply \
  --approve-wave13-image-materialization \
  --confirm-seven-brand-stage5-scope \
  --confirm-target-brands-only \
  --confirm-house-of-originals-excluded \
  --confirm-no-morgans-originals-writes \
  --confirm-no-radisson-collection-changes \
  --confirm-no-protected-39-brand-changes \
  --confirm-no-company-validation-changes \
  --confirm-no-source-library-status-changes \
  --confirm-no-registry-approval-changes \
  --confirm-no-brand-status-changes \
  --confirm-no-release-field-writes \
  --confirm-image-uniqueness \
  --confirm-image-role-match \
  --confirm-scene7-filename-aware-distinct-images \
  --confirm-cala-first-openings-priority \
  --confirm-international-reference-labels-where-needed \
  --confirm-no-logo-only-filler \
  --confirm-no-wrong-brand-images \
  --confirm-no-sibling-brand-images \
  --confirm-no-content-rewrites \
  --confirm-no-so-steward-data-fills
```

## Guardrails

- Target brands only (seven Stage 5 brands)
- Image / caption fields only — no Presentation Body content rewrites (except openings card body on create + IR titles)
- No Brand Status / release / CV / Source Library / Registry writes
- No SO/ steward fills (`snapshot.*`, `footprint.primary_regions`)
- No protected 39 image or content writes
- CALA-first openings; International Reference labels when non-CALA
- Image uniqueness + role-match required (Scene7/filename-aware)
- Accor `ahstatic.com` URLs proxied via `wsrv.nl` for Airtable attachment fetch

## Fixtures

- `fixtures/wave13-{slug}-gallery-pool.json` (from `scripts/harvest-wave13-image-pools.mjs`)
- Sibling/nearby hotel thumbs embedded on Accor ALL pages are rejected when URL hotel code ≠ property code

## Identity note

- SO/ Basics Brand Name = `SO/` · factory Presentation Brand Name = `SO/ Hotels & Resorts`
- Live Basics record ID: `recTJdPlr4mDs9app` (stale `recPCWbTmBPe5SMm0` removed)

## Reports

- `reports/brand-explorer-wave13-image-materialization.{json,md}`
- `reports/brand-explorer-wave13-image-materialization-{slug}.md`

## Ready statement

`wave13_image_materialization_ready_for_post_image_cleanup`
