# Wave 15 — Image / Visual Materialization

Stage 5 materializes gallery, scenario, and openings images for the eight Wave 15 Hilton factory-preview brands (Hilton Hotels & Resorts, Homewood Suites by Hilton, Home2 Suites by Hilton, Tru by Hilton, DoubleTree by Hilton, Hampton by Hilton, Hilton Garden Inn, Spark by Hilton).

## Commands

```bash
npm run brand-explorer-wave15-factory -- --stage image-materialization --dry-run
npm run brand-explorer-wave15-factory -- --stage image-materialization --apply \
  --approve-wave15-image-materialization \
  --confirm-eight-brand-stage5-scope \
  --confirm-target-brands-only \
  --confirm-protected-54-identity-preflight-passed \
  --confirm-no-protected-54-brand-changes \
  --confirm-no-marriott-hotels-writes \
  --confirm-no-four-points-flex-writes \
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
  --confirm-hilton-brand-family-separated \
  --confirm-hilton-hotels-not-hilton-corporate \
  --confirm-homewood-not-home2 \
  --confirm-home2-not-homewood-or-tru \
  --confirm-tru-not-spark-or-hampton \
  --confirm-spark-not-tru-or-hampton \
  --confirm-cala-first-openings-priority \
  --confirm-americas-reference-before-international-reference \
  --confirm-property-url-matches-required-for-named-gallery \
  --confirm-cleanly-unavailable-for-unsupported-property-images
```

## Protected 54 identity preflight (mandatory)

Runs before planning / applying. Fails loudly with `stopRecommended` if:

- Live Active/Live universe count !== 54
- Marriott Hotels brand (recordId `recn59UtkyyoYwzSz`) is missing OR its name has drifted back to bare "Marriott" (must match `/Marriott Hotels/i`)
- Four Points Flex by Sheraton re-entered Active/Live (must remain held)

## Guardrails

- Target brands only (eight Wave 15 Hilton family brands)
- No writes to Marriott Hotels, Four Points Flex by Sheraton, House of Originals, Morgans, Radisson Collection
- No protected 54 brand changes
- No Brand Status / release / CV / Source / Registry writes
- No content body rewrites
- Hilton Hotels & Resorts ≠ Hilton Worldwide (corporate)
- Homewood ≠ Home2; Tru ≠ Spark ≠ Hampton; DoubleTree ≠ Hilton flagship
- CALA-first openings where supported; Americas / International Reference otherwise
- Cleanly unavailable for unsupported property images

## Fixtures

- `fixtures/wave15-{slug}-gallery-pool.json` (from `scripts/harvest-wave15-image-pools.mjs`)

## Reports

- `reports/brand-explorer-wave15-image-materialization.{json,md}`
- `reports/brand-explorer-wave15-image-materialization-{slug}.md`

## Hilton CDN ingest (durable)

Do **not** proxy Hilton CDN hosts (`hilton.com/im/`, `hiltonstatic.com`, `cache.hilton.com`) through wsrv — weserv returns 404. Append Hilton `impolicy` size params so Airtable receives a full JPEG (~200KB), not the bare ~2KB thumbnail. Implemented in `toAirtableFetchableImageUrl` (`brand-explorer-lane2-image-materialization.js`).

## Stage 5 acceptance (2026-08-04 apply)

- 8/8 image materialization · gallery 6/6 · scenario 3/3 · openings 3/3
- All eight remain Under Review / factory preview only
- Uniqueness + role-match PASS; no-empty + golden PASS; protected 54 PASS; semantic C/H/M = 0
- Remaining completeness fails are steward `snapshot.typical_keys` only (not Stage 5)
- Ready: `wave15_image_materialization_ready_for_post_image_cleanup`

Dry-run ready: `wave15_stage5_image_materialization_dry_run_ready`
