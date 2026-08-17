# Wave 15 — Post-Image Content Cleanup

Stage 6 cleans residual post-image issues for the eight Wave 15 Hilton factory-preview brands before founder review.

## Primary fix

Overview **Typical Keys Range** (`snapshot.typical_keys`) is derived from **Brand Setup - Portfolio & Performance** `Minimum Property Size (Rooms)` / `Maximum Property Size (Rooms)` (NBSP-safe live max column name).

Stage 6 copies steward **Project Fit** `Min - Room Count` / `Max - Room Count` into those Portfolio fields (Choice-batch pattern) for all eight brands when blank or stale.

## Commands

```bash
npm run brand-explorer-wave15-factory -- --stage post-image-content-cleanup --dry-run
npm run brand-explorer-wave15-factory -- --stage post-image-content-cleanup --apply \
  --approve-wave15-post-image-content-cleanup \
  --confirm-eight-brand-stage6-scope \
  --confirm-target-brands-only \
  --confirm-all-eight-remain-under-review \
  --confirm-snapshot-typical-keys-handled \
  --confirm-no-brand-status-changes \
  --confirm-no-release-field-writes \
  --confirm-no-company-validation-changes \
  --confirm-no-source-library-status-changes \
  --confirm-no-registry-approval-changes \
  --confirm-no-public-restore-registry-changes \
  --confirm-no-protected-54-brand-changes \
  --confirm-no-marriott-hotels-writes \
  --confirm-no-four-points-flex-writes \
  --confirm-no-house-of-originals-writes \
  --confirm-no-morgans-originals-writes \
  --confirm-no-radisson-collection-changes \
  --confirm-no-broad-rewrites \
  --confirm-no-wrong-brand-images \
  --confirm-no-sibling-brand-images \
  --confirm-hilton-brand-family-separated \
  --confirm-no-internal-source-language \
  --confirm-no-raw-urls \
  --confirm-recent-momentum-semantics-preserved \
  --confirm-portfolio-mix-structured \
  --confirm-openings-use-actual-property-names \
  --confirm-geo-footprint-source-supported-or-cleanly-unavailable
```

## Guardrails

- Eight Wave 15 Hilton brands only; remain Under Review / factory preview
- No Brand Status / release / CV / Source / Registry / protected-54 writes
- No Marriott Hotels / Four Points Flex / House of Originals / Morgans / Radisson Collection writes
- No inventing key counts — Project Fit ranges only
- No broad Presentation rewrites; no new image materialization

## Reports

- `reports/brand-explorer-wave15-post-image-cleanup-failures.{json,md}`
- `reports/brand-explorer-wave15-post-image-cleanup.{json,md}`
- `reports/brand-explorer-wave15-post-image-cleanup-{slug}.md`

Ready: `wave15_post_image_cleanup_ready_for_founder_review` (apply) · `wave15_stage6_post_image_cleanup_dry_run_ready` (dry-run)

## Stage 6 acceptance notes (2026-08-04)

- Synced Project Fit Min/Max Room Count → Portfolio property size for all eight brands (blank fill + stale 200–1000 reconciliation).
- Registered Wave 15 entries in `CALA_AVAILABLE_BY_SLUG` so CALA-labeled momentum/openings are not forced to International Reference.
- Added Wave 14 Active Marriott cohort to `EXTRA_ACTIVE_IDENTITY_ANCHORS` (Accor graduation pattern) so protected-54 PVQL no longer `brand_not_found`.
- Completeness / no-empty / golden / uniqueness / role-match / momentum / protected-54 / semantic PASS after apply.
- All eight remain Under Review / factory preview; no Brand Status / release / CV / Source / Registry / protected-54 content writes.
