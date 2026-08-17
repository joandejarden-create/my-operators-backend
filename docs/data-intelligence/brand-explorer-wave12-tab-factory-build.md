# Wave 12 Tab Factory Build

Stage 4 of the Wave 12 factory generates owner-facing Presentation packs for 12 Under Review brands from Stage 3 source packs.

- Reports: `reports/brand-explorer-wave12-tab-factory-build.{json,md}`
- Per-brand: `reports/brand-explorer-wave12-tab-factory-build-{slug}.md`
- Target Guest Segments: `reports/brand-explorer-wave12-target-guest-segments.md`

## Allowed writes

- Presentation Title / Body / Case Summary / chips
- Brand Basics: Brand Positioning, Guest Psychographics Description
- Brand Basics: Target Guest Segments only when validated

## Forbidden

- Brand Status, release fields, Company Validated, Source Library, Registry
- Protected 27 brands, Radisson Collection, images

## Commands

```bash
npm run brand-explorer-wave12-factory -- --stage tab-factory-build --dry-run
npm run brand-explorer-wave12-factory -- --stage tab-factory-build --apply \
  --approve-wave12-tab-factory-build \
  --confirm-target-brands-only \
  --confirm-source-pack-grounded \
  --confirm-no-company-validation-changes \
  --confirm-no-source-library-status-changes \
  --confirm-no-registry-approval-changes \
  --confirm-no-brand-status-changes \
  --confirm-no-release-field-writes \
  --confirm-no-protected-27-brand-changes \
  --confirm-no-radisson-collection-changes \
  --confirm-no-image-writes \
  --confirm-no-broad-rewrites \
  --confirm-target-guest-segments-validated
```
