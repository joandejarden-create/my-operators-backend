# Explorer Media Promotion Writer v7

Generated: 2026-07-08T10:00:28.216Z
Mode: **dry-run** · Airtable modified: **no**
Brand: Tribute Portfolio `recCvV0PuZOi8c3hC`

## Summary

- Registry records scanned: **27**
- Formally approved assets available: **9**
- Eligible assets for promotion: **9**
- Ineligible assets: **18**
- Brand Setup media fields touched: **no**
- Explorer presentation records touched: **no**
- Company Validated fields untouched: **yes**

## Current Brand Setup Media State

- Logo field: `Logo` (populated)
- Hero field: `null` (schema-not-found)
- Hero verification field: `Explorer Hero Verification`
- Hero data-source field: `Explorer Hero Data Source`

## Proposed Promotions

- Logo: record `reczTkwignWPydWJp` -> `Logo`
- Hero: record `rec0tC9UF3R9peC9D` -> `overview.hero (presentation slot)`
- Gallery slots: materials.gallery.1:none, materials.gallery.2:none, materials.gallery.4:none, materials.gallery.5:none, materials.gallery.6:none
- Value drivers: Resort:none, Urban:none

## Slots Left Unchanged

- materials.gallery.3
- overview.scenario.3
- footprint.openings
- PR / Opening Link
- Value Driver: Boutique / Lifestyle
- Value Driver: Mixed-Use
- Value Driver: Conversion / Adaptive Reuse

## Apply Blockers / Overwrite Risks

- Logo field Logo already populated; use --allow-logo-overwrite
- Hero slot blocked: Slot overview.hero already has nonblank image
- Slot materials.gallery.1 already has nonblank image
- Slot materials.gallery.2 already has nonblank image
- Slot materials.gallery.4 already has nonblank image
- Slot materials.gallery.5 already has nonblank image
- Slot materials.gallery.6 already has nonblank image
- Slot overview.scenario.1 already has nonblank image
- Slot overview.scenario.2 already has nonblank image

## Apply Command

```bash
npm run explorer-media-promotion-writer -- --brand tribute-portfolio --apply --approve-explorer-media-promotion
```

Optional overwrite flags (only when intentionally needed):
- --allow-logo-overwrite
- --allow-nonblank-hero-overwrite
- --allow-presentation-slot-overwrite
- --allow-presentation-slot-image-patch
