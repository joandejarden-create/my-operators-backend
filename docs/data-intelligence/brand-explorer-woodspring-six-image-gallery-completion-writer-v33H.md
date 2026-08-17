# Brand Explorer WoodSpring Six-Image Gallery Completion v33H

Founder review requires **six visible** Brand Materials gallery cards with durable hotel/property photography materialized on presentation **Image** fields. Registry linkage alone does not render photos in Brand Explorer UI.

## Scope

- `materials.gallery.1–6` for WoodSpring Suites (`recsOd51NzRPYsMko` / `woodspring-suites`)
- Brand Asset Registry rows for gallery assets
- Official Choice hoteldam discovery (Orlando `flf21`, Charlotte `ncb10`, Raleigh `nc936`)

## Does not modify

- Company Validated / Company Validation Date
- Source Library approval statuses
- Openings, Momentum, Proof, Standard Detail rows
- Summary URL / View Summary URL
- Everhome Suites, Suburban Studios, non-target brands

## Run

```bash
npm run brand-explorer-woodspring-six-image-gallery-completion-writer -- --brand woodspring-suites --dry-run
```

## Apply gates

All required:

- `--apply`
- `--approve-brand-explorer-v33H-woodspring-six-image-gallery-completion`
- `--founder-approved-woodspring-gallery-hotel-images`
- `--confirm-official-source-images-only`
- `--confirm-minimum-six-visible-gallery-images`
- `--confirm-no-company-validation-claim`
- `--confirm-no-summary-url-field`
- `--confirm-no-openings-momentum-proof-standard-changes`
- `--confirm-woodspring-only`

## Active-profile gallery rule (v33H auditor)

- Minimum **6 visible** gallery cards with API `imageUrl`
- Hidden gallery rows do **not** count
- Registry-only rows without presentation Image attachment do **not** count

## Data contract

| Item | Value |
|------|--------|
| Presentation table | Brand Setup - Brand Explorer Presentation |
| Registry table | Partner Intelligence - Brand Asset Registry |
| Image source | Choice hoteldam CDN URLs only |
| External Display Status (show) | `null` (clears Do Not Display) |
| External Display Status (hide) | Do Not Display |
