# Ascend Hotel Collection — Source Gap Resolution

Generated: 2026-07-07T06:39:24.248Z
Mode: **dry_run**

## Summary

| Field | Value |
|-------|-------|
| Brand record | `reclkgOzvAcBheUSo` |
| Explorer | Active |
| Profile status | Active — Evidence Package Needed |
| Local PDFs in brand folder | 4 |
| FDD on disk | yes |
| Can proceed without local PDF | **no** |
| Pipeline ready | **yes** |
| Recommendation | **register_local_brochure_plus_consumer_press** |
| Process as | **mini-batch-3-single-brand** |

## URL probes

### Consumer — https://www.choicehotels.com/ascend

- HTTP 200 · 3036875 bytes · readable text 5829

### Press kit — https://media.choicehotels.com/ascend-hotel-collection-press-kit

- HTTP 200 · 136071 bytes · readable text 6412

### Development — https://www.choicehotelsdevelopment.com/our-brands/upscale/ascend

- JS-shell risk: **high**
- Recommendation: blocked_acquire_pdf_first

## Local files

- Scanned: `Choice Hotels International/Ascend Collection`
- Scanned: `Choice Hotels International/Ascend Hotel Collection`
- Scanned: `Choice Hotels International/Ascend`

- `Choice Hotels International/Ascend Collection/ASC_OnePager_2024_PRINT.pdf` (3466 chars)
- `Choice Hotels International/Ascend Collection/Ascend Development GTM Deck_14FEB2024.pdf` (14962 chars)
- `Choice Hotels International/Ascend Collection/Ascend Fact Sheet.pdf` (2 chars)
- `Choice Hotels International/Ascend Collection/brochure--ascend.pdf` (3518 chars)
- `Choice Hotels International/choice-dev-site-text/our-brands__upscale__ascend.txt` (2057 chars)
- `Choice Hotels International/choice-media-center-text/ascend-hotel-collection-press-kit.txt` (5995 chars)
- `Choice Hotels International/FDDs/Ascend Hotel Collection FDD 2026.pdf` (853287 chars)
- `Choice Hotels International/FDDs/Ascend Hotel Collection FDD 2025.pdf` (856995 chars)

- FDD candidate: `Ascend Hotel Collection FDD 2026.pdf`

## Duplicate checks

- Existing PI sources: **0**
- p0_consumer_page: duplicate=no
- p0_development_page: duplicate=no
- p0_development_pdf: duplicate=no
- p0_press_kit: duplicate=no

## Next apply command

```bash
npm run choice-legacy-batch-pipeline -- --batch mini-batch-3 --apply --approve-choice-legacy-batch-pipeline
```

### Prerequisites

- npm run choice-legacy-batch-pipeline -- --batch mini-batch-3 --dry-run
- npm run choice-legacy-batch-pipeline -- --batch mini-batch-3 --apply --approve-choice-legacy-batch-pipeline

## Does not do

- Rebuild Brand Explorer content
- Overwrite Brand Setup fields
- Register or approve sources in dry-run
- Approve facts or publish governance
- Set Company Validated or Company Validation Date