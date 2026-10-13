# Root Cause

**Classification:** `PDF_ONLY_ON_FILESYSTEM` (+ archive `pdfStatus: MISSING`)

## Exact cause

1. `generateNewDraft` created archive review `adp_mr_hilton_times_square_2026-10_v2` linked to certified period `adp_period_adp_hilton_times_square_20261005122652_63a1d8`.
2. The Performance Review PDF was written only to `reports/adp/hilton-times-square-performance-review/`.
3. Canonical attach (`attachPdfToReview` → archive `report.pdf` + index `pdfStatus: READY`) was never called.
4. Admin Reviews coverage requires `resolveAdpReportPdf` → archive `report.pdf` present; without it `hasPdf: false` / `NEEDS_REBUILD` / View PDF blocked.

## Not the cause

- Missing review registry record (record existed)
- Hotel ID mismatch
- Wrong certified period on v2
- Frontend hardcode
- Legacy Sep 27 period on current v2
