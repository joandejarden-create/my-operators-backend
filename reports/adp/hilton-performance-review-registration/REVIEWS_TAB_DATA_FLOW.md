# Reviews Tab Data Flow

```
/app#/admin/ai-demand?tab=reviews
  → public/app/admin/ai-demand-admin.html (tab shell)
  → public/js/admin-ai-demand-reviews.js
  → GET /api/admin/ai-demand-positioning/monthly-reviews
  → api/admin-adp-monthly-reviews.js#getAdminMonthlyReviews
  → listArchiveReviews (filesystem index)
  → buildPublishedAdpReviewCoverageV1 (one row per published ADP property)
  → resolveAdpReportPdf({ reviewId, publishedPeriodId, requirePublishedPeriodMatch })
  → archive .../report.pdf served by GET .../monthly-reviews/:reviewId/pdf
```

## Data store

- SoT: `reports/ai-demand-positioning/monthly-review/archive/index.json`
- Artifacts: `reports/ai-demand-positioning/monthly-review/archive/{propertyId}/{YYYY-MM}/{reviewId}/`
  - `review.json`, `metadata.json`, `report.pdf`, fingerprints, generation-log
- PDF public/admin URL: `/api/admin/ai-demand-positioning/monthly-reviews/{reviewId}/pdf`
  - `?download=1` for download disposition

## Visibility rules (coverage mode — default UI)

- Property must be in published ADP universe
- Latest preferred archive review with PDF READY (period match to published)
- `hasPdf` true only when archive `report.pdf` exists and period matches
- View/Download refuse when `pdfUnavailableReason` / `hasPdf === false`
