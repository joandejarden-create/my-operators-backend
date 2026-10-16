# Six-combination Admin verification — 2026-10-17

Artifact cause: Admin Reviews (`GET /api/admin/ai-demand-positioning/monthly-reviews` → `buildPublishedAdpReviewCoverageV1`) is the monthly performance-review archive. Westin and Wyndham were eligible and had a separate current-report PDF, but no archive review, so the row was `NEEDS_REBUILD` / `review_artifact_missing` with blank month, monitoring date, and edition, and only “Generate PDF”. The current-report PDF was not used to satisfy that row.

Repair: `generateNewDraft` (0 provider calls) plus `renderAndAttachMonthlyReviewPdfV1` for those two properties only. Mayflower remains `review_artifact_missing`. Westin GDI opportunities were not published.

| Hotel | Product | Row identity | Period / edition | Status | Open / Copy client | View / Download PDF | Proof |
|---|---|---|---|---|---|---|---|
| Renaissance New York Times Square | ADP | `adp_mr_renaissance_times_square_2026-10_v2` | October 2026 · V2 | READY | Share probe available | Existing archive PDF, distinct from current-report PDF | Unchanged |
| Renaissance New York Times Square | GDI | `recG66DQJKP2c0UNh` | Existing published set | READY | Existing share | Existing GDI PDF | Unchanged; ledger $4.31 / $7.69 |
| Westin Tampa Waterside | ADP | `adp_mr_westin_tampa_waterside_2026-10_v1` | October 2026 · monitoring 2026-10-13 · V1 | READY | Share probe available | Performance-review PDF 245,713 bytes, 7 pages, not the current-report PDF | Cover text: hotel, AI Demand Performance Review, Oct 13 2026 |
| Westin Tampa Waterside | GDI | `recdmo0CnhIZAwHGC` | — | 0 facing | Do not treat as customer-ready | Existing PDF may exist | 21 stored Airtable records were not published |
| Wyndham San Diego Bayside | ADP | `adp_mr_wyndham_san_diego_baysid_2026-10_v1` | October 2026 · monitoring 2026-10-13 · V1 | READY | Share probe available | Performance-review PDF 251,788 bytes, 8 pages, not the current-report PDF | Cover text: hotel, AI Demand Performance Review, Oct 13 2026 |
| Wyndham San Diego Bayside | GDI | `recnPB3Q02NO8Ma1o` | Published Airtable mirror | READY | Existing share | Existing GDI PDF | 51 facing, customer-ready 5 |

Authenticated Admin clicks are not verified. Local `/app/admin/ai-demand-admin.html` shows **Access restricted / Authentication required**. A missing Bearer token against this local server returned **403** because `DEV_AUTH_BYPASS_EMAIL` is set and that identity is not an AI Demand Reviews admin. `Authorization: Bearer not-a-jwt` returned **401**. Neither result proves View or Download.
