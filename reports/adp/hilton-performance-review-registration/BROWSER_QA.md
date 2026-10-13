# Browser / Visibility QA

Automated browser hit `/app#/admin/ai-demand?tab=reviews` showed Memberstack sign-in wall (no session in Cursor browser).

Validated through the same backend path the Reviews tab uses (`getAdminMonthlyReviews` + `resolveAdpReportPdf`):

| Check | Result |
|-------|--------|
| List handler ok | true |
| Hilton in coverage catalog | true |
| Hotel name | Hilton New York Times Square |
| Coverage status | READY |
| hasPdf | true |
| Review ID | `adp_mr_hilton_times_square_2026-10_v2` |
| Reporting month | October 2026 |
| Monitoring date | 2026-10-05 |
| View URL | `/api/admin/ai-demand-positioning/monthly-reviews/adp_mr_hilton_times_square_2026-10_v2/pdf` |
| PDF available | true |
| PDF magic %PDF | true |
| PDF bytes | 265102 |
| Oct READY count (Hilton) | 1 |
| Duplicate current READY | 1 |
| Bethesda regression READY | true |
| Source pack preserved | true |
| PDF regenerated | NO |

Manual founder check after login: open Reviews tab → Hilton New York Times Square → View PDF.
