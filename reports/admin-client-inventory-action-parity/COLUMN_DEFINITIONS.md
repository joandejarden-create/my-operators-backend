# GDI Reports columns

| Column | Meaning | Source |
| --- | --- | --- |
| Client Opportunities | External client-list count, before presentation sanitization | `filterCustomerFacingOpportunities(filterSalespersonView(canonical opportunities), { hotelId })` |
| Ready | Strict customer-ready count in the current GDI report | `executiveSummary.customerReadyCount` via `isGdiCustomerOpportunityReady` |
| Action Set | Action-set size in the current GDI report | `executiveSummary.actionSetCount` |
| Watch | Future-watch cards included in the current report | sample `futureWatchCount` (max 12); total `futureWatchTotal`; `watchSampleCapped` when total is larger |
| Report Status | READY if strict ready > 0, else WATCH ONLY if the watch sample > 0, else NO READY OPPORTUNITIES. Read failure is BLOCKED / Unavailable counts | `deriveGdiReportStatus` |
| PDF | Whether the GDI PDF artifact for this hotel exists | `hasGdiReportPdf` / PDF store `generatedAt` |
| Actions | Open Client Link, Copy Client Link, View PDF, Download PDF | `resolveExternalClientLinks` for the row hotel and product. GDI PDF URL is `/api/admin/group-demand-intelligence/hotels/{hotelId}/report-pdf` |

Hotel identity for these rows:

- Renaissance New York Times Square: HPC `recG66DQJKP2c0UNh`, ADP `adp_renaissance_times_square`
- Westin Tampa Waterside: HPC `recdmo0CnhIZAwHGC`, ADP `adp_westin_tampa_waterside`
- Wyndham San Diego Bayside: HPC `recnPB3Q02NO8Ma1o`, ADP `adp_wyndham_san_diego_bayside`

ADP monthly performance-review PDFs stay on the Reviews tab and use `reviewId`. They are not the GDI PDF and not the current-report web print.
