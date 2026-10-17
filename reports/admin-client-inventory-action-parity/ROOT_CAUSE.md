# Admin GDI client inventory and actions

## Why the table did not show client delivery

The GDI Reports row was a report-summary row. Ready, Action Set, and Watch came from `getGdiPdfReportData` executive-summary metrics. They were never the external client-list count.

Open Client Link was added only when `gdiShareAvailable` was true, and Copy Client Link lived inside More. A valid external bearer can still open the client page when Admin cannot reconstruct that bearer in the current process (missing `GDI_SHARE_CAPABILITY_SECRET`, or confirm rejecting a non-production host). The icon was therefore absent even though the issued client URL worked. The action was also not tied to a strict-ready count; the missing icon was the share-availability gate, not Ready = 0.

## What 2 / 2 / 12 and 5 / 5 / 11 were

Re-read from canonical Airtable on 2026-10-17 via `getGdiPdfReportData` (strict canonical, no filesystem fallback):

| Hotel | Client Opportunities | Reviewed/facing subset | Ready | Action Set | Watch |
| --- | --- | --- | --- | --- | --- |
| Renaissance New York Times Square `recG66DQJKP2c0UNh` | 44 | 42 | 2 | 2 | 12 of 12 |
| Wyndham San Diego Bayside `recnPB3Q02NO8Ma1o` | 51 | 51 | 5 | 5 | 11 of 11 |
| Westin Tampa Waterside `recdmo0CnhIZAwHGC` | 0 | 0 | 0 | 0 | 12 sample of 15 |

The screenshot values 2/2/12, 5/5/11, and 0/0/12 are Ready / Action Set / Watch sample. Watch 12 on Westin is the report-builder cap (`slice(0, 12)`), not the full watch inventory (15). Production client list for Renaissance resolved to 44 opportunities on the issued share.

PDF files exist locally for all three (`hasGdiReportPdf` true). PDF READY does not mean the client list is non-empty. Westin is a working empty client list when a share verifies, shown as “No Published Opportunities” plus “Working empty share”. A missing share is “Client link missing”. A canonical read failure is “Unavailable”, not 0.

## What changed

- Client Opportunities is counted with `filterCustomerFacingOpportunities(filterSalespersonView(raw opportunities))` before enrichment and presentation sanitization.
- Watch sample and watch total stay separate. A capped sample is labeled `sample of {total}`.
- Open Client Link and Copy Client Link are primary Actions when the shared resolver has a usable hotel/product URL. They do not require Ready > 0 or a PDF.
- View PDF and Download PDF stay on `/api/admin/group-demand-intelligence/hotels/{hotelId}/report-pdf`. ADP monthly PDFs stay bound to `reviewId`.
- GDI share ranking prefers `production-distribution`, then a client-share label, then the newest active row, and tries later rows if the first signature does not verify. It does not mint a replacement when active rows already exist.
- Read failure nulls client, ready, action, and watch. Airtable mode still does not fall back to `opportunities.json` (PR #131). ADP published-manifest fallback (PR #132) is unchanged.

## Verification limits

Local `:8080` served the updated table script. The table render above used that script with the canonical counts. Memberstack founder session was not present: production and local both showed Access restricted until the local render harness unhid the workspace. Open Client Link in that harness resolved the Renaissance row to the Renaissance GDI path. Copy hit the clipboard API and, in the unfocused agent browser, failed until the legacy copy fallback. Those clicks are not a Memberstack Admin pass.

Production `/health` returned `{ok:true}` without a build SHA. Production `admin-gdi-reports.js` still gated “Open GDI” on share availability at the time of this report. Deploy of this change is still required before production Admin shows the new column.
