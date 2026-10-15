# AI Demand Admin — column definitions & metric sources

## GDI Reports

| Column | Meaning | Source |
|--------|---------|--------|
| Hotel | Display name (primary) | `listGdiSelectableHotels` + report data hotel name |
| Location | Market / city | Report data `hotel.market` or hotel config location |
| Ready | Customer-ready prospect count in **current GDI report data** executive summary | `getGdiPdfReportData` → `executiveSummary.customerReadyCount` |
| Action Set | Action-set recommendation count in current GDI report data | `executiveSummary.actionSetCount` |
| Watch | Future-watch count in current GDI report data | `executiveSummary.futureWatchCount` |
| Report Status | READY / WATCH ONLY / NO READY OPPORTUNITIES / NEEDS BUILD / BLOCKED | `deriveGdiReportStatus(summary)` |
| PDF | READY / MISSING / GENERATING / FAILED | PDF store `hasGdiReportPdf` + in-flight UI state |
| Last Generated | PDF artifact generation time (not research date) | PDF store `meta.generatedAt` |

**Not equivalent:** Ready ≠ live customer list facing count alone when report data is stale; catalog rebuilds from live `getGdiPdfReportData` on load. Archive PDF versions are separate (Report Archive tab).

## AI Demand Reviews

| Column | Meaning |
|--------|---------|
| Hotel / Location | Property name + market |
| Baseline / Monitoring | Reporting month label |
| Last Measurement | Current monitoring date |
| Prior Run | Prior run date |
| Review Version | Version label |
| Report / Generation / Coverage | Review workflow statuses |
| Client Issued | Issued flag |
| PDF | Performance Review PDF availability |
| Action Plan | Action count |
| Last Generated | Review generation timestamp |

## Report Archive

Rows are **report versions**, not unique hotels. Date = archived/report date; Type = ADP|GDI; PDF = whether binary was archived.

## ADP Action Plan

Hotel selection drives Management Action Agenda rows (priority, status, title, owner, due, …). Opened from Reviews “Action plan” more-menu via `sessionStorage.dealality_adp_admin_property`.
