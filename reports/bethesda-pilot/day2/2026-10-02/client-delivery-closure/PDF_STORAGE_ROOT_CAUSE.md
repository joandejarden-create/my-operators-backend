# PDF_STORAGE_ROOT_CAUSE.md

## Classification

**Primary:** `OBJECT_NOT_FOUND` caused by **EPHEMERAL_PATH / ARTIFACT_NOT_ON_PRODUCTION_FS**  
**Secondary:** no generate-on-miss on share download  
**Contributing:** lean `.railwayignore` omitted `reports/group-demand-intelligence/pdf/**`

## Reproduction

Admin Generate GDI PDF → writes `reports/group-demand-intelligence/pdf/{hotelId}/report.pdf`  
Day-1 lean production deploy did not include that tree → share `report-pdf` returned **404**

## Fix (generic, all hotels)

1. `gdi-pdf-store-v1.js` — env root `DEALALITY_REPORT_PDF_ROOT` / `GDI_PDF_DATA_DIR`; versioned `archive/vN/`
2. Share `getGdiShareReportPdf` — **generate-once-and-persist on miss**
3. Report Archive immutable ledger under `data/dealality-report-archive/`
4. `.railwayignore` allowlists for PDF + archive artifacts

## Verification (production 2026-10-02)

- Deployment `6de62ff2-a4dd-4439-b1de-f7477b80f3f3`
- Share report-pdf → **200** `%PDF-` **243,766 bytes**
- GDI binary 404 **RESOLVED = YES**
