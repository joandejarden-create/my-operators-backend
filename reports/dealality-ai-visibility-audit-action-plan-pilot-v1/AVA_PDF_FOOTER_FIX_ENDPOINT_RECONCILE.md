# AVA PDF footer-fix — archive + endpoint reconcile

**Contract:** `AVA_PDF_FOOTER_FIX_ENDPOINT_RECONCILE_V1`  
**Generated:** 2026-11-05  
**Publication authorized:** no  
**Paid research:** $0

## Port 8080 identity

| Field | Value |
|---|---|
| PID | 16992 |
| Process | `node.exe` |
| Command | `node server.js` |
| Parent | `cmd.exe /d /s /c node server.js` |
| Intended code? | **Yes** — AVA `storage-status` reports `mode=local`, root `C:\Dev\dealality\my-operators-backend\data\ava-data`; AVA PDF routes respond with footer-fix versions |
| Action | Process **not** stopped |

## Archive reconcile

- Durable `data/ava-data/pdfs/{propertyId}/current.pdf` = corrected SHA for all six.
- Legacy `reports/ai-visibility-audit-action-plan/archive/{propertyId}/current.pdf` synced to the same bytes; prior legacy `current.pdf` preserved as `pre_footer_fix_{sha12}.pdf`.
- `AUDIT_META` / checksums updated to corrected `pdfSha256` + `pdfRelativePath=pdfs/{id}/current.pdf`.
- Approval / publication fields unchanged (`FOUNDER_REVIEWED_STAGED` for Spice Island; `AUDIT_VALIDATED` for the other five; `publicationAuthorizationStatus=NOT_AUTHORIZED`).

## Preview vs Download

For all six hotels, Admin Preview (`GET .../pdf`) and Download (`GET .../pdf?download=1`) return **identical bytes** matching the corrected SHA. Download `Content-Disposition` / `a.download` filenames match the canonical AVA pattern.

Artifacts:

- `AVA_PDF_FOOTER_FIX_ARCHIVE_RECONCILE.json`
- `AVA_PDF_PREVIEW_DOWNLOAD_RECONCILE_PROBE.json`
- `AVA_PDF_CONFIDENTIALITY_FOOTER_FIX.json`
