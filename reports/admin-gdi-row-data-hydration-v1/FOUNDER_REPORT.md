# GDI Reports Row Data Hydration V1 — Founder Report

**Date:** 2026-10-02  
**Branch:** `cursor/local-system-startup-recovery`  
**FINAL SHA:** `e2a6a791c96c637147787e9f2c39c47ed218c7e0`  
**Pushed:** `origin/cursor/local-system-startup-recovery`

## Verdict

Row hydration for AI Demand Admin → GDI Reports is fixed locally.

Bethesda Marriott resolves to:

| Field | Value |
|---|---|
| Report Status | **READY** |
| PDF | **READY** (View / Download) |
| GDI Client | **Open / Copy URL** |
| ADP Client | **Open / Copy URL** |
| Last Generated | `2026-10-02T12:36:43.337Z` |

Summary cards now reconcile from the **same normalized row model** as the table:

| Card | Count |
|---|---:|
| GDI Hotels | 6 |
| Report Ready | 4 |
| PDF Ready | 1 |
| Needs PDF | 5 |
| Blocked | 0 |

## Root cause

UI columns existed and the catalog path partially returned opportunity counts / PDF readiness, but:

1. Share availability / report status / lastGenerated / summary counts were not consistently normalized into one row contract.
2. Frontend relied on API `counts` without re-deriving from rows (cards could show 0 while rows had data).
3. ADP “available” could be true with a **localhost** reconstructed URL — Open/Copy must reject that.

## Fix

- New normalizer: `lib/admin/gdi-admin-report-row-v1.js` (`GDI_ADMIN_REPORT_ROW_V1`)
- Catalog uses `normalizeGdiAdminReportRow` + `reconcileGdiAdminReportCounts`
- Share flags via probes + usable external URL confirm (no mint; Bethesda sealed tokens unchanged)
- Frontend: normalize rows client-side; **cards always from rows**; Copied toast; refuse localhost client URLs
- Cache-bust: `admin-gdi-reports.js?v=gdi-row-hydration-v1`

## Security

- Catalog payload carries `gdiClient.available` / `adpClient.available` only (no raw tokens/URLs).
- Open/Copy fetch full production URL via existing `/api/admin/ai-demand/hotels/:id/external-client-links`.
- Bethesda GDI token `gdisht_47c25d74c79216021fb36150` fingerprint unchanged (`9237540e872c`).
- Bethesda ADP token `sht_24ff4ada4a622db62a228d3f` fingerprint unchanged (`c5e256b6277e`).

## Non-Bethesda GDI shares

Local runtime lacks `GDI_SHARE_CAPABILITY_SECRET` and has no sealed GDI contract for other hotels → GDI Client correctly shows **—** (no auto-mint).

ADP Client for non-Bethesda shows **—** locally when only a localhost reconstruct would be available (production host + secret required for buttons).

## Deploy

See `PRODUCTION_SMOKE.md`. Production deploy of this hydration pack must be confirmed separately if not yet on Railway tip.
