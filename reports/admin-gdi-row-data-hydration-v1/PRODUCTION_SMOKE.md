# Production Smoke — GDI Row Hydration V1

## Local smoke (PASS)

| Check | Result |
|---|---|
| Catalog schema `GDI_ADMIN_REPORT_ROW_V1` | PASS |
| Bethesda reportStatus READY | PASS |
| Bethesda PDF READY + lastGeneratedAt | PASS |
| Bethesda GDI Client available | PASS |
| Bethesda ADP Client available | PASS |
| Bethesda GDI Open (unauth production URL) | PASS (HTTP 200) |
| Bethesda ADP Open (unauth production URL) | PASS (HTTP 200) |
| GDI token fingerprint unchanged | PASS `gdisht_47c25d74…` sha12 `9237540e872c` |
| ADP token fingerprint unchanged | PASS `sht_24ff4ada…` sha12 `c5e256b6277e` |
| Cards reconcile with table | PASS |

### Hotel-by-hotel (local catalog)

| Hotel | Report | PDF | GDI share | ADP share | Last generated |
|---|---|---|---|---|---|
| Bethesda Marriott | READY | READY | YES | YES | 2026-10-02T12:36:43.337Z |
| Cambridge Beaches Resort & Spa | WATCH_ONLY | MISSING | NO | NO* | — |
| Hilton New York Times Square | READY | MISSING | NO | NO | — |
| NOW NOW NOHO | WATCH_ONLY | MISSING | NO | NO* | — |
| Renaissance New York Times Square Hotel | READY | MISSING | NO | NO* | — |
| Waterstone Resort & Marina Boca Raton | READY | MISSING | NO | NO* | — |

\* Local: ADP registry exists but only localhost reconstruct would be available → treated as unavailable (no localhost Open/Copy). Production with `DEALALITY_PUBLIC_BASE_URL` + share secrets may flip ADP to YES without minting.

## Production deploy checklist

1. Deploy commit containing:
   - `lib/admin/gdi-admin-report-row-v1.js`
   - `api/admin-gdi-reports.js`
   - `public/js/admin-gdi-reports.js`
   - `public/app/admin/ai-demand-admin.html` (`?v=gdi-row-hydration-v1`)
2. Open production AI Demand Admin → GDI Reports (hard refresh).
3. Bethesda: Open/Copy GDI + ADP; View/Download PDF; Archive.
4. Paste copied URLs in clean unauthenticated browser → must load client share pages.
5. Confirm cards: Report Ready / PDF Ready / Needs PDF / Blocked match visible rows.

## Production deployed

**NO** (this pack verified locally; Railway tip not updated in this pass unless a follow-up deploy lands the commit).
