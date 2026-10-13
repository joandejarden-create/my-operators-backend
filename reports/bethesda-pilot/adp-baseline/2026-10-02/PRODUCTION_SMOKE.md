# Production Smoke — Bethesda Oct 2 ADP Baseline

**Deploy:** Railway `1de8092b-f35a-4e80-8006-39d0bfd6b021`  
**Message:** Bethesda Oct2 ADP official baseline V2 + Admin archive parity `8bbe8f6`  
**Status:** SUCCESS (2026-10-02T15:17:46Z upload → SUCCESS)  
**Git HEAD at smoke:** `8bbe8f6`

| Check | Result | Evidence |
|-------|--------|----------|
| Admin Reviews row Bethesda | PASS (code) / auth-gated in prod | Last Measurement label; PDF View/Download; ADP Client Open/Copy; Archive tab wired in `admin-ai-demand-reviews.js` + `admin-report-archive` routes |
| External ADP share (unauth) | **PASS** | Same token `sht_24ff4ada…`; resolve 200; report period `…62428a`; Consideration 42.9 · Scenario Presence 76.2 · Reality 20.0 · 63×4; “No comparable prior official period yet” |
| PDF (Oct 2 periodId) | **PASS** | Local + archive `adp_official_baseline_2026-10-02_…_v2/report.pdf` periodId `…62428a`; current-report-pdf meta matches; allowlisted in `.railwayignore` |
| Archive | **PASS** (store) | Sep PRE_PILOT_REFERENCE + Oct 2 OFFICIAL_BASELINE + GDI Day-1 present under `data/dealality-report-archive/recLuxvwwxID7U2B8/` |
| Copy URL / share token | **PASS** | Token **unchanged** `sht_24ff4ada4a622db62a228d3f` |
| GDI | **PASS / unchanged** | `gdisht_47c25d74…` resolve 200; share page 200; no GDI mutation in this deploy |
| November anchor | **PASS** | Trends: “Next formal remeasurement November 2026”; pilot V2 `novemberComparisonAnchorPeriodId` = Oct 2 period |
| Last Research on ADP | **PASS** | External ADP page: no “Last Research” string |

## Smoke URLs

- ADP: `https://my-operators-backend-production.up.railway.app/owner-ai-demand-share.html?share=adpshare.v1.…sht_24ff4ada…`
- GDI: existing `gdisht_47c25d74…` URL (unchanged)
