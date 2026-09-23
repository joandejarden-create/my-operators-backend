# GDI Customer CSV Export Fix — Report

**Date:** 2026-09-23  
**Deploy:** Not deployed in this task

---

## ROOT CAUSE

**A + partial B — button targeted the JSON list endpoint.**

1. Export CSV used `<a href="/api/.../hotels/:id/opportunities?format=csv" download>`.
2. That is the **same route** as the opportunity-list JSON API (`getGdiOpportunities`).
3. When `format=csv` is missing, ignored, cached wrong, or the browser saves the prior list response, the download is the JSON body:

```json
{ "ok": true, "hotelId": "...", "schemaVersion": "...", "opportunities": [...] }
```

4. A dedicated `/opportunities/export.csv` route already existed but the UI did not use it.
5. Secondary gaps: export omitted commercial progression; columns were internal/enum-heavy; meta comments leaked `hotelId` / schema; filters beyond weekly/priority were not honored for “what I’m viewing.”

---

## FILES CHANGED

- `lib/group-demand-intelligence/customer-csv-export.js` *(new)*
- `lib/group-demand-intelligence/live-commercial-quality-v1.js`
- `lib/group-demand-intelligence/index.js`
- `api/group-demand-intelligence.js`
- `public/js/group-demand-intelligence/app.js`
- `public/js/group-demand-intelligence/share-app.js`
- `scripts/test-gdi-customer-csv-export.mjs` *(new)*
- `scripts/test-gdi-live-commercial-quality-v1.mjs`
- `package.json`

---

## CSV ROUTE

**Auth:**  
`GET /api/group-demand-intelligence/hotels/:hotelId/opportunities/export.csv`

**Share:**  
`GET /api/group-demand-intelligence/share/hotels/:hotelId/opportunities/export.csv?share=…`

Compat: `?format=csv` on the list route still returns CSV (same builder).

UI now: fetch → require `text/csv` → blob download (refuses JSON).

---

## CONTENT TYPE

`text/csv; charset=utf-8`  
(+ UTF-8 BOM in body for Excel)

## CONTENT DISPOSITION

`attachment; filename="Bethesda Marriott - GDI Opportunities.csv"; filename*=UTF-8''…`

---

## BETHESDA ROWS

**30** (salesperson view of current canonical set)

## INTERNAL FIELDS EXPOSED

**0** (`hotelId`, `schemaVersion`, `_listDto`, `hotelFitScore`, `evidenceConfidence`, JSON wrapper — all asserted absent)

---

## AUTH / SHARE / EXCEL

| Check | Result |
|-------|--------|
| AUTH TEST | **PASS** (same `gdiPilotReadAuth` / flag gate as list; export.csv dedicated) |
| SHARE TEST | **PASS** (same `requireGdiShare` + hotel scope 403; share URLs unchanged) |
| EXCEL-SAFE | **PASS** (BOM, CRLF, quoted commas, formula injection guard, ISO dates) |

---

## TESTS

| Suite | Result |
|-------|--------|
| `test:gdi-customer-csv-export` | **13/13** |
| `test:gdi-live-commercial-quality-v1` | **15/15** |

**TESTS: 28/28**

---

## COMMIT

Not committed (no commit requested).

## DEPLOY

Not deployed.

---

## FINAL VERDICT

**CSV EXPORT FIXED — CUSTOMER-READY**

(Ready after deploy of these files; share capability URLs unchanged.)
