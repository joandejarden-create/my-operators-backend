# Production Smoke — Bethesda GDI Client Share + PDF P0

**Deploy:** Railway `057dad12-27f4-40a5-9b99-1c8fce255bc9`  
**CLI message:** `deploy:production cursor/local-system-startup-recovery de65312`  
**Status:** SUCCESS  
**Smoke ts:** 2026-10-02T12:19:23Z  
**Evidence:** `PRODUCTION_SMOKE.json`, `adminSim`, `production-gdi-share-report.pdf`, `production-cover-page-1.png`

---

## Revision match

| Check | Result |
|-------|--------|
| Live `/js/admin-gdi-reports.js` contains `servedFromProductionHost` | YES |
| Production revision match (Admin harden live) | YES |
| Health `/health` | 200 `{ok:true}` |

---

## Admin Open / Copy (simulated + asset-proven)

Auth-gated Admin UI was not driven with Memberstack credentials in this smoke. Equivalent proof:

| Check | Result |
|-------|--------|
| Admin JS Open/Copy path live | YES (`servedFromProductionHost`) |
| Resolver returns available GDI URL | YES |
| Host | `my-operators-backend-production.up.railway.app` |
| Token id | `gdisht_47c25d74c79216021fb36150` |
| Envelope masked | `gdishare.v1.eyJ2Ij…VOQXY-gc` (len=320) |
| Contract fingerprint sha12 | `9237540e872c` (unchanged) |
| Open destination = Copy destination | YES (same sealed contract URL) |

**GDI ADMIN OPEN CLIENT:** PASS (code path + sealed production URL)  
**GDI ADMIN COPY URL:** PASS

---

## Unauthenticated client

| Check | Result |
|-------|--------|
| Resolve GET | 200 · Bethesda Marriott · `recLuxvwwxID7U2B8` |
| Share page inactive banner | NO |
| Hotel correct | YES |
| Demand Report | PASS |
| Opportunities | PASS |
| PDF API (`pdf-report` data) | 200 |
| PDF binary (`report-pdf`) | 200 · `Dealality_GDI_Bethesda_Marriott_2026-10-01.pdf` · 16 pages |
| Invalid token safe | YES |
| No token safe | YES |
| Admin login required for client URL | NO |

**GDI UNAUTHENTICATED CLIENT:** PASS

---

## PDF production cover

Canonical ADP-family cover live on production binary:

- GROUP & DEMAND INTELLIGENCE
- Bethesda Marriott
- Washington metropolitan area
- COMMERCIAL DEMAND REVIEW
- OCTOBER 2026 · REPORT DATE OCT 1, 2026
- Dealality logo + red dot
- No bare “DMV”; no ISO-only primary date; no INT E L L IGENCE bug

---

## Link stability

| Action | External URL changes? |
|--------|------------------------|
| Refresh GDI data | NO (stable `gdisht_47c25d74…` contract) |
| Regenerate PDF | NO |
| Deploy | NO (HMAC + contract token preserved) |

---

## Verdict

Production smoke **PASS** for client share + PDF cover on the deployed revision.
