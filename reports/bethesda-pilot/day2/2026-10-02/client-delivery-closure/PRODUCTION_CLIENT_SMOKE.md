# PRODUCTION_CLIENT_SMOKE.md

## Status: PASS

**Deployment:** `6de62ff2-a4dd-4439-b1de-f7477b80f3f3`  
**Base:** `https://my-operators-backend-production.up.railway.app`  
**GDI token ID:** `gdisht_47c25d74c79216021fb36150` (unchanged)

| Probe | Result |
|---|---|
| ADP share page | PASS |
| ADP resolve | PASS |
| ADP report (42.1 / 81 / period 9f3a60) | PASS |
| GDI share page | PASS |
| GDI resolve | PASS |
| GDI opportunities (35) | PASS |
| GDI Demand Report (`/pdf-report`) | PASS |
| GDI report-pdf binary | PASS (243,766; `%PDF-`) |
| ADP PDF render HTML | PASS |
| Report Archive JS shipped | PASS |
| Unauthenticated (share only) | PASS |

Artifacts: `PRODUCTION_CLIENT_SMOKE_RESULT.json`, `_prod-external-urls.json`, `_prod-gdi-report.pdf`, `_prod-adp-report.pdf`
