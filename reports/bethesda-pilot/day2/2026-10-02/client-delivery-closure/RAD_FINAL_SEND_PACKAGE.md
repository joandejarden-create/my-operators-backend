# RAD_FINAL_SEND_PACKAGE.md

## Status: READY TO SEND

### SEND

1. **ADP external client URL** — from `_prod-external-urls.json` → `adp.url`  
   (tokenId `sht_24ff4ada4a622db62a228d3f`; do not rotate)
2. **GDI external client URL** — from `_prod-external-urls.json` → `gdi.url`  
   (tokenId `gdisht_47c25d74c79216021fb36150`; **must not rotate**)
3. **ADP PDF** — `reports/ai-demand-positioning/current-report-pdf/adp_bethesda_marriott/report.pdf`  
   (or closure `_prod-adp-report.pdf`)
4. **GDI PDF** — `reports/group-demand-intelligence/pdf/recLuxvwwxID7U2B8/report.pdf`  
   (or closure `_prod-gdi-report.pdf`)
5. **30-Day Action Plan** — `reports/group-demand-intelligence/bethesda-gm-package-v1/BETHESDA_30_DAY_ACTION_PLAN.md`

### OPTIONAL

- Executive Summary (GM package)

### DO NOT SEND

- Founder / QA / archive internals  
- Raw structured JSON  
- Internal IDs / technical docs  

### Email

Use `../RAD_FOLLOW_UP_EMAIL.md` (client-facing body only; strip INTERNAL NOTE).
