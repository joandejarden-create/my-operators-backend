# GDI local / production parity handoff

**Date:** 2026-10-15  
**Preferred workspace:** `C:\Dev\dealality\my-operators-backend`  
**Primary local URL:** http://127.0.0.1:8080/group-demand-intelligence.html  
**Production URL:** https://my-operators-backend-production.up.railway.app/group-demand-intelligence.html

## Environment table

| Environment | URL | Source folder | Branch/SHA | Data adapter | Checks |
| --- | --- | --- | --- | --- | --- |
| Local (primary) | http://127.0.0.1:8080 | `C:\Dev\dealality\my-operators-backend` | see reconciliation closeout; assets `gdi-tile-contact-toolbar-v1` | `GDI_OPPORTUNITIES_PERSISTENCE=airtable` | `npm run gdi:local:acceptance` — Mayflower **30**, San Diego **38** |
| Production | Railway | clean `main` deploy | see `reports/gdi-release-reconciliation/FOUNDER_REPORT.md` | Airtable | Auth GDI path; DEV share secret **0** |
| Hold | n/a | `C:\Dev\dealality\gdi-release-main` | concurrent ADP/census | Mixed FS | Do not serve as primary |
| Consolidate hold | n/a | `C:\Dev\dealality\gdi-main-consolidate` | hold | n/a | Preserve |

## Authoritative customer-list counts (post filter-order fix)

| Hotel | Customer list (`isCustomerFacingOpportunity` on canonical fields) | Broad stamp | Notes |
| --- | ---: | ---: | --- |
| Mayflower `rec0eTEnCqlsGJJt9` | **30** | 35 | 5 overflow/missing-basis rows retained in store, not on list |
| San Diego `recnPB3Q02NO8Ma1o` | **38** | 46 | 7 PE enrichment-queue + Shipyard missing basis; do not treat pre-fix API **45** as inventory truth |

Full ID tables: `reports/gdi-release-reconciliation/FOUNDER_REPORT.md`.

## Presentation release (PR #95) + reconciliation fix

1. Compact tile contact + toolbar row + static hotel-fit tiles — asset `gdi-tile-contact-toolbar-v1`.
2. **Reconciliation fix:** auth list/export must **filter before presentation sanitize** so sanitize cannot invent travel evidence via `summaryWhat` fallback.

## Research history (honest)

Footer may show History/Status unavailable while Last published / Last source verification show Oct 15 stamps. Oct 29 simulation remains excluded. Do not invent Completed from runId alone.

## Local start

```powershell
cd C:\Dev\dealality\my-operators-backend
$env:PORT='8080'
$env:GDI_OPPORTUNITIES_PERSISTENCE='airtable'
$env:GDI_SHARE_CAPABILITY_ALLOW_DEV_SECRET='1'
npm start
```

```powershell
npm run gdi:local:diagnostic
npm run gdi:local:acceptance
npm run test:gdi-compact-tile-contact-v1
npm run test:gdi-list-visibility-before-sanitize-v1
```

## Deploy

```powershell
npm run deploy:production
```

No dirty/hotfix override. Keep `GDI_SHARE_CAPABILITY_ALLOW_DEV_SECRET=0` in production.

## Open in Cursor

**`C:\Dev\dealality\my-operators-backend`**
