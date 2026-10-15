# GDI local / production parity handoff

**Date:** 2026-10-15  
**Preferred workspace:** `C:\Dev\dealality\my-operators-backend`  
**Primary local URL:** http://127.0.0.1:8080/group-demand-intelligence.html  
**Production URL:** https://my-operators-backend-production.up.railway.app/group-demand-intelligence.html

## Environment table

| Environment | URL | Source folder | Branch/SHA | Data adapter | Checks |
| --- | --- | --- | --- | --- | --- |
| Local (primary) | http://127.0.0.1:8080 | `C:\Dev\dealality\my-operators-backend` | `gdi/local-production-parity` @ pre-merge work; assets `gdi-parity-v1` | `GDI_OPPORTUNITIES_PERSISTENCE=airtable` (canonical Airtable read) | `npm run gdi:local:acceptance` PASS; Mayflower ACEC/ASID/Aero fallbacks; AHIMA drawer; history Oct 15 Completed (not Oct 29) |
| Production | https://my-operators-backend-production.up.railway.app | Railway from clean `main` | Deploy after merge (see deploy record) | Airtable (production) | Same acceptance cases post-deploy |
| Hold / unfinished tree | n/a (do not serve) | `C:\Dev\dealality\gdi-release-main` | `gdi/parallel-provider-san-diego` dirty | Mixed FS pilot data | Preserve ADP/census/unrelated work; do not use as primary server |
| Consolidate hold | n/a | `C:\Dev\dealality\gdi-main-consolidate` | `gdi/consolidate-hold` @ prior main | n/a | Non-destructive hold after main ownership moved |

## What caused the divergence

`my-operators-backend` was left on a detached obsolete checkout (`0d2ca4f3`) while production deploys came from `gdi-main-consolidate` / later closeout merges. Local Node on :8080/:8081 was still serving older assets (`gdi-drawer-quality-v1`). Matching opportunity counts alone did not mean code parity.

## Local start (required first step)

```powershell
cd C:\Dev\dealality\my-operators-backend
$env:PORT='8080'
$env:GDI_OPPORTUNITIES_PERSISTENCE='airtable'
$env:GDI_SHARE_CAPABILITY_ALLOW_DEV_SECRET='1'
npm start
```

Then:

```powershell
npm run gdi:local:diagnostic
npm run gdi:local:acceptance
npm run gdi:test:drawer-fit
npm run gdi:test:history-type
```

Obsolete preview ports: **8081**, **3000** (do not use for GDI parity).

## Fixes in this parity pass

1. **Share sanitize no longer drops company-contact fields** before list DTO (`contactEmail` / `contactPhone` / org routes).
2. **Contact display surface** treats `administrator@` as org route; promotes firstname@ + role evidence (Aero Club → Stephanie); unused stored emails become usable org fallbacks (ACEC / ASID).
3. Asset bust **`gdi-parity-v1`**.
4. Repeatable scripts: `gdi:local:diagnostic`, `gdi:local:acceptance`.

## Local acceptance (Mayflower)

- Duplicate Hotel Fit: single Overall provisional tile on AHIMA.
- Contactability visible; dimension tiles labeled (“Needs verification”), not blank.
- Provisional Overall **61** (not withheld+number contradiction); Evidence confidence **38 / 100**.
- ACEC: `info@acecmw.org` + `202-644-8260`.
- ASID: `administrator@dcmetro.asid.org`.
- Aero Club: Stephanie + `stephanie@aeroclub.org` + company phone.
- Research history: **Oct 15, 2026 / Completed** (Oct 29 false Completed not shown).
- Opportunity Type filter categories present (Scaling / PE / Association / Corporate Meetings).

## Deploy gate

Only after local acceptance PASS:

```powershell
npm run deploy:production
```

No dirty/hotfix override.

## Open in Cursor

**`C:\Dev\dealality\my-operators-backend`**
