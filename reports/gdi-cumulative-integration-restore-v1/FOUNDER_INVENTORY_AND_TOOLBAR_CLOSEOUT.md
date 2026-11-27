# GDI Founder Inventory & List Toolbar — Closeout

**Status:** `GDI_FOUNDER_INVENTORY_AND_LIST_TOOLBAR_VERIFIED — AWAITING_FOUNDER_ACCEPTANCE`  
**Date:** 2026-11-27  
**PR:** [#626](https://github.com/joandejarden-create/my-operators-backend/pull/626) (cumulative)  
**Branch:** `feat/gdi-cumulative-es-list-staging-20261127`  
**:8080:** `C:\Dev\dealality\ws-gdi`

## 1. Historical Wyndham “68”

**No Wyndham publication package with count=68 exists.**  
**68 = Bethesda Marriott STAGED** (`recLuxvwwxID7U2B8`).  

Full audit: `WYNDHAM_HISTORICAL_68_RECONCILIATION.md`.

Wyndham historical packages (not activated except CLEAN11): 89, 59, 18, 11, 9.

## 2. Founder-visible inventory counts (Wyndham)

| Mode | Count | Classification |
|------|------:|----------------|
| Staged | **11** | CLEAN11 STAGED qualified |
| Proposed | **26** | Locked accepted proposed qualified write-set (not activated) |
| HOLD | **2** | Not qualified |
| Continuations | **25** | WHO/HOW research — not qualified |
| Published | **0** | No CURRENT |

Access inside normal GDI Opportunities via **Founder inventory** selector (and shell **Proposed**). List/Tiles + drawer. Not JSON/HTML/API-only.

## 3. List toolbar

Order on one row: Export CSV → Sort Direction → Sort By → **Columns SVG** → List/Tiles.  
Tooltip/label: Customize Columns. Tiles unchanged.

## 4. Screenshots

- `screenshots/wyndham-proposed-26-list-toolbar.png` — Proposed 26 + single-row Columns SVG  
- Prior cumulative set retained under same folder

## 5. PowerShell

```powershell
powershell -NoProfile -File scripts/validate-gdi-cumulative-integrated-staging-v1.ps1
```

## 6. Governance

No paid rediscovery · no Airtable writes · no STAGED activate · no CURRENT publish · locked 26 immutable · Bethesda/CLEAN11 freezes preserved · no independent merge to `main`.
