# GDI Cumulative Integration Restore — Founder Closeout

**Status:** `GDI_CUMULATIVE_INTEGRATION_RESTORED — AWAITING_FOUNDER_REVIEW`  
**Date:** 2026-11-27  
**Workspace:** `C:\Dev\dealality\ws-gdi`  
**Branch:** `feat/gdi-cumulative-es-list-staging-20261127`  
**Serving:** Port **8080** from this workspace (PID recorded in `tmp/gdi-local-8080.meta.json`)

## Root causes (exact)

1. **Wrong `:8080` owner** — listener cwd was `C:\Dev\dealality\wt-he-unified-theme-v1` (`feat/he-unified-theme-v1`), not GDI.
2. That checkout lacked Wyndham **STAGED** publication → localhost founder default `STAGED` → **409 `STAGED_PUBLICATION_PREVIEW_UNAVAILABLE`** → UI **0 of 0**.
3. That checkout lacked Executive Summary + sortable List View assets → only the original three tabs.

**Not** a deletion of CLEAN11 inventory. CLEAN11 STAGED package remained intact under GDI.

## Integration status (GitHub)

| PR | Scope | Status | Base |
|----|--------|--------|------|
| #618 | Executive Summary / Research Receipt | OPEN | `d97d293…` |
| #624 | Sortable GDI List View | OPEN | `d97d293…` |
| #620 | Wyndham Checkpoint B | OPEN — **independently governed** | `d97d293…` |

Local cumulative branch surgically combines **#618 + #624** surfaces for founder review on `:8080` (no blind merge; no merge to `main` from GDI chat). Integration Controller owns GitHub merge + CI.

## Live `:8080` verification

| Check | Result |
|-------|--------|
| Serving cwd | `C:\Dev\dealality\ws-gdi` |
| Served assets | `executive-summary-tab.js` + `gdi-commercial-list-table-v1.js` (`?v=gdi-cumulative-es-list-v1`) |
| Four left tabs | Present; Opportunities default |
| Wyndham STAGED | **11** (`gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261125_3283ee92`) |
| Bethesda CURRENT | **67** (preserved); STAGED also present **68** (founder default when STAGED exists) |
| YOTEL Geneva Lake | **42** STAGED / **42** CURRENT — no inventory regression |
| Proposed write-set review API | HTTP 200 — locked **26** qualified (11 in STAGED + 15 review-only); HOLD 2; no apply |
| Non-regression unit gate | PASS |
| PowerShell validator | PASS |

## Wyndham inventory reconciliation

See `WYNDHAM_INVENTORY_RECONCILIATION.md`.

- Discovered corpus **161** — not all qualified.
- CLEAN11 STAGED **11** — live founder Opportunities inventory.
- Locked proposed qualified **26** — review via `/api/admin/group-demand-intelligence/hotels/recnPB3Q02NO8Ma1o/proposed-write-set-review` (no activate / no CURRENT publish).
- WHO/HOW continuation **25** — not auto-qualified.

## Screenshots

Under `reports/gdi-cumulative-integration-restore-v1/screenshots/`:

- `wyndham-app-shell-staged-mode-11.png` — Local Founder + **Staged** inventory toggle + 11 of 11
- `wyndham-four-tabs-staged-11.png` — four tabs + filters at 11
- `wyndham-list-view-11.png` — sortable List View
- `wyndham-tiles-11.png` — Tiles unchanged
- `wyndham-executive-summary.png` — ES + Research Receipt (STAGED not customer-authorized metrics)
- `bethesda-tiles-68.png` / `bethesda-executive-summary-67.png` / `bethesda-opportunity-drawer.png`
- `yotel-geneva-lake-42.png`

## Repeatable PowerShell

```powershell
powershell -NoProfile -File scripts/validate-gdi-cumulative-integrated-staging-v1.ps1
```

Fails closed when `:8080` is wrong cwd, ES/List assets missing, Wyndham STAGED empty, Bethesda CURRENT empty, or unit gates fail.

Unit gate:

```powershell
npm run test:gdi-cumulative-integrated-staging-non-regression-v1
```

## Governance honored

NO_PAID_RESEARCH · NO_PAID_ENRICHMENT · NO_UNAUTHORIZED_AIRTABLE_WRITES · NO_CURRENT_PUBLISH · NO_PRODUCTION_DEPLOYMENT · no Wyndham baseline mutation · Bethesda/CLEAN11 freezes preserved · no unauthorized qualification changes · no unrelated worktree resets · no independent merges to `main`.

## Permanent docs

Constitution §§19–21 · BUILD_DECISIONS `:8080` cumulative staging · `DEALALITY_ADDITIVE_NON_REGRESSION_V1`.

## Founder review URLs

- App shell (inventory mode chrome): `http://localhost:8080/app#/group-demand-intelligence?hotelId=recnPB3Q02NO8Ma1o`
- Standalone: `http://localhost:8080/group-demand-intelligence.html?hotelId=recnPB3Q02NO8Ma1o&founderPreview=1`
- Bethesda: `…?hotelId=recLuxvwwxID7U2B8&founderPreview=1`
- YOTEL: `…?hotelId=recrPQcZg7SFARRb2&founderPreview=1`

**STOP — no Executive Summary Phase 2 / no additional GDI enhancements until founder review.**
