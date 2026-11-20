# GDI 3.0 — Dealality toast for comparison loading

**Status:** READY FOR FOUNDER VISUAL REVIEW  
**PR:** #462 OPEN — no competing branch; no production writes / Surfe / publication

## Canonical toast reused

Same Brand AI / GDI / ADP lineage:

- Markup: `.bdd-toast.aiv-loading-toast` + wave orb (`.toast-wave-container`)
- Controller: `DealalityGdiUi.setLoadingToast`
- Styles: `ai-visibility-shared.css` + `deal-workspace-shell.css`

No new toast system.

## Sheraton comparison changes

| Before | After |
| --- | --- |
| Panel bodies showed `Loading…` / `gdi-cmp-loading` | Panels clear (no stale content, no generic Loading text) |
| No page toast | Canonical top-right toast: **Loading opportunity comparison…** |
| Race / stale risk on fast switch | `loadSeq` ignores superseded responses |
| Fast flicker | Min toast visible ~320ms |
| Hung fetch | 45s abort → error state + toast dismiss |

Files:

- `public/group-demand-intelligence-drawer-comparison.html` — toast mount; removed panel Loading copy; cache `v10-dealality-toast`
- `public/js/group-demand-intelligence/drawer-comparison-app.js` — toast lifecycle + race + timeout
- `public/js/group-demand-intelligence/dealality-gdi-ui.js` — drawer detail message → `Loading opportunity…` (wave-orb chrome unchanged)

## Broader GDI audit (reuse already present)

| Surface | Loading UX |
| --- | --- |
| Main GDI page | Canonical `#gdiLoading` toast via `setLoadingToast` |
| Opportunity drawer open | Inline `detailLoadingHtml()` (same wave orb; not plain Loading text) |
| Share surface | `#gdiShareLoading` toast |
| Hotel Commercial Priorities tab | Page toast |
| Sheraton comparison | **Fixed** — page toast |

## Browser acceptance

URL: http://localhost:8095/group-demand-intelligence-drawer-comparison.html?hotelId=recCAGqkERSXgiku4&opportunityId=paid3_565ce4588e6d&v=cmp10toast3  
Server: ws-gdi **8095**

Screenshots:

1. Loading — `screenshots/05-dealality-toast-loading.png` (toast visible; panels empty; no panel Loading…)
2. Loaded — `screenshots/06-dealality-toast-loaded.png` (EUROGUSS drawers; toast dismissed; contacts intact)

DOM checks:

- `.gdi-cmp-loading` absent
- Toast `aria-busy` clears on success
- RIGHT still shows recovered Gissella channels

## STOP

Founder visual review required.
