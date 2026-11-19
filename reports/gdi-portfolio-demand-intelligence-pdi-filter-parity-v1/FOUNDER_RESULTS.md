# GDI PDI filter UI parity + local/external consistency

## Root causes

1. **Local tab absence (primary `:8080`)**  
   Primary checkout `C:\Dev\dealality\my-operators-backend` is a **detached HEAD** (`c448e4529…`) **without** `GDI_PORTFOLIO_TAB` in `dealality-gdi-ui.js`.  
   Registered GDI home `C:\Dev\dealality\ws-gdi` **does** include the Portfolio tab.  
   External Railway serves a build that includes the tab.  
   → Local vs external mismatch was **checkout/commit skew**, not a missing feature flag on `ws/gdi`.

2. **Misleading “Please log in again” + “No markets”**  
   PDI called `fetchMyDealsApi` → `authFetch` → `alert(LOGIN_MSG)` on missing JWT.  
   Thrown errors lacked `AUTH_REQUIRED` classification, so `shellHtml` painted a **LOAD_FAILED** banner with the login message **and** a Market `<select>` showing **“No markets”**.  
   Registry not configured (`503 REGISTRY_NOT_CONFIGURED`) was sometimes conflated with login failure.

3. **Filter visual divergence**  
   PDI used custom `.gdi-pdi-controls` / `.gdi-pdi-select` (light-theme) instead of Opportunities `.filters-section` / `.filter-group` / `.filter-select` / Reset View.

## Fixes

- Reuse Opportunities filter chrome for Market / Participating hotel / Hotel fit + Reset View.
- Distinct states: `AUTH_REQUIRED`, `OPERATOR_NOT_MAPPED`, `UNAVAILABLE` (no fake demo membership).
- Auth via `getAuthHeaders` (no stacked login alert) + in-page messaging.
- Canonical GDI loading toast via `onLoading` → `setLoading`.
- Cache-bust `?v=gdi-pdi-filter-parity-v1`.
- Preserve single `#gdiDrawer` / contact-card parity path.

## Local consistency guidance

- Use registered workspace server: `ws-gdi` (e.g. `:8095` with `DEALALITY_ALLOW_WORKTREE_SERVER=1`).  
- Do **not** treat detached primary `:8080` as the GDI product home.  
- Unrelated primary dirty work left untouched.

## Evidence

- Local demo screenshot: `local-pdi-filters-demo.png` (`?pdiDemo=1` on `:8095`)
- Commit: `75c4ba3a11335295eb455da342aff015a1a1d92c`
- PR: https://github.com/joandejarden-create/my-operators-backend/pull/467
- Cache marker: `gdi-pdi-filter-parity-v1`

## STOP

Founder visual review required before declaring UI parity complete.  
Awaiting Integration Controller merge + external deploy; do **not** claim EXTERNALLY_VERIFIED until AFTER screenshots on Railway.
