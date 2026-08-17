# Phase 6 — Real Workspace Switcher QA

## Summary

The left-nav control at the former “Dev Workspace” placement is now the **real workspace switcher**. It drives app-shell navigation context only. Permissions remain on `/api/me` `workspaceAccess` and server middleware.

Command Center dashboard preview remains separate (`dc_dashboard_role_view`).

## Files changed

| File | Change |
|------|--------|
| `public/app.js` | Active workspace model, `/api/me`-driven options, nav routing, localStorage |
| `public/app.html` | Workspace switcher markup, Demo Mode badge/helper, localhost dev tools |
| `public/app.css` | Demo badge/helper + dev tools styles |
| `public/app/home.html` | Command Center label → **Preview Sample Dashboard As** |
| `public/app/dashboard.js` | Comment: `dc_dashboard_role_view` ≠ `dealality_active_workspace` |
| `public/js/dealality-ui-labels.js` | (unchanged — already provides Owner-Side / Operator-Side / Brand-Side / Demo Mode) |

**Not modified:** Airtable, Memberstack plans, middleware write gates, Operator Alignment Snapshot.

## Storage keys

| Key | Purpose |
|-----|---------|
| `dealality_active_workspace` | Real shell context — values `Owner`, `Operator`, or `Brand` |
| `dc_dashboard_role_view` | Command Center sample dashboard preview only (`owner` / `brand` / `operator`) |
| `DEALALITY_DEV_NAV_OVERRIDE` | Localhost only — optional `admin` / `all` nav override (does not change active workspace) |

## UI

- Label: **Workspace** (never “Dev Workspace” in production/staging).
- Options (dynamic): **Owner-Side**, **Operator-Side**, **Brand-Side** from `workspaceAccess` + `demoPreviewWorkspaces` when Demo is present.
- When Demo: **Demo Mode** badge + helper (sentence case): *Preview workspace views without production permissions.*
- Localhost: extra **Admin** / **All Workspaces** override + Webflow preview link (dev tools block).

## Switcher visibility

| User | Switcher |
|------|----------|
| Single workspace, no Demo | Hidden |
| Multiple workspaces (e.g. Owner-Operator) | Shown |
| Demo (only or combined) | Shown |

## Active workspace resolution

1. Read `dealality_active_workspace` from localStorage.
2. If invalid for current user → ignore, use default (first of Owner → Operator → Brand in allowed list; Owner-Operator defaults to **Owner**).
3. Allowed list = production workspaces in `workspaceAccess` ∪ `demoPreviewWorkspaces` when `isDemo`.
4. If `workspaceAccess` empty → legacy `dealality.role` / `legacyRole` fallback.

## Navigation / landing on workspace change

Active workspace maps to nav role: `Owner` → `owner`, `Operator` → `operator`, `Brand` → `brand`.

| Nav role | Landing route when current route incompatible |
|----------|-----------------------------------------------|
| owner | `/home` |
| operator | `/operator-development-dashboard` |
| brand | `/brand-development-dashboard` |

If the current hash route is allowed for the new nav role, the user **stays on the page**; nav re-renders only.

`canNavigateToRoute` uses **active nav role**, not legacy auth role — Demo users can preview Operator/Brand nav without gaining write access.

## Manual QA matrix

### 1. Owner-only user

- [ ] Switcher hidden.
- [ ] Owner nav (My Deals, etc.) visible.
- [ ] Set `localStorage.dealality_active_workspace = 'Operator'` → reload → ignored; still Owner nav.

### 2. Operator-only user

- [ ] Switcher hidden.
- [ ] Operator nav (My Operator Deals, Operator Setup) visible.

### 3. Brand-only user

- [ ] Switcher hidden.
- [ ] Brand nav (My Brand Deals, etc.) visible.

### 4. Hotel Owner - Operator user

- [ ] Switcher shown with Owner-Side + Operator-Side.
- [ ] Brand-Side not listed unless Brand or Demo access exists.
- [ ] Default active workspace = Owner after clear storage.
- [ ] Switch to Operator-Side → My Operator Deals nav appears; My Deals owner items follow nav role rules.
- [ ] Reload → selection persists.

### 5. Demo-only user

- [ ] Switcher shows Owner-Side, Operator-Side, Brand-Side (from `demoPreviewWorkspaces`).
- [ ] Demo Mode badge + sentence-case helper visible.
- [ ] Can preview each side’s nav; production writes still blocked (My Deals / Operator Deals APIs).
- [ ] Active workspace persists in `dealality_active_workspace` (not `Demo`).

### 6. Demo + Owner user

- [ ] Owner production nav available when Owner-Side selected.
- [ ] Operator/Brand preview available via switcher (Demo previews).
- [ ] Write access from Owner workspace on server, not from Demo badge alone.

### 7. Command Center preview

- [ ] Home **Preview Sample Dashboard As** toggles sample metrics only.
- [ ] `dc_dashboard_role_view` changes; `dealality_active_workspace` unchanged.
- [ ] Changing workspace in sidebar does not change dashboard preview role.

### 8. Regression

- [ ] `/api/me` returns `workspaceAccess`, `demoPreviewWorkspaces`, `isDemo`.
- [ ] My Deals still allowed for Hotel Owner - Operator (Owner workspace).
- [ ] My Operator Deals still allowed for Hotel Owner - Operator (Operator workspace).
- [ ] Operator Explorer loads.
- [ ] Company Settings saves.
- [ ] Demo is not treated as Admin in nav.

## Change impact

**Medium** — app shell navigation and localStorage only; no Airtable writes.

**Rollback:** Revert `public/app.js`, `public/app.html`, `public/app.css`, `public/app/home.html`, `public/app/dashboard.js`.
