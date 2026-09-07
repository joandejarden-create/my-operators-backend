# 01 — Current Admin Architecture

## Shell

- Entry: `/app` hash SPA (`public/app.html` + `public/app.js`)
- Pages load as iframes via `ROUTES[route].file`
- Admin Resources group already hosts AI Demand Reviews, runbooks, Route Map

## Auth pattern (reused)

- Client: `SupportAdminGate.requireAdmin` + `DealalityMemberstackAuth.authFetch`
- Server: `adminAuth` = `[memberstackAuth, requireDealalityUser, requireAdminAccess]`

## Template

Closest peer: `/admin/ai-demand-reviews` → static HTML under `public/app/admin/` + gated `/api/admin/...` routes.

## Helena addition

| Layer | Path |
|---|---|
| Nav + route | `public/app.js` → `/admin/helena-cmo` |
| Badge CSS | `public/app.css` → `.nav-badge` |
| Page | `public/app/admin/helena-cmo.html` |
| Client | `public/js/admin-helena-cmo.js` |
| Styles | `public/css/admin-helena-cmo.css` |
| API | `api/admin-helena-cmo.js` wired in `server.js` |
| View model | `lib/helena-cmo/founder-console/brief-view-model.js` |

## Non-goals for this architecture

- No static serving of `reports/` as the app data model
- No public website routes changed
- No parallel CMO database
