# AI Demand Leak Audit — Admin Placement QA

## Scope

Private admin placement for Limited AI Demand Leak Audits: left nav, report picker, shared client preview. Not public website content.

## Routes

| Nav item | Path |
| --- | --- |
| Audit Reports | `/admin/adp-leak-audits/reports` |
| New Audit | `/admin/adp-leak-audits/new` |
| Portfolio Audits | `/admin/adp-leak-audits/portfolios` |
| Sample Reports | `/admin/adp-leak-audits/samples` |
| Converted Leads | `/admin/adp-leak-audits/converted` |

`/admin/adp-leak-audits` redirects to `/admin/adp-leak-audits/reports`.

Request queue (Approve / Run) remains at `/admin/adp-leak-audits/queue`.

## Root cause (dealalitydemo nav missing)

**Cause:** Leak Audit routes use `roles: ['admin']` and only show when `/api/me` → `dealality.isAdmin === true`. Items like Route Map / Validation Scorecard can still appear on localhost via `isDevMode`, and Helena via founder/demo overrides — so Admin Resources looked “admin” while Leak Audits stayed hidden.

Elevation for governed emails (`assignments.v1.json` `adminEmails`, including `dealalitydemo@dealality.com`) lived in `/api/me`, but originally used **Airtable Users email only**. When that field was empty or mismatched the Memberstack login email, `isAdmin` stayed false.

**Fix:**

- Elevate with Airtable email, then fall back to Memberstack token email (`api/me.js`).
- Place Leak Audit links **inside** the Admin Resources group (top of that menu), not as a separate sibling section users miss.
- Cache-bust: `public/app.html` → `/app.js?v=ma-1.3.6-leak-nav`.

**Fallback:** On paid ADP admin (`/app/admin/ai-demand-reviews.html`): **Open AI Demand Leak Audits** → `/admin/adp-leak-audits/reports`.

**Diagnostics:** `/admin/adp-leak-audits/reports?debug=nav` shows `isAdmin`, `governedPlatformAdminElevated`, and nav source.

## Checks

1. Left nav **Admin Resources** shows Audit Reports / New Audit / Portfolio Audits / Sample Reports / Converted Leads for `dealalitydemo` after login (**restart local server** so `/api/me` picks up elevation, then hard refresh).
2. Market Intelligence → AI Demand Positioning stays separate.
3. Reports page title/subtitle match product copy.
4. Filters: Report Type, Status, Source; dropdown newest-first labels `Name — Type — Status — Date`.
5. **Load Report** iframes `/adp-leak-audit/sample`, `/adp-leak-audit/sample-portfolio`, or `/adp-leak-audit/:reportId` (shared renderer).
6. **Download PDF** uses `print()` on that same renderer (3-page layout).
7. **Copy Share Link** copies the client-safe URL.
8. **Mark Sent** / **Promote** only for non-sample requests with confirmation modal; no production ADP writes unless promoted (Phase 1 stub).
9. `public/sitemap.xml` has no leak-audit URLs; admin pages use `noindex,nofollow`.

## Local QA (dealalitydemo)

1. **Restart** the local Node server (required for `api/me.js` elevation change).
2. Log in as `dealalitydemo` / `dealalitydemo@dealality.com`.
3. Hard refresh `/app` (`Ctrl+Shift+R`) so `app.js?v=ma-1.3.6-leak-nav` loads.
4. Expand **Admin Resources** — confirm Audit Reports, New Audit, Portfolio Audits, Sample Reports, Converted Leads at the top.
5. Open each nav item; confirm URLs.
6. Hit `/admin/adp-leak-audits` → lands on `/reports`.
7. Load Cambridge + Dovetail samples; preview iframe + Download PDF.
8. Optional: open `/admin/adp-leak-audits/reports?debug=nav` and confirm `isAdmin: true` and `governedPlatformAdminElevated: true`.
9. From AI Demand Reviews, use **Open AI Demand Leak Audits** fallback.

## Gates

```bash
npm run test:adp-leak-audit-admin-placement-v1
npm run test:adp-leak-audit-admin-nav-visibility-v1
```
