# Surfe Usage Admin Authentication Fix V1 — Founder Report

**Date:** 2026-11-21  
**Branch:** `fix/gdi-surfe-usage-admin-auth-v1`  
**Base:** `origin/main` @ `bf8c4f398` (#500 tip)

## Exact authentication root cause

1. **Primary (confirmed in live shell session):**  
   `public/app/admin/surfe-usage.html` loaded `/js/memberstack-auth.js`, which **does not exist (HTTP 404)**.  
   Working admin pages (AI Demand Admin) load `/js/dealality-memberstack-auth.js`.  
   Without `window.DealalityMemberstackAuth`, `SupportAdminGate.requireAdmin()` returned `auth_unavailable` and showed:

   > Access restricted — Sign in through the Dealality app to view this page.

   Shell `/api/me` as Demo Dealality (`dealalitydemo@dealality.com`) already returned `isAdmin: true` — the session was authorized; the **iframe never received the auth helper**.

2. **Secondary (localhost embed timing):**  
   Even after loading the correct helper, embed `authFetch` waited up to ~15–20s for a parent Memberstack JWT. Local Integrated Staging uses the established shell path `DealalityAppShellAuth.tryLocalDevAuthFixture` (no-Bearer `/api/me` → server `DEV_AUTH_BYPASS`). Embeds did not share that path promptly.

## Fix (smallest focused change)

| File | Change |
|------|--------|
| `public/app/admin/surfe-usage.html` | Load `dealality-memberstack-auth.js` + wave loader (same as AI Demand Admin) |
| `public/js/dealality-memberstack-auth.js` | Localhost same-origin: short JWT probe then no-Bearer fetch (shell DEV_AUTH_BYPASS parity). Non-local still requires JWT. |
| `public/js/support-admin-gate.js` | `isAdminMe` aligned with `requireAdminAccess` (`isAdmin` / `flags.isAdmin` / `localDealalityAdminAccess`); distinct not-signed-in vs unauthorized messages |
| `public/js/admin-surfe-usage.js` | Fail closed if auth helper missing; no bare `fetch` for admin APIs |
| `public/app.js` | Surfe `platformAdmin` nav/route parity with local QA admin |
| `scripts/test-gdi-surfe-usage-admin-auth-v1.mjs` | Permanent regression gate |

## Admin permission contract

- **Shell nav:** `hasAdminNavAccess()` OR `hasAdpMonthlyReviewAdminNavAccess()` (local QA).
- **Embed page:** `SupportAdminGate.requireAdmin()` → `isAdminMe` (isAdmin / flags.isAdmin / localDealalityAdminAccess).
- **API:** `adminAuth` = `memberstackAuth` + `requireDealalityUser` + `requireAdminAccess` (isAdmin / workspace Admin / localDealalityAdminAccess).
- Demo Mode badge alone does **not** grant access.
- No JWT in query strings; no forced `isAdmin = true` client-side; no Airtable activation; no paid Surfe.

## Backend authorization confirmation

| Request | Result |
|---------|--------|
| `GET /api/me` (localhost, no Bearer) | 200 · `dealalitydemo@` · `isAdmin: true` |
| `GET /api/admin/surfe-usage/summary` (localhost admin) | 200 · `ok: true` · hotels include Bethesda / Radisson Toronto / Sheraton Guadalajara |
| `GET /api/admin/surfe-usage/summary` + `Authorization: Bearer not.a.jwt` | **401** `invalid_token` |

## Authenticated browser verification

**URL:** `http://localhost:8080/app#/admin/surfe-usage`  
**Session:** Demo Dealality / local DEV_AUTH_BYPASS platform admin (Memberstack Test Mode present; no live eyJ JWT — same shell fixture used for Admin Resources).

**Observed:** Admin Resources → Surfe Usage visible; embed renders dashboard (not Access restricted); KPIs + hotel tables with Bethesda Marriott, Radisson Blu Toronto Downtown, Sheraton Guadalajara Expo; Hide fixture/test on; refresh preserves access.

**Screenshot:** `reports/gdi-surfe-usage-admin-auth-v1/screenshots/01-authenticated-surfe-usage-shell.png`

## Regression / CI

- `npm run test:gdi-surfe-usage-admin-auth-v1` → PASS  
- `npm run test:gdi-surfe-usage-v1-3` → PASS  
- `npm run test:adp-admin-auth-split-brain-v1` → PASS  

## Out of scope (preserved)

- Airtable Surfe sync remains `PROPOSED_AWAITING_FOUNDER`
- No production deploy
- No paid Surfe calls
- Dirty product worktrees preserved
