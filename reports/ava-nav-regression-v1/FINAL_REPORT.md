# AI_VISIBILITY_AUDIT_NAVIGATION_REGRESSION_FIXED

## 1. Exact root cause

Two stacked regressions on `release/dealality-unified-production-rc-v1` (localhost:8080 → `my-operators-backend-unified-rc`):

1. **Silent tab no-op:** `public/js/admin-ai-demand-admin.js` `ALLOWED` omitted `"visibility-audit"`. Clicks on AI Visibility Audit fell through `if (!ALLOWED[tab]) tab = "reviews"`, so the AVA panel never activated.
2. **Truncated destination markup:** `public/app/admin/ai-demand-admin.html` closed `#supportGateContent` before the AVA panel and dropped `#avaTable`, `#avaTableBody`, and `#avaDrawer`. Even after ALLOWED was restored, hotel list + audit drawer could not mount.

Server route `/api/admin/ai-visibility-audit/hotels` and `admin-ai-visibility-audit.js` were intact.

## 2. Previously working implementation recovered

- Shell wiring recovered to match working AVA ALLOWED + `setActiveTab` handlers (canonical pattern also present on `my-operators-backend` feat branch).
- Full AVA panel HTML (results row, table, drawer) restored from working `my-operators-backend` admin page; panel kept **inside** `#supportGateContent`.
- Standalone `?tab=` sync added via `syncOwnSearchTab` so route reflects the active tab.

## 3. Files modified

- `public/js/admin-ai-demand-admin.js`
- `public/app/admin/ai-demand-admin.html`
- `scripts/test-adp-admin-workspace-navigation-v1.mjs` (new)
- `scripts/verify-ava-nav-browser-acceptance-v1.mjs` (new acceptance helper)
- `lib/dealality-pr-check-matrix.js`
- `docs/dealality-pr-validation-matrix.md`
- `reports/ava-nav-regression-v1/*` (screenshots + acceptance)

## 4. Navigation route used

- Tab id: `visibility-audit`
- Panel: `#adaPanelVisibilityAudit`
- Standalone URL: `/app/admin/ai-demand-admin.html?tab=visibility-audit`
- Embedded parent hash: `#/admin/ai-demand?tab=visibility-audit`
- Hotels API: `GET /api/admin/ai-visibility-audit/hotels`
- Detail: `GET /api/admin/ai-visibility-audit/:propertyId` (drawer)

## 5. Actual browser verification

- Port 8080 PID cwd = `C:\Dev\dealality\my-operators-backend-unified-rc\`
- Clicked AI Visibility Audit → panel visible, 38 hotels loaded
- Selected Waterstone → Open Audit → drawer shows `AUDIT_VALIDATED`, recommendations, PDF channels
- Tab round-trip Reviews ↔ Visibility Audit works
- Mobile viewport screenshot captured
- Regression test: `npm run test:adp-admin-workspace-navigation-v1` → PASS (38 rows)

## 6. Screenshots

- `01-ava-tab-open-desktop.png`
- `02-ava-waterstone-audit-drawer-desktop.png`
- `03-ava-tab-open-mobile.png`
- `04-ava-waterstone-drawer-live-browser.png`

## 7. Regression tests added

- `npm run test:adp-admin-workspace-navigation-v1`
- Wired into `lib/dealality-pr-check-matrix.js` + PR validation matrix for ADA/AVA HTML/JS paths
- Covers all five tabs: reviews, action-plan, gdi-reports, report-archive, visibility-audit

## 8. Branch / SHA

Recorded at commit time in this folder’s git metadata (see commit).

## 9. Focused PR

Create from `release/dealality-unified-production-rc-v1` if/when merging to main — do not include unrelated ADP/GDI dirtiness.

## 10. Production deployment status

**Not deployed.** No Railway release. No production Airtable writes. No audit approvals/publishes. No share-token changes.
