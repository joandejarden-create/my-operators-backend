# Brand Explorer Comparison — permanent Admin Resources navigation

**Contract:** `BE_ADMIN_NAV_BRAND_EXPLORER_COMPARISON_V1`

## Navigation

**Dealality → Admin Resources → Brand Explorer Comparison**

- Route: `/app#/brand-explorer-compare?beInternalPreview=1`
- Embed (origin-relative): `/brand-explorer-compare.html?embed=1&appShell=1&beInternalPreview=1`
- Access: platform admin only (`internalRunbookOnly` + `SupportAdminGate.requireInternalRunbook`)
- Single nav registration (`navId: brand-explorer-comparison`) — do not duplicate

## Environments

Works on the **current application origin** (never hardcode `127.0.0.1:8082`):

- Canonical local app `:8080`
- BE development preview `:8082`
- Authorized deployed internal admin hosts

## Founder workspace

Comparison Studio remains the permanent review surface:

- Brand selection
- Ten Brand Explorer tabs
- CURRENT saved (left) vs PROPOSED staged (right)
- Highlight Changes / Changes Only / Issues panel
- Founder decisions + publication controls (Airtable writes still gated)

Persistent review records: `/api/brand-explorer-compare/review` (internal runbook auth) under `reports/brand-explorer/comparison-studio/`.

## Tests

```bash
npm run test:be-admin-nav-brand-explorer-comparison-v1
npm run test:be-comparison-studio-v1
```
