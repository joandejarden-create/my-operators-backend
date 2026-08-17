# CoStar exports for internal GTM owner target list

Place licensed CoStar **Properties** exports here (CSV, XLS, or XLSX).

This folder is **gitignored**. Files are for internal GTM use only — not for Dealality product or public distribution.

## Commands

```bash
# 1. Set AIRTABLE_GTM_BASE_ID in .env, then create schema:
node scripts/ensure-gtm-owner-target-base.mjs --apply

# 2. Preview import:
node scripts/import-gtm-owner-target-costar.mjs

# 3. Apply import:
node scripts/import-gtm-owner-target-costar.mjs --apply
```

See [docs/gtm-owner-target-list.md](../../docs/gtm-owner-target-list.md).
