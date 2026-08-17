# Brand Explorer Active Universe — Source of Truth

Audit-only. Establishes the canonical Brand Explorer **active** universe from Brand Basics.

## Source of truth

- **Name:** Brand Basics `Brand Status` Active/Live
- **Formula:** `OR({Brand Status}='Active', {Brand Status}='Live')`
- **Code:** `lib/brand-status-active.js` → `BRAND_STATUS_ACTIVE_FORMULA`
- **APIs:** `GET /api/brand-library/brands`, `GET /api/brand-explorer/brands`

Code lists (`PRIMARY_RELEASE_SLUGS`, `LEGACY_SEED_BRANDS`, Lane 1/2, intentional restore registry, prior 23 reconciliation) are **operational cohorts**, not the active universe.

## Run

```bash
npm run brand-explorer-active-universe-source-of-truth -- --dry-run
```

## Outputs

- `reports/brand-explorer-active-universe-source-of-truth.json`
- `reports/brand-explorer-active-universe-source-of-truth.md`
- `reports/brand-explorer-active-universe-cohort-diff.md`
- `reports/brand-explorer-active-universe-missing-brand.md`

## Rules

- No Airtable writes
- No Company Validated / Source Library / Registry / release / content changes
- Do not use stale 23-brand reconciliation lists as active source of truth

Latest run: see reports (generated 2026-08-09T21:12:55.241Z).

Inventory size: **62** · reconciles to 54: **false**

