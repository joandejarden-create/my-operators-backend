# Brand Explorer Active Universe Normalization

Normalizes Brand Explorer OS / PVQL / restore logic around the canonical **Brand Status Active/Live** universe (24).

## Source of truth

- `lib/brand-status-active.js` — `BRAND_STATUS_ACTIVE_FORMULA`
- `lib/partner-intelligence/brand-explorer-active-universe.js` — `loadActiveUniverse()`
- APIs: `GET /api/brand-library/brands`, `GET /api/brand-explorer/brands`

Operational cohorts (`PRIMARY_RELEASE_SLUGS`, Lane 1/2, intentional restore, prior 23) are overlays — **not** the active universe.

## Run

```bash
npm run brand-explorer-active-universe-normalization -- --dry-run
```

## Outputs

- `reports/brand-explorer-active-universe-normalization.json`
- `reports/brand-explorer-active-universe-normalization.md`
- `reports/brand-explorer-active-universe-pvql-repair-plan.md`
- `reports/brand-explorer-active-universe-unconfigured-brands.md`
- `reports/brand-explorer-active-universe-status-conflicts.md`

## Rules

- No Airtable writes in normalization
- No Company Validated / Source Library / Registry / Brand Status / release changes
- No content apply until a separate approved scrub/build task

Latest run: 2026-07-23T10:30:05.963Z

