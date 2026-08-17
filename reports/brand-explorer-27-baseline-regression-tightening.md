# Brand Explorer — 27 Baseline Regression Tightening

Generated: 2026-07-24  
Airtable writes: **false**

## Problem

Wave 12 Stage 0 preflight showed:

| Gate | Result |
| --- | --- |
| `test:brand-explorer-27-active-public-full-baseline` | PASS |
| Fresh `test:brand-explorer-public-visibility-quality-lock -- --public-full-only` | FAIL |

That split is unacceptable. Regression reused an on-disk PVQL report that still had per-brand `lockPass=true` within the age window, while a later live public-full lock failed on Preferred / Radisson Individuals / SLH (`tab_factory_audit` + `generic_copy_scan` via golden `generic_audience_prose`).

Also, brand-level generic checks only counted `mechanicalHits`, so golden `generic_audience_prose` did not fail the regression even when present on a live PVQL row.

## Changes (`lib/partner-intelligence/brand-explorer-27-active-public-full-baseline.js`)

1. **Fresh PVQL by default** — regression always re-runs live public-full PVQL unless `--allow-cached-pvql-if-pass` is explicitly set.
2. **Cached reuse (opt-in only)** requires:
   - `summary.overallPass === true`
   - `publicFullProfileCount === 27`
   - every frozen brand: `lockPass`, `tab_factory_audit.pass`, `generic_copy_scan.pass`
   - `raw_url_scan` hits = 0
   - `forbidden_owner_facing_language` hits = 0
3. **Aggregate fails when**:
   - `summary.overallPass !== true`
   - public-full count ≠ 27
   - public-full pass count ≠ 27
   - any public-full brand fails `tab_factory_audit`
   - any public-full brand fails `generic_copy_scan`
4. **Per-brand fails when**:
   - `generic_copy_scan.pass !== true` (including golden failures, not only mechanical hits)
   - `tab_factory_audit.pass !== true`
   - existing `raw_url_scan` / `forbidden_owner_facing_language` / `shouldRenderFullProfile_false` checks

## Test entry (`scripts/test-brand-explorer-27-active-public-full-baseline.mjs`)

- Default: force live PVQL
- Opt-in cache: `--allow-cached-pvql-if-pass`

## Acceptance implication

`npm run test:brand-explorer-27-active-public-full-baseline` cannot PASS while a fresh public-full PVQL lock fails on Preferred, Radisson Individuals, SLH, or any other public-full brand.
