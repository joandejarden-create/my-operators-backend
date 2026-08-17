# Brand Explorer 39 — Live PVQL Regression Tightening

Generated: 2026-07-26  
Related: Protected 39 live ADR scenario scrub / Wave 13 Stage 0

## Problem

`npm run test:brand-explorer-39-active-public-full-baseline -- --allow-cached-pvql-if-pass` could PASS while a fresh live PVQL public-full-only run FAILED (ADR in `valueOwners.scenario.*` Body).

Wave preflight must not treat a cached PVQL snapshot as proof that the protected 39 universe is live-clean.

## Rules (enforced)

1. **Normal baseline regression** may reuse on-disk PVQL only with the explicit flag `--allow-cached-pvql-if-pass`, and only when that report is gate-clean for all 39 public-full brands.
2. **Default / CI / Wave preflight** use fresh live PVQL (`--force-live-pvql` or omit `--allow-cached-pvql-if-pass`).
3. **Wave 13 preflight** (`npm run brand-explorer-wave13-factory -- --stage preflight --dry-run`):
   - Always runs fresh PVQL public-full-only
   - Runs quality audit dry-run
   - Runs `test:brand-explorer-39-active-public-full-baseline -- --force-live-pvql`
   - **Never** passes `--allow-cached-pvql-if-pass`
   - Fails if any public-full brand has `lockPass=false`
   - Fails if quality audit returns `remediation_required` / not `ready_to_freeze_39_active_public_full_baseline`
   - Fails if ADR / RevPAR / fee-stack / FDD / Item 19 / LOI appear in forbidden-language hits
4. If fresh PVQL fails, Wave preflight fails.
5. Wave 13 source packs / content generation remain deferred until preflight ready statement is `protected_39_live_clean_wave13_may_resume`.

## Commands

```bash
# Local thrash avoidance only — not for Wave preflight
npm run test:brand-explorer-39-active-public-full-baseline -- --allow-cached-pvql-if-pass

# Wave / acceptance
npm run test:brand-explorer-39-active-public-full-baseline -- --force-live-pvql
npm run brand-explorer-wave13-factory -- --stage preflight --dry-run
```

## Airtable

This tightening is report/code-only. No Airtable writes.
