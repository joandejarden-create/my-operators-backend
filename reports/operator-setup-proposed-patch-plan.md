# Stage 10 — Proposed Safe Patch Plan (No Implementation)

Generated: 2026-06-02T07:42:44.724Z

## Batch 1: Guard rails
- Risk: Low
- Files/areas: api/operator-explorer.js
- Changes:
  - Disable mock fallback in operator explorer API for production.
  - Enforce rec-id-only detail lookup contract.

## Batch 2: Write path hardening
- Risk: Medium
- Files/areas: api/third-party-operator-intake.js, env/feature-flag docs
- Changes:
  - Lock intake to canonical new-base path for go-live mode.
  - Keep legacy shadow writes only in non-prod diagnostics mode.

## Batch 3: Mapping completeness
- Risk: Medium-High
- Files/areas: api/lib/operator-setup-new-base-build-sheet-rows.json, mapping scripts, UI form names
- Changes:
  - Resolve high-risk unmapped My Operator keys from Stage 2.
  - Update build-sheet JSON/CSV and add validation assertions.

## Batch 4: Option normalization
- Risk: Medium-High
- Files/areas: field maps, form option providers, scoring normalizers
- Changes:
  - Define canonical option sets and alias maps (non-destructive migration).
  - Update validators + UI option sources + scoring normalizers.
