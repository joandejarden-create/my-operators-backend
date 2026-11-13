# GDI Guided Tour Production Visual Regression V1 — Founder Report

**Verdict:** **CLIENT READY** (tour visual + shared UI)

**Package / fingerprint (unchanged):**  
`gdi_pub_recLuxvwwxID7U2B8_20261113_reconciliation_v1`  
`441773211494cebaf50c03ef8921ccac6c57f31ac5cb2398585352b425e36f7f`

## Root cause

Demoting Opportunity Details from `showModal()` → `show()` for popover hit-testing left a non-modal `<dialog>` at **`y ≈ 416`**. With a ~40vh highlight cutout and a `9999px` scrim, step 6 rendered as a blank dark overlay. Production had been stuck on **`gdi-howto-read-v1-1e`**. Evidence: `00-REPRO-prod.json`, `01-ROOT-CAUSE.md`.

## Fix

Shared CSS `.gdi-drawer--tour-nonmodal` pins the demoted drawer to the fixed right rail; engine uses full-rail drawer highlights and left-column coachmarks; version **`GDI_GUIDED_REPORT_TOUR_V1_1g`**. New gate: `npm run test:gdi-guided-tour-visual-acceptance-v1`.

## Ship path

| Item | Value |
|------|-------|
| PR | [#251](https://github.com/joandejarden-create/my-operators-backend/pull/251) |
| Merge SHA | `ea6d9440e7ad1b3b26484b5f719cd888e6c3c579` |
| GHA release | [43425889151](https://github.com/joandejarden-create/my-operators-backend/actions/runs/43425889151) |
| Railway deployment | `16dc6716-3f73-4b1f-b0f5-d9588620224d` |

Main-branch release dispatch failed `WRONG_BRANCH_GUARD` on detached artifact HEAD; live deploy used control plane `release/gha-deploy-detached-head-guard-v1` (same pattern as prior GDI releases).

## Production verification

- HTML marker live: `gdi-howto-read-v1-1g` (after Railway build finished; ~12 min after `railway up --detach`)
- EOL-normalized asset SHA match vs local tip: **PASS**
- Desktop visual acceptance on live Bethesda share: **PASS** (step 6 drawer `y=0`, full-rail highlight, coachmark left of drawer, no blank center)
- Mobile 390×844 visual geometry: **PASS**
- Chrome inventory: **All 67 / Hotel 61 / Competitive 10**, Reset View present, Need Dates Hotel Confirmed
- Package fingerprint on disk unchanged

Prior Next-only certificates are **invalidated** for tour UX (`03-PRIOR-FALSE-PASS-GAP.md`).

## Remaining limitations (non-blocking for this tour defect)

1. `/health` `buildSha` can lag lean path-as-root uploads — use asset hashes, not health alone.
2. Detached-HEAD deploy guard still lives on the release control-plane branch; merge that onto `main` to avoid failed main dispatches.
