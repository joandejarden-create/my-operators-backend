# INVALIDATED — FULL_10_TAB_PRODUCT_QUALITY_CERTIFIED (PR #478)

**Date:** 2026-11-20  
**Lesson:** `BE-QL-STALE-PROPOSED-CONTENT-001`

## Why invalidated

Founder screenshot on `http://localhost:8080/app#/brand-explorer-combined?beInternalPreview=1` showed:

1. Heading `Commercial Platform And Lobby Experience` (must be `&`)
2. Why Value bullets without bold Lead: explanation structure
3. Incomplete / non-finalized proposed editorial content

Prior `FULL_10_TAB_PRODUCT_QUALITY_CERTIFIED` from PR #478 is **revoked** until these rendered-text gates pass on the live 8080 authenticated popup.

## Root cause (proven)

1. **Mode soft-default:** `brand-explorer-route-state.js` set `beProfileMode: 'saved'` when the query omitted the param. `getProfileMode()` trusted that soft default, so founder preview showed **CURRENT Airtable** (And heading) instead of cumulative Proposed.
2. **Why-value regression:** Candidate `overview.why_value` is a thin fragment dump; assembler previously allowed it to replace CURRENT colon-insight bullets.
3. **False cert:** Ten-tab gate checked section presence / substance length, not ampersand headings or bold insight leads.

## Candidate

- Version: `phase2h-component-parity`
- Overlay: `/brand-explorer-phase2e-preview/curio-collection-phase2h-candidate.json`

## Status

`NOT_CERTIFIED_FAIL` until re-verification evidence in this folder proves the screenshot defects are gone.
