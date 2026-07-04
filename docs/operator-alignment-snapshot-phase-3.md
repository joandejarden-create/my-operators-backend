# Operator Alignment Snapshot — Phase 3 (My Deals integration)

**Date:** 2026-05-25  
**Status:** Implemented  
**Depends on:** [operator-alignment-snapshot-phase-2.md](./operator-alignment-snapshot-phase-2.md)

## Goal

Add an owner-facing **My Deals** entry point and compact preview for the profile-level Operator Alignment Snapshot. No named operating companies, no scoring changes, no Airtable changes.

## Files created

| File | Purpose |
|------|---------|
| `public/css/operator-alignment-my-deals-preview.css` | Compact modal preview styles (dark My Deals shell) |
| `docs/operator-alignment-snapshot-phase-3.md` | This document |

## Files modified

| File | Change |
|------|--------|
| `public/my-deals.html` | Action icon, modal shell, preview loader, click handler |
| `public/js/operator-alignment-snapshot.js` | `renderMyDealsPreview`, top-profile sort, preview HTML builder |
| `scripts/test-operator-alignment-snapshot-page.mjs` | My Deals integration checks |
| `docs/operator-alignment-snapshot-implementation-checklist.md` | Phase 3 marked complete |

**Not modified:** Airtable, `api/operator-alignment-snapshot.js` contract, BAS, OCS, `scoreOperatorMatchForDeal`, operator-match-score-breakdown.

## Where the My Deals action appears

**Deals table** row actions (same column as Deal Readiness, Brand Alignment, Operator Capability):

1. Deal Readiness Review  
2. Brand Alignment Snapshot  
3. Operator Capability Snapshot  
4. **Operator Alignment Snapshot** (clipboard icon, `data-action="operator-alignment"`)  
5. View (Deal Brief)  
6. Edit  
7. More  

Tooltip: **View Operator Alignment Snapshot**

## Modal behavior

- Overlay: `#myDealsOperatorAlignmentModal` (same pattern as Brand Alignment / Operator Capability)
- API: `GET /api/operator-alignment-snapshot/:dealId/profile` via `DealalityMemberstackAuth.fetchMyDealsApi`
- Renderer: `OperatorAlignmentSnapshot.renderMyDealsPreview()` — **not** the full five-card document
- Shows top **3** profiles sorted by alignment band (Strong → Insufficient) then `sortPriority`
- Humanized relevance chips via existing `humanizeSignalKey` / `humanizeSignalKeys`
- Workflow bullets strip duplicated “Suggested workflow action:” prefix
- Footer buttons:
  - **Open full snapshot** → `/operator-alignment-snapshot.html?dealId=rec…`
  - **Print / Save as PDF** → same URL with `&print=1` (new tab)

## Full snapshot URL

```
http://localhost:8080/operator-alignment-snapshot.html?dealId=recXXXXXXXX
```

Optional auto-print:

```
http://localhost:8080/operator-alignment-snapshot.html?dealId=recXXXXXXXX&print=1
```

## Humanized signal keys

Reuses `SIGNAL_KEY_LABELS` and `humanizeSignalKey()` from `public/js/operator-alignment-snapshot.js` (exported on `window.OperatorAlignmentSnapshot`). No raw `new_build_project`-style keys in the modal UI.

## Operating Companies for Consideration (placeholder)

**Rendered** in the My Deals modal footer area as a dashed callout:

> Company-level operator alignment is not included in this profile-level snapshot. Operating companies for consideration will appear in a later version once operator setup profiles are complete enough for company-level alignment.

No mock operator names, no rankings, no recommendations language.

The standalone full snapshot page is **unchanged** (no placeholder added there in Phase 3).

## Error / empty states

| Case | Copy / behavior |
|------|-----------------|
| Auth 401/403 | Sign-in message |
| API / deal error | Neutral error + link to full snapshot when `dealId` known |
| Empty `profilesForReview` | Additional information may be needed… + link to full snapshot |
| Renderer missing | Snapshot preview failed to load |

## Known limitations

- Preview only; full report remains on standalone page
- Top 3 profiles only in modal
- No My Deals embed on other tabs (Matched Brands, etc.) — Deals table only
- No named operators until Phase 4
- `humanizeSignalKey` lives in browser renderer only (not shared server-side)

## What remains for Phase 4

- **Operating Companies for Consideration** — company-level alignment using Operator Setup / Explorer + `scoreOperatorMatchForDeal`
- Optional operator shortlist/cache (product decision)
- Deeper My Deals embed parity with Brand Alignment if needed

## Validation

```bash
node scripts/test-operator-alignment-snapshot-page.mjs
```

## Manual test

1. Start server (`npm start` or your usual command)  
2. Sign in on `http://localhost:8080/my-deals.html`  
3. Click **Operator Alignment Snapshot** on a deal row  
4. Confirm modal: review signal chips, 3 profile cards, workflow/gap summaries, future placeholder  
5. **Open full snapshot** — five profiles on standalone page  
6. **Print / Save as PDF** — opens standalone with print dialog  
