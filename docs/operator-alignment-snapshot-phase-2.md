# Operator Alignment Snapshot — Phase 2 (Standalone Print Page)

**Date:** 2026-05-25  
**Status:** Implemented  
**Depends on:** [operator-alignment-snapshot-phase-1.md](./operator-alignment-snapshot-phase-1.md)

## What was implemented

A standalone, print-friendly **Operator Alignment Snapshot** page that loads profile-level data from the Phase 1 API and renders a single-scroll document (no page-flip book).

## Files created

| File | Purpose |
|------|---------|
| `public/operator-alignment-snapshot.html` | Standalone page shell, loading state, API fetch |
| `public/js/operator-alignment-snapshot.js` | Document renderer + print (clone to `#bas-print-host`) |
| `public/css/operator-alignment-snapshot.css` | Profile cards, alignment bands, print rules |
| `scripts/test-operator-alignment-snapshot-page.mjs` | Static validation (copy, print CSS, five profiles) |
| `docs/operator-alignment-snapshot-phase-2.md` | This document |

## Files modified

| File | Change |
|------|--------|
| `server.js` | HTML routes + cache headers for `operator-alignment-snapshot.html` |
| `server.upload-ready.js` | Same HTML routes |
| `public/css/snapshot-page-shell.css` | Full-page width for `.operator-alignment-snapshot` |
| `docs/operator-alignment-snapshot-implementation-checklist.md` | Phase 2 marked complete |

## Convention (matches Brand Alignment Snapshot)

| Brand Alignment | Operator Alignment |
|-----------------|-------------------|
| `public/brand-alignment-snapshot.html` | `public/operator-alignment-snapshot.html` |
| `public/js/brand-alignment-snapshot.js` | `public/js/operator-alignment-snapshot.js` |
| `public/css/brand-alignment-snapshot.css` | `public/css/operator-alignment-snapshot.css` (+ shared BAS CSS) |

OAS also loads:

- `/css/brand-alignment-snapshot.css` — toolbar, cover, print host, typography
- `/css/snapshot-page-shell.css` — full-page shell

## Page URL

| URL | Notes |
|-----|--------|
| `/operator-alignment-snapshot.html?dealId=recXXXXXXXX` | Primary |
| `/operator-alignment-snapshot?dealId=rec…` | Redirects to `.html` |
| `?print=1` | Auto-triggers print after load |

### Example browser URL

```
http://localhost:3000/operator-alignment-snapshot.html?dealId=recYOUR_DEAL_ID
```

Optional print:

```
http://localhost:3000/operator-alignment-snapshot.html?dealId=recYOUR_DEAL_ID&print=1
```

## API route used

```
GET /api/operator-alignment-snapshot/:dealId/profile
```

Fetched via `DealalityMemberstackAuth.fetchMyDealsApi` when available (same pattern as Brand Alignment Snapshot and Operator Capability Snapshot pages).

## Page sections

1. **Header / cover** — Title, subtitle, generated date, neutral disclaimer  
2. **Deal Context** — Grid of deal fields; missing → **Not provided**  
3. **Operator Review Signal** — High / Medium / Low / Insufficient Data + rationale + humanized relevance chips  
4. **Operator Profiles for Review** — Five profile cards with alignment band, signals, considerations, questions, gaps, workflow actions  
5. **Data Gaps** — Aggregated from API  
6. **Suggested Workflow Actions** — Aggregated from API (not “Recommended Next Steps”)  

## Print / PDF notes

- **Print / Save as PDF** toolbar button (same `data-bas-print` + `#bas-print-host` pattern as BAS/OCS)
- User should disable browser headers/footers and enable background graphics (toolbar tip)
- `@media print` in `operator-alignment-snapshot.css` — `page-break-inside: avoid` on profile cards
- Single continuous document (better for profile cards than two-page book flip)

## How this differs from Operator Capability Snapshot (OCS)

| | OCS | OAS (Phase 2) |
|---|-----|----------------|
| API | `GET /api/deals/:dealId/operator-capability-snapshot` | `GET /api/operator-alignment-snapshot/:dealId/profile` |
| Content | Deal capability themes | Operator **profile categories** |
| Layout | 2-page book + flip | Single scroll document |
| Operators | None named | None named (Phase 4 adds companies) |

## Phase 2 polish pass

**Date:** 2026-05-25

### Layout clipping fix

- Replaced reliance on Operator Capability Snapshot grid classes (`ocs-grid-2`) with OAS-owned `.oas-deal-grid` / `.oas-deal-kv`.
- Overrode BAS dark `.bas-content-page` styles so the narrative body uses a **light paper** background with readable ink (fixes left-edge clip and low-contrast labels in print).
- Set `overflow: visible` on the book inner viewport for OAS so print/PDF does not crop the Deal Context block.

### Humanized signal keys

- Added `humanizeSignalKey` / `SIGNAL_KEY_LABELS` in `public/js/operator-alignment-snapshot.js`.
- Operator Review Signal and profile cards show **chips** with owner-facing labels; unmapped keys fall back to title case from underscores.
- Removed `<code class="oas-code">` raw key rendering.

### Profile card hierarchy

- Hero block: prominent **display label** + **alignment band** + lead (best use case).
- Structured chips for matched / conditional / missing deal signals (parsed from API `matchedDealSignals` and explanation fragments).
- Section lists: Alignment signals, Review considerations, Questions, Data gaps, Suggested workflow actions.

### Workflow language

- `stripWorkflowActionPrefix` removes duplicated “Suggested workflow action:” inside bullets (API/fixture strings unchanged).

### Print / PDF improvements

- `@page` margins (12mm × 14mm).
- Cover `page-break-after: always`; deal context and review signal avoid awkward breaks; profile cards `avoid` where possible.
- Larger print title/band; `print-color-adjust: exact` on cover and badges.
- Tighter but readable list spacing; section titles in sentence case (not heavy uppercase).

### Remaining limitations

- Conditional/missing signal chips are parsed from explanation text when not present as arrays in the API payload.
- Cover page remains dark (BAS pattern); body pages are light for readability.
- Requires authenticated session for real deals (`myDealsDealAuth`).
- No My Deals launch button yet (Phase 3).
- No embed modal yet.
- Renderer is client-side only; no server-side HTML export.

## Known limitations (pre-polish baseline)

- Requires authenticated session for real deals (`myDealsDealAuth`)
- No My Deals launch button yet (Phase 3)
- No embed modal yet
- Renderer is client-side only; no server-side HTML export

## What remains for Phase 3

- My Deals action icon + modal (`data-action="operator-alignment"`)
- Link from deal workspace to standalone page with `dealId`
- Optional: embed mode (`?embed=1`) for modal host

## Validation

```bash
node scripts/validate-operator-profile-archetypes.mjs
node scripts/test-operator-alignment-snapshot-page.mjs
```

## Manual test checklist

1. Start server (`npm run dev` or your usual command)  
2. Sign in (Memberstack / session used by My Deals)  
3. Open `/operator-alignment-snapshot.html?dealId=rec…` for a deal you can access  
4. Confirm five profile cards and **Not provided** where fields are empty  
5. Use **Print / Save as PDF** and verify layout  
