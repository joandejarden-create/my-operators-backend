# DealSignal Opportunities UI Cleanup V1 — Founder Report

**Branch:** `feat/dealsignal-opportunities-ui-filters-logos-v1`  
**Local verify:** `http://localhost:8080/deal-opportunities.html` (served from this worktree with `DEALALITY_CANONICAL_CHECKOUT=1` + `DEALALITY_ALLOW_WORKTREE_SERVER=1`)  
**CUSTOMER_BLOCKED** preserved. No Airtable schema/research writes.

## A. Root cause of global filter options

Dropdown populators (`populateBrandFilter` / `populateCountryFilter` / `populateDecisionWindowFilter`) iterated **`state.opps` (all rows)**. Tab clicks only called `render()` and never rebuilt options. Market/Timing stayed static HTML unions.

## B. Dynamic filter implementation

- `public/js/deal-opportunities-tab-filters-v1.js` — tab universe + option derivation.
- `repopulateFiltersForTab()` rebuilds Market, Country, Brand, Timing, Decision Window from the active tab’s eligible records; invalid selections reset to `all`; chain-scale selection resets when absent from the tab universe.
- Tab click, load, and Reset View all call repopulate.

## C. Tab-by-tab option verification (:8080)

| Tab | Country sample | Notes |
|---|---|---|
| Qualified | Bahamas, India, Trinidad and Tobago, United States | No Mexico/Colombia |
| Emerging & Watchlist | Includes Mexico, Colombia, Brazil, … | Broader watch universe |
| Brand Conversion / Management | Qualified ∩ kind BCT/MCT | Membership preserved |
| All Intelligence | Full population | |

## D. Root cause of missing logos

1. UI mapped logos to **`/api/brand-library/logo/:id`**, but **the route did not exist (404)**.
2. Fallback used **`onerror="this.remove()"`** → empty white square.
3. Direct Airtable CDN URLs from `/api/brand-library/brands` were **short-lived signed links**; browser `force-cache` kept expired tokens → load failure → blank squares.

## E. Brand logo mapping / asset recovery

- Added `GET /api/brand-library/logo/:recordId` (`api/brand-library-logo.js`) — fresh Airtable Logo attachment stream (same pattern as Brand Explorer presentation images).
- `deal-opportunities-brand-logos-v1.js` prefers same-origin proxy when Brand Basics `id` + logo exist; aliases + parenthetical stripping; portfolio multi-brand → neutral fallback (no arbitrary flag).
- Independent → dark glyph; unknown → gray fallback glyph (never blank white).

## F. Before / after screenshots

- After: `qualified-logos-after.png` (Qualified, logos via proxy for Courtyard/Hilton/Kimpton; Independent/portfolio/unknown use glyphs).
- Sample streamed assets: `courtyard-logo.png`, `hilton-logo.png`.

## G. Qualified eligibility compatibility

Depends on merged PR **#563** (`10f53941f`). Live Qualified count remains **7**; AC Hotel Cartagena appears under Watch with **current AC** logo (not former Holiday Inn as current).

## H. Counts / sorting / CSV

- Showing X of Y uses tab pool; chain-scale legend uses tab + filters (`skipChainScale` for counts).
- Local CSV export downloads **current `selection()`** rows (filtered/sorted); still CUSTOMER_BLOCKED local-only.

## I. Browser verification on :8080

Verified on founder port after restarting this worktree’s `server.js`. Filters repopulate on tab switch; logo proxy returns `200 image/png`; Courtyard/Hilton/Kimpton images load with `naturalWidth > 0`.

## J. Tests / CI

`npm run test:deal-opportunities-ui-filters-logos-v1` — pass locally. CI via PR.

## K. PR / merge status

Opened for Integration Controller merge (product chat does not merge).

## L. Remaining missing brand assets

- **Four Seasons** — not in Brand Library Active logo set → graceful fallback.
- **Voir Beach / unaffiliated development** — no verified brand logo → fallback.
- **AHIP multi-brand portfolio** — intentional portfolio glyph (no single-flag pick).
- Add Four Seasons (and other luxury) Brand Basics Logo attachments when available.
