# Royal Palm / Product Popup Visual Quality — Completion Report

**Contract:** `BE_PRODUCT_POPUP_VISUAL_QUALITY_V1`  
**Lesson:** `BE-QL-PRODUCT-POPUP-VISUAL-001`  
**Airtable writes:** 0  
**Curio / Ascend:** NOT_APPROVED  

## Founder defects (8080 product popup)

Observed at Footprint & Growth → Royal Palm Galápagos (above Recent Momentum):

| ID | Defect | Status |
|----|--------|--------|
| A | Travel Weekly masthead as hotel photo | **FIXED** |
| B | Half-empty horizontal space (orphan last card) | **FIXED** |
| C | Long ALL-CAPS yellow description | **FIXED** |
| D | Parity PASS despite visual defects | **GATE ADDED** |

## Exact root cause

1. **Popup mount path:** `openBrandEducationPopupForBrand` called `mountIntoRoot(popRoot, dataB.brand)` with raw Airtable CURRENT, ignoring founder **Proposed Update**. Page `#beAtelierRoot` could show Hilton aerial while the visible **product popup** still showed the Travel Weekly graphic via Airtable CDN.
2. **Integrity filename gate:** Prior fix required `-tw.jpg` markers. Airtable CDN URLs strip original filenames, so `isUnsuitablePropertyDisplayImage` returned suitable=false and verified Royal Palm replacement never applied to CURRENT/popup payloads.
3. **Typography:** Five-block body parser treated a full prose paragraph as `property-example-card__scenario`, and CSS forced `text-transform: uppercase` + yellow accent.
4. **Layout:** Two-column grid left the odd last card (3rd/5th) alone at half width with vacant right column.

## Fixes

| Layer | Change |
|-------|--------|
| Integrity (Node + browser) | Verified identity replacements always apply by title; CDN regression covered |
| Atelier renderer | `resolvePropertyDisplayImage`; short-scenario heuristic; caption from verified row |
| Product popup | Proposed Update uses `buildCumulativeProposedBrandAsync`; `finishMount` remounts popup root |
| CSS | Scenario `text-transform: none`; single-card + odd-last-child span with max-width |
| Gate | `npm run test:be-product-popup-visual-quality-v1` + lesson registry |

## Corrected image

- **URL:** Hilton Stories aerial courtesy Royal Palm Galápagos  
  `…/Royal-Palm-Galapagos-Curio-Collection-by-Hilton_Aerial_Courtesy-of-Royal-Palm-Galapagos-Curio-Collection-by-Hilton.jpg`
- **Caption:** `Royal Palm Galápagos \| Hotel & Guest Villas`
- Travel Weekly retained only as research citation in body when present (not as display image)

## Browser evidence (8080)

| Shot | File |
|------|------|
| BEFORE | `screenshots/01-BEFORE-popup-royal-palm-footprint.png` |
| AFTER (image + type) | `screenshots/02-AFTER-popup-royal-palm-footprint.png` |
| AFTER (layout) | `screenshots/03-AFTER-popup-royal-palm-layout-fixed.png` |

Verified post-fix (popup Footprint):

- `srcHost=stories-editor.hilton.com`
- `scenarioTransform=none`
- Teaser sentence-case
- `gridColumn=1 / -1` for orphan last card

## Candidate / commit notes

- Staged candidate remains `phase2h-component-parity` (registry). Footprint openings primarily come from CURRENT Airtable presentation; integrity runs on cumulative assembly and at render time.
- No Airtable writeback in this repair.

## Remaining

- Full ten-tab visual sweep screenshots for Ascend should be captured on founder session after merge to 8080 tip.
- Curio/Ascend remain unpublished / NOT_APPROVED.
