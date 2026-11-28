# Hotel Explorer — Global Background Consistency Acceptance (V1)

**Date:** 2026-11-28  
**Branch:** `feat/he-global-background-consistency-v1`  
**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Host:** Opportunity Radar popup on `:8080` (temporary CSS staged on `ws-gdi` for live verify)

## Root cause

Nested panel/list/chip surfaces used translucent `rgba(8,15,37,α)` fills that muddied over `--he-section` cards and looked like patches/seams. PR #625 fixed shell canvas continuity but not the nested inventory.

## Fix

1. Expanded `hotel-explorer-theme-tokens-v1.css` (cache `?v=3`) — solid `--he-table` / `--he-section` for nested HDP/HIW/OI/HCI surfaces.
2. Replaced source `rgba(8,15,37,*)` panel backgrounds in HE module CSS with canonical tokens.
3. Removed `#0b1228` org-profile gradient.
4. Gate: `npm run test:he-global-background-consistency-v1` (+ HE50 wiring).

## Browser evidence (:8080)

| Gate | Result |
| --- | --- |
| `HE_GLOBAL_BACKGROUND_CONSISTENCY` | **PASS** (0 translucent canvas fills on all 11 tabs) |
| `HE_ELEVEN_TAB_THEME_PARITY` | **PASS** |
| `HE_NO_UNINTENDED_BACKGROUND_SEAMS` | **PASS** |
| `HE_SCROLL_VISUAL_STABILITY` | **PASS** (scroll top/mid/bottom = `rgb(8,15,37)`) |
| `HE_CANONICAL_BROWSER_VALIDATED` | **PASS** |

Tokens computed: `--he-canvas #080f25`, `--he-section #101935`, `--he-table #0d152c`.  
Theme stylesheet: `hotel-explorer-theme-tokens-v1.css?v=3` (loads last).

Approved non-navy accents (map dots, status chips, purple hover tints) retained.

## Regression

```bash
npm run test:he-unified-theme-v1
npm run test:he-global-background-consistency-v1
# with live :8080:
npm run test:he-global-background-consistency-v1 -- --require-browser
```

`FULL_HE_E2E_VALIDATED=NO`
