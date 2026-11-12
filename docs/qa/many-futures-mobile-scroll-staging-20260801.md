# QA — Many Futures mobile open scroll (staging)

**Date:** 2026-08-01  
**Scope:** Mobile only (`max-width: 767px`) on homepage Features / Many Futures (`#many-futures`)  
**Staging:** https://mvp-deal-capture.webflow.io/  
**Production:** not published

## Problem

On mobile, tapping a Many Futures question relocates the answer workspace under that question. If a previous answer was open above the tap target, the layout collapse jumps the page so the selected question sits above the viewport — users have to scroll up before they can read by scrolling down.

## Fix

1. Companion script `dealality-old-home-many-futures-mobile-scroll.v20260801a.js`  
   - Path-gated to `/` and `/old-home`  
   - On click/keyboard activate of `.mf-q[data-q]`, after layout settles, scrolls the active question under sticky `#nav`
2. Boot guard `old-home-boot-guard.v20260801x` loads that script (applied site header script `oldhomebootguard01x`)
3. Source patch in `webflow-embeds/many-futures/many-futures.js` for the same behavior when the jsDelivr pin is rebuilt later

## Assets

| Asset | CDN |
| --- | --- |
| MF mobile scroll | `…/6a6e6f0c582749ce2c2c9de6_dealality-old-home-many-futures-mobile-scroll.v20260801a.js` |
| Boot guard 01x | `…/6a6e6f269f8471792251af4d_old-home-boot-guard.v20260801x.js` |

## Staging checks

1. Open staging on a phone or DevTools ≤767px.
2. Confirm header loads `old-home-boot-guard.v20260801x.js`.
3. Scroll to Features / Many Futures; open question 01, then tap a lower question (e.g. 05–09).
4. Expect: selected question title visible just under the sticky nav; content readable by scrolling down (no need to scroll up first).
5. Desktop (>767px): no forced scroll change on question select.
6. Production domains unchanged until an explicit publish.

## Automated wire-up (2026-08-01)

- Staging HTML includes `old-home-boot-guard.v20260801x.js`
- Boot guard loads `6a6e6f0c582749ce2c2c9de6_dealality-old-home-many-futures-mobile-scroll.v20260801a.js`
- Helper CDN returns 200 and contains `scrollQuestionIntoView` + `max-width: 767px` gate
- Production still on `old-home-boot-guard.v20260801v.js` (staging-only publish)
