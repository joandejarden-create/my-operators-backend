# HE Independent Data Loading & Canonical Wave — Founder Preview v12

**Date:** 2026-11-19  
**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Surface:** `http://127.0.0.1:8091/hotel-explorer-preview.html` · Phase 3 Proposed  
**PR:** #463  
**Lesson:** HE-QL-024 · Quality Standard v1.13 §9e  
**Airtable / paid / deploy:** none

## Verdict

Independent section loading with the canonical Dealality wave is validated in Founder Preview. Brand & Operator overview remains interactive while Market Alerts news loads or fails; Guest Sentiment failures stay isolated; empty news stays hidden; Surfe enrichment uses a per-person compact wave.

## Architecture

| Piece | Role |
|---|---|
| `public/js/dealality-wave-loader.js` + CSS | Canonical wave; `compactHtml` / inline / reduced-motion |
| `public/js/he-async-section-v1.js` | Controllers: IDLE/LOADING/READY/EMPTY/ERROR/STALE, AbortController, cache TTL, generation guard, `clearCache` |
| Brand / Operator | Overview paints immediately; `newsAsync` + `sentimentAsync` mount independently |
| Sources | Market Alerts evidence via same GET-only news path (`sourcesMaAsync`) |
| Contact Intelligence | Compact wave for tab load; Surfe enrich wave only on the person card |
| Ownership & Control Network | Compact wave on expand / network fetch — does not replace ownership summary |

## Founder validation matrix

| Check | Result | Evidence |
|---|---|---|
| Brand overview renders while News still loading | PASS | Compact wave “Loading hotel news… / Market Alerts” with Franchise + Operator + Sentiment still visible |
| Slow Market Alerts isolated | PASS | Simulated 8s delay via fetch patch; siblings unaffected |
| Guest Sentiment provider failure isolated | PASS | User-facing “temporarily unavailable” + Retry; no stack/JSON; Brand overview intact |
| Cached failure → STALE freshness | PASS | Fail-with-cache showed “Showing last available sentiment while refresh continues.” |
| Empty News hidden | PASS | Bethesda has no qualifying Market Alerts articles → news host `hidden` |
| Canonical wave only | PASS | `.dealality-wave-loader` compact; no alternate spinner system |
| Hotel switch cancels obsolete work | PASS (unit) | `test:he-async-section-loading-v1` generation/abort coverage |
| Surfe enrich does not block tab | PASS | Per-person wave “Looking up contact details… / Surfe on demand”; other contacts remain |
| Ownership / Contacts responsive | PASS | Tabs switch while Brand async in flight; OCN + Contacts render |
| No raw errors / diagnostics | PASS | No TypeError / Simulated / stack in DOM |
| No GET writes / paid calls | PASS | Existing route headers + quality gates |

## Screenshots

`reports/hotel-explorer/phase4-quality/phase4-founder-preview-screenshots/`

- `quality-v12-async-brand-loading.png` — News LOADING wave; Brand + Sentiment READY  
- `quality-v12-async-brand-ready.png` — Overview + Sentiment READY; News EMPTY (hidden)  
- `quality-v12-async-news-empty.png` — Same empty-news posture  
- `quality-v12-async-sentiment-error.png` — Sentiment ERROR + Retry; Brand READY  
- `quality-v12-async-contacts-ready.png` — Contact Intelligence READY  

Also under `reports/hotel-explorer/async-loading-v12/`.

## Tests

```bash
npm run test:he-async-section-loading-v1
npm run test:he-product-quality-gates-v1
```

## Persistence

`PROTOTYPE` for loading UX only — no Airtable schema or production writes.
