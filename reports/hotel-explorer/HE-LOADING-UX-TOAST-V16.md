# HE Loading UX — Compact Dealality Toast (HE-QL-028 / V16)

**Date:** 2026-11-19  
**Stream:** Hotel Explorer (`ws-hotel-explorer`)  
**PR:** #463 OPEN  
**Standard:** `hotel-explorer-quality-standard-v1.17` · Lessons `v1.17` · **HE-QL-028**

## Problem

Contact Intelligence (and other HE async sections) rendered a full-width dark loading strip because `dealality-wave-loader--compact` forced `width: 100%` inside section hosts.

## Fix

| Layer | Change |
| --- | --- |
| `DealalityWaveLoader.toastHtml` | Compact centered toast (wave orb + toast chrome) |
| Compact CSS | `width: auto` / max 360px — never full-width strip |
| `he-loading-ux-v1.js` | Shared adapter: toast / wave / inline |
| `HeAsyncSection.waveCompact` | Prefers toast via `HeLoadingUx` |
| Contact Intelligence | `Loading Contacts…` centered toast; Surfe stays inline |
| Brand News / Sentiment | Same toast via workspace helper |
| OCN expand | Inline indicator |

## Browser evidence (Founder Preview)

| Scenario | Result |
| --- | --- |
| CI loading toast | Centered ~172px; message `Loading Contacts…` |
| News-style toast | Centered ~184px; not full-width |
| Surfe inline | Compact loader on card only (~218px); no toast host |
| Dismissal | Loading root gone; READY with 3 people; no stuck wave |

Screenshots:

- `phase4-founder-preview-screenshots/quality-v16-loading-toast-ci.png`
- `phase4-founder-preview-screenshots/quality-v16-loading-toast-news.png`
- `phase4-founder-preview-screenshots/quality-v16-loading-inline-surfe.png`

## Tests

```bash
npm run test:he-loading-ux-toast-v1
npm run test:he-async-section-loading-v1
npm run test:he-product-quality-gates-v1
npm run test:he-contact-intelligence-directory-ux-v1
```

## Safety

No Airtable apply · no paid research · no deploy · unrelated dirty work preserved.
