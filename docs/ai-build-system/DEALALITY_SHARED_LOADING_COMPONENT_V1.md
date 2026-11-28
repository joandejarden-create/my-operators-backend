# Dealality shared loading component V1

**Contract:** `DEALALITY_SHARED_LOADING_COMPONENT_V1`  
**Canonical markup:** `public/js/dealality-wave-loader.js`  
**Canonical styles:** `public/css/dealality-wave-loader.css`  
**Shared UX adapter:** `public/js/dealality-shared-loading-ux-v1.js`  
**HE adapter:** `public/js/he-loading-ux-v1.js` (delegates to shared UX)

## Presentation (founder-approved)

Animated Dealality Wave + Compact Black Rounded Container + Contextual Loading Message.

Only the message varies by product/view. Do not redesign the wave animation or invent a second loader system.

## When to show

Show only when an async request is pending or a user-triggered operation is processing.

Do **not** show when data is already in memory / cached, a tab switch uses cached content, or the view is immediately renderable.

## Lifecycle

Appear → remain while pending → disappear on READY / EMPTY / ERROR. No indefinite loaders. Prefer section toast over full-screen overlays.

## Products

| Product | Entry surfaces |
|---|---|
| GDI | Page corner toast, drawer detail, Executive Summary, Portfolio, Commercial Priorities |
| ADP | AI Demand Positioning loading toast |
| Brand Explorer | Brand directory / profile wave loaders |
| Hotel Explorer | Async sections across 11 active tabs (Guest Sentiment remains disabled) |

## Gates

```bash
npm run test:dealality-shared-loader-contract-v1
npm run test:dealality-shared-loader-contract-v1 -- --product he
pwsh -File scripts/Invoke-DealalitySharedLoaderValidate.ps1 -Product all
```

Machine gates: `DEALALITY_SHARED_LOADER_PARITY`, `DEALALITY_WAVE_ANIMATION_VALIDATED`, `DEALALITY_BLACK_TOAST_STYLE_VALIDATED`, `DEALALITY_LOADING_LIFECYCLE_VALIDATED`, `DEALALITY_NO_DUPLICATE_LOADER_IMPLEMENTATIONS`, `DEALALITY_NO_UNNECESSARY_LOADING`.

Browser-dependent checks return `NOT_TESTED` unless `--browser` is passed.
