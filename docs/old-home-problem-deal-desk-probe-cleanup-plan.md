# Deal Desk — temporary Designer probe cleanup plan

**Date:** 2026-07-31  
**Page:** Old Home `/old-home` (`68108c2a063eeb5d1bd7ae90`)  
**Element:** HtmlEmbed `#oh-deal-desk-embed`  
**ID:** `{ component: "68108c2a063eeb5d1bd7ae90", element: "a64ef2f7-2f5f-ab92-9711-5f43f9eeb3fa" }`  
**Setting key:** `code`

## Exact probe content (identified)

During the interrupted polish-v2 Designer push, a connectivity probe was written via `set_settings` on the HtmlEmbed `code` field. Confirmed applied content:

```html
<!-- polish-v2 probe -->
<link id="oh-deal-desk" rel="stylesheet" href="https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6bdf56570ac7789fe12096_oh-deal-desk-polish-v2.css">
<div class="dealality-problem-desk" data-visual="polish-v2">probe</div>
```

This **replaced** the prior Phase A Deal Desk storyboard markup in the embed. It is temporary test markup, not the cinematic-v1 composition.

## What must not be touched

- Native Webflow eyebrow / headline / lead / chapter markers / SR summary
- Unrelated sections outside `#about`
- Root homepage `/`
- Site footer / Problem runtime disable markers
- Testimonials `#oh-tt` head styles

## Clean replacement plan (when approved to push)

1. **Verify live embed** — `get_settings` on `#oh-deal-desk-embed` `code`; confirm probe (or note if already restored).
2. **Backup** — write current `code` value to `docs/old-home-problem-deal-desk-pre-cinematic-embed-backup.json` before overwrite.
3. **Build Designer-safe embed** from cinematic-v1:
   - HTML: `public/marketing/old-home-problem-deal-desk.v1.html`
   - CSS: scoped `public/marketing/old-home-problem-deal-desk.v1.css` inlined as `<style id="oh-deal-desk">` **inside** the HtmlEmbed (Designer canvas does not reliably apply page-head `<link>` to HtmlEmbed)
   - Hotel image: CDN JPEG already uploaded  
     `https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6bde85c014ee4e80e65c24_deal-desk-coastal-hotel-480.jpg`
4. **`set_settings`** only on HtmlEmbed `code` with the built cinematic embed (no Phase B JS load).
5. **Head** — keep `#oh-tt`; optional CDN CSS link as Preview backup only; do not remove testimonials styles.
6. **Verify** — embed contains `data-visual="cinematic-v1"`, all six `data-story-state` values work via attribute, no `<script>` autoplay, no `@keyframes`, no probe text.
7. **Do not publish** until separately approved.

## Recovery if cinematic push fails

- Restore Phase A backup: `docs/old-home-problem-deal-desk-phaseA-backup-20260730.json`
- Or re-apply the pre-cinematic embed backup from step 2
- Prior polish-v2 CSS CDN remains available at asset `6a6bdf56570ac7789fe12096` if needed as interim

## Status

- **Local cinematic-v1:** ready for review  
- **Webflow push:** **complete (2026-07-31)** — probe removed; cinematic-v1 hybrid embed applied to `#oh-deal-desk-embed`  
  - CSS method: in-embed `<style id="oh-deal-desk">@import url(CDN cinematic-v1.css)</style>`  
  - CDN CSS asset: `6a6c3db76db7f7c28a1f6fe0` (`oh-deal-desk-cinematic-v1.css`)  
  - Hotel CDN: `6a6bde85c014ee4e80e65c24`  
  - Pre-push probe backup: `docs/old-home-problem-deal-desk-pre-cinematic-embed-backup-20260731.json`  
- **No timed animation / no publish** in this cleanup plan
