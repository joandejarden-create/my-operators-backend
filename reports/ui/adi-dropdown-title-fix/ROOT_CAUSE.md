# ADI dropdown title truncation — root cause

## Affected control (traced)

| Item | Value |
|---|---|
| Page | `/owner-ai-demand.html` (route `/owner/ai-demand`) |
| Element | `#adpProperty` |
| Markup | `<select class="filter-select" id="adpProperty">` |
| Shared CSS | `public/css/deal-workspace-shell.css` |
| Option population | `public/js/ai-demand-positioning/ai-demand-positioning.js` → `loadProperties()` |

## Root cause

**Vertical clipping** of the closed-state selected label (ascenders/descenders cut), not ADI business logic.

Cause chain:

1. ADI page / shell inherit `line-height: 1.5` (body / aiv-page).
2. Native `<select class="filter-select">` inherited that line-height.
3. Combined with `padding: 10px 14px`, `font-size: 0.875rem`, and `box-sizing: border-box`, Windows UA select chrome clipped glyphs (e.g. tops of capitals; descender of “g” in Galicia).
4. A first pass (`line-height: normal` + `min-height: 2.75rem`) reduced but did not fully clear clipping when border-box height still fought vertical padding.

Horizontal truncation on narrow viewports is separate and expected for native selects; mitigated with `option.title` / `select.title`.

## Fix

1. Shared `.filter-select` / `.filter-input`: `line-height: normal`; `min-height: 2.75rem`.
2. `select.filter-select` specific: `height/min-height: 3rem`; vertical padding `0`; horizontal padding kept for chevron; `line-height: 1.35` so content fits inside border-box without glyph clip.
3. JS: full label on `option.title` + sync `select.title` on load/change for tooltip / accessible full title when horizontally ellipsized.

## Explicit non-changes

- No ADI scoring, periods, peers, share tokens, or report logic changes.
- No custom dropdown rewrite.
