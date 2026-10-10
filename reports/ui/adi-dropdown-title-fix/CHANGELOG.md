# ADI dropdown title truncation — changelog

## Files

| File | Change |
|---|---|
| `public/css/deal-workspace-shell.css` | `.filter-select`/`filter-input` line-height + min-height; `select.filter-select` 3rem height, zero vertical padding |
| `public/js/ai-demand-positioning/ai-demand-positioning.js` | Option + select `title` sync for full label |

## Intent
Fix vertical clipping of `#adpProperty` closed-state title; keep full string available via title attribute when width ellipsizes.
