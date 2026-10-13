# Property dropdown normalization — changelog

## Code
- `lib/group-demand-intelligence/repository.js` — `locationLine` falls back to human-readable `country` when `state` missing; reads top-level `config.city`
- `public/css/group-demand-intelligence.css` — `.gdi-page select.filter-select` matches shell height/padding/line-height (shared)

## Result
- W Rome: `W Rome — Rome` → `W Rome — Rome, Italy`
- YOTEL: `YOTEL Geneva Lake — Founex` → `YOTEL Geneva Lake — Founex, Switzerland` (same shared fix)

## Non-changes
- No hotel identity changes
- No ADP / share-token changes
