# Many Futures — local prototype

Interactive “One Hotel. Many Futures.” section for Dealality Old Home (`/old-home`), above `#platform-features`.

## Phase status

| Phase | Status |
|---|---|
| 1 Audit | Complete |
| 2 Local prototype | Complete |
| **2.5 Refinement** | **Ready for review** — see `PHASE-2.5-REVIEW.md` |
| 3 Webflow insert | Blocked until 2.5 approval |
| 4 Publish QA | Not started |

## Source layout

```
webflow-embeds/many-futures/
  markup.html          # Embed markup (readable)
  many-futures.css     # Scoped CSS
  many-futures.js      # Interaction
  index.html           # Dev shell (fetch markup)
  preview.html         # Inlined local preview
  preview-eyebrow-b.html
  build.cjs            # Minified production embed
  PHASE-2.5-REVIEW.md  # Deliverables + audits
  assets/              # Hotel temp + source screens + crops/
  screenshots/         # Breakpoint captures
  scripts/generate-crops.py
webflow-embeds/many-futures-section.html  # Production embed (~33.5k chars)
```

## Commands

```bash
# Local preview
python3 -m http.server 8765 --bind 127.0.0.1
# open http://127.0.0.1:8765/preview.html

# Rebuild production embed
node build.cjs

# Regenerate curated crops
python3 scripts/generate-crops.py
```

## Asset notes

- Curated crops are presentation frames from real Dealality screens — not invented dashboards.
- Hotel image is temporary and marked in UI; proposed final is `assets/hotel-proposed-final.jpg`.
- Phase 3 must upload crops + approved hotel to Webflow CDN and update `build.cjs` asset map.
