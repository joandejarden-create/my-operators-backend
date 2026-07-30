# Many Futures — local prototype

Source of truth for the Dealality Old Home interactive section.

## Files

| File | Purpose |
|------|---------|
| `markup.html` | Semantic HTML fragment |
| `many-futures.css` | Scoped CSS (`#dealality-many-futures` only) |
| `many-futures.js` | Isolated interaction controller |
| `preview.html` | Self-contained local preview (intro + embed) |
| `index.html` | Dev harness that loads split files |
| `build.cjs` | Combines into `../many-futures-section.html` |
| `assets/` | Local images for preview |
| `screenshots/` | Phase 2 breakpoint captures |

## Asset URL configuration points

Replace these `data-mf-asset` image `src` values (in `markup.html`, then rebuild):

| `data-mf-asset` | Current Phase 2 source | Production intent |
|-----------------|------------------------|------------------|
| `hotel` | `assets/hotel-temp.jpg` (temporary Unsplash) | Approved Webflow Asset URL |
| `rebrand` | Brand Explorer CDN / local PNG | Existing CDN asset |
| `new-operator` | Operator Explorer CDN / local PNG | Existing CDN asset |
| `soft-brand` | Fee Estimator CDN / local PNG | Existing CDN asset |
| `independent` | Radar CDN / local PNG | Existing CDN asset |
| `branded-residences` | Opportunity Review CDN / local PNG | Existing CDN asset |

Production CDN bases (already wired by `build.cjs` for product shots):

```
https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/
```

## Preview

```bash
cd webflow-embeds/many-futures
python3 -m http.server 8765
# open http://127.0.0.1:8765/preview.html
node build.cjs   # refreshes ../many-futures-section.html
```

## Notes

- Do not insert into Webflow until Phase 3 approval.
- Hotel image is temporary and must be approved/replaced before Phase 3.
