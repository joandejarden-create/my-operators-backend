# Harbour House product marketing assets

Publication-ready stills and short product videos for the Dealality public website.

## Demo narrative

**Harbour House Hotel** · Cartagena, Colombia · 118-key independent upscale hotel  
Badge: **Illustrative Opportunity**  
All brands, operators, contacts, proposals, and statuses are fictional.

## Layout

| Folder | Contents |
|--------|----------|
| `scenes/` | Capture HTML scenes + stage controller (not for public embed) |
| `stills/` | PNG stills (full 1600×1000, wide 1600×800, feature 1200×900 @2x) |
| `video/` | MP4 (H.264) + WebM (VP9) clips |
| `posters/` | Poster PNG per clip |
| `source/` | Internal GIF previews only (not primary web format) |

## Regenerate

```bash
# Serve public/ (product server cwd guard may block npm start in this environment)
python3 -m http.server 8080 --directory public

# Capture / re-capture
node scripts/capture-harbour-house-product-assets.mjs --base=http://127.0.0.1:8080

# Rebuild manifest + ASSET-REPORT from files on disk
node scripts/rebuild-harbour-house-asset-manifest.mjs
```

## Implementation metadata

- `manifest.json` — full inventory, durations, sizes, final selection
- `website-placement.json` — section, caption, alt, playback recommendations
- `ASSET-REPORT.md` — human-readable capture report

Default video behaviour: muted, playsinline, loop, preload=metadata, poster, play near viewport, pause offscreen, poster-only for `prefers-reduced-motion`.
