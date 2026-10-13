# Hilton Development — External Share Links Handoff

**Date:** 2026-10-02  
**Environment:** localhost (not production-deployed)  
**QA:** Logged-out Playwright — all four pages `ok: true` (`LOGGED_OUT_QA.json`)

## External URLs (localhost)

Radar / Scout:
http://localhost:8080/radar-share.html?pack=mexico

Brand AI Positioning:
http://localhost:8080/brand-ai-visibility-share.html?share=baiparent.v1.…  
(full sealed URL in `LOCAL_SHARE_URLS.json` → `urls.brandAi`)

Brand Explorer / Hilton:
http://localhost:8080/brand-explorer-share.html?pack=hilton

Optional Hilton profile deep-link:
http://localhost:8080/brand-explorer-share.html?pack=hilton&id=receQkxgjlezsc1xg  
(Curio Collection by Hilton)

GDI:
http://localhost:8080/group-demand-intelligence-share.html?share=gdishare.v1.…  
(full sealed URL in `LOCAL_SHARE_URLS.json` → `urls.gdi`)

## What was reused

- Existing `*-share.html` + signed capability pattern (no parallel `/share/[module]/[slug]` router)
- Brand AI parent token id `sht_baip_5de428d7d284054b8756cf0a` (re-sealed for local DEV secret)
- GDI Hilton Times Square hotel `rec35fExUxCClpOP6`
- Prior GDI ACTIVE token `gdisht_150bd6bba6e32716d3a84999` **preserved** (not revoked)
- Brand Explorer pack pattern (Choice / IHG) extended with Hilton

## What was added

| File | Purpose |
|------|---------|
| `public/radar-share.html` | Scout Market Map share shell (Mexico pack) |
| `public/css/radar-share-shell.css` | Share chrome styling |
| `public/js/scout-market-map.js` | `share=1` / `embed=1` read-only lock (no watchlist writes; country lock) |
| `public/app/scout-market-map.html` | `noindex,nofollow` |
| `public/brand-explorer-share.html` | `SHARE_PACKS.hilton` |
| `server.js` | `/radar-share` aliases |
| `scripts/issue-bai-share-capability-v1.mjs` | Local/prod BAI parent issuer |
| `scripts/gdi-issue-share.mjs` | `--hotelId=` support |
| `scripts/qa-hilton-development-share-logged-out.mjs` | Logged-out QA + screenshots |

## Tokens

| Product | Token id | Notes |
|---------|----------|--------|
| Brand AI | `sht_baip_5de428d7d284054b8756cf0a` | Existing Hilton parent; local DEV re-seal |
| GDI (new local) | `gdisht_800341812ba2709b600b2041` | Hilton TS read-only for localhost handoff |
| GDI (prior) | `gdisht_150bd6bba6e32716d3a84999` | Still ACTIVE; unchanged |

## Screenshots

`reports/hilton-development-share/2026-10-02/screenshots/`

- `radar-desktop.png` / `radar-mobile.png`
- `brand-ai-desktop.png` / `brand-ai-mobile.png`
- `brand-explorer-desktop.png` / `brand-explorer-mobile.png`
- `gdi-desktop.png` / `gdi-mobile.png`

## Production deploy (not run)

Do **not** send local DEV-signed BAI/GDI URLs to Hilton — they will not verify on Railway.

1. Deploy static/share code:
   - `public/radar-share.html`, `public/css/radar-share-shell.css`
   - `public/js/scout-market-map.js`, `public/app/scout-market-map.html`
   - `public/brand-explorer-share.html`
   - `server.js` radar-share routes
2. On production (with production secrets):
   ```bash
   node scripts/issue-bai-share-capability-v1.mjs --parent=hilton --token-id=sht_baip_5de428d7d284054b8756cf0a --label=hilton-development-share:prod
   node scripts/gdi-issue-share.mjs --hotelId=rec35fExUxCClpOP6 --label=hilton-development-share:HiltonTS:prod
   ```
   Or re-seal against existing `gdisht_150bd6…` if `tokenId` support is wired for GDI issue.
3. Smoke production hosts:
   - `https://my-operators-backend-production.up.railway.app/radar-share.html?pack=mexico`
   - `…/brand-explorer-share.html?pack=hilton`
   - sealed Brand AI + GDI URLs from step 2
4. Confirm logged-out / incognito: no Memberstack redirect, read-only chrome, Mexico map loads, Hilton pack shows ~13 brands.

## Security notes

- Auth not globally weakened; product-scoped share only for BAI/GDI
- Radar V1 uses curated pack URL (same posture as Brand Explorer packs); signed `radarshare.v1.` deferred
- Scout share mode disables watchlist POST/PATCH UX
- `noindex,nofollow` on share shells
- No automatic production deploy performed
