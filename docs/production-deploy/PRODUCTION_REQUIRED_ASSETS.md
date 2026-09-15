# Production required assets

Source of truth: `config/production-required-assets.json` (`production-required-assets-v1`, updated 2026-09-11).

Purpose: hard gate so Railway CLI uploads cannot ship ADP-only or HI-only trees that wipe peer surfaces.

## Production base URL

https://my-operators-backend-production.up.railway.app

## Surfaces (summary)

| Surface | Role |
|---------|------|
| `HOTEL_INTELLIGENCE` | Hotel Explorer / HI share + golden-demo + Research Center assets/APIs |
| `ADP` | Owner AI Demand share + API + ADP share registry token file |
| `BRAND_EXPLORER` | Brand Explorer share |
| `OPERATOR_EXPLORER` | Operator Explorer share |
| `BRAND_AI` | Brand AI Visibility page + share resolve |
| `PLATFORM_CORE` | `server.js`, `package.json`, `railway.toml`, `.railwayignore`, `public/404.html` |

HI note in manifest: historically **HOTFIX_ONLY** until PR #40 merges onto `main`.

## Golden Four hotel IDs (smoke)

- `recUNycnMwOVFX0hc`
- `recIwaP1etgx2g9nA`
- `recsYJb2R1jarPpK3`
- `recTYaiA4S6fR6ixx`

Postdeploy query helper: `autopen=1&share=1&hexTab=ownership`

## HI required files (must exist on disk before upload)

- `public/hotel-intelligence-golden-demo.html`
- `public/hotel-explorer-share.html`
- `public/js/hotel-explorer.js`
- `public/js/hotel-intelligence-golden-demo-route.js`
- `public/js/hotel-intelligence-research-center.js`
- `public/js/hotel-intelligence-dossier.js`
- `public/js/hotel-detail-panel.js`
- `public/css/hotel-explorer.css`
- `public/css/hotel-intelligence-research-center.css`
- `public/css/hotel-intelligence-dossier.css`
- `public/css/hotel-detail-panel.css`
- `api/hotel-intelligence-research.js`
- `api/hotel-intelligence-dossier.js`
- `lib/hotel-intelligence/golden-demo/mexico-radar-fixture-fallback.js`

Critical static paths that 404'd during the ADP wipe:

- `/hotel-intelligence-golden-demo.html`
- `/css/hotel-intelligence-research-center.css`
- `/js/hotel-explorer.js`
- HI APIs under `/api/hotel-intelligence/…`

## Gate commands

```bash
npm run assert:production-assets
npm run assert:production-assets:main   # --check-main
npm run assert:hotel-explorer-share-assets   # alias / HI-focused assert
npm run test:production-routes-local
npm run test:production-critical-surfaces
npm run test:hotel-explorer-share-drawer-visible
```

Deploy policy fields in manifest:

- Normal branch: `main`
- Hotfix override: `DEALALITY_ALLOW_HOTFIX_DEPLOY=1`
- Dirty tree override: `DEALALITY_ALLOW_DIRTY_DEPLOY=1`
- Skip postdeploy smoke: `DEALALITY_SKIP_POSTDEPLOY_SMOKE=1`
- Canonical command: `npm run deploy:production`
