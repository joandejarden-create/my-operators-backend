# Railway static asset audit

How customer-facing HTML/JS/CSS reach production for `my-operators-backend`.

## Upload boundary

| Mechanism | Effect |
|-----------|--------|
| `railway up` | Whole working-tree replace (minus ignore) |
| `.railwayignore` | Excludes bulk `reports/*`, `data/*` (with ADP/BAI allowlists) |
| Not ignored | `public/`, CSS under `public/css/`, app JS, APIs, `server.js` |

Audited absences: no `Dockerfile`, no `.dockerignore`, no `Procfile`, no `railway.json`. Build uses **RAILPACK** from `railway.toml`.

## Runtime delivery

1. **`express.static('public')`** — URL `/js/…`, `/css/…`, `/hotel-*.html`, share HTML, etc. map to files under `public/`.
2. **Explicit `sendFile` routes in `server.js`** — product entrypoints that must still exist as files on disk after upload.
3. **API modules** — HI/ADP/Brand AI routes must be present in the uploaded tree; missing modules → 404/500, not a CDN miss.

## Wipe evidence (ADP-only upload)

Missing from production after incomplete upload (restored via hotfix deploy `2222ebdc`):

- Golden-demo HTML
- Research Center CSS
- `hotel-explorer.js`
- HI API surface

`.railwayignore` did **not** cause this — the upload tree simply lacked the files.

## Predeploy static checks

```bash
npm run assert:production-assets
npm run assert:hotel-explorer-share-assets
npm run test:production-routes-local
```

Manifest `postdeploy_smoke.static_paths` includes golden-demo, Research Center CSS, `hotel-explorer.js`, ADP share, Brand/Operator Explorer share, Brand AI page.

## Operator note

If a static path 404s in production after deploy, treat it as **deploy-tree incompleteness** first — not a Webflow or CDN problem. Confirm the file exists locally, is not ignored, and was included in the last `deploy:production` / `railway up`.
