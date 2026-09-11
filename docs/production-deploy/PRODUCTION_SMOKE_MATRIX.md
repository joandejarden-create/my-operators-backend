# Production smoke matrix

Base URL: https://my-operators-backend-production.up.railway.app

Use after every production deploy. Automate via `deploy:production` postdeploy smoke + critical-surface tests when possible.

## Static paths (must 200)

From manifest `postdeploy_smoke.static_paths`:

| Path | Surface |
|------|---------|
| `/hotel-intelligence-golden-demo.html` | HI |
| `/css/hotel-intelligence-research-center.css` | HI Research Center |
| `/js/hotel-explorer.js` | HI |
| `/owner-ai-demand-share.html` | ADP |
| `/brand-explorer-share.html` | Brand Explorer |
| `/operator-explorer-share.html` | Operator Explorer |
| `/ai-visibility-brand.html` | Brand AI |

Also confirm share HTML:

| Path | Surface |
|------|---------|
| `/hotel-explorer-share.html` | HI share |

## Golden Four HI (functional)

Hotel IDs:

- `recUNycnMwOVFX0hc`
- `recIwaP1etgx2g9nA`
- `recsYJb2R1jarPpK3`
- `recTYaiA4S6fR6ixx`

Query helper: `autopen=1&share=1&hexTab=ownership`

Check: golden-demo / share loads, drawer usable, Research Center CSS applied (not unstyled), research/dossier APIs respond (auth rules as designed — not 404 for missing module).

## API markers (existence, not full auth matrix)

| Area | Marker |
|------|--------|
| HI | `/api/hotel-intelligence/` research + dossiers |
| ADP | `/api/ai-demand-positioning/share/resolve` |
| Brand AI | `/api/ai-visibility/brand/share/resolve` |

## Local / CI commands

```bash
npm run test:production-routes-local
npm run test:production-critical-surfaces
npm run test:hotel-explorer-share-drawer-visible
npm run assert:production-assets
```

## Packet note (2026-09-11)

- Production HI restored and healthy on deploy `2222ebdc`.
- Full Golden Four / share-drawer visual re-runs are **not required to re-execute in this docs packet**; treat prior restore verification as PASS, or mark DEFERRED with that note (see completion report).
