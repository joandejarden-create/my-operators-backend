# Production deploy guard

Hard guards that make CLI deploy safe short-term. Raw unguarded `railway up` is the wipe vector.

## Canonical entrypoint

```bash
npm run deploy:production
npm run deploy:production:dry-run
```

Do **not** use raw `railway up` for production. Conceptual block:

```bash
npm run preflight:railway-raw
```

Emergency raw bypass only (discouraged; **skips asset gates**):

```bash
DEALALITY_ALLOW_RAW_RAILWAY_UP=1 railway up --detach
```

## Policy

| Rule | Enforcement |
|------|-------------|
| Normal deploy from `main` only | `deploy:production` fails on other branches |
| Hotfix from non-main | `DEALALITY_ALLOW_HOTFIX_DEPLOY=1` |
| Dirty working tree | refused unless `DEALALITY_ALLOW_DIRTY_DEPLOY=1` |
| Incomplete product tree | `assert:production-assets` fails before upload |
| ESM boot imports | `smoke:esm-imports` fails if critical modules cannot load |
| Postdeploy smoke | runs unless `DEALALITY_SKIP_POSTDEPLOY_SMOKE=1` (includes `/health`, 15s timeout) |

## What the wrapper runs (order)

1. Branch / dirty-tree guards
2. `smoke:esm-imports` (boot-critical ESM import check)
3. `assert:production-assets` (manifest `config/production-required-assets.json`)
4. Local route smoke (`test:production-routes-local` path inside wrapper)
5. `railway up` (unless dry-run)
6. Postdeploy smoke against https://my-operators-backend-production.up.railway.app (includes `/health`)
7. Deploy record under `reports/deployments/`

## Hotfix example

```bash
DEALALITY_ALLOW_HOTFIX_DEPLOY=1 DEALALITY_ALLOW_DIRTY_DEPLOY=1 npm run deploy:production
```

Asset gates still run. Hotfix does **not** excuse an HI-only or ADP-only tree.

## Recommendation

- **Short-term:** KEEP CLI WITH HARD GUARDS.
- **After PR #40 merges** and Railway GitHub deploy can target `main` safely: MOVE TO MAIN-DRIVEN auto-deploy; keep assert gates in CI/predeploy.
