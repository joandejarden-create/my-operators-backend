# Production deploy runbook

Target service: `my-operators-backend` on Railway project `serene-reverence`, environment `production`.  
URL: https://my-operators-backend-production.up.railway.app

## Before every deploy

1. Confirm working tree contains **all** required surfaces (`HOTEL_INTELLIGENCE`, `ADP`, Explorers, Brand AI, platform core).
2. Prefer a clean checkout of the intended deploy SHA.
3. Run:

```bash
npm run assert:production-assets
npm run test:production-routes-local
npm run test:production-critical-surfaces
```

Optional HI extras:

```bash
npm run assert:hotel-explorer-share-assets
npm run test:hotel-explorer-share-drawer-visible
```

4. Dry-run:

```bash
npm run deploy:production:dry-run
```

## Normal deploy (from `main`)

Preconditions:

- Branch is `main`
- Tree clean (or intentional dirty override)
- PR #40 merged so HI is on main (until then, normal path is blocked by incompleteness / policy)

```bash
npm run deploy:production
```

## Emergency hotfix (non-main)

Used for audited restore path (deploy `2222ebdc` class):

```bash
DEALALITY_ALLOW_HOTFIX_DEPLOY=1 DEALALITY_ALLOW_DIRTY_DEPLOY=1 npm run deploy:production
```

Still must pass asset assert. Expect possible wipe of surfaces missing from the hotfix tree (e.g. ADP Casas registry risk when HI hotfix re-uploaded).

## Forbidden

```bash
railway up
```

Use only after understanding wipe risk, and only with `DEALALITY_ALLOW_RAW_RAILWAY_UP=1` if forcing CLI outside the wrapper. Prefer fixing the tree and using `deploy:production`.

## After deploy

1. Hit postdeploy static paths (see `PRODUCTION_SMOKE_MATRIX.md`).
2. Spot-check Golden Four HI share + one ADP share URL.
3. Confirm deployment ID / SHA recorded under `reports/deployments/`.
4. If hotfix: open follow-up to restore any wiped peer registry/data allowlists.

## Migration note

When Railway GitHub deploy targets `main` safely post–PR #40 + durability packet: switch runbook primary path to merge-to-main; keep assert gates in CI; retire ad-hoc CLI as the default.
