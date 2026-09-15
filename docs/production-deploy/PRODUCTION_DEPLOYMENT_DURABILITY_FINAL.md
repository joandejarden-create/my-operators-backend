# Production deployment durability — FINAL (FROZEN)

**Status:** `DEPLOYMENT_DURABILITY = FROZEN`  
**Date:** 2026-09-11

## Root cause

Railway production is a **single service**. Incomplete tree uploads replace the whole service and wipe peer product assets (Hotel Intelligence vs ADP).

## Resolution

1. **PR #40** merged — commit **`5590512`**
2. Coexistence assets + production gates live on **main**
3. Production deploy **`8066c423`** SUCCESS from that main tip

## Current production source of truth

| Field | Value |
|-------|--------|
| Branch | **main** |
| SHA | **`55905125eeba9f92fd234e9db035c07a15a66e8e`** |
| Railway deploy ID | **`8066c423-e7a2-4a3b-bc68-b945b8786436`** |
| PR | #40 MERGED |

## Canonical deploy command

```bash
npm run deploy:production
```

**Raw `railway up`:** NOT approved as normal workflow.

### Emergency hotfix

```bash
DEALALITY_ALLOW_HOTFIX_DEPLOY=1 DEALALITY_ALLOW_DIRTY_DEPLOY=1 npm run deploy:production
```

## Predeploy gates

- `npm run assert:production-assets`
- `npm run test:production-routes-local`
- `npm run test:production-critical-surfaces`
- `npm run test:hotel-explorer-share-drawer-visible` (4/4 visible)

## Postdeploy verified 2026-09-11

Golden Four 200 · Research Center CSS 200 · drawer visual 4/4 · ADP share 200 · Brand/Operator share 200

## Incomplete tree

Missing golden-demo → assert exits non-zero → deploy refuses.

## Freeze

No further deploy-architecture work unless regression. Main is the production source of truth.
