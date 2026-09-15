# Production branch durability

## Failure mode (root cause of wipe)

Railway CLI `railway up` uploads the **entire working tree** (minus `.railwayignore`) and **replaces the service**.

Sequence that broke Golden Four HI:

1. An ADP-focused tree was uploaded.
2. HI public/API files were absent from that tree.
3. Production lost HI entrypoints → **404** for:
   - `/hotel-intelligence-golden-demo.html`
   - Research Center CSS
   - `/js/hotel-explorer.js`
   - HI APIs

Symmetric risk: an HI-only hotfix upload can wipe ADP (including share-registry / Casas state). Audited note: after hotfix redeploy `2222ebdc`, **Casas ADP registry may have been wiped** even though HI is current.

## Why `main` is not yet a safe sole source

- `origin/main` tip `f509b87` (ADP PR #43) does **not** include HI golden-demo.
- Production durability therefore cannot yet be “always ship `main`” without first merging PR #40.

## Durability rules

1. **Never upload a product-incomplete tree.**
2. **Normal deploys: from `main` only** after main contains all required surfaces.
3. **Hotfix:** `DEALALITY_ALLOW_HOTFIX_DEPLOY=1` (still must pass asset gates).
4. **Dirty tree:** `DEALALITY_ALLOW_DIRTY_DEPLOY=1` only for emergency.
5. **Raw `railway up`:** blocked conceptually via `npm run preflight:railway-raw` + docs; prefer `npm run deploy:production`.

## Canonical protection

- Manifest: `config/production-required-assets.json`
- Preflight: `npm run assert:production-assets`
- Wrapper: `npm run deploy:production` / `deploy:production:dry-run`

## Target end state

After PR #40 merges and durability scripts are on main: **main-driven Railway GitHub deploy**. Until then: **KEEP CLI WITH HARD GUARDS**.
