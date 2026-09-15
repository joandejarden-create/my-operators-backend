#!/usr/bin/env node
/**
 * Guard for accidental raw `railway up`.
 * Prefer: npm run deploy:production
 *
 * Bypass only with: DEALALITY_ALLOW_RAW_RAILWAY_UP=1
 */
if (process.env.DEALALITY_ALLOW_RAW_RAILWAY_UP === "1") {
  console.warn(
    "WARN raw railway up allowed via DEALALITY_ALLOW_RAW_RAILWAY_UP=1 — asset gates NOT run"
  );
  process.exit(0);
}

console.error(`
BLOCKED: raw Railway production upload is not the Dealality deploy path.

Use:
  npm run deploy:production

This runs assert:production-assets, local route smoke, records SHA, then railway up,
then postdeploy smoke.

Emergency hotfix from a non-main branch:
  DEALALITY_ALLOW_HOTFIX_DEPLOY=1 DEALALITY_ALLOW_DIRTY_DEPLOY=1 npm run deploy:production

To force a raw CLI upload (discouraged):
  DEALALITY_ALLOW_RAW_RAILWAY_UP=1 railway up --detach
`);
process.exit(1);
