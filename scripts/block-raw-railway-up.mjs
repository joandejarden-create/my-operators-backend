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

Use the shared release coordinator:
  .\\scripts\\Run-Dealality-Production-Release.ps1 -Product <ADP|AVA|GDI> -CommitSha <sha> -Authorize

Or (after acquiring the release lock):
  npm run deploy:production

Raw railway up bypasses asset gates, the shared release lock, and product certification.
Do not use it for ADP/AVA/GDI production cutovers.

Emergency hotfix from a non-main branch (still requires release lock unless bypassed):
  DEALALITY_ALLOW_HOTFIX_DEPLOY=1 DEALALITY_ALLOW_DIRTY_DEPLOY=1 npm run deploy:production

To force a raw CLI upload (discouraged; founder auth only):
  DEALALITY_ALLOW_RAW_RAILWAY_UP=1 railway up --detach
`);
process.exit(1);
