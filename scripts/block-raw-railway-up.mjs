#!/usr/bin/env node
/**
 * Guard for accidental raw `railway up`.
 * Sole live production entry: GitHub Actions workflow dealality-production-release.
 *
 * Bypass only with: DEALALITY_ALLOW_RAW_RAILWAY_UP=1 (founder emergency; skips gates)
 */
if (process.env.DEALALITY_ALLOW_RAW_RAILWAY_UP === "1") {
  console.warn(
    "WARN raw railway up allowed via DEALALITY_ALLOW_RAW_RAILWAY_UP=1 — asset gates NOT run"
  );
  process.exit(0);
}

console.error(`
BLOCKED: raw Railway production upload is not the Dealality deploy path.

Sole live production entry point:
  GitHub Actions → Actions → Dealality production release
  workflow: dealality-production-release
  inputs: product, approved_sha, authorization_phrase=I_AUTHORIZE_PRODUCTION_RELEASE, dry_run

Local dry-run only (no railway up):
  npm run deploy:production:dry-run

Raw railway up bypasses asset gates, the shared release lock, GitHub Actions concurrency,
and product certification. Do not use it for ADP/AVA/GDI production cutovers.

To force a raw CLI upload (discouraged; founder auth only):
  DEALALITY_ALLOW_RAW_RAILWAY_UP=1 railway up --detach
`);
process.exit(1);
