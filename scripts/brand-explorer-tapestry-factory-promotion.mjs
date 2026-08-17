#!/usr/bin/env node
/**
 * Tapestry Collection by Hilton — Setup AI Factory promotion orchestrator.
 *
 *   npm run brand-explorer-tapestry-factory-promotion -- --stage preflight
 *   npm run brand-explorer-tapestry-factory-promotion -- --stage tab-factory-completion --dry-run
 *   npm run brand-explorer-tapestry-factory-promotion -- --stage founder-review
 *   npm run brand-explorer-tapestry-factory-promotion -- --stage status-promotion --apply ...flags
 */
import "../load-env.js";
import {
  PROMOTION_VERSION,
  STAGES,
  STAGE_REQUIRED_APPLY_FLAGS,
  runTapestryFactoryPromotion,
} from "../lib/partner-intelligence/brand-explorer-tapestry-factory-promotion.js";

function hasFlag(name) {
  return process.argv.includes(name);
}

function argValue(name, fallback = "") {
  const i = process.argv.indexOf(name);
  if (i < 0) return fallback;
  return process.argv[i + 1] || fallback;
}

async function main() {
  const stage = argValue("--stage", "preflight");
  const apply = hasFlag("--apply");
  const dryRun = hasFlag("--dry-run") || !apply;

  if (!STAGES.includes(stage)) {
    console.error(`[${PROMOTION_VERSION}] Unknown stage '${stage}'. Allowed:\n  ${STAGES.join("\n  ")}`);
    process.exit(1);
  }

  if (apply && STAGE_REQUIRED_APPLY_FLAGS[stage]) {
    const missing = STAGE_REQUIRED_APPLY_FLAGS[stage].filter((f) => !hasFlag(f));
    if (missing.length) {
      console.error(`[${PROMOTION_VERSION}] Apply for ${stage} requires:\n  ${missing.join("\n  ")}`);
      process.exit(1);
    }
  }

  console.log(`[${PROMOTION_VERSION}] stage=${stage} mode=${dryRun ? "dry-run" : "APPLY"}`);
  const result = await runTapestryFactoryPromotion({
    stage,
    apply: apply && !dryRun,
    argv: process.argv.slice(2),
  });

  const paths = result?.paths || {};
  for (const [k, v] of Object.entries(paths)) {
    if (typeof v === "string") console.log(`Wrote ${k}: ${v}`);
  }
  const report = result?.report || result;
  if (report?.freezeDecision) console.log(`Freeze decision: ${report.freezeDecision}`);
  if (report?.tapestry?.brandStatus) console.log(`Brand Status: ${report.tapestry.brandStatus}`);
  if (report?.summary) console.log(`Summary: ${JSON.stringify(report.summary)}`);
  if (report?.identityIssues?.length) {
    console.error(`Identity issues: ${report.identityIssues.join("; ")}`);
    process.exit(1);
  }
  if (report?.applyResult?.error) {
    console.error(report.applyResult.error);
    process.exit(1);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
