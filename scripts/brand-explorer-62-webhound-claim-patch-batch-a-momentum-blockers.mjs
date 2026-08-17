#!/usr/bin/env node
/**
 * Brand Explorer 62 — Webhound Claim Patch Batch A (Recent Momentum blockers).
 *
 *   node scripts/brand-explorer-62-webhound-claim-patch-batch-a-momentum-blockers.mjs --dry-run
 *   node scripts/brand-explorer-62-webhound-claim-patch-batch-a-momentum-blockers.mjs --apply \
 *     --confirm-batch-a-only \
 *     --confirm-no-batch-b-f \
 *     --confirm-recent-momentum-presentation-only \
 *     --confirm-no-brand-setup-writes \
 *     --confirm-no-census-writes \
 *     --confirm-no-brand-status-changes \
 *     --confirm-no-release-field-writes \
 *     --confirm-no-company-validated-writes \
 *     --confirm-no-brand-verified-writes \
 *     --confirm-founder-approved-batch-a
 */
import "../load-env.js";
import {
  APPLY_FLAGS,
  BATCH_A_VERSION,
  runBatchAClaimPatch,
  writeBatchAArtifacts,
} from "../lib/partner-intelligence/brand-explorer-62-webhound-claim-patch-batch-a-momentum-blockers.js";

async function main() {
  const argv = process.argv.slice(2);
  const apply = argv.includes("--apply");
  const dryRun = argv.includes("--dry-run") || !apply;

  if (apply && dryRun && !argv.includes("--apply")) {
    // unreachable
  }
  if (!apply && !argv.includes("--dry-run")) {
    console.error("Require --dry-run or --apply with confirm flags.");
    process.exit(2);
  }
  if (apply) {
    const missing = APPLY_FLAGS.filter((f) => !argv.includes(f));
    if (missing.length) {
      console.error(`[${BATCH_A_VERSION}] Apply requires flags:\n${missing.join("\n")}`);
      process.exit(2);
    }
  }

  console.log(`[${BATCH_A_VERSION}] mode=${apply ? "apply" : "dry-run"}`);
  const report = await runBatchAClaimPatch({ apply, argv });
  const paths = writeBatchAArtifacts(report);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(`Wrote ${paths.docsPath}`);
  console.log(`Status: ${report.status}`);
  console.log(`Summary: ${JSON.stringify(report.summary)}`);
  if (report.preflightIssues?.length) {
    console.error(`Preflight issues: ${report.preflightIssues.join("; ")}`);
    process.exit(2);
  }
  if (apply && report.summary?.airtableWrites === 0 && report.summary?.alreadyHiddenNoop === 0) {
    console.error("Apply produced zero writes.");
    process.exit(2);
  }
}

main().catch((err) => {
  console.error(err?.stack || err?.message || String(err));
  process.exit(1);
});
