#!/usr/bin/env node
/**
 * Brand Explorer 62 — Webhound Public Tabs Batch C (owner-facing claims).
 *
 *   node scripts/brand-explorer-62-webhound-public-tabs-batch-c-owner-facing-claims.mjs --dry-run
 *   node scripts/brand-explorer-62-webhound-public-tabs-batch-c-owner-facing-claims.mjs --apply \
 *     --confirm-batch-c-only \
 *     --confirm-no-batch-a-reapply \
 *     --confirm-no-batch-b-d-e-f \
 *     --confirm-public-tabs-presentation-only \
 *     --confirm-no-brand-setup-writes \
 *     --confirm-no-census-writes \
 *     --confirm-no-brand-status-changes \
 *     --confirm-no-release-field-writes \
 *     --confirm-no-company-validated-writes \
 *     --confirm-no-brand-verified-writes \
 *     --confirm-founder-approved-batch-c
 */
import "../load-env.js";
import {
  APPLY_FLAGS,
  BATCH_C_VERSION,
  runBatchCPublicTabsPatch,
  writeBatchCArtifacts,
} from "../lib/partner-intelligence/brand-explorer-62-webhound-public-tabs-batch-c-owner-facing-claims.js";

async function main() {
  const argv = process.argv.slice(2);
  const apply = argv.includes("--apply");
  if (!apply && !argv.includes("--dry-run")) {
    console.error("Require --dry-run or --apply with confirm flags.");
    process.exit(2);
  }
  if (apply) {
    const missing = APPLY_FLAGS.filter((f) => !argv.includes(f));
    if (missing.length) {
      console.error(`[${BATCH_C_VERSION}] Apply requires:\n${missing.join("\n")}`);
      process.exit(2);
    }
  }

  console.log(`[${BATCH_C_VERSION}] mode=${apply ? "apply" : "dry-run"}`);
  const report = await runBatchCPublicTabsPatch({ apply, argv });
  const paths = writeBatchCArtifacts(report);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(`Wrote ${paths.docsPath}`);
  console.log(`Status: ${report.status}`);
  console.log(`Summary: ${JSON.stringify(report.summary)}`);
  if (report.preflightIssues?.length) {
    console.error(`Preflight issues: ${report.preflightIssues.join("; ")}`);
    process.exit(2);
  }
}

main().catch((err) => {
  console.error(err?.stack || err?.message || String(err));
  process.exit(1);
});
