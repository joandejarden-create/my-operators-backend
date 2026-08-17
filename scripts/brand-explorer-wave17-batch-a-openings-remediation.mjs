#!/usr/bin/env node
/**
 * Wave 17 Batch A — openings shortfall remediation CLI.
 *
 *   node scripts/brand-explorer-wave17-batch-a-openings-remediation.mjs --dry-run
 *   node scripts/brand-explorer-wave17-batch-a-openings-remediation.mjs --apply ...flags
 */
import "../load-env.js";
import {
  WAVE17_BATCH_A_OPENINGS_REMEDIATION_VERSION,
  WAVE17_BATCH_A_OPENINGS_REMEDIATION_APPLY_FLAGS,
  runWave17BatchAOpeningsRemediation,
} from "../lib/partner-intelligence/brand-explorer-wave17-batch-a-openings-remediation.js";

async function main() {
  const argv = process.argv.slice(2);
  const dryRun = !argv.includes("--apply") || argv.includes("--dry-run");
  console.log(`[${WAVE17_BATCH_A_OPENINGS_REMEDIATION_VERSION}] dryRun=${dryRun}`);
  const result = await runWave17BatchAOpeningsRemediation({ dryRun, argv });
  console.log(`Ready: ${result.readyStatement}`);
  console.log(
    `Research: ${result.research?.length || 0} · diffs: ${result.diffs?.length || 0} · writes: ${result.apply?.writes?.length || 0}`
  );
  console.log(
    `Gates: wrongBrand=${result.totals?.wrongBrand} wrongProperty=${result.totals?.wrongProperty} broken=${result.totals?.brokenImages} uniq=${result.totals?.uniquenessPass} role=${result.totals?.roleMatchPass} openings33=${result.totals?.openings33}`
  );
  if (result.blockers?.length) {
    console.error(`Blockers: ${JSON.stringify(result.blockers)}`);
  }
  if (
    result.readyStatement === "wave17_batch_a_openings_remediation_blocked_shared_issue" ||
    (result.apply?.errors || []).length
  ) {
    process.exitCode = 1;
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
