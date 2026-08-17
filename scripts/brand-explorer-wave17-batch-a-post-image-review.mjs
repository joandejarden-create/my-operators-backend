#!/usr/bin/env node
/**
 * Wave 17 Batch A — post-image review CLI.
 *
 *   npm run brand-explorer-wave17-batch-a-post-image-review -- --dry-run
 *   npm run brand-explorer-wave17-batch-a-post-image-review -- --apply ...flags
 */
import "../load-env.js";
import {
  WAVE17_BATCH_A_POST_IMAGE_VERSION,
  runWave17BatchAPostImageReview,
} from "../lib/partner-intelligence/brand-explorer-wave17-batch-a-post-image-review.js";

async function main() {
  const argv = process.argv.slice(2);
  const dryRun = !argv.includes("--apply") || argv.includes("--dry-run");
  console.log(`[${WAVE17_BATCH_A_POST_IMAGE_VERSION}] dryRun=${dryRun}`);
  const result = await runWave17BatchAPostImageReview({ dryRun, argv });
  console.log(`Ready: ${result.readyStatement}`);
  console.log(
    `Defects C/H/M/L initial: ${result.initialDefectCountsBySeverity?.CRITICAL}/${result.initialDefectCountsBySeverity?.HIGH}/${result.initialDefectCountsBySeverity?.MEDIUM}/${result.initialDefectCountsBySeverity?.LOW}`
  );
  console.log(
    `Defects C/H/M/L final: ${result.finalDefectCountsBySeverity?.CRITICAL}/${result.finalDefectCountsBySeverity?.HIGH}/${result.finalDefectCountsBySeverity?.MEDIUM}/${result.finalDefectCountsBySeverity?.LOW}`
  );
  console.log(`Patches planned: ${result.patchesPlanned || 0} · applied: ${result.applyResults?.length || 0}`);
  if (result.stopRecommended || result.pass === false) {
    process.exitCode = dryRun ? 0 : 1;
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
