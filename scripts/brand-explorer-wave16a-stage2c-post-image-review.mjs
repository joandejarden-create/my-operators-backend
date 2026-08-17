#!/usr/bin/env node
/**
 * Wave 16A Stage 2C — post-image review CLI.
 *
 *   npm run brand-explorer-wave16a-stage2c-post-image-review -- --dry-run
 *   npm run brand-explorer-wave16a-stage2c-post-image-review -- --apply ...flags
 */
import "../load-env.js";
import {
  WAVE16A_STAGE2C_VERSION,
  runWave16aStage2cPostImageReview,
} from "../lib/partner-intelligence/brand-explorer-wave16a-stage2c-post-image-review.js";

async function main() {
  const argv = process.argv.slice(2);
  const dryRun = !argv.includes("--apply") || argv.includes("--dry-run");
  console.log(`[${WAVE16A_STAGE2C_VERSION}] dryRun=${dryRun}`);
  const result = await runWave16aStage2cPostImageReview({ dryRun, argv });
  console.log(`Ready: ${result.readyStatement}`);
  console.log(
    `Defects C/H/M/L: ${result.defectCountsBySeverity?.CRITICAL}/${result.defectCountsBySeverity?.HIGH}/${result.defectCountsBySeverity?.MEDIUM}/${result.defectCountsBySeverity?.LOW}`
  );
  console.log(`Patches: ${result.patchesPlanned?.length || 0}`);
  if (result.stopRecommended || result.pass === false) {
    process.exitCode = dryRun && result.readyStatement?.includes("dry_run") ? 0 : 1;
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
