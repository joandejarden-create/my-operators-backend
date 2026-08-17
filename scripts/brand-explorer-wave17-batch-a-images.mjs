#!/usr/bin/env node
/**
 * Wave 17 Batch A — LOW-risk image materialization CLI.
 *
 *   npm run brand-explorer-wave17-batch-a-images -- --dry-run
 *   npm run brand-explorer-wave17-batch-a-images -- --apply ...flags
 */
import "../load-env.js";
import {
  WAVE17_BATCH_A_IMAGE_VERSION,
  WAVE17_BATCH_A_IMAGE_APPLY_FLAGS,
  runWave17BatchAImageMaterialization,
} from "../lib/partner-intelligence/brand-explorer-wave17-batch-a-image-materialization.js";

async function main() {
  const argv = process.argv.slice(2);
  const dryRun = !argv.includes("--apply") || argv.includes("--dry-run");
  console.log(`[${WAVE17_BATCH_A_IMAGE_VERSION}] dryRun=${dryRun}`);
  const result = await runWave17BatchAImageMaterialization({ dryRun, argv });
  console.log(`Ready: ${result.readyStatement}`);
  console.log(
    `Summary: brands=${result.counts?.brands} ready=${result.counts?.ready} blocked=${result.counts?.blocked} patches=${result.counts?.patches}`
  );
  if (result.stopRecommended || (result.brands || []).some((b) => b.blocked)) {
    console.error(`STOP: ${JSON.stringify(result.preflight?.issues || result.brands?.filter((b) => b.blocked) || [])}`);
    process.exitCode = 1;
  } else if (result.pass === false) {
    console.warn(`PARTIAL: ${result.readyStatement}`);
    process.exitCode = 0;
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
