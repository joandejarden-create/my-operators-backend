#!/usr/bin/env node
/**
 * Wave 16A Stage 2B — LOW-risk image materialization CLI.
 *
 *   npm run brand-explorer-wave16a-stage2b-images -- --dry-run
 *   npm run brand-explorer-wave16a-stage2b-images -- --apply ...flags
 */
import "../load-env.js";
import {
  WAVE16A_STAGE2B_IMAGE_VERSION,
  WAVE16A_STAGE2B_APPLY_FLAGS,
  runWave16aStage2bImageMaterialization,
} from "../lib/partner-intelligence/brand-explorer-wave16a-stage2b-image-materialization.js";

async function main() {
  const argv = process.argv.slice(2);
  const dryRun = !argv.includes("--apply") || argv.includes("--dry-run");
  console.log(`[${WAVE16A_STAGE2B_IMAGE_VERSION}] dryRun=${dryRun}`);
  const result = await runWave16aStage2bImageMaterialization({ dryRun, argv });
  console.log(`Ready: ${result.readyStatement}`);
  console.log(
    `Summary: brands=${result.counts?.brands} ready=${result.counts?.ready} blocked=${result.counts?.blocked} patches=${result.counts?.patches}`
  );
  if (result.stopRecommended || result.pass === false) {
    console.error(`STOP: ${JSON.stringify(result.preflight?.issues || result.brands?.filter((b) => b.blocked) || [])}`);
    process.exitCode = 1;
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
