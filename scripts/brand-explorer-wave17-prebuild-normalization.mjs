#!/usr/bin/env node
/**
 * Wave 17 pre-build normalization CLI.
 *
 *   npm run brand-explorer-wave17-prebuild-normalization -- --dry-run
 *   npm run brand-explorer-wave17-prebuild-normalization -- --apply \
 *     --confirm-active-universe-65 \
 *     --confirm-status-only-writes \
 *     --confirm-no-presentation-images-momentum \
 *     --confirm-dream-create-if-missing
 */
import "../load-env.js";
import { runWave17PrebuildNormalization } from "../lib/partner-intelligence/brand-explorer-wave17-prebuild-normalization.js";

async function main() {
  const argv = process.argv.slice(2);
  const dryRun = !argv.includes("--apply") || argv.includes("--dry-run");
  const result = await runWave17PrebuildNormalization({
    apply: !dryRun,
    argv,
  });
  console.log(`Ready: ${result.readyStatement}`);
  console.log(`Universe: ${result.universeBefore?.totalCount} → ${result.universeAfter?.totalCount}`);
  console.log(`Dream created: ${result.dream?.created === true} id=${result.dream?.recordId || "n/a"}`);
  console.log(`Dream identity: ${result.dream?.identityResult || "n/a"}`);
  console.log(`Batch A ready: ${result.batchAReady === true}`);
  if (result.issues?.length) console.log("Issues:", result.issues.join("; "));
  if (result.pass === false && !dryRun) process.exitCode = 1;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
