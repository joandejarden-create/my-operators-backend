#!/usr/bin/env node
/**
 * Wave 16A LOW-risk status promotion CLI.
 *
 *   npm run brand-explorer-wave16a-low-risk-status-promotion -- --dry-run
 *   npm run brand-explorer-wave16a-low-risk-status-promotion -- --apply …flags
 */
import "../load-env.js";
import { runWave16aLowRiskStatusPromotion } from "../lib/partner-intelligence/brand-explorer-wave16a-low-risk-status-promotion.js";

async function main() {
  const argv = process.argv.slice(2);
  const dryRun = !argv.includes("--apply") || argv.includes("--dry-run");
  const result = await runWave16aLowRiskStatusPromotion({
    apply: !dryRun,
    argv,
  });
  console.log(`Ready: ${result.readyStatement}`);
  console.log(
    `Universe: ${result.universeBefore?.totalCount} → ${result.universeAfter?.totalCount}`
  );
  if (result.pass === false) process.exitCode = dryRun ? 0 : 1;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
