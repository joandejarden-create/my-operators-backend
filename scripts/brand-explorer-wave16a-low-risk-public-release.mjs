#!/usr/bin/env node
/**
 * Wave 16A LOW-risk public release CLI.
 *
 *   npm run brand-explorer-wave16a-low-risk-public-release -- --dry-run
 *   npm run brand-explorer-wave16a-low-risk-public-release -- --apply …flags
 */
import "../load-env.js";
import { runWave16aLowRiskPublicRelease } from "../lib/partner-intelligence/brand-explorer-wave16a-low-risk-public-release.js";

async function main() {
  const argv = process.argv.slice(2);
  const dryRun = !argv.includes("--apply") || argv.includes("--dry-run");
  const result = await runWave16aLowRiskPublicRelease({
    apply: !dryRun,
    argv,
  });
  console.log(`Ready: ${result.readyStatement}`);
  console.log(`Universe: ${result.universe?.totalCount}`);
  if (result.pass === false) process.exitCode = dryRun ? 0 : 1;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
