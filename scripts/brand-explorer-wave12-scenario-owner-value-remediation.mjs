#!/usr/bin/env node
/**
 * Wave 12 scenario owner-value remediation CLI.
 *
 *   npm run brand-explorer-wave12-scenario-owner-value -- --dry-run --brands voco-hotels,avid-hotels,ac-hotels-by-marriott
 *   npm run brand-explorer-wave12-scenario-owner-value -- --apply ...flags --brands ...
 */
import "../load-env.js";
import { runWave12ScenarioOwnerValueRemediation } from "../lib/partner-intelligence/brand-explorer-wave12-scenario-owner-value-remediation.js";

function parseBrands(argv) {
  const idx = argv.indexOf("--brands");
  if (idx >= 0 && argv[idx + 1]) {
    return argv[idx + 1]
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
  }
  return null;
}

async function main() {
  const argv = process.argv.slice(2);
  const dryRun = argv.includes("--dry-run") || !argv.includes("--apply");
  const brands = parseBrands(argv);
  console.log(
    `[wave12-scenario-owner-value] dryRun=${dryRun} brands=${(brands || ["all"]).join(",")}`
  );
  const result = await runWave12ScenarioOwnerValueRemediation({ dryRun, argv, brands });
  if (result.paths?.mdPath) console.log(`Wrote ${result.paths.mdPath}`);
  if (result.paths?.jsonPath) console.log(`Wrote ${result.paths.jsonPath}`);
  console.log(
    `summary: patches=${result.summary.plannedPatches} repeatedDiligence=${result.summary.brandsWithRepeatedDiligence} imageFix=${result.summary.brandsNeedingImageFix}`
  );
  if (!dryRun && result.flagCheck && !result.flagCheck.ok) {
    console.error(`Missing apply flags: ${(result.flagCheck.missing || []).join(" ")}`);
    process.exit(2);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
