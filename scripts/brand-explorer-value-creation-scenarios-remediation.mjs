#!/usr/bin/env node
/**
 * Value Creation Scenarios remediation CLI.
 *
 *   node scripts/brand-explorer-value-creation-scenarios-remediation.mjs --dry-run
 *   node scripts/brand-explorer-value-creation-scenarios-remediation.mjs --apply ...flags
 */
import "../load-env.js";
import { runValueCreationScenariosRemediation } from "../lib/partner-intelligence/brand-explorer-value-creation-scenarios-remediation.js";

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
    `[value-creation-scenarios] dryRun=${dryRun} brands=${(brands || ["all-active-packages"]).join(",")}`
  );
  const result = await runValueCreationScenariosRemediation({ dryRun, argv, brands });
  if (result.paths?.mdPath) console.log(`Wrote ${result.paths.mdPath}`);
  console.log(
    `summary: brands=${result.summary.brands} writes=${result.summary.plannedPatches} create=${result.summary.creates} patch=${result.summary.patches}`
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
