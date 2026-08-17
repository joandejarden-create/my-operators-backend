/**
 * Editorial scenario owner-value remediation CLI (Title/Body on overview.scenario.1–3).
 *
 *   node scripts/brand-explorer-scenario-owner-value-editorial-remediation.mjs --dry-run
 *   node scripts/brand-explorer-scenario-owner-value-editorial-remediation.mjs --dry-run --brands ascend,kimpton
 *   node scripts/brand-explorer-scenario-owner-value-editorial-remediation.mjs --apply [flags…]
 */
import "../load-env.js";
import { runEditorialScenarioOwnerValueRemediation } from "../lib/partner-intelligence/brand-explorer-scenario-owner-value-editorial-remediation.js";
import { EDITORIAL_SCENARIO_APPLY_FLAGS } from "../lib/partner-intelligence/brand-explorer-scenario-owner-value-editorial-remediation.js";

function parseBrands(argv) {
  const idx = argv.indexOf("--brands");
  if (idx < 0 || !argv[idx + 1]) return null;
  return argv[idx + 1]
    .split(",")
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean);
}

async function main() {
  const argv = process.argv.slice(2);
  const apply = argv.includes("--apply") && !argv.includes("--dry-run");
  const brands = parseBrands(argv);

  console.log(
    `[scenario-owner-value-editorial] dryRun=${!apply} brands=${(brands || ["all-packaged"]).join(",")}`
  );
  if (apply) {
    console.log(`[scenario-owner-value-editorial] require flags: ${EDITORIAL_SCENARIO_APPLY_FLAGS.join(" ")}`);
  }

  const result = await runEditorialScenarioOwnerValueRemediation({
    dryRun: !apply,
    argv,
    brands,
  });

  console.log(`Wrote ${result.paths?.mdPath}`);
  console.log(
    `brands=${result.counts.brands} patches=${result.counts.patches} missing=${result.counts.missing} applied=${result.counts.applied}`
  );
  if (apply && result.applyResult?.error) {
    console.error(`Apply blocked: ${result.applyResult.error}`);
    if (result.applyResult.flagCheck?.missing?.length) {
      console.error(`Missing flags: ${result.applyResult.flagCheck.missing.join(" ")}`);
    }
    process.exitCode = 1;
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
