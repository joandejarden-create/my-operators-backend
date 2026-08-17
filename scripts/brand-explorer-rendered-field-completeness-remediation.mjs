#!/usr/bin/env node
/**
 * Rendered field-completeness remediation CLI (plan / apply).
 */
import "dotenv/config";
import {
  TARGET_BRANDS,
  REQUIRED_APPLY_FLAGS,
  parseRemediationApplyFlags,
  runRenderedFieldCompletenessRemediation,
  writeRemediationPlanReports,
  REMEDIATION_VERSION,
} from "../lib/partner-intelligence/brand-explorer-rendered-field-completeness-remediation.js";

function parseArgs(argv) {
  const brandsIdx = argv.indexOf("--brands");
  const brands =
    brandsIdx >= 0 && argv[brandsIdx + 1]
      ? argv[brandsIdx + 1]
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean)
      : [...TARGET_BRANDS];
  const applyRequested = argv.includes("--apply");
  const flagCheck = parseRemediationApplyFlags(argv);
  if (applyRequested && !flagCheck.ok) {
    throw new Error(
      `Apply requires all gates:\n  ${REQUIRED_APPLY_FLAGS.join("\n  ")}\nMissing: ${flagCheck.missing.join(", ")}`
    );
  }
  return { brands, dryRun: !applyRequested, argv };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  console.log(`[${REMEDIATION_VERSION}] dryRun=${opts.dryRun}`);
  console.log(`  brands: ${opts.brands.join(", ")}`);
  const report = await runRenderedFieldCompletenessRemediation(opts);
  const paths = writeRemediationPlanReports(report);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(
    `Summary: patches=${report.summary.totalPatches} blocked=${report.summary.blockedBrands} writes=${report.summary.writes}`
  );
  for (const b of report.brandResults) {
    console.log(
      `  ${b.brandSlug}: patches=${b.patches.length}${b.blocked ? " [BLOCKED]" : ""} decision=${b.releaseQualityDecision}`
    );
  }
  if (report.summary.blockedBrands > 0) process.exit(2);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
