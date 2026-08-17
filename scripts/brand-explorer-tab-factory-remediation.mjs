#!/usr/bin/env node
import "dotenv/config";
import {
  runTabFactoryRemediation,
  REMEDIATION_VERSION,
  parseTabFactoryApplyFlags,
} from "../lib/partner-intelligence/brand-explorer-tab-factory-remediation.js";
import { TAB_FACTORY_TARGET_BRANDS } from "../lib/partner-intelligence/brand-explorer-tab-contracts.js";

function parseBrands(argv) {
  const idx = argv.indexOf("--brands");
  if (idx >= 0 && argv[idx + 1]) {
    return argv[idx + 1].split(",").map((s) => s.trim()).filter(Boolean);
  }
  return [...TAB_FACTORY_TARGET_BRANDS];
}

async function main() {
  const argv = process.argv.slice(2);
  const brands = parseBrands(argv);
  const flags = parseTabFactoryApplyFlags(argv);
  console.log(`[${REMEDIATION_VERSION}] dryRun=${!flags.apply}`);
  console.log(`  brands: ${brands.join(", ")}`);
  if (flags.apply && !flags.ok) {
    console.error(`Missing apply flags:\n  ${flags.missing.join("\n  ")}`);
    process.exit(2);
  }
  const { report, jsonPath, mdPath } = await runTabFactoryRemediation({
    brands,
    apply: flags.apply,
    argv,
  });
  console.log(`Wrote ${jsonPath}`);
  console.log(`Wrote ${mdPath}`);
  console.log(
    `Summary: patches=${report.summary.patches} blocked=${report.summary.blocked} writes=${report.summary.writes}`
  );
  for (const b of report.brandResults) {
    console.log(
      `  ${b.brandSlug}: patches=${b.patches.length} blocked=${b.blocked} priorAuditPass=${b.auditPass}`
    );
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
