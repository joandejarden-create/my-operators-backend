#!/usr/bin/env node
/**
 * Section Pattern Parity remediation — dry-run by default; --apply needs confirmation flags.
 */
import "dotenv/config";
import {
  REMEDIATION_VERSION,
  REQUIRED_APPLY_FLAGS,
  parseSectionPatternApplyFlags,
  planSectionPatternParityRemediation,
  applySectionPatternParityRemediation,
  writeSectionPatternParityRemediationReports,
  evaluateProjectedParity,
} from "../lib/partner-intelligence/brand-explorer-section-pattern-parity-remediation.js";
import { resolveSectionPatternBrandList } from "../lib/partner-intelligence/brand-explorer-section-pattern-parity-audit.js";

function parseArgs(argv) {
  const brandsIdx = argv.indexOf("--brands");
  const brands =
    brandsIdx >= 0 && argv[brandsIdx + 1]
      ? argv[brandsIdx + 1]
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean)
      : null;
  return {
    brands: brands ? resolveSectionPatternBrandList(brands) : null,
    apply: argv.includes("--apply"),
    dryRun: argv.includes("--dry-run") || !argv.includes("--apply"),
    argv,
  };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  console.log(`[${REMEDIATION_VERSION}] section pattern parity remediation`);
  console.log(`Brands: ${(opts.brands || ["(default)"]).join(", ")}`);
  console.log(`Mode: ${opts.apply ? "APPLY" : "dry-run"}`);

  const report = await planSectionPatternParityRemediation({ brands: opts.brands });

  for (const brand of report.brands || []) {
    if (!(brand.patches || []).length) {
      console.log(`  ${brand.brandSlug}: ${brand.skippedReason || "no patches"}`);
      continue;
    }
    const projected = await evaluateProjectedParity(brand.brandSlug, brand.patches);
    brand.projectedPass = projected.pass === true;
    brand.projectedGates = projected.gates;
    console.log(
      `  ${brand.brandSlug}: ${brand.patches.length} patches · projectedPass=${brand.projectedPass}`
    );
  }

  if (opts.apply) {
    const flags = parseSectionPatternApplyFlags(opts.argv);
    if (!flags.ok) {
      console.error("Missing apply flags:", flags.missing.join(", "));
      console.error("Required:", REQUIRED_APPLY_FLAGS.join(" "));
      process.exit(1);
    }
  }

  const applyResult = await applySectionPatternParityRemediation({
    report,
    apply: opts.apply,
    argv: opts.argv,
  });
  const paths = writeSectionPatternParityRemediationReports(report, applyResult);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(
    `Summary: patches=${report.summary.patchCount} applied=${applyResult.applied === true}`
  );
  if (!report.validation.pass) process.exitCode = 2;
  if (opts.apply && applyResult.applied !== true) process.exitCode = 3;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
