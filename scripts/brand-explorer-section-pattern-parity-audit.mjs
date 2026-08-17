#!/usr/bin/env node
/**
 * Section Pattern Parity audit — dry-run by default.
 */
import "dotenv/config";
import {
  SECTION_PATTERN_PARITY_VERSION,
  runSectionPatternParityAudit,
  writeSectionPatternParityReports,
  resolveSectionPatternBrandList,
} from "../lib/partner-intelligence/brand-explorer-section-pattern-parity-audit.js";

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
    brands: resolveSectionPatternBrandList(brands),
    dryRun: argv.includes("--dry-run") || !argv.includes("--apply"),
  };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  console.log(`[${SECTION_PATTERN_PARITY_VERSION}] section pattern parity audit`);
  console.log(`Brands: ${opts.brands.join(", ")}`);
  const report = await runSectionPatternParityAudit({
    brands: opts.brands,
    dryRun: opts.dryRun,
  });
  const paths = writeSectionPatternParityReports(report);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(
    `Summary: pass=${report.summary.pass} fail=${report.summary.fail} failing=${report.summary.failingSlugs.join(",") || "none"}`
  );
  if (report.summary.fail > 0) process.exitCode = 2;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
