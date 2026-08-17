#!/usr/bin/env node
/**
 * Pre-v43: fill Case Summary modal fields on lifestyle CALA openings.
 */
import "dotenv/config";
import {
  TARGET_BRANDS,
  runLifestyleOpeningModalFill,
  writeOpeningModalReports,
} from "../lib/partner-intelligence/brand-explorer-v43-lifestyle-opening-modal-fill.js";

function parseArgs(argv) {
  const brandsIdx = argv.indexOf("--brands");
  const brands =
    brandsIdx >= 0 && argv[brandsIdx + 1]
      ? argv[brandsIdx + 1]
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean)
      : [...TARGET_BRANDS];
  return { brands, dryRun: !argv.includes("--apply") };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  console.log(`[v43-pre] Opening modal fill (dryRun=${opts.dryRun})`);
  console.log(`  brands: ${opts.brands.join(", ")}`);
  const report = await runLifestyleOpeningModalFill(opts);
  const paths = writeOpeningModalReports(report);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(
    `Summary: patches=${report.summary.totalPatches} projectedPass=${report.summary.projectedExternalOwnerPass} writes=${report.summary.presentationWrites}`
  );
  for (const b of report.brandResults) {
    console.log(
      `  ${b.brandSlug}: patches=${b.patches.length} readiness ${b.readinessBefore.pass}→${b.readinessAfter.pass}`
    );
  }
  if (report.brandResults.some((b) => !b.readinessAfter.pass)) {
    console.error("Projected external-owner readiness still failing for one or more brands.");
    process.exit(2);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
