#!/usr/bin/env node
/**
 * Brand Explorer Active Universe Normalization — read-only dry-run.
 */
import "dotenv/config";
import {
  NORMALIZATION_VERSION,
  runActiveUniverseNormalization,
  writeActiveUniverseNormalizationReports,
} from "../lib/partner-intelligence/brand-explorer-active-universe-normalization.js";

function parseArgs(argv) {
  return { dryRun: !argv.includes("--apply") };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  if (!opts.dryRun) {
    console.error(
      "This normalization is read-only. Use --dry-run only (default). Do not pass --apply."
    );
    process.exit(2);
  }
  console.log(`[${NORMALIZATION_VERSION}] dryRun=true readOnly=true noWrites=true`);

  const report = await runActiveUniverseNormalization({ dryRun: true });
  const paths = writeActiveUniverseNormalizationReports(report);

  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(`Wrote ${paths.pvqlPath}`);
  console.log(`Wrote ${paths.unconfiguredPath}`);
  console.log(`Wrote ${paths.conflictsPath}`);
  console.log(`Wrote ${paths.docPath}`);
  console.log(
    `Active universe: ${report.activeUniverse.totalCount} (reconcilesTo24=${report.activeUniverse.reconcilesTo24})`
  );
  for (const bucket of Object.keys(report.byBucket)) {
    const slugs = report.byBucket[bucket] || [];
    if (!slugs.length) continue;
    console.log(`  [${bucket}] ${slugs.join(", ")}`);
  }
  console.log(
    `PVQL repair field rows: ${report.pvqlRepairPlan.fieldRowCount} across ${report.pvqlRepairPlan.brandCount} brands`
  );
  console.log(`Unconfigured: ${(report.unconfiguredBrands || []).map((u) => u.slug).join(", ") || "—"}`);
  console.log(
    `Status conflicts (excluded from 24): ${(report.statusConflicts || []).map((c) => c.slug).join(", ") || "—"}`
  );

  const a = report.acceptance;
  if (!a.all24ListedAndClassified || !a.noAirtableWrites) {
    console.error("ACCEPTANCE FAIL", a);
    process.exit(1);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
