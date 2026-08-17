#!/usr/bin/env node
/**
 * Active Brand Completion Reconciliation — read-only dry-run audit.
 */
import "dotenv/config";
import {
  RECONCILIATION_VERSION,
  runActiveBrandCompletionReconciliation,
  writeActiveBrandCompletionReconciliationReports,
} from "../lib/partner-intelligence/brand-explorer-active-brand-completion-reconciliation.js";

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
    dryRun: !argv.includes("--apply"),
    brands,
  };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  if (!opts.dryRun) {
    console.error("This audit is read-only. Use --dry-run only (default). Do not pass --apply.");
    process.exit(2);
  }
  console.log(`[${RECONCILIATION_VERSION}] dryRun=true readOnly=true noWrites=true`);

  const report = await runActiveBrandCompletionReconciliation({ slugs: opts.brands });
  const paths = writeActiveBrandCompletionReconciliationReports(report);

  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(`Wrote ${paths.invPath}`);
  console.log(`Wrote ${paths.restorePath}`);
  console.log(`Wrote ${paths.planPath}`);
  console.log(`Wrote ${paths.docPath}`);
  console.log(
    `Summary: brands=${report.summary.brandCount} publicFullClean=${report.summary.publicFullClean} readyRestore=${report.summary.readyToRestore} minor=${report.summary.minorFix} image=${report.summary.imageRemediation} content=${report.summary.contentRemediation} incomplete=${report.summary.trueIncomplete} mapping=${report.summary.slugMapping}`
  );
  for (const bucket of Object.keys(report.byBucket)) {
    const slugs = report.byBucket[bucket] || [];
    if (!slugs.length) continue;
    console.log(`  [${bucket}] ${slugs.join(", ")}`);
  }
  if (!report.acceptance.everyBrandClassified || !report.acceptance.companyValidatedUntouched) {
    process.exitCode = 1;
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
