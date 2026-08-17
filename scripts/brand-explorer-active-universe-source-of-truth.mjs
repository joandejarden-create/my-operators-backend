#!/usr/bin/env node
/**
 * Brand Explorer Active Universe Source-of-Truth — read-only dry-run audit.
 */
import "dotenv/config";
import {
  AUDIT_VERSION,
  runActiveUniverseSourceOfTruthAudit,
  writeActiveUniverseSourceOfTruthReports,
} from "../lib/partner-intelligence/brand-explorer-active-universe-source-of-truth.js";

function parseArgs(argv) {
  return {
    dryRun: !argv.includes("--apply"),
  };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  if (!opts.dryRun) {
    console.error(
      "This audit is read-only. Use --dry-run only (default). Do not pass --apply."
    );
    process.exit(2);
  }
  console.log(`[${AUDIT_VERSION}] dryRun=true readOnly=true noWrites=true`);

  const report = await runActiveUniverseSourceOfTruthAudit({ dryRun: true });
  const paths = writeActiveUniverseSourceOfTruthReports(report);

  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(`Wrote ${paths.cohortPath}`);
  console.log(`Wrote ${paths.missingPath}`);
  console.log(`Wrote ${paths.docPath}`);
  console.log(
    `Active universe: ${report.activeSourceOfTruth.totalCount} (reconcilesTo46=${report.reconcilesTo46})`
  );
  console.log(
    `Prior23 missing from active: ${(report.prior23Comparison.priorSlugsNoLongerActive || []).join(", ") || "—"}`
  );
  console.log(
    `Active missing from prior23: ${(report.prior23Comparison.activeSlugsMissingFromPrior || []).join(", ") || "—"}`
  );
  for (const bucket of Object.keys(report.byBucket)) {
    const slugs = report.byBucket[bucket] || [];
    if (!slugs.length) continue;
    console.log(`  [${bucket}] ${slugs.join(", ")}`);
  }
  if (!report.acceptance.everyActiveHasSlugAndRecordId) {
    console.error("ACCEPTANCE FAIL: missing slug or recordId");
    process.exit(1);
  }
  if (!report.reconcilesTo46) {
    console.warn("WARN: live count is not 46 — see report countExplanation");
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
