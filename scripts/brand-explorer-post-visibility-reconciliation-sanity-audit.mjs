#!/usr/bin/env node
/**
 * Post-visibility reconciliation sanity audit — read-only.
 */
import "dotenv/config";
import {
  runPostVisibilityReconciliationSanityAudit,
  writePostVisibilityReconciliationSanityAuditReports,
  SANITY_AUDIT_VERSION,
} from "../lib/partner-intelligence/brand-explorer-post-visibility-reconciliation-sanity-audit.js";

function parseArgs(argv) {
  const brandsIdx = argv.indexOf("--brands");
  const brands =
    brandsIdx >= 0 && argv[brandsIdx + 1]
      ? argv[brandsIdx + 1].split(",").map((s) => s.trim()).filter(Boolean)
      : null;
  return {
    dryRun: !argv.includes("--apply"),
    brands,
    includeOsEval: !argv.includes("--skip-os-eval"),
  };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  if (!opts.dryRun) {
    console.error("This audit is read-only. Use --dry-run only (default).");
    process.exit(2);
  }
  console.log(`[${SANITY_AUDIT_VERSION}] dryRun=true auditOnly=true`);
  const report = await runPostVisibilityReconciliationSanityAudit({
    slugs: opts.brands,
    includeOsEval: opts.includeOsEval,
  });
  const paths = writePostVisibilityReconciliationSanityAuditReports(report);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(
    `Summary: brands=${report.summary.brandCount} eqlCohort=${report.summary.externalQualityLockCohortSize} restoredVisible=${report.summary.restoredLegacyVisible} primaryLocked=${report.summary.primaryLocked} mismatches=${report.summary.mismatchCount}`
  );
  console.log(`Radisson: ${report.radissonIndividualsConflict.conclusion.slice(0, 180)}…`);
  for (const slug of report.internalPreviewCopyInvestigation.focusBrands || []) {
    const r = report.internalPreviewCopyInvestigation.results?.[slug];
    if (!r) continue;
    console.log(
      `  owner-copy ${slug}: liveHits=${r.liveInternalPreviewHits.length} projectedHits=${(r.projectedInternalPreviewHits || []).length} publicHits=${r.publicExternalHits.length}`
    );
  }
  for (const b of (report.brandResults || []).filter((x) => (x.mismatchIssues || []).length)) {
    console.log(`  mismatch ${b.slug}: ${(b.mismatchIssues || []).join(", ")}`);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
