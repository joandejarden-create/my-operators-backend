#!/usr/bin/env node
/**
 * 24-brand tab/section public-full quality audit — dry-run only, no Airtable writes.
 *
 * Usage:
 *   npm run brand-explorer-24-tab-section-quality-audit -- --dry-run
 */
import "dotenv/config";
import {
  AUDIT_VERSION,
  run24TabSectionQualityAudit,
  write24TabSectionQualityReports,
} from "../lib/partner-intelligence/brand-explorer-24-tab-section-quality-audit.js";

async function main() {
  const argv = process.argv.slice(2);
  if (argv.includes("--apply")) {
    console.error("Refusing --apply. This is an audit-only command. Use --dry-run.");
    process.exit(2);
  }
  if (!argv.includes("--dry-run")) {
    console.error("Require --dry-run (audit-only, no writes).");
    process.exit(2);
  }

  const brandsIdx = argv.indexOf("--brands");
  const brands =
    brandsIdx >= 0 && argv[brandsIdx + 1]
      ? argv[brandsIdx + 1]
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean)
      : null;

  console.log(`[${AUDIT_VERSION}] 24-brand tab/section quality audit (dry-run, no writes)`);
  if (brands?.length) console.log(`  brands filter: ${brands.join(", ")}`);
  const report = await run24TabSectionQualityAudit({ dryRun: true, brands });
  const paths = write24TabSectionQualityReports(report);

  console.log(`Active universe: ${report.activeCount}`);
  console.log(`Audited: ${report.auditedCount}`);
  console.log(`Baseline decision: ${report.baselineFreezeDecision}`);
  console.log(`Recommendations: ${JSON.stringify(report.recommendationCounts)}`);
  console.log(`Cross-brand image groups: ${report.crossBrandImageIssues.length}`);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(`Wrote ${paths.imgJsonPath}`);
  console.log(`Wrote ${paths.imgMdPath}`);
  console.log(`Wrote ${paths.docsPath}`);
  console.log(`Per-brand reports: ${paths.perBrandPaths.length}`);

  for (const b of report.brandResults) {
    console.log(
      `  ${b.slug}: ${b.overallRecommendation} composite=${b.scores.composite} blockers=${b.scores.blockerCount} images=${b.imageFindings.length}`
    );
  }

  if (report.baselineFreezeDecision === "do_not_freeze_remediation_required") {
    process.exitCode = 3;
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
