#!/usr/bin/env node
/**
 * Dry-run rendered field-by-field completeness audit.
 */
import "dotenv/config";
import {
  runRenderedFieldCompletenessAudit,
  writeRenderedFieldCompletenessReports,
  AUDIT_VERSION,
} from "../lib/partner-intelligence/brand-explorer-rendered-field-completeness-audit.js";
import { TARGET_BRANDS } from "../lib/partner-intelligence/brand-explorer-rendered-field-completeness-inventory.js";

function parseBrands(argv) {
  const idx = argv.indexOf("--brands");
  if (idx >= 0 && argv[idx + 1]) {
    return argv[idx + 1]
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
  }
  return [...TARGET_BRANDS];
}

async function main() {
  const brands = parseBrands(process.argv.slice(2));
  console.log(`[${AUDIT_VERSION}] dry-run audit`);
  console.log(`  brands: ${brands.join(", ")}`);
  const report = await runRenderedFieldCompletenessAudit({ brands, includeBenchmarks: true });
  const paths = writeRenderedFieldCompletenessReports(report);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  for (const [slug, p] of Object.entries(paths.brandPaths || {})) {
    console.log(`  ${slug}: ${p}`);
  }
  console.log(
    `Summary: passFindings=${report.summary.totalPass} failFindings=${report.summary.totalFail} auditComplete=${report.auditComplete} patchPlanComplete=${report.patchPlanComplete} auditPass=${report.auditPass}`
  );
  for (const b of report.brandResults) {
    console.log(
      `  ${b.brandSlug}: decision=${b.releaseQualityDecision} fail=${b.summary.totalFail} patches=${b.patchPlan.length} auditPass=${b.auditPass} patchPlanComplete=${b.patchPlanComplete}`
    );
  }
  if (!report.patchPlanComplete) {
    console.error(
      "\nAudit incomplete: every visible fail must be complete, intentionally suppressed, cleanly unavailable, or included in a patch plan."
    );
    process.exit(2);
  }
  if (!report.auditPass) {
    console.error(
      "\nRelease gate not met: auditPass requires failFindings = 0 after remediation. Patch plan may be complete, but founder_review_ready / active_profile_ready remain blocked."
    );
    process.exit(3);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
