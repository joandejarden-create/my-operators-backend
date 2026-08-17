#!/usr/bin/env node
/**
 * Brand Explorer — Profile in Preparation visibility audit (dry-run).
 */
import "dotenv/config";
import {
  runProfilePreparationVisibilityAudit,
  writeProfilePreparationVisibilityAuditReports,
  VISIBILITY_AUDIT_VERSION,
} from "../lib/partner-intelligence/brand-explorer-profile-preparation-visibility-audit.js";

function parseArgs(argv) {
  const brandsIdx = argv.indexOf("--brands");
  const brands =
    brandsIdx >= 0 && argv[brandsIdx + 1]
      ? argv[brandsIdx + 1].split(",").map((s) => s.trim()).filter(Boolean)
      : null;
  return {
    dryRun: !argv.includes("--apply"),
    onlyLocked: argv.includes("--only-locked"),
    brands,
  };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  console.log(`[${VISIBILITY_AUDIT_VERSION}] dryRun=${opts.dryRun}`);
  const report = await runProfilePreparationVisibilityAudit({
    slugs: opts.brands,
    onlyLocked: opts.onlyLocked,
  });
  report.dryRun = opts.dryRun;
  const paths = writeProfilePreparationVisibilityAuditReports(report);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(
    `Summary: brands=${report.summary.brandCount} preparationShell=${report.summary.profileInPreparationCount} restore=${report.summary.readyToRestore} pendingMigration=${report.summary.pendingMigration}`
  );
  for (const b of report.profileInPreparationBrands || []) {
    console.log(
      `  LOCKED ${b.slug}: ${b.classification} · state=${b.currentDisplayState} · action=${b.recommendedAction}`
    );
  }
  for (const b of (report.brandResults || []).filter((x) =>
    ["legacy_approved_ready_to_restore", "legacy_approved_pending_migration"].includes(x.classification)
  )) {
    console.log(
      `  RESTORE ${b.slug}: ${b.classification} · external=${b.currentExternalBehavior}`
    );
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
