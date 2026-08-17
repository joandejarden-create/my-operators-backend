#!/usr/bin/env node
import "dotenv/config";
import {
  runLegacyApprovedProfileReconciliation,
  writeLegacyReconciliationReports,
  applyLegacyApprovedProfileReconciliation,
  parseLegacyApplyFlags,
  LEGACY_RECONCILIATION_VERSION,
} from "../lib/partner-intelligence/brand-explorer-legacy-approved-profile-reconciliation.js";

async function main() {
  const argv = process.argv.slice(2);
  const flags = parseLegacyApplyFlags(argv);
  console.log(`[${LEGACY_RECONCILIATION_VERSION}] dryRun=${!flags.apply}`);
  const report = await runLegacyApprovedProfileReconciliation({});
  report.dryRun = !flags.apply;
  const paths = writeLegacyReconciliationReports(report);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(
    `Summary: brands=${report.summary.brandCount} migrate=${report.summary.migrateCount} lockedHistorical=${report.summary.lockedHistoricallyApproved}`
  );
  for (const b of report.brandResults.filter((x) =>
    ["migrate_to_active_profile_ready", "image_remediation_required", "needs_new_gate_validation_before_migration"].includes(
      x.classification
    )
  )) {
    console.log(`  ${b.brandSlug}: ${b.classification} · external=${b.currentExternalState} · os=${b.currentOsState}`);
  }

  if (flags.apply) {
    if (!flags.ok) {
      console.error(`Missing apply flags: ${flags.missing.join(", ")}`);
      process.exit(2);
    }
    const applied = await applyLegacyApprovedProfileReconciliation({
      report,
      apply: true,
      argv,
    });
    console.log(`Apply: ${JSON.stringify(applied)}`);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
