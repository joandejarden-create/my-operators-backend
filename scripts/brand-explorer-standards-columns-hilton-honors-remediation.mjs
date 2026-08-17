#!/usr/bin/env node
/**
 * Standards columns + Hilton Honors Diamond Reserve remediation.
 *
 *   npm run brand-explorer-standards-columns-hilton-honors-remediation -- --dry-run
 *   npm run brand-explorer-standards-columns-hilton-honors-remediation -- --apply \
 *     --approve-standards-columns-hilton-honors-remediation \
 *     --confirm-presentation-only \
 *     --confirm-no-brand-status-changes \
 *     --confirm-no-company-validation-changes \
 *     --confirm-no-source-library-status-changes \
 *     --confirm-no-registry-approval-changes
 */
import "../load-env.js";
import {
  REQUIRED_APPLY_FLAGS,
  WRITER_VERSION,
  runStandardsColumnsHiltonHonorsRemediation,
  writeRemediationReports,
} from "../lib/partner-intelligence/brand-explorer-standards-columns-hilton-honors-remediation.js";

function hasFlag(name) {
  return process.argv.includes(name);
}

function argValue(name) {
  const i = process.argv.indexOf(name);
  if (i < 0) return "";
  return process.argv[i + 1] || "";
}

async function main() {
  const apply = hasFlag("--apply");
  const dryRun = hasFlag("--dry-run") || !apply;
  const brandsArg = argValue("--brands");
  const slugs = brandsArg
    ? brandsArg
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean)
    : null;

  const approveFlags = REQUIRED_APPLY_FLAGS.every((f) => hasFlag(f));
  if (apply && !approveFlags) {
    console.error(`[${WRITER_VERSION}] Apply requires:\n  ${REQUIRED_APPLY_FLAGS.join("\n  ")}`);
    process.exit(1);
  }

  console.log(`[${WRITER_VERSION}] ${dryRun ? "dry-run" : "APPLY"}`);
  const report = await runStandardsColumnsHiltonHonorsRemediation({
    apply: apply && !dryRun,
    approveFlags,
    slugs,
  });
  const paths = writeRemediationReports(report);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Patches: ${report.patchCount} · Applied: ${report.applied?.length || 0} · Errors: ${report.errors?.length || 0}`);
  for (const b of report.brandResults || []) {
    if (b.error) {
      console.log(`  FAIL ${b.slug}: ${b.error}`);
      continue;
    }
    console.log(
      `  ${b.slug}: standards=${b.standardsPatches} loyaltyCreates=${b.loyaltyCreates} DR_before=${b.hasDiamondReserveBefore}`
    );
  }
  if (report.errors?.length) process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
