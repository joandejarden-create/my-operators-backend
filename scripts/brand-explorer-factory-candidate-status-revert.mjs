#!/usr/bin/env node
/**
 * Revert factory candidates Brand Status → Under Review.
 *
 *   npm run brand-explorer-factory-candidate-status-revert -- --dry-run
 *   npm run brand-explorer-factory-candidate-status-revert -- --apply \
 *     --approve-factory-candidate-status-revert \
 *     --confirm-brand-status-only \
 *     --confirm-under-review-target \
 *     --confirm-no-company-validation-changes \
 *     --confirm-no-source-library-status-changes \
 *     --confirm-no-registry-approval-changes \
 *     --confirm-no-presentation-writes
 */
import "../load-env.js";
import {
  REQUIRED_APPLY_FLAGS,
  WRITER_VERSION,
  runFactoryCandidateStatusRevert,
  writeStatusRevertReports,
} from "../lib/partner-intelligence/brand-explorer-factory-candidate-status-revert.js";

function hasFlag(name) {
  return process.argv.includes(name);
}

async function main() {
  const apply = hasFlag("--apply");
  const dryRun = hasFlag("--dry-run") || !apply;
  const approveFlags = REQUIRED_APPLY_FLAGS.every((f) => hasFlag(f));
  if (apply && !approveFlags) {
    console.error(`[${WRITER_VERSION}] Apply requires:\n  ${REQUIRED_APPLY_FLAGS.join("\n  ")}`);
    process.exit(1);
  }

  console.log(`[${WRITER_VERSION}] ${dryRun ? "dry-run" : "APPLY"}`);
  const report = await runFactoryCandidateStatusRevert({
    apply: apply && !dryRun,
    approveFlags,
  });
  const paths = writeStatusRevertReports(report);
  console.log(`Wrote ${paths.mdPath}`);
  for (const c of report.candidates) {
    if (c.error) console.log(`  FAIL ${c.slug}: ${c.error}`);
    else
      console.log(
        `  ${c.slug}: ${c.currentBrandStatus} → ${c.targetBrandStatus} (needsRevert=${c.needsRevert})`
      );
  }
  console.log(`Applied: ${report.applied?.length || 0} · Errors: ${report.errors?.length || 0}`);
  if (report.errors?.length) process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
