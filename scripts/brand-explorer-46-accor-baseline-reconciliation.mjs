#!/usr/bin/env node
/**
 * Protected 46 Accor Wave 13 baseline reconciliation (code/report only).
 *
 *   npm run brand-explorer-46-accor-baseline-reconciliation -- --dry-run
 *   npm run brand-explorer-46-accor-baseline-reconciliation -- --apply \
 *     --approve-accor-baseline-resolver-reconciliation \
 *     --confirm-code-or-report-only \
 *     --confirm-no-airtable-writes \
 *     --confirm-no-presentation-writes \
 *     --confirm-no-image-writes \
 *     --confirm-no-brand-status-changes \
 *     --confirm-no-release-field-writes \
 *     --confirm-no-company-validation-changes \
 *     --confirm-no-source-library-status-changes \
 *     --confirm-no-registry-approval-changes \
 *     --confirm-no-wave14-writes \
 *     --confirm-no-gate-weakening \
 *     --confirm-footnote-enriched-path-preserved
 *
 * Optional: --refresh-accor-quality (merge Accor 24-tab re-audit into canonical)
 * Optional: --probe-pvql (live Accor PVQL during probe)
 */
import "../load-env.js";
import {
  ACCOR_BASELINE_RECONCILIATION_VERSION,
  APPLY_FLAGS,
  runAccorBaselineReconciliation,
  writeReconciliationReports,
  updateWave14WatchNote,
} from "../lib/partner-intelligence/brand-explorer-46-accor-baseline-reconciliation.js";

async function main() {
  const argv = process.argv.slice(2);
  const apply = argv.includes("--apply");
  const dryRun = argv.includes("--dry-run") || !apply;
  if (apply && dryRun && !argv.includes("--dry-run")) {
    // apply wins when both absent of explicit dry-run conflict
  }
  if (!apply && !argv.includes("--dry-run")) {
    console.error("Require --dry-run or --apply");
    process.exit(2);
  }
  if (apply && argv.includes("--dry-run")) {
    console.error("Pass either --dry-run or --apply, not both");
    process.exit(2);
  }

  console.log(
    `[${ACCOR_BASELINE_RECONCILIATION_VERSION}] ${apply ? "apply (code/report only)" : "dry-run"}`
  );
  if (apply) {
    const missing = APPLY_FLAGS.filter((f) => !argv.includes(f));
    if (missing.length) {
      console.error(`Missing apply flags:\n  ${missing.join("\n  ")}`);
      process.exit(2);
    }
  }

  const { report, failurePaths } = await runAccorBaselineReconciliation({
    apply,
    argv,
    refreshQuality: apply && argv.includes("--refresh-accor-quality"),
    probePvql: argv.includes("--probe-pvql"),
  });

  const paths = writeReconciliationReports(report);
  const watchPath = updateWave14WatchNote(report);

  console.log(`Failures: ${failurePaths.jsonPath}`);
  console.log(`Failures: ${failurePaths.mdPath}`);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(`Wrote ${paths.docsPath}`);
  console.log(`Updated ${watchPath}`);
  console.log(`Active universe: ${report.resolverProbe?.activeUniverseCount}`);
  console.log(`Resolver allPass: ${report.resolverProbe?.allResolverPass}`);
  console.log(`Quality freeze: ${report.qualityFreezeCount}/46`);
  console.log(`Ready: ${report.readyState}`);
  console.log(`Airtable writes: ${report.airtableWrites}`);

  for (const p of report.resolverProbe?.probes || []) {
    console.log(
      `  ${p.slug}: resolver=${p.resolverPass} inUniverse=${p.inActiveUniverse} liveSlug=${p.liveUniverseSlug || "—"} pvql=${p.pvql ? (p.pvql.lockPass ? "PASS" : `FAIL:${(p.pvql.failures || []).join("|")}`) : "skipped"}`
    );
  }

  if (report.contentDefectStop) {
    console.error("STOP: classified as real content/release defect — no auto patch");
    process.exit(4);
  }
  if (apply && report.readyState !== "protected_46_accor_baseline_reconciled_wave14_may_resume") {
    // Apply may still need post-validation (full PVQL / baseline test)
    if (!report.resolverProbe?.allResolverPass) process.exit(3);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
