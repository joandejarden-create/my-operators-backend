#!/usr/bin/env node
/**
 * Wave 15 Medium cleanup + Spark featured_application blocker.
 *
 *   npm run brand-explorer-wave15-medium-cleanup -- --dry-run
 *   npm run brand-explorer-wave15-medium-cleanup -- --apply [flags...]
 */
import "dotenv/config";
import {
  WAVE15_MEDIUM_CLEANUP_VERSION,
  WAVE15_MEDIUM_CLEANUP_APPLY_FLAGS,
  runWave15MediumCleanup,
} from "../lib/partner-intelligence/brand-explorer-wave15-medium-cleanup.js";

async function main() {
  const argv = process.argv.slice(2);
  const apply = argv.includes("--apply");
  if (apply && argv.includes("--dry-run")) {
    console.error("Pass either --dry-run or --apply, not both.");
    process.exit(2);
  }

  console.log(`[${WAVE15_MEDIUM_CLEANUP_VERSION}] ${apply ? "APPLY" : "dry-run"}`);
  if (apply) {
    console.log(`Required flags (${WAVE15_MEDIUM_CLEANUP_APPLY_FLAGS.length}):`);
    for (const f of WAVE15_MEDIUM_CLEANUP_APPLY_FLAGS) console.log(`  ${f}`);
  }

  const report = await runWave15MediumCleanup({
    dryRun: !apply,
    argv: apply ? argv : [...argv, "--dry-run"],
  });

  console.log(`Planned: ${report.plannedCount} · patches needing write: ${report.patchCount}`);
  console.log(`Airtable writes: ${report.airtableWrites}`);
  console.log(`Ready: ${report.readyStatement}`);
  if (report.flagCheck?.missing?.length) {
    console.error(`Missing flags: ${report.flagCheck.missing.join(" ")}`);
  }
  if (
    report.readyStatement?.includes("blocked") ||
    report.readyStatement?.includes("incomplete") ||
    report.applyResults?.some((r) => r.applied === false)
  ) {
    process.exitCode = 2;
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
