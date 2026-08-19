#!/usr/bin/env node
/**
 * Seed committed published snapshots for all pilot ADP properties.
 *
 *   node scripts/seed-ai-demand-positioning-published.mjs --dry-run
 *   node scripts/seed-ai-demand-positioning-published.mjs --apply
 */

import "../load-env.js";
import { loadLatestPeriod, loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import {
  buildPublishedSnapshotBundle,
  savePublishedSnapshotBundle,
} from "../lib/ai-demand-positioning/published-snapshot.js";

const PILOT_PROPERTIES = [
  "adp_waterstone_boca_raton",
  "adp_renaissance_times_square",
  "adp_cambridge_beaches_bermuda",
];

const dryRun = !process.argv.includes("--apply");

async function main() {
  console.log(`\n=== Seed ADP Published Snapshots ===`);
  console.log(`Mode: ${dryRun ? "DRY RUN" : "APPLY"}\n`);

  const results = [];
  for (const propertyId of PILOT_PROPERTIES) {
    const profile = loadPropertyProfile(propertyId);
    const period = loadLatestPeriod(propertyId);
    if (!profile || !period) {
      results.push({ propertyId, ok: false, error: "missing profile or period" });
      continue;
    }

    const bundle = buildPublishedSnapshotBundle({ period, profile });
    if (!bundle.ok) {
      results.push({ propertyId, ok: false, error: bundle.error });
      continue;
    }

    if (!dryRun) {
      savePublishedSnapshotBundle(bundle, { seed: true });
    }

    results.push({
      propertyId,
      ok: true,
      periodId: bundle.summary.periodId,
      demandCapture: bundle.summary.demandCapture,
      payloadBytes: bundle.summary.payloadBytes,
      evidenceBytes: bundle.summary.evidenceBytes,
    });
  }

  console.table(results);
  if (dryRun) {
    console.log("\n[DRY RUN] Re-run with --apply to write fixtures/ai-demand-positioning/published/");
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
