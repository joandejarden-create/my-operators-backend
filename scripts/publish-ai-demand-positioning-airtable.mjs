#!/usr/bin/env node
/**
 * Publish all pilot ADP snapshots to the dedicated Airtable base.
 *
 *   node scripts/publish-ai-demand-positioning-airtable.mjs --dry-run
 *   ADP_AIRTABLE_PUBLISH_APPLY=true node scripts/publish-ai-demand-positioning-airtable.mjs --apply
 *
 * Requires ADP_AIRTABLE_BASE_ID in .env (from adp:create-base --apply).
 */

import "../load-env.js";
import { loadLatestPeriod, loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import {
  buildPublishedSnapshotBundle,
} from "../lib/ai-demand-positioning/published-snapshot.js";
import { upsertPublishedReportToAirtable } from "../lib/ai-demand-positioning/airtable-published-report.js";
import { resolveCensusRecordIdForPublish } from "../lib/ai-demand-positioning/census-link-registry.js";

const PILOT_PROPERTIES = [
  "adp_waterstone_boca_raton",
  "adp_renaissance_times_square",
  "adp_cambridge_beaches_bermuda",
];

const dryRun = !process.argv.includes("--apply");

async function main() {
  if (!process.env.ADP_AIRTABLE_BASE_ID) {
    console.error("Set ADP_AIRTABLE_BASE_ID in .env (run adp:create-base --apply first).");
    process.exit(1);
  }

  console.log(`\n=== Publish ADP to Airtable ===`);
  console.log(`Base: ${process.env.ADP_AIRTABLE_BASE_ID}`);
  console.log(`Mode: ${dryRun ? "DRY RUN" : "APPLY"}\n`);

  if (!dryRun && process.env.ADP_AIRTABLE_PUBLISH_APPLY !== "true") {
    console.error("Blocked: set ADP_AIRTABLE_PUBLISH_APPLY=true to write.");
    process.exit(1);
  }

  const results = [];
  for (const propertyId of PILOT_PROPERTIES) {
    const profile = loadPropertyProfile(propertyId);
    const period = loadLatestPeriod(propertyId);
    if (!profile || !period) {
      results.push({ propertyId, ok: false, error: "missing profile or period" });
      continue;
    }

    const censusRecordId = resolveCensusRecordIdForPublish(propertyId);
    const bundle = buildPublishedSnapshotBundle({ period, profile, censusRecordId });
    if (!bundle.ok) {
      results.push({ propertyId, ok: false, error: bundle.error });
      continue;
    }

    try {
      const upsert = await upsertPublishedReportToAirtable(bundle, {
        dryRun,
        censusRecordId,
        payloadStoreRef: `published/${propertyId}/${bundle.manifest.reportFile}`,
      });
      results.push({
        propertyId,
        ok: true,
        recordId: upsert.recordId || "(dry-run)",
        demandCapture: bundle.summary.demandCapture,
        censusRecordId: censusRecordId || "(none)",
      });
    } catch (err) {
      results.push({ propertyId, ok: false, error: err.message });
    }
  }

  console.table(results);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
