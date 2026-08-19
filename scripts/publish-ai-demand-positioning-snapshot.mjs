#!/usr/bin/env node
/**
 * Publish AI Demand Positioning snapshot from latest monitoring period.
 *
 * Usage:
 *   node scripts/publish-ai-demand-positioning-snapshot.mjs --property adp_waterstone_boca_raton --dry-run
 *   node scripts/publish-ai-demand-positioning-snapshot.mjs --property adp_waterstone_boca_raton --apply
 *   node scripts/publish-ai-demand-positioning-snapshot.mjs --property adp_waterstone_boca_raton --apply --seed
 *   node scripts/publish-ai-demand-positioning-snapshot.mjs --property adp_waterstone_boca_raton --apply --airtable
 *
 * Options:
 *   --seed       Write to fixtures/ai-demand-positioning/published (committed deploy seed)
 *   --airtable   Upsert Live row to Airtable (requires ADP_AIRTABLE_PUBLISH_APPLY=true with --apply)
 *   --census-id  Optional Hotel Property Census rec… id for linking
 */

import "../load-env.js";
import { loadLatestPeriod, loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import {
  buildPublishedSnapshotBundle,
  savePublishedSnapshotBundle,
} from "../lib/ai-demand-positioning/published-snapshot.js";
import { upsertPublishedReportToAirtable } from "../lib/ai-demand-positioning/airtable-published-report.js";
import { resolveCensusRecordIdForPublish } from "../lib/ai-demand-positioning/census-link-registry.js";

const args = process.argv.slice(2);
const propertyId = args.find((a, i) => args[i - 1] === "--property");
const censusArg = args.find((a, i) => args[i - 1] === "--census-id") || null;
const dryRun = !args.includes("--apply");
const seed = args.includes("--seed");
const airtable = args.includes("--airtable");

if (!propertyId) {
  console.error("Usage: --property <adp_property_id> [--apply] [--seed] [--airtable] [--census-id rec…]");
  process.exit(1);
}

async function main() {
  const profile = loadPropertyProfile(propertyId);
  if (!profile) {
    console.error(`Property profile not found: ${propertyId}`);
    process.exit(1);
  }

  const period = loadLatestPeriod(propertyId);
  if (!period) {
    console.error(`No monitoring period found for ${propertyId}`);
    process.exit(1);
  }

  const censusRecordId = resolveCensusRecordIdForPublish(propertyId, censusArg);

  const bundle = buildPublishedSnapshotBundle({ period, profile, censusRecordId });
  if (!bundle.ok) {
    console.error("Failed to build published snapshot:", bundle.error || bundle.message);
    process.exit(1);
  }

  console.log("\n=== ADP Published Snapshot ===");
  console.log(`Property: ${profile.name}`);
  console.log(`Period: ${bundle.summary.periodId}`);
  console.log(`Demand Capture: ${bundle.summary.demandCapture}`);
  console.log(`Payload size: ${bundle.summary.payloadBytes} bytes`);
  console.log(`Evidence index size: ${bundle.summary.evidenceBytes} bytes`);
  console.log(`Mode: ${dryRun ? "DRY RUN" : "APPLY"}${seed ? " (seed)" : ""}${airtable ? " + Airtable" : ""}`);

  if (dryRun) {
    console.log("\n[DRY RUN] Manifest preview:");
    console.log(JSON.stringify(bundle.manifest, null, 2));
    console.log("\nNo files written.");
    return;
  }

  const saved = savePublishedSnapshotBundle(bundle, { seed });
  console.log(`\nSaved published snapshot:`);
  console.log(`  ${saved.manifestFile}`);
  console.log(`  ${saved.reportFile}`);
  console.log(`  ${saved.evidenceFile}`);

  if (airtable) {
    if (process.env.ADP_AIRTABLE_PUBLISH_APPLY !== "true") {
      console.error("\nAirtable publish blocked: set ADP_AIRTABLE_PUBLISH_APPLY=true to write.");
      process.exit(1);
    }
    const airtableResult = await upsertPublishedReportToAirtable(bundle, {
      dryRun: false,
      censusRecordId,
      payloadStoreRef: `published/${propertyId}/${bundle.manifest.reportFile}`,
      linkedCensusRecordIds: censusRecordId ? [censusRecordId] : undefined,
    });
    console.log(`\nAirtable upsert: record ${airtableResult.recordId}`);
    console.log("Field mapping:", airtableResult.fieldMapping.table);
  }

  console.log("\nDone.");
}

main().catch((err) => {
  console.error("Fatal error:", err.message);
  if (err.validation) console.error(JSON.stringify(err.validation, null, 2));
  process.exit(1);
});
