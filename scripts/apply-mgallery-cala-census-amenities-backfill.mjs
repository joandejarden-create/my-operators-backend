#!/usr/bin/env node
/**
 * Backfill Amenities for CALA MGallery Collection census rows from all.accor.com.
 *
 *   node scripts/apply-mgallery-cala-census-amenities-backfill.mjs --dry-run
 *   node scripts/apply-mgallery-cala-census-amenities-backfill.mjs --apply
 *   node scripts/apply-mgallery-cala-census-amenities-backfill.mjs --apply --refresh-amenities
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import Airtable from "airtable";
import { planMgalleryAmenitiesBackfill } from "../lib/mgallery-census-enrichment.js";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";

const REPORT = join("reports", "mgallery-cala-amenities-backfill-plan.json");
const LOG = join("reports", "mgallery-cala-amenities-backfill-apply-log.json");
const BATCH = 10;

const APPLY = process.argv.includes("--apply");
const DRY_RUN = process.argv.includes("--dry-run") || !APPLY;
const REFRESH = process.argv.includes("--refresh-amenities");

async function main() {
  mkdirSync("reports", { recursive: true });
  console.log(`=== MGallery amenities (${DRY_RUN ? "DRY RUN" : "LIVE"}${REFRESH ? " refresh" : ""}) ===\n`);

  const plan = await planMgalleryAmenitiesBackfill({
    refreshAmenities: REFRESH,
    onProgress: (m) => console.log(" ", m),
  });
  writeFileSync(REPORT, JSON.stringify(plan, null, 2));

  console.log("Catalog MGA:", plan.catalogCount);
  console.log("Census scanned:", plan.censusRowsScanned);
  console.log("Ready:", plan.readyToApply);
  console.log("Skipped:", plan.skipped.length);
  console.log("Steward extras (not on catalog):", plan.stewardExtras.length);
  plan.stewardExtras.forEach((s) => console.log("  EXTRA", s.censusName));
  plan.planRows.forEach((r) =>
    console.log(`  ${r.propertyId} | ${r.censusName} | ${r.amenitiesText.slice(0, 100)}`)
  );

  if (DRY_RUN) {
    console.log("\nDry-run only — pass --apply to write.");
    return;
  }

  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );
  let updated = 0;
  let batch = [];
  const logRows = [];
  for (const row of plan.planRows) {
    batch.push({ id: row.censusRecordId, fields: row.applyFields });
    logRows.push(row);
    if (batch.length >= BATCH) {
      await base(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
      updated += batch.length;
      batch = [];
    }
  }
  if (batch.length) {
    await base(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
    updated += batch.length;
  }
  writeFileSync(LOG, JSON.stringify({ generatedAt: new Date().toISOString(), updated, logRows }, null, 2));
  console.log(`\nApplied ${updated}. Log: ${LOG}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
