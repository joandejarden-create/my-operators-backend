#!/usr/bin/env node
/**
 * Backfill Amenities (and blank Hotel Description) for CALA SLH census rows
 * from slh.com hotelsearchresults keyFeatures.
 *
 *   node scripts/apply-slh-census-amenities-backfill.mjs --audit
 *   node scripts/apply-slh-census-amenities-backfill.mjs --dry-run
 *   node scripts/apply-slh-census-amenities-backfill.mjs --apply
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import Airtable from "airtable";
import {
  auditSlhAmenitiesCoverage,
  planSlhCensusAmenitiesBackfill,
} from "../lib/hotel-census/plan-slh-census-amenities-backfill.js";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";

const REPORT = join("reports", "slh-census-amenities-backfill-plan.json");
const LOG = join("reports", "slh-census-amenities-backfill-apply-log.json");
const BATCH = 10;

function parseArgs() {
  const args = process.argv.slice(2);
  return {
    audit: args.includes("--audit"),
    apply: args.includes("--apply"),
    dryRun: args.includes("--dry-run"),
    refreshAmenities: args.includes("--refresh-amenities"),
  };
}

async function main() {
  const opts = parseArgs();
  mkdirSync("reports", { recursive: true });

  if (opts.audit || (!opts.apply && !opts.dryRun)) {
    const audit = await auditSlhAmenitiesCoverage();
    console.log("=== SLH CALA Amenities coverage ===\n");
    console.log("Total Affiliation=SLH rows:", audit.total);
    console.log("Blank Amenities:", audit.blankAmenities);
    if (!opts.apply && !opts.dryRun) {
      console.log("\nRun --dry-run or --apply to backfill from slh.com keyFeatures.");
      return;
    }
  }

  if (!opts.apply && !opts.dryRun) return;

  console.log(`\n=== Plan (${opts.apply && !opts.dryRun ? "LIVE" : "DRY RUN"}) ===\n`);
  const plan = await planSlhCensusAmenitiesBackfill({
    refreshAmenities: opts.refreshAmenities,
    onProgress: (msg) => console.log(" ", msg),
  });
  writeFileSync(REPORT, JSON.stringify(plan, null, 2));

  console.log("Scanned:", plan.censusRowsScanned);
  console.log("Ready:", plan.readyToApply);
  console.log("Skipped:", plan.skipped.length);
  console.log("Report:", REPORT);

  for (const row of plan.planRows.slice(0, 12)) {
    console.log(`  ${row.censusName} | ${Object.keys(row.applyFields).join(", ")}`);
    console.log(`    ${String(row.amenitiesText || "").slice(0, 100)}`);
  }

  if (opts.dryRun) return;

  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );

  let updated = 0;
  /** @type {{ id: string, fields: Record<string, unknown> }[]} */
  let batch = [];
  /** @type {object[]} */
  const logRows = [];

  for (const row of plan.planRows) {
    batch.push({ id: row.censusRecordId, fields: row.applyFields });
    logRows.push({ ...row, action: "update" });
    if (batch.length >= BATCH) {
      await base(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
      updated += batch.length;
      console.log(`Updated ${updated}/${plan.planRows.length}`);
      batch = [];
    }
  }
  if (batch.length) {
    await base(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
    updated += batch.length;
  }

  const postAudit = await auditSlhAmenitiesCoverage();
  writeFileSync(LOG, JSON.stringify({ generatedAt: new Date().toISOString(), updated, postAudit, logRows }, null, 2));
  console.log(`\nApplied ${updated}. Blank Amenities now: ${postAudit.blankAmenities}/${postAudit.total}`);
  console.log("Log:", LOG);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
