#!/usr/bin/env node
/**
 * Backfill Design Hotels census amenities from marriott.com HWS subpages using MARSHA / Property ID.
 *
 *   node scripts/apply-design-hotels-census-marriott-amenities-backfill.mjs --audit
 *   node scripts/apply-design-hotels-census-marriott-amenities-backfill.mjs --dry-run
 *   node scripts/apply-design-hotels-census-marriott-amenities-backfill.mjs --apply
 *   node scripts/apply-design-hotels-census-marriott-amenities-backfill.mjs --apply --record-id=recPtyDuFADdczVA3
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import Airtable from "airtable";
import {
  auditDesignHotelsMarriottAmenitiesTargets,
  planDesignHotelsCensusMarriottAmenitiesBackfill,
} from "../lib/hotel-census/plan-design-hotels-census-marriott-amenities-backfill.js";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";

const REPORT = join("reports", "design-hotels-census-marriott-amenities-backfill-plan.json");
const LOG = join("reports", "design-hotels-census-marriott-amenities-backfill-apply-log.json");
const BATCH = 10;

function parseArgs() {
  const args = process.argv.slice(2);
  const get = (flag) => {
    const eq = args.find((a) => a.startsWith(`${flag}=`));
    if (eq) return eq.slice(flag.length + 1);
    const i = args.indexOf(flag);
    return i >= 0 ? args[i + 1] : null;
  };
  return {
    audit: args.includes("--audit"),
    apply: args.includes("--apply"),
    dryRun: args.includes("--dry-run"),
    refreshAmenities: args.includes("--refresh-amenities"),
    fetchDelayMs: Number(get("--fetch-delay-ms") || 700),
    limit: Number(get("--limit") || 0),
    recordId: get("--record-id"),
  };
}

async function main() {
  const opts = parseArgs();

  if (opts.audit || (!opts.apply && !opts.dryRun)) {
    const audit = await auditDesignHotelsMarriottAmenitiesTargets();
    console.log("=== Design Hotels MARSHA amenities targets ===\n");
    console.log("Total CALA rows:", audit.total);
    console.log("With Property ID:", audit.withMarsha);
    console.log("Blank amenities (with MARSHA):", audit.blankAmenitiesWithMarsha);
    if (audit.blankAmenityNames.length) {
      console.log("Blank:", audit.blankAmenityNames.join(", "));
    }
    if (!opts.apply && !opts.dryRun) {
      console.log("\nRun --dry-run or --apply to fetch marriott.com subpages.");
      return;
    }
  }

  if (!opts.apply && !opts.dryRun) return;

  console.log(`\n=== Plan (${opts.apply && !opts.dryRun ? "LIVE" : "DRY RUN"}) ===\n`);

  const plan = await planDesignHotelsCensusMarriottAmenitiesBackfill({
    refreshAmenities: opts.refreshAmenities,
    fetchDelayMs: opts.fetchDelayMs,
    limit: opts.limit,
    recordIds: opts.recordId ? [opts.recordId] : undefined,
    onProgress: (msg) => console.log(" ", msg),
  });

  mkdirSync("reports", { recursive: true });
  writeFileSync(REPORT, JSON.stringify(plan, null, 2), "utf8");

  console.log("\nScanned:", plan.censusRowsScanned);
  console.log("Ready to apply:", plan.readyToApply);
  console.log("Skipped:", plan.skipped.length);
  console.log("Fetch errors:", plan.fetchErrors.length);
  console.log("Report:", REPORT);

  for (const row of plan.planRows) {
    const fields = Object.keys(row.applyFields).join(", ");
    const amenPreview = String(row.applyFields.Amenities || row.amenitiesTextSuggested || "").slice(0, 80);
    console.log(`  ${row.marshaCode} | ${row.censusName} | ${fields}`);
    if (amenPreview) console.log(`    amenities: ${amenPreview}${amenPreview.length >= 80 ? "…" : ""}`);
  }

  if (opts.dryRun) return;

  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );

  /** @type {object[]} */
  const logRows = [];
  let updated = 0;
  /** @type {{ id: string, fields: Record<string, unknown> }[]} */
  let batch = [];

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

  const postAudit = await auditDesignHotelsMarriottAmenitiesTargets();
  writeFileSync(
    LOG,
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        updated,
        postAudit,
        logRows,
      },
      null,
      2
    ),
    "utf8"
  );

  console.log(`\nApplied ${updated} update(s).`);
  console.log(`Blank amenities (with MARSHA): ${postAudit.blankAmenitiesWithMarsha}`);
  console.log("Log:", LOG);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
