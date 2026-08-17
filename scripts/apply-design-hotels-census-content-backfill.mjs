#!/usr/bin/env node
/**
 * Backfill Design Hotels CALA census description, amenities, rooms, and Y/N flags
 * from designhotels.com overview / fact sheet pages.
 *
 *   node scripts/apply-design-hotels-census-content-backfill.mjs --audit
 *   node scripts/apply-design-hotels-census-content-backfill.mjs --dry-run
 *   node scripts/apply-design-hotels-census-content-backfill.mjs --apply
 *   node scripts/apply-design-hotels-census-content-backfill.mjs --apply --limit=5
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import Airtable from "airtable";
import {
  auditDesignHotelsCensusContentBlanks,
  planDesignHotelsCensusContentBackfill,
} from "../lib/hotel-census/plan-design-hotels-census-content-backfill.js";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";
import { parseDesignHotelsOverviewHtml } from "../lib/design-hotels-hotel-content-fetch.js";

const REPORT = join("reports", "design-hotels-census-content-backfill-plan.json");
const LOG = join("reports", "design-hotels-census-content-backfill-apply-log.json");
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
    limit: Number(get("--limit") || 0),
    fetchDelayMs: Number(get("--fetch-delay-ms") || 800),
    file: get("--file"),
    recordId: get("--record-id"),
  };
}

async function main() {
  const opts = parseArgs();

  if (opts.file && !opts.apply) {
    const { readFileSync } = await import("node:fs");
    const html = readFileSync(opts.file, "utf8");
    console.log(JSON.stringify(parseDesignHotelsOverviewHtml(html), null, 2));
    return;
  }

  if (opts.audit || (!opts.apply && !opts.dryRun)) {
    const audit = await auditDesignHotelsCensusContentBlanks();
    console.log("=== Design Hotels CALA content blanks ===\n");
    console.log("Total rows:", audit.total);
    console.log("Needs content backfill:", audit.needsContent);
    for (const [field, count] of Object.entries(audit.blankCounts)) {
      console.log(`  Blank ${field}: ${count}/${audit.total}`);
    }
    if (!opts.apply && !opts.dryRun) {
      console.log("\nRun --dry-run or --apply to fetch designhotels.com pages.");
      return;
    }
  }

  if (!opts.apply && !opts.dryRun) return;

  console.log(`\n=== Plan (${opts.apply && !opts.dryRun ? "LIVE" : "DRY RUN"}) ===\n`);

  const plan = await planDesignHotelsCensusContentBackfill({
    limit: opts.limit,
    fetchDelayMs: opts.fetchDelayMs,
    recordIds: opts.recordId ? [opts.recordId] : undefined,
    onProgress: (msg) => console.log(" ", msg),
  });

  mkdirSync("reports", { recursive: true });
  writeFileSync(REPORT, JSON.stringify(plan, null, 2), "utf8");

  console.log("\nScanned:", plan.censusRowsScanned);
  console.log("Ready:", plan.readyToApply);
  console.log("Skipped:", plan.skipped.length);
  console.log("Fetch errors:", plan.fetchErrors.length);
  console.log("Report:", REPORT);

  if (opts.dryRun) {
    for (const row of plan.planRows.slice(0, 10)) {
      console.log(`\n[dry-run] ${row.censusName} (${row.censusRecordId})`);
      console.log(" fields:", Object.keys(row.applyFields).join(", "));
      console.log(" desc len:", row.descriptionSuggested?.length || 0);
      console.log(" amenities len:", row.amenitiesTextSuggested?.length || 0);
      console.log(" rooms:", row.roomsSuggested ?? "-");
    }
    return;
  }

  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT;
  const base = new Airtable({ apiKey }).base(baseId);

  /** @type {object[]} */
  const logRows = [];
  let updated = 0;
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

  writeFileSync(
    LOG,
    JSON.stringify(
      {
        appliedAt: new Date().toISOString(),
        updated,
        logRows,
        skipped: plan.skipped,
        fetchErrors: plan.fetchErrors,
      },
      null,
      2
    ),
    "utf8"
  );

  console.log(`\nDone: ${updated} updated`);
  console.log("Log:", LOG);

  const after = await auditDesignHotelsCensusContentBlanks();
  console.log("\n=== After ===");
  console.log("Needs content:", after.needsContent);
  console.log("Blank Amenities:", after.blankCounts.Amenities);
  console.log("Blank Hotel Description:", after.blankCounts["Hotel Description"]);
  console.log("Blank rooms:", after.blankCounts.rooms);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
