#!/usr/bin/env node
/**
 * Backfill Hotel Census Property ID for Hilton-parent rows from hilton.com ctyhocn codes.
 *
 *   node scripts/sync-hilton-census-property-id.mjs
 *   node scripts/sync-hilton-census-property-id.mjs --apply
 *   node scripts/sync-hilton-census-property-id.mjs --apply --fill-blank-only
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import { planHiltonCensusPropertyIdSync } from "../lib/hotel-census/plan-hilton-census-property-id-sync.js";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const BATCH = 10;

function parseArgs() {
  const args = process.argv.slice(2);
  return {
    apply: args.includes("--apply"),
    dryRun: args.includes("--dry-run"),
    fillBlankOnly: args.includes("--fill-blank-only"),
  };
}

function csvEscape(v) {
  const s = String(v ?? "");
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

async function main() {
  const opts = parseArgs();

  console.log("=== Plan Hilton Census Property ID sync ===\n");

  const plan = await planHiltonCensusPropertyIdSync({
    fillBlankOnly: opts.fillBlankOnly,
  });

  mkdirSync(REPORTS, { recursive: true });
  const jsonPath = join(REPORTS, "hilton-census-property-id-sync-plan.json");
  writeFileSync(jsonPath, JSON.stringify({ generatedAt: new Date().toISOString(), ...plan }, null, 2));

  const skipReasons = {};
  for (const s of plan.skipped) {
    skipReasons[s.reason] = (skipReasons[s.reason] || 0) + 1;
  }

  console.log("=== Summary ===");
  console.log("  Field:", plan.propertyIdField);
  console.log("  Enrichment plan matches:", plan.enrichmentPlanMatches);
  console.log("  Census rows scanned:", plan.censusRowsScanned);
  console.log("  Ready to apply:", plan.readyToApply);
  console.log("  Skipped:", plan.skipped.length, skipReasons);
  console.log("Report:", jsonPath);

  if (!opts.apply) {
    console.log("\nRun with --apply to write Property ID to Hotel Census.");
    return;
  }

  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT;
  const base = new Airtable({ apiKey }).base(baseId);

  let updated = 0;
  let errors = 0;
  let batch = [];
  const log = [];

  for (const row of plan.planRows) {
    log.push({
      recordId: row.censusRecordId,
      name: row.censusName,
      ctyhocn: row.ctyhocn,
      source: row.source,
      previousPropertyId: row.currentPropertyId,
    });
    batch.push({ id: row.censusRecordId, fields: row.applyFields });
    if (batch.length >= BATCH) {
      if (!opts.dryRun) {
        try {
          await base(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
          updated += batch.length;
        } catch (err) {
          errors += batch.length;
          console.error("Batch failed:", err?.message || err);
        }
      } else updated += batch.length;
      batch = [];
    }
  }
  if (batch.length) {
    if (!opts.dryRun) {
      try {
        await base(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
        updated += batch.length;
      } catch (err) {
        errors += batch.length;
        console.error("Batch failed:", err?.message || err);
      }
    } else updated += batch.length;
  }

  const logPath = join(REPORTS, "hilton-census-property-id-apply-log.csv");
  writeFileSync(
    logPath,
    `recordId,name,ctyhocn,source,previousPropertyId\n${log
      .map((r) =>
        [r.recordId, csvEscape(r.name), r.ctyhocn, r.source, csvEscape(r.previousPropertyId)].join(",")
      )
      .join("\n")}\n`
  );

  console.log(`\n${opts.dryRun ? "Would update" : "Updated"}: ${updated}`);
  console.log("Errors:", errors);
  console.log("Log:", logPath);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
