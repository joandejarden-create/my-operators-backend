#!/usr/bin/env node
/**
 * Audit and backfill Hilton census fields (fill-blank):
 * Hotel Description, Amenities, Website, Open Date, Year Affiliated
 *
 *   node scripts/backfill-hilton-census-fields.mjs --audit
 *   node scripts/backfill-hilton-census-fields.mjs --apply
 *   node scripts/backfill-hilton-census-fields.mjs --apply --dry-run
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import {
  auditHiltonCensusFieldBlanks,
  planHiltonCensusFieldBackfill,
} from "../lib/hotel-census/plan-hilton-census-field-backfill.js";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";
import { CENSUS_FORMULA_AFFILIATION_FIELDS } from "../lib/hotel-census/hilton-census-field-backfill-contract.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const BATCH = 10;

function parseArgs() {
  const args = process.argv.slice(2);
  return {
    audit: args.includes("--audit"),
    apply: args.includes("--apply"),
    dryRun: args.includes("--dry-run"),
    fetchDelayMs: (() => {
      const i = args.indexOf("--fetch-delay-ms");
      return i >= 0 ? Number(args[i + 1] || 200) : 200;
    })(),
  };
}

function csvEscape(v) {
  const s = String(v ?? "");
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

async function main() {
  const opts = parseArgs();

  if (opts.audit || (!opts.apply && !opts.audit)) {
    const audit = await auditHiltonCensusFieldBlanks();
    console.log("=== Hilton Census Field Blanks (before) ===\n");
    console.log("Total Hilton rows:", audit.total);
    console.log("Writable fields:", audit.writable.join(", "));
    if (audit.formula.length) {
      console.log("Formula (auto-derived, not written):", audit.formula.join(", "));
    }
    console.log("\nBlank counts:");
    for (const [k, v] of Object.entries(audit.blankCounts)) {
      console.log(`  ${k}: ${v}`);
    }
    if (!opts.apply) console.log("\nRun with --apply to fill blanks from hilton.com.");
    if (opts.audit && !opts.apply) return;
  }

  if (!opts.apply) return;

  console.log("\n=== Plan Hilton field backfill ===\n");
  const plan = await planHiltonCensusFieldBackfill({
    fetchDelayMs: opts.fetchDelayMs,
    onProgress: (msg) => console.log(" ", msg),
  });

  mkdirSync(REPORTS, { recursive: true });
  const jsonPath = join(REPORTS, "hilton-census-field-backfill-plan.json");
  writeFileSync(jsonPath, JSON.stringify({ generatedAt: new Date().toISOString(), ...plan }, null, 2));

  console.log("\n=== Plan summary ===");
  console.log("  Scanned:", plan.censusRowsScanned);
  console.log("  Ready:", plan.readyToApply);
  console.log("  Skipped:", plan.skipped.length);
  console.log("  Fields to fill:", plan.fieldFillCounts);
  console.log("Report:", jsonPath);

  if (plan.formulaFields?.length) {
    console.log(
      "\nNote:",
      CENSUS_FORMULA_AFFILIATION_FIELDS.join(", "),
      "are formula fields — filled automatically when Open Date / Year Affiliated are set."
    );
  }

  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT;
  const base = new Airtable({ apiKey }).base(baseId);

  let updated = 0;
  let batch = [];
  const log = [];

  for (const row of plan.planRows) {
    log.push({
      recordId: row.censusRecordId,
      name: row.censusName,
      fields: Object.keys(row.applyFields).join("; "),
    });
    batch.push({ id: row.censusRecordId, fields: row.applyFields });
    if (batch.length >= BATCH) {
      if (!opts.dryRun) await base(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
      updated += batch.length;
      batch = [];
    }
  }
  if (batch.length) {
    if (!opts.dryRun) await base(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
    updated += batch.length;
  }

  const logPath = join(REPORTS, "hilton-census-field-backfill-apply-log.csv");
  writeFileSync(
    logPath,
    `recordId,name,fields\n${log.map((r) => [r.recordId, csvEscape(r.name), r.fields].join(",")).join("\n")}\n`
  );

  console.log(`\n${opts.dryRun ? "Would update" : "Updated"}: ${updated}`);
  console.log("Log:", logPath);

  const after = await auditHiltonCensusFieldBlanks();
  console.log("\n=== Blank counts (after) ===");
  for (const [k, v] of Object.entries(after.blankCounts)) {
    console.log(`  ${k}: ${v}`);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
