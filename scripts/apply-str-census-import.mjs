/**
 * Apply STR Excel enrichment to Hotel Census for "Matched by STR ID" rows only.
 *
 * Full sync for Matched by STR ID:
 *   Market ← STR Market | Submarket ← STR Submarket | city | country | name
 * Default: writes every non-empty Excel value (use --only-changed for diff-only).
 * Does NOT touch Radar, Brand Explorer, or Brand Alias Mapping.
 *
 * Usage:
 *   node scripts/apply-str-census-import.mjs --dry-run
 *   node scripts/apply-str-census-import.mjs
 *   node scripts/apply-str-census-import.mjs --dir="C:/path/to/excel"
 */
import "../load-env.js";
import { readFileSync, existsSync, readdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";
import { readStrExcelDirectory } from "../lib/str-census-import/excel-parse.mjs";
import {
  buildCensusIndexes,
  matchExcelRow,
  buildCensusUpdateFields,
} from "../lib/str-census-import/match-excel-to-census.mjs";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const DEFAULT_EXCEL_DIR = join(__dirname, "..", "data", "str-imports");
const CENSUS_SUMMARY = join(REPORTS, "hotel-census-str-inventory-summary.json");
const APPLY_LOG = join(REPORTS, "str-census-import-apply-log.csv");
const APPLY_SUMMARY = join(REPORTS, "str-census-import-apply-summary.json");

const BATCH_SIZE = 10;

function parseArgs() {
  let dir = DEFAULT_EXCEL_DIR;
  let dryRun = false;
  let onlyChanged = false;
  for (const arg of process.argv.slice(2)) {
    if (arg === "--dry-run") dryRun = true;
    else if (arg === "--only-changed") onlyChanged = true;
    else if (arg.startsWith("--dir=")) dir = arg.slice("--dir=".length).replace(/^"|"$/g, "");
  }
  return { dir, dryRun, force: !onlyChanged };
}

function loadCensusMapping() {
  if (!existsSync(CENSUS_SUMMARY)) {
    throw new Error(`Missing ${CENSUS_SUMMARY}. Run inventory-hotel-census-for-str-import.mjs first.`);
  }
  const summary = JSON.parse(readFileSync(CENSUS_SUMMARY, "utf8"));
  const mapping = summary.recommendedFieldMapping || {};
  if (!mapping.strId) throw new Error("Census STR ID field not mapped.");
  return { summary, mapping };
}

function listExcelFiles(dir) {
  if (!existsSync(dir)) return [];
  return readdirSync(dir).filter((f) => /\.(xlsx|xls)$/i.test(f));
}

async function flushBatch(base, batch, dryRun) {
  if (!batch.length) return { updated: 0, errors: 0 };
  if (dryRun) return { updated: batch.length, errors: 0 };

  try {
    await base(HOTEL_CENSUS_TABLE).update(
      batch.map((b) => ({ id: b.id, fields: b.fields })),
      { typecast: true }
    );
    return { updated: batch.length, errors: 0 };
  } catch (err) {
    console.error("Batch update failed:", err.message);
    return { updated: 0, errors: batch.length };
  }
}

async function main() {
  const { dir, dryRun, force } = parseArgs();
  const { mapping } = loadCensusMapping();

  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT;
  if (!apiKey || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID_ALT");

  console.log(`=== STR → Hotel Census apply (${dryRun ? "DRY RUN" : "LIVE"}) ===\n`);
  console.log("Scope: Match status = Matched by STR ID only");
  console.log("Census table:", HOTEL_CENSUS_TABLE);
  console.log("STR ID field:", mapping.strId);
  console.log("Excel STR Market  → Census:", mapping.strMarket);
  console.log("Excel STR Submarket → Census:", mapping.strSubmarket);
  console.log("Mode:", force ? "full Excel sync (all non-empty cells)" : "only-changed fields");

  const { allRows: excelRows } = readStrExcelDirectory(dir, listExcelFiles);
  const excelStrIdCounts = new Map();
  for (const row of excelRows) {
    if (!row.strId) continue;
    excelStrIdCounts.set(row.strId, (excelStrIdCounts.get(row.strId) || 0) + 1);
  }

  const base = new Airtable({ apiKey }).base(baseId);
  console.log("\nLoading Hotel Census...");
  const records = await base(HOTEL_CENSUS_TABLE).select({ pageSize: 100 }).all();
  const { byStrId, byNcc } = buildCensusIndexes(records, mapping);

  const logRows = [];
  const updatesByRecordId = new Map();

  for (const ex of excelRows) {
    const m = matchExcelRow(ex, byStrId, byNcc, excelStrIdCounts);
    if (m.status !== "Matched by STR ID" || !m.censusEntry) continue;

    const patch = buildCensusUpdateFields(mapping, ex, m.censusEntry, { force });
    const fieldNames = Object.keys(patch);

    if (!fieldNames.length) {
      logRows.push({
        action: "skipped",
        recordId: m.matchedRecordId,
        strId: ex.strId,
        sourceFile: ex.sourceFile,
        rowNumber: ex.rowNumber,
        fieldsUpdated: "",
        notes: "Already aligned with Excel",
      });
      continue;
    }

    const existing = updatesByRecordId.get(m.matchedRecordId);
    if (existing) {
      Object.assign(existing.fields, patch);
      existing.sources.push(`${ex.sourceFile}:${ex.rowNumber}`);
    } else {
      updatesByRecordId.set(m.matchedRecordId, {
        id: m.matchedRecordId,
        fields: { ...patch },
        strId: ex.strId,
        sources: [`${ex.sourceFile}:${ex.rowNumber}`],
      });
    }
  }

  let updated = 0;
  let skipped = logRows.filter((r) => r.action === "skipped").length;
  let errors = 0;
  let batch = [];

  for (const item of updatesByRecordId.values()) {
    logRows.push({
      action: dryRun ? "would_update" : "updated",
      recordId: item.id,
      strId: item.strId,
      sourceFile: item.sources.join("; "),
      rowNumber: "",
      fieldsUpdated: Object.keys(item.fields).join("; "),
      notes: JSON.stringify(item.fields),
    });

    batch.push({ id: item.id, fields: item.fields });
    if (batch.length >= BATCH_SIZE) {
      const result = await flushBatch(base, batch, dryRun);
      updated += result.updated;
      errors += result.errors;
      batch = [];
      if (!dryRun && updated % 100 === 0 && updated > 0) {
        console.log(`  …${updated} records updated`);
      }
    }
  }

  if (batch.length) {
    const result = await flushBatch(base, batch, dryRun);
    updated += result.updated;
    errors += result.errors;
  }

  const summary = {
    generatedAt: new Date().toISOString(),
    dryRun,
    forceFullExcelSync: force,
    scope: "Matched by STR ID only",
    excelRowsScanned: excelRows.length,
    recordsWithChanges: updatesByRecordId.size,
    recordsSkippedNoDiff: skipped,
    recordsUpdated: updated,
    errors,
    fieldMapping: mapping,
  };

  writeCsv(APPLY_LOG, logRows, [
    "action",
    "recordId",
    "strId",
    "sourceFile",
    "rowNumber",
    "fieldsUpdated",
    "notes",
  ]);
  writeJson(APPLY_SUMMARY, summary);

  console.log("\n--- Apply summary ---");
  console.log("  Excel rows scanned:", excelRows.length);
  console.log("  Census records to update:", updatesByRecordId.size);
  console.log("  Skipped (no diff):", skipped);
  console.log("  Updated:", updated, dryRun ? "(dry-run)" : "");
  console.log("  Errors:", errors);
  console.log("\nReports:");
  console.log(" ", APPLY_LOG);
  console.log(" ", APPLY_SUMMARY);
  console.log(dryRun ? "\nDry run only — no Airtable writes." : "\nDone. Hotel Census updated.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
