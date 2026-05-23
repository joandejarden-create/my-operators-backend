/**
 * Step 2 — Read-only inventory of STR Excel files (no Airtable changes).
 *
 * Usage:
 *   node scripts/inventory-str-excel-files.mjs
 *   node scripts/inventory-str-excel-files.mjs --dir="C:/path/to/folder"
 */
import "../load-env.js";
import { existsSync, readdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { readStrExcelDirectory } from "../lib/str-census-import/excel-parse.mjs";
import { nameCityCountryKey } from "../lib/str-census-import/normalize.mjs";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const DEFAULT_DIR = join(__dirname, "..", "data", "str-imports");
const REPORTS = join(__dirname, "..", "reports");

function parseArgs() {
  let dir = DEFAULT_DIR;
  for (const arg of process.argv.slice(2)) {
    if (arg.startsWith("--dir=")) dir = arg.slice("--dir=".length).replace(/^"|"$/g, "");
  }
  return { dir };
}

function listExcelFiles(dir) {
  if (!existsSync(dir)) return [];
  return readdirSync(dir).filter((f) => /\.(xlsx|xls)$/i.test(f));
}

function main() {
  const { dir } = parseArgs();
  console.log("=== STR Excel inventory (read-only) ===\n");
  console.log("Input directory:", dir);

  const { files, inventoryRows, allRows } = readStrExcelDirectory(dir, listExcelFiles);

  if (!files.length) {
    console.warn(`No .xlsx/.xls files in ${dir}`);
  }

  const strIdGlobal = new Map();
  const nccGlobal = new Map();
  const duplicateRows = [];

  for (const row of allRows) {
    if (row.strId) {
      if (!strIdGlobal.has(row.strId)) strIdGlobal.set(row.strId, []);
      strIdGlobal.get(row.strId).push(row);
    }
    const ncc = nameCityCountryKey(row.hotelName, row.city, row.country);
    if (ncc !== "||") {
      if (!nccGlobal.has(ncc)) nccGlobal.set(ncc, []);
      nccGlobal.get(ncc).push(row);
    }
  }

  let missingStrId = 0;
  for (const row of allRows) {
    if (!row.strId) missingStrId++;
  }

  for (const [strId, occurrences] of strIdGlobal) {
    if (occurrences.length <= 1) continue;
    const variants = new Set();
    for (const o of occurrences) {
      variants.add(
        `${o.hotelName}|${o.city}|${o.country}|${o.strMarket}|${o.strSubmarket}`
      );
    }
    duplicateRows.push({
      issueType:
        variants.size > 1 ? "Duplicate STR ID in Excel (conflicting rows)" : "Duplicate STR ID in Excel",
      strId,
      occurrenceCount: occurrences.length,
      sources: [...new Set(occurrences.map((o) => `${o.sourceFile}:${o.sheetName}:${o.rowNumber}`))].join(
        "; "
      ),
      notes: variants.size > 1 ? "Same STR ID, different hotel/location/market data" : "",
    });
  }

  for (const [ncc, occurrences] of nccGlobal) {
    if (occurrences.length <= 1) continue;
    const strIds = new Set(occurrences.map((o) => o.strId).filter(Boolean));
    duplicateRows.push({
      issueType:
        strIds.size > 1
          ? "Duplicate Hotel+City+Country in Excel (different STR IDs)"
          : "Duplicate Hotel+City+Country in Excel",
      strId: [...strIds].join(" | ") || "(none)",
      occurrenceCount: occurrences.length,
      sources: [...new Set(occurrences.map((o) => `${o.sourceFile}:${o.rowNumber}`))].join("; "),
      notes: ncc,
    });
  }

  const INVENTORY_CSV = join(REPORTS, "str-excel-inventory.csv");
  const DUPES_CSV = join(REPORTS, "str-excel-duplicates.csv");
  const SUMMARY_JSON = join(REPORTS, "str-excel-inventory-summary.json");

  writeCsv(INVENTORY_CSV, inventoryRows, [
    "fileName",
    "sheetName",
    "headerRowIndex",
    "rowCount",
    "columnsFound",
    "missingExpectedColumns",
    "hasStrId",
    "hasCity",
    "hasHotelName",
    "hasCountry",
    "hasStrMarket",
    "hasStrSubmarket",
  ]);

  writeCsv(DUPES_CSV, duplicateRows, [
    "issueType",
    "strId",
    "occurrenceCount",
    "sources",
    "notes",
  ]);

  const summary = {
    generatedAt: new Date().toISOString(),
    inputDirectory: dir,
    filesScanned: files,
    totalDataRows: allRows.length,
    rowsMissingStrId: missingStrId,
    duplicateStrIdGroups: [...strIdGlobal.values()].filter((a) => a.length > 1).length,
    duplicateNccGroups: [...nccGlobal.values()].filter((a) => a.length > 1).length,
    duplicateIssueCount: duplicateRows.length,
    sheets: inventoryRows,
    reportFiles: { inventory: INVENTORY_CSV, duplicates: DUPES_CSV },
  };

  writeJson(SUMMARY_JSON, summary);

  console.log("\nFiles scanned:", files.length ? files.join(", ") : "(none)");
  console.log("Total data rows:", allRows.length);
  console.log("Rows missing STR ID:", missingStrId);
  console.log("Duplicate STR ID groups:", summary.duplicateStrIdGroups);
  console.log("\nReports:");
  console.log(" ", INVENTORY_CSV);
  console.log(" ", DUPES_CSV);
  console.log(" ", SUMMARY_JSON);
  console.log("\nDone. No Airtable changes were made.");
}

main();
