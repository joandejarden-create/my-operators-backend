/**
 * Step 3 — STR Excel → Hotel Census dry-run matcher (read-only, no Airtable updates).
 *
 * Prerequisite: run inventory scripts first.
 *
 * Usage:
 *   node scripts/import-str-census-dry-run.mjs
 *   node scripts/import-str-census-dry-run.mjs --dir="C:/path/to/excel"
 *
 * Output:
 *   reports/str-census-import-match-report.csv
 *   reports/str-census-import-reviewed.example.json
 */
import "../load-env.js";
import { readFileSync, existsSync, readdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";
import { fieldValue } from "../lib/str-census-import/field-mapping.mjs";
import { readStrExcelDirectory } from "../lib/str-census-import/excel-parse.mjs";
import {
  normalizeStrId,
  normalizeKey,
  nameCityCountryKey,
} from "../lib/str-census-import/normalize.mjs";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const DEFAULT_EXCEL_DIR = join(__dirname, "..", "data", "str-imports");
const CENSUS_SUMMARY = join(REPORTS, "hotel-census-str-inventory-summary.json");
const MATCH_CSV = join(REPORTS, "str-census-import-match-report.csv");
const REVIEWED_JSON = join(REPORTS, "str-census-import-reviewed.example.json");

function parseArgs() {
  let dir = DEFAULT_EXCEL_DIR;
  for (const arg of process.argv.slice(2)) {
    if (arg.startsWith("--dir=")) dir = arg.slice("--dir=".length).replace(/^"|"$/g, "");
  }
  return { dir };
}

function loadCensusMapping() {
  if (!existsSync(CENSUS_SUMMARY)) {
    throw new Error(
      `Missing ${CENSUS_SUMMARY}. Run: node scripts/inventory-hotel-census-for-str-import.mjs`
    );
  }
  const summary = JSON.parse(readFileSync(CENSUS_SUMMARY, "utf8"));
  const mapping = summary.recommendedFieldMapping || {};
  if (!mapping.strId) {
    throw new Error(
      "Census inventory did not detect an STR ID field. Review hotel-census-str-inventory-summary.json before dry-run."
    );
  }
  return { summary, mapping };
}

function listExcelFiles(dir) {
  if (!existsSync(dir)) return [];
  return readdirSync(dir).filter((f) => /\.(xlsx|xls)$/i.test(f));
}

function parseExcelDir(dir) {
  return readStrExcelDirectory(dir, listExcelFiles).allRows;
}

function fieldsThatWouldChange(existing, proposed, keys) {
  const changed = [];
  for (const { label, key } of keys) {
    const a = normalizeKey(existing[key]);
    const b = normalizeKey(proposed[key]);
    if (b && a !== b) changed.push(label);
  }
  return changed.join("; ");
}

function buildCensusIndexes(records, mapping) {
  const byStrId = new Map();
  const byNcc = new Map();

  for (const rec of records) {
    const f = rec.fields || {};
    const strId = normalizeStrId(fieldValue(f, mapping.strId));
    const name = fieldValue(f, mapping.hotelName);
    const city = fieldValue(f, mapping.city);
    const country = fieldValue(f, mapping.country);

    const entry = {
      recordId: rec.id,
      fields: f,
      strId,
      name,
      city,
      country,
      strMarket: fieldValue(f, mapping.strMarket),
      strSubmarket: fieldValue(f, mapping.strSubmarket),
    };

    if (strId) {
      if (!byStrId.has(strId)) byStrId.set(strId, []);
      byStrId.get(strId).push(entry);
    }
    const ncc = nameCityCountryKey(name, city, country);
    if (ncc !== "||") {
      if (!byNcc.has(ncc)) byNcc.set(ncc, []);
      byNcc.get(ncc).push(entry);
    }
  }

  return { byStrId, byNcc };
}

function censusSnapshot(entry, mapping) {
  return {
    censusStrId: entry.strId,
    censusHotelName: entry.name,
    censusCity: entry.city,
    censusCountry: entry.country,
    existingStrMarket: entry.strMarket,
    existingStrSubmarket: entry.strSubmarket,
  };
}

async function main() {
  const { dir } = parseArgs();
  const { summary, mapping } = loadCensusMapping();

  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT;
  if (!apiKey || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID_ALT");

  console.log("=== STR → Hotel Census dry-run (no Airtable writes) ===\n");
  console.log("Census STR ID field:", mapping.strId);
  console.log("Excel directory:", dir);

  const excelRows = parseExcelDir(dir);
  console.log(`Excel rows loaded: ${excelRows.length}`);

  const excelStrIdCounts = new Map();
  for (const row of excelRows) {
    if (!row.strId) continue;
    excelStrIdCounts.set(row.strId, (excelStrIdCounts.get(row.strId) || 0) + 1);
  }

  const base = new Airtable({ apiKey }).base(baseId);
  console.log(`Loading "${HOTEL_CENSUS_TABLE}" (read-only)...`);
  const records = await base(HOTEL_CENSUS_TABLE).select({ pageSize: 100 }).all();
  const { byStrId, byNcc } = buildCensusIndexes(records, mapping);
  console.log(`Census records: ${records.length}\n`);

  const compareKeys = [
    { label: "STR Market", key: "strMarket" },
    { label: "STR Submarket", key: "strSubmarket" },
    { label: "City", key: "city" },
    { label: "Country", key: "country" },
    { label: "Hotel Name", key: "hotelName" },
  ];

  const matchRows = [];
  const statusCounts = {};

  for (const ex of excelRows) {
    let status = "No Match";
    let confidence = "None";
    let needsReview = "Yes";
    let notes = [];
    let matchedRecordId = "";
    let census = {
      censusStrId: "",
      censusHotelName: "",
      censusCity: "",
      censusCountry: "",
      existingStrMarket: "",
      existingStrSubmarket: "",
    };

    const proposed = {
      strMarket: ex.strMarket,
      strSubmarket: ex.strSubmarket,
      city: ex.city,
      country: ex.country,
      hotelName: ex.hotelName,
    };

    if (ex.strId && (excelStrIdCounts.get(ex.strId) || 0) > 1) {
      status = "Duplicate STR ID in Excel";
      confidence = "Low";
      notes.push("STR ID appears on multiple Excel rows");
    } else if (ex.strId && byStrId.has(ex.strId)) {
      const matches = byStrId.get(ex.strId);
      if (matches.length > 1) {
        status = "Duplicate STR ID in Census";
        confidence = "Low";
        notes.push(`${matches.length} census records share this STR ID`);
        matchedRecordId = matches.map((m) => m.recordId).join("; ");
        census = censusSnapshot(matches[0], mapping);
      } else {
        const m = matches[0];
        matchedRecordId = m.recordId;
        census = censusSnapshot(m, mapping);

        const nameMismatch =
          normalizeKey(ex.hotelName) &&
          normalizeKey(m.name) &&
          normalizeKey(ex.hotelName) !== normalizeKey(m.name);
        if (nameMismatch) {
          status = "Conflict";
          confidence = "Medium";
          notes.push("STR ID match but hotel name differs");
        } else {
          status = "Matched by STR ID";
          confidence = "High";
          needsReview = "No";
        }
      }
    } else {
      const ncc = nameCityCountryKey(ex.hotelName, ex.city, ex.country);
      if (ncc !== "||" && byNcc.has(ncc)) {
        const matches = byNcc.get(ncc);
        if (matches.length > 1) {
          status = "Needs Human Review";
          confidence = "Low";
          notes.push("Multiple census rows share Name+City+Country");
          matchedRecordId = matches.map((m) => m.recordId).join("; ");
          census = censusSnapshot(matches[0], mapping);
        } else if (ex.strId && matches[0].strId && ex.strId !== matches[0].strId) {
          status = "Conflict";
          confidence = "Medium";
          notes.push("Name/City/Country match but STR ID differs");
          matchedRecordId = matches[0].recordId;
          census = censusSnapshot(matches[0], mapping);
        } else {
          status = "Matched by Name City Country";
          confidence = "Medium";
          needsReview = "Yes";
          matchedRecordId = matches[0].recordId;
          census = censusSnapshot(matches[0], mapping);
          if (!ex.strId) notes.push("Excel row missing STR ID; matched on name/location");
        }
      } else if (!ex.strId) {
        notes.push("No STR ID and no census match on Name+City+Country");
      } else {
        notes.push("STR ID not found in census; no fallback match");
      }
    }

    const existingForCompare = {
      strMarket: census.existingStrMarket,
      strSubmarket: census.existingStrSubmarket,
      city: census.censusCity,
      country: census.censusCountry,
      hotelName: census.censusHotelName,
    };

    const changed = fieldsThatWouldChange(existingForCompare, proposed, compareKeys);

    statusCounts[status] = (statusCounts[status] || 0) + 1;

    matchRows.push({
      "Match Status": status,
      "Source File": ex.sourceFile,
      "Import Row Number": ex.rowNumber,
      "Excel STR ID": ex.strId,
      "Excel Hotel Name": ex.hotelName,
      "Excel City": ex.city,
      "Excel Country": ex.country,
      "Excel STR Market": ex.strMarket,
      "Excel STR Submarket": ex.strSubmarket,
      "Matched Airtable Record ID": matchedRecordId,
      "Census STR ID": census.censusStrId,
      "Census Hotel Name": census.censusHotelName,
      "Census City": census.censusCity,
      "Census Country": census.censusCountry,
      "Existing STR Market": census.existingStrMarket,
      "Existing STR Submarket": census.existingStrSubmarket,
      "Proposed STR Market": ex.strMarket,
      "Proposed STR Submarket": ex.strSubmarket,
      "Proposed City": ex.city,
      "Proposed Country": ex.country,
      "Proposed Hotel Name": ex.hotelName,
      "Fields That Would Change": changed,
      Confidence: confidence,
      "Needs Human Review": needsReview,
      Notes: notes.join(" | "),
    });
  }

  const headers = [
    "Match Status",
    "Source File",
    "Import Row Number",
    "Excel STR ID",
    "Excel Hotel Name",
    "Excel City",
    "Excel Country",
    "Excel STR Market",
    "Excel STR Submarket",
    "Matched Airtable Record ID",
    "Census STR ID",
    "Census Hotel Name",
    "Census City",
    "Census Country",
    "Existing STR Market",
    "Existing STR Submarket",
    "Proposed STR Market",
    "Proposed STR Submarket",
    "Proposed City",
    "Proposed Country",
    "Proposed Hotel Name",
    "Fields That Would Change",
    "Confidence",
    "Needs Human Review",
    "Notes",
  ];

  writeCsv(MATCH_CSV, matchRows, headers);

  const reviewCandidates = matchRows
    .filter(
      (r) =>
        r["Match Status"] === "Matched by STR ID" &&
        r["Fields That Would Change"] &&
        r["Needs Human Review"] === "No"
    )
    .slice(0, 50)
    .map((r) => ({
      airtableRecordId: r["Matched Airtable Record ID"],
      strId: r["Excel STR ID"],
      approved: false,
      updates: {
        [mapping.strMarket]: r["Proposed STR Market"],
        [mapping.strSubmarket]: r["Proposed STR Submarket"],
      },
      notes: "Example only — set approved:true after human review. Apply script not built yet.",
    }));

  writeJson(REVIEWED_JSON, {
    generatedAt: new Date().toISOString(),
    censusFieldMapping: mapping,
    censusInventoryGeneratedAt: summary.generatedAt,
    dryRunOnly: true,
    instructions:
      "Do not apply until census + Excel inventories and match report are reviewed. No apply script ships in this phase.",
    statusCounts,
    reviewCandidates,
  });

  console.log("Match summary:");
  Object.entries(statusCounts)
    .sort((a, b) => b[1] - a[1])
    .forEach(([k, v]) => console.log(`  ${k}: ${v}`));

  console.log("\nReports:");
  console.log(" ", MATCH_CSV);
  console.log(" ", REVIEWED_JSON);
  console.log("\nDone. No Airtable changes were made.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
