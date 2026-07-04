/**
 * Analyze remaining STR import rows (not Matched by STR ID).
 * Read-only on census except reading Development Cost for context.
 *
 * Output:
 *   reports/str-census-conflicts-resolution.csv
 *   reports/str-census-duplicates-resolution.csv
 *   reports/str-census-no-match-resolution.csv
 *   reports/str-census-ncc-match-resolution.csv
 *   reports/str-census-remaining-summary.json
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
} from "../lib/str-census-import/match-excel-to-census.mjs";
import {
  hotelNamesEquivalent,
  hotelNamesLooselyEquivalent,
  locationEquivalent,
  normalizeHotelName,
} from "../lib/str-census-import/name-compare.mjs";
import { normalizeKey } from "../lib/str-census-import/normalize.mjs";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const DEFAULT_DIR = join(__dirname, "..", "data", "str-imports");
const CENSUS_SUMMARY = join(REPORTS, "hotel-census-str-inventory-summary.json");

function loadMapping() {
  const summary = JSON.parse(readFileSync(CENSUS_SUMMARY, "utf8"));
  return summary.recommendedFieldMapping;
}

function listExcelFiles(dir) {
  return existsSync(dir) ? readdirSync(dir).filter((f) => /\.(xlsx|xls)$/i.test(f)) : [];
}

function scoreDuplicateCandidate(ex, entry) {
  let score = 0;
  if (hotelNamesEquivalent(ex.hotelName, entry.name)) score += 50;
  else if (normalizeHotelName(ex.hotelName) && normalizeHotelName(entry.name)) {
    const a = normalizeHotelName(ex.hotelName);
    const b = normalizeHotelName(entry.name);
    if (a.slice(0, 12) === b.slice(0, 12)) score += 20;
  }
  if (normalizeKey(ex.city) === normalizeKey(entry.city)) score += 15;
  if (normalizeKey(ex.country) === normalizeKey(entry.country)) score += 15;
  if (entry.strMarket) score += 5;
  if (entry.strSubmarket) score += 5;
  return score;
}

async function main() {
  const mapping = loadMapping();
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT;
  if (!apiKey || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID_ALT");

  const { allRows: excelRows } = readStrExcelDirectory(DEFAULT_DIR, listExcelFiles);
  const excelStrIdCounts = new Map();
  for (const row of excelRows) {
    if (row.strId) excelStrIdCounts.set(row.strId, (excelStrIdCounts.get(row.strId) || 0) + 1);
  }

  const base = new Airtable({ apiKey }).base(baseId);
  console.log("Loading Hotel Census...");
  const records = await base(HOTEL_CENSUS_TABLE).select({ pageSize: 100 }).all();
  const { byStrId, byNcc } = buildCensusIndexes(records, mapping);

  const conflicts = [];
  const duplicates = [];
  const noMatch = [];
  const nccMatch = [];

  for (const ex of excelRows) {
    const m = matchExcelRow(ex, byStrId, byNcc, excelStrIdCounts);

    if (m.status === "Conflict" && m.censusEntry) {
      const nameEq = hotelNamesEquivalent(ex.hotelName, m.censusEntry.name);
      const locEq = locationEquivalent(
        { city: ex.city, country: ex.country },
        { city: m.censusEntry.city, country: m.censusEntry.country }
      );
      let resolution = "MANUAL_REVIEW";
      let recommendedAction = "Review in Airtable; do not auto-apply";
      if (nameEq && locEq) {
        resolution = "AUTO_NAME_VARIANT";
        recommendedAction = "Safe: apply Excel Market/Submarket/City/Country/Name (wording-only name diff)";
      } else if (nameEq && !locEq) {
        resolution = "LOCATION_MISMATCH";
        recommendedAction = "Manual: city/country differ — confirm which is correct before apply";
      } else if (!nameEq && locEq) {
        if (hotelNamesLooselyEquivalent(ex.hotelName, m.censusEntry.name)) {
          resolution = "LOOSE_NAME_MISMATCH";
          recommendedAction = "Apply with --conflicts-loose (city/country match, wording-only name diff)";
        } else {
          resolution = "NAME_MISMATCH";
          recommendedAction = "Manual: different hotel name at same STR ID — verify same property";
        }
      } else {
        resolution = "NAME_AND_LOCATION";
        recommendedAction = "Manual: name and location both differ";
      }
      conflicts.push({
        resolution,
        recommendedAction,
        excelStrId: ex.strId,
        excelHotelName: ex.hotelName,
        censusHotelName: m.censusEntry.name,
        excelCity: ex.city,
        censusCity: m.censusEntry.city,
        excelCountry: ex.country,
        censusCountry: m.censusEntry.country,
        excelStrMarket: ex.strMarket,
        excelStrSubmarket: ex.strSubmarket,
        recordId: m.matchedRecordId,
        sourceFile: ex.sourceFile,
        rowNumber: ex.rowNumber,
        nameEquivalent: nameEq ? "yes" : "no",
        locationEquivalent: locEq ? "yes" : "no",
      });
    }

    if (m.status === "Duplicate STR ID in Census" && ex.strId && byStrId.has(ex.strId)) {
      const candidates = byStrId.get(ex.strId);
      const scored = candidates
        .map((c) => ({ ...c, score: scoreDuplicateCandidate(ex, c) }))
        .sort((a, b) => b.score - a.score);
      const winner = scored[0];
      const loser = scored[1];
      duplicates.push({
        excelStrId: ex.strId,
        excelHotelName: ex.hotelName,
        winnerRecordId: winner?.recordId || "",
        winnerName: winner?.name || "",
        winnerScore: winner?.score ?? 0,
        loserRecordId: loser?.recordId || "",
        loserName: loser?.name || "",
        loserScore: loser?.score ?? 0,
        recommendedAction:
          winner && loser && winner.score > loser.score
            ? `Apply Excel to ${winner.recordId}; archive or fix duplicate ${loser.recordId}`
            : "Manual: scores tied — pick canonical record",
        sourceFile: ex.sourceFile,
        rowNumber: ex.rowNumber,
      });
    }

    if (m.status === "No Match") {
      noMatch.push({
        excelStrId: ex.strId,
        excelHotelName: ex.hotelName,
        excelCity: ex.city,
        excelCountry: ex.country,
        excelStrMarket: ex.strMarket,
        excelStrSubmarket: ex.strSubmarket,
        recommendedAction:
          "Create new Hotel Census row or add STR Number to existing property if found manually",
        sourceFile: ex.sourceFile,
        rowNumber: ex.rowNumber,
      });
    }

    if (m.status === "Matched by Name City Country" && m.censusEntry) {
      nccMatch.push({
        excelStrId: ex.strId,
        censusStrId: m.censusEntry.strId,
        excelHotelName: ex.hotelName,
        censusHotelName: m.censusEntry.name,
        recordId: m.matchedRecordId,
        recommendedAction: "Apply Excel fields; confirm STR ID alignment",
        sourceFile: ex.sourceFile,
        rowNumber: ex.rowNumber,
      });
    }
  }

  const autoConflicts = conflicts.filter((c) => c.resolution === "AUTO_NAME_VARIANT");
  const summary = {
    generatedAt: new Date().toISOString(),
    totals: {
      conflicts: conflicts.length,
      autoNameVariant: autoConflicts.length,
      manualConflicts: conflicts.length - autoConflicts.length,
      duplicateStrIdInCensus: duplicates.length,
      noMatch: noMatch.length,
      nameCityCountry: nccMatch.length,
    },
    recommendedOrder: [
      "1. Apply AUTO_NAME_VARIANT conflicts (scripts/apply-str-import-remaining.mjs --auto-conflicts)",
      "2. Apply duplicate winners where score > loser ( --duplicates )",
      "3. Apply Name+City+Country matches ( --name-city-country )",
      "4. Manual review LOCATION_MISMATCH / NAME_MISMATCH / tied duplicates",
      "5. No Match: create census rows or investigate missing STR Number ( --no-match-export only )",
    ],
    developmentCostMarkerSuggestion: {
      batch1MatchedByStrId: 1,
      batch2RemainingApplied: 2,
      notTouched: "empty or 0",
    },
  };

  writeCsv(join(REPORTS, "str-census-conflicts-resolution.csv"), conflicts);
  writeCsv(join(REPORTS, "str-census-duplicates-resolution.csv"), duplicates);
  writeCsv(join(REPORTS, "str-census-no-match-resolution.csv"), noMatch);
  writeCsv(join(REPORTS, "str-census-ncc-match-resolution.csv"), nccMatch);
  writeJson(join(REPORTS, "str-census-remaining-summary.json"), summary);

  console.log("=== STR import remaining analysis ===\n");
  console.log("Conflicts:", conflicts.length, `(auto name variant: ${autoConflicts.length})`);
  console.log("Duplicate STR ID in Census:", duplicates.length);
  console.log("No Match:", noMatch.length);
  console.log("Matched by Name City Country:", nccMatch.length);
  console.log("\nReports in reports/");
  console.log("\nRecommended next command:");
  console.log(
    "  node scripts/apply-str-import-remaining.mjs --auto-conflicts --duplicates --name-city-country"
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
