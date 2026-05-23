/**
 * Apply STR Excel to remaining Hotel Census buckets (after Matched by STR ID).
 *
 * Usage:
 *   node scripts/apply-str-import-remaining.mjs --dry-run --auto-conflicts --duplicates --name-city-country
 *   node scripts/apply-str-import-remaining.mjs --auto-conflicts --duplicates --name-city-country
 *
 * Flags:
 *   --auto-conflicts     Conflicts where name+location are equivalent (wording-only)
 *   --duplicates         Pick best census row per duplicate STR ID (higher score)
 *   --name-city-country  2 rows matched on name/city/country only
 *   --conflicts-loose    Conflicts: city/country match, name differs only by wording/brands
 *   --mark-dev-cost-2    Set Development Cost = 2 on updated rows (batch 2 marker)
 *   --mark-dev-cost-3    Set Development Cost = 3 (use with --conflicts-loose)
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
import {
  hotelNamesEquivalent,
  hotelNamesLooselyEquivalent,
  locationEquivalent,
  normalizeHotelName,
} from "../lib/str-census-import/name-compare.mjs";
import { normalizeKey } from "../lib/str-census-import/normalize.mjs";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

const DEVELOPMENT_COST_FIELD = "Development Cost";
const BATCH2_MARKER = 2;
const BATCH3_MARKER = 3;
const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const DEFAULT_DIR = join(__dirname, "..", "data", "str-imports");
const CENSUS_SUMMARY = join(REPORTS, "hotel-census-str-inventory-summary.json");
const BATCH_SIZE = 10;

function parseArgs() {
  const flags = {
    dryRun: false,
    autoConflicts: false,
    duplicates: false,
    nameCityCountry: false,
    conflictsLoose: false,
    markDevCost2: true,
    markDevCost3: false,
  };
  for (const arg of process.argv.slice(2)) {
    if (arg === "--dry-run") flags.dryRun = true;
    else if (arg === "--auto-conflicts") flags.autoConflicts = true;
    else if (arg === "--duplicates") flags.duplicates = true;
    else if (arg === "--name-city-country") flags.nameCityCountry = true;
    else if (arg === "--conflicts-loose") flags.conflictsLoose = true;
    else if (arg === "--mark-dev-cost-3") flags.markDevCost3 = true;
    else if (arg === "--no-mark") {
      flags.markDevCost2 = false;
      flags.markDevCost3 = false;
    }
  }
  if (flags.conflictsLoose && !process.argv.includes("--mark-dev-cost-2")) {
    flags.markDevCost3 = true;
    flags.markDevCost2 = false;
  }
  return flags;
}

function loadMapping() {
  return JSON.parse(readFileSync(CENSUS_SUMMARY, "utf8")).recommendedFieldMapping;
}

function listExcelFiles(dir) {
  return existsSync(dir) ? readdirSync(dir).filter((f) => /\.(xlsx|xls)$/i.test(f)) : [];
}

function scoreDuplicateCandidate(ex, entry) {
  let score = 0;
  if (hotelNamesEquivalent(ex.hotelName, entry.name)) score += 50;
  else if (normalizeHotelName(ex.hotelName) && normalizeHotelName(entry.name)) {
    if (normalizeHotelName(ex.hotelName).slice(0, 12) === normalizeHotelName(entry.name).slice(0, 12))
      score += 20;
  }
  if (normalizeKey(ex.city) === normalizeKey(entry.city)) score += 15;
  if (normalizeKey(ex.country) === normalizeKey(entry.country)) score += 15;
  if (entry.strMarket) score += 5;
  if (entry.strSubmarket) score += 5;
  return score;
}

function pickDuplicateWinner(ex, candidates) {
  const scored = candidates
    .map((c) => ({ entry: c, score: scoreDuplicateCandidate(ex, c) }))
    .sort((a, b) => b.score - a.score);
  if (!scored.length) return null;
  if (scored.length === 1) return scored[0].entry;
  if (scored[0].score > scored[1].score) return scored[0].entry;
  return null;
}

async function flushBatch(base, batch, dryRun) {
  if (!batch.length) return { ok: 0, err: 0 };
  if (dryRun) return { ok: batch.length, err: 0 };
  try {
    await base(HOTEL_CENSUS_TABLE).update(
      batch.map((b) => ({ id: b.id, fields: b.fields })),
      { typecast: true }
    );
    return { ok: batch.length, err: 0 };
  } catch (err) {
    console.error("Batch failed:", err.message);
    return { ok: 0, err: batch.length };
  }
}

async function main() {
  const flags = parseArgs();
  if (!flags.autoConflicts && !flags.duplicates && !flags.nameCityCountry && !flags.conflictsLoose) {
    console.error(
      "Pick at least one: --auto-conflicts --duplicates --name-city-country --conflicts-loose"
    );
    process.exit(1);
  }

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
  const records = await base(HOTEL_CENSUS_TABLE).select({ pageSize: 100 }).all();
  const { byStrId, byNcc } = buildCensusIndexes(records, mapping);

  const updatesByRecordId = new Map();
  const log = [];

  function queueUpdate(recordId, ex, censusEntry, bucket, strId) {
    const patch = buildCensusUpdateFields(mapping, ex, censusEntry, { force: true });
    if (flags.markDevCost3) patch[DEVELOPMENT_COST_FIELD] = BATCH3_MARKER;
    else if (flags.markDevCost2) patch[DEVELOPMENT_COST_FIELD] = BATCH2_MARKER;

    const prev = updatesByRecordId.get(recordId);
    if (prev) Object.assign(prev.fields, patch);
    else updatesByRecordId.set(recordId, { id: recordId, fields: { ...patch }, bucket, strId });

    log.push({
      bucket,
      recordId,
      strId,
      fieldsUpdated: Object.keys(patch).join("; "),
    });
  }

  for (const ex of excelRows) {
    const m = matchExcelRow(ex, byStrId, byNcc, excelStrIdCounts);

    if (flags.autoConflicts && m.status === "Conflict" && m.censusEntry) {
      const nameEq = hotelNamesEquivalent(ex.hotelName, m.censusEntry.name);
      const locEq = locationEquivalent(
        { city: ex.city, country: ex.country },
        { city: m.censusEntry.city, country: m.censusEntry.country }
      );
      if (nameEq && locEq) {
        queueUpdate(m.matchedRecordId, ex, m.censusEntry, "AUTO_CONFLICT", ex.strId);
      }
    }

    if (flags.duplicates && m.status === "Duplicate STR ID in Census" && ex.strId) {
      const candidates = byStrId.get(ex.strId);
      const winner = pickDuplicateWinner(ex, candidates);
      if (winner) queueUpdate(winner.recordId, ex, winner, "DUPLICATE_WINNER", ex.strId);
    }

    if (flags.nameCityCountry && m.status === "Matched by Name City Country" && m.censusEntry) {
      queueUpdate(m.matchedRecordId, ex, m.censusEntry, "NAME_CITY_COUNTRY", ex.strId);
    }

    if (flags.conflictsLoose && m.status === "Conflict" && m.censusEntry) {
      const locEq = locationEquivalent(
        { city: ex.city, country: ex.country },
        { city: m.censusEntry.city, country: m.censusEntry.country }
      );
      const strictName = hotelNamesEquivalent(ex.hotelName, m.censusEntry.name);
      const looseName = hotelNamesLooselyEquivalent(ex.hotelName, m.censusEntry.name);
      if (locEq && looseName && !strictName) {
        queueUpdate(m.matchedRecordId, ex, m.censusEntry, "CONFLICT_LOOSE", ex.strId);
      }
    }
  }

  let ok = 0;
  let err = 0;
  let batch = [];
  for (const item of updatesByRecordId.values()) {
    batch.push(item);
    if (batch.length >= BATCH_SIZE) {
      const r = await flushBatch(base, batch, flags.dryRun);
      ok += r.ok;
      err += r.err;
      batch = [];
    }
  }
  if (batch.length) {
    const r = await flushBatch(base, batch, flags.dryRun);
    ok += r.ok;
    err += r.err;
  }

  const byBucket = {};
  for (const item of updatesByRecordId.values()) {
    byBucket[item.bucket] = (byBucket[item.bucket] || 0) + 1;
  }

  const summary = {
    generatedAt: new Date().toISOString(),
    dryRun: flags.dryRun,
    recordsUpdated: ok,
    errors: err,
    byBucket,
    developmentCostMarker: flags.markDevCost2 ? BATCH2_MARKER : null,
  };

  const logName = flags.conflictsLoose && !flags.autoConflicts && !flags.duplicates && !flags.nameCityCountry
    ? "str-census-import-batch3-apply-log.csv"
    : "str-census-import-batch2-apply-log.csv";
  const summaryName = logName.replace("apply-log", "summary");
  writeCsv(join(REPORTS, logName), log);
  writeJson(join(REPORTS, summaryName), summary);

  const batchLabel = flags.markDevCost3 ? "batch 3" : "batch 2";
  console.log(`=== STR import ${batchLabel} (${flags.dryRun ? "DRY RUN" : "LIVE"}) ===\n`);
  console.log("By bucket:", byBucket);
  console.log("Records updated:", ok);
  console.log("Errors:", err);
  if (flags.markDevCost3) console.log(`Development Cost = ${BATCH3_MARKER} on batch 3 rows`);
  else if (flags.markDevCost2) console.log(`Development Cost = ${BATCH2_MARKER} on batch 2 rows`);
  if (flags.conflictsLoose) {
    console.log("\nStill manual after loose pass:");
    console.log("  - Conflicts: LOCATION_MISMATCH / NAME_AND_LOCATION (city/country differ)");
    console.log("  - Conflicts: NAME_MISMATCH where loose name match failed");
  } else {
    console.log("\nManual review still needed:");
    console.log("  - Conflicts: LOCATION_MISMATCH / NAME_MISMATCH (see str-census-conflicts-resolution.csv)");
  }
  console.log("  - Duplicate ties (winner score not > loser)");
  console.log("  - No Match:", excelRows.length, "rows — see str-census-no-match-resolution.csv");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
