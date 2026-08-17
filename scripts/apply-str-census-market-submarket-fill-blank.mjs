#!/usr/bin/env node
/**
 * Fill blank Market and Submarket from STR Excel (fill-blank only).
 *
 * Match buckets (Market/Submarket fields only — no city/country/name/dev-cost):
 *   - Matched by STR ID
 *   - Matched by Name City Country
 *   - Duplicate STR ID in Census (best-scoring row)
 *   - Conflict when name + location are equivalent (strict or loose wording)
 *
 * Usage:
 *   node scripts/apply-str-census-market-submarket-fill-blank.mjs --dry-run
 *   node scripts/apply-str-census-market-submarket-fill-blank.mjs
 */
import "../load-env.js";
import { readFileSync, existsSync, readdirSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
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

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const DEFAULT_EXCEL_DIR = join(__dirname, "..", "data", "str-imports");
const CENSUS_SUMMARY = join(REPORTS, "hotel-census-str-inventory-summary.json");
const BATCH_SIZE = 10;

function parseArgs() {
  return { dryRun: process.argv.includes("--dry-run") };
}

function loadCensusMapping() {
  if (!existsSync(CENSUS_SUMMARY)) {
    throw new Error(`Missing ${CENSUS_SUMMARY}. Run inventory-hotel-census-for-str-import.mjs first.`);
  }
  const summary = JSON.parse(readFileSync(CENSUS_SUMMARY, "utf8"));
  const mapping = summary.recommendedFieldMapping || {};
  if (!mapping.strId) throw new Error("Census STR ID field not mapped.");
  return { mapping };
}

function listExcelFiles(dir) {
  if (!existsSync(dir)) return [];
  return readdirSync(dir).filter((f) => /\.(xlsx|xls)$/i.test(f));
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
  if (!batch.length) return { updated: 0, errors: 0 };
  if (dryRun) return { updated: batch.length, errors: 0 };
  try {
    await base(HOTEL_CENSUS_TABLE).update(
      batch.map((b) => ({ id: b.id, fields: b.fields })),
      { typecast: true }
    );
    return { updated: batch.length, errors: 0 };
  } catch (err) {
    console.error("Batch update failed:", err?.message || err);
    return { updated: 0, errors: batch.length };
  }
}

/**
 * @returns {{ recordId: string, censusEntry: object, bucket: string } | null}
 */
function resolveMatch(ex, m, byStrId) {
  if (m.status === "Matched by STR ID" && m.censusEntry) {
    return { recordId: m.matchedRecordId, censusEntry: m.censusEntry, bucket: "STR_ID" };
  }

  if (m.status === "Matched by Name City Country" && m.censusEntry) {
    return { recordId: m.matchedRecordId, censusEntry: m.censusEntry, bucket: "NAME_CITY_COUNTRY" };
  }

  if (m.status === "Duplicate STR ID in Census" && ex.strId) {
    const candidates = byStrId.get(ex.strId);
    const winner = pickDuplicateWinner(ex, candidates);
    if (winner) return { recordId: winner.recordId, censusEntry: winner, bucket: "DUPLICATE_WINNER" };
  }

  if (m.status === "Conflict" && m.censusEntry) {
    const locEq = locationEquivalent(
      { city: ex.city, country: ex.country },
      { city: m.censusEntry.city, country: m.censusEntry.country }
    );
    if (!locEq) return null;
    const strictName = hotelNamesEquivalent(ex.hotelName, m.censusEntry.name);
    const looseName = hotelNamesLooselyEquivalent(ex.hotelName, m.censusEntry.name);
    if (strictName || looseName) {
      return {
        recordId: m.matchedRecordId,
        censusEntry: m.censusEntry,
        bucket: strictName ? "AUTO_CONFLICT" : "CONFLICT_LOOSE",
      };
    }
  }

  return null;
}

async function main() {
  const { dryRun } = parseArgs();
  const { mapping } = loadCensusMapping();
  const geoFields = [mapping.strMarket, mapping.strSubmarket].filter(Boolean);

  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT;
  if (!apiKey || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID_ALT");

  console.log(`=== STR Market/Submarket fill-blank (${dryRun ? "DRY RUN" : "LIVE"}) ===\n`);
  console.log("Fields:", geoFields.join(", "));

  const { allRows: excelRows } = readStrExcelDirectory(DEFAULT_EXCEL_DIR, () =>
    listExcelFiles(DEFAULT_EXCEL_DIR)
  );
  const excelStrIdCounts = new Map();
  for (const row of excelRows) {
    if (!row.strId) continue;
    excelStrIdCounts.set(row.strId, (excelStrIdCounts.get(row.strId) || 0) + 1);
  }

  const base = new Airtable({ apiKey }).base(baseId);
  console.log("\nLoading Hotel Census...");
  const records = await base(HOTEL_CENSUS_TABLE).select({ pageSize: 100 }).all();
  const { byStrId, byNcc } = buildCensusIndexes(records, mapping);

  const updatesByRecordId = new Map();
  const logRows = [];
  const byBucket = {};

  for (const ex of excelRows) {
    const m = matchExcelRow(ex, byStrId, byNcc, excelStrIdCounts);
    const resolved = resolveMatch(ex, m, byStrId);
    if (!resolved) continue;

    const patch = buildCensusUpdateFields(mapping, ex, resolved.censusEntry, {
      fillBlankOnly: true,
      fieldsOnly: geoFields,
    });
    if (!Object.keys(patch).length) continue;

    byBucket[resolved.bucket] = (byBucket[resolved.bucket] || 0) + 1;

    const existing = updatesByRecordId.get(resolved.recordId);
    if (existing) Object.assign(existing.fields, patch);
    else {
      updatesByRecordId.set(resolved.recordId, {
        id: resolved.recordId,
        fields: { ...patch },
        bucket: resolved.bucket,
        strId: ex.strId,
      });
    }
  }

  let updated = 0;
  let errors = 0;
  let batch = [];

  for (const item of updatesByRecordId.values()) {
    logRows.push({
      action: dryRun ? "would_update" : "updated",
      recordId: item.id,
      strId: item.strId,
      bucket: item.bucket,
      fieldsUpdated: Object.keys(item.fields).join("; "),
      notes: JSON.stringify(item.fields),
    });

    batch.push({ id: item.id, fields: item.fields });
    if (batch.length >= BATCH_SIZE) {
      const result = await flushBatch(base, batch, dryRun);
      updated += result.updated;
      errors += result.errors;
      batch = [];
      if (!dryRun && updated > 0 && updated % 100 === 0) console.log(`  …${updated} records updated`);
    }
  }

  if (batch.length) {
    const result = await flushBatch(base, batch, dryRun);
    updated += result.updated;
    errors += result.errors;
  }

  const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const logPath = join(REPORTS, `str-census-market-submarket-fill-blank-${stamp}.csv`);
  const summaryPath = join(REPORTS, `str-census-market-submarket-fill-blank-${stamp}.json`);

  writeCsv(logPath, logRows, ["action", "recordId", "strId", "bucket", "fieldsUpdated", "notes"]);
  writeJson(summaryPath, {
    generatedAt: new Date().toISOString(),
    dryRun,
    excelRowsScanned: excelRows.length,
    recordsWithChanges: updatesByRecordId.size,
    recordsUpdated: updated,
    errors,
    byBucket,
    fieldMapping: { market: mapping.strMarket, submarket: mapping.strSubmarket },
  });

  console.log("\n--- Summary ---");
  console.log("  Unique records to update:", updatesByRecordId.size);
  console.log("  By bucket:", byBucket);
  console.log("  Updated:", updated, dryRun ? "(dry-run)" : "");
  console.log("  Errors:", errors);
  console.log("\nReports:", logPath, summaryPath);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
