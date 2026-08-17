#!/usr/bin/env node
/**
 * Apply fill-blank Market / Submarket fallbacks (STR direct + census-derived).
 *
 * Usage:
 *   node scripts/apply-hotel-census-market-submarket-fallback.mjs --dry-run
 *   node scripts/apply-hotel-census-market-submarket-fallback.mjs
 */
import "../load-env.js";
import { existsSync, readdirSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { getPlatformBase } from "../lib/hotel-census/platform-base.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { readStrExcelDirectory } from "../lib/str-census-import/excel-parse.mjs";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";
import {
  buildMarketFallbackIndexes,
  indexStrExcelById,
  proposeMarketSubmarketFallback,
} from "../lib/hotel-census/geography-fallback-enrichment.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const EXCEL_DIR = join(__dirname, "..", "data", "str-imports");
const BATCH = 10;

async function main() {
  const dryRun = process.argv.includes("--dry-run");
  const base = getPlatformBase();

  const records = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: [
        CENSUS_FIELDS.name,
        CENSUS_FIELDS.city,
        CENSUS_FIELDS.country,
        CENSUS_FIELDS.market,
        CENSUS_FIELDS.submarket,
        "STR Number",
      ],
      pageSize: 100,
    })
    .all();

  const indexes = buildMarketFallbackIndexes(records);
  const excelFiles = existsSync(EXCEL_DIR)
    ? readdirSync(EXCEL_DIR).filter((f) => /\.(xls|xlsx)$/i.test(f))
    : [];
  const { allRows: excelRows } = readStrExcelDirectory(EXCEL_DIR, () => excelFiles);
  const excelByStrId = indexStrExcelById(excelRows);

  const plans = [];
  const sourceCounts = {};

  for (const rec of records) {
    const { fields, sources } = proposeMarketSubmarketFallback(rec.fields, excelByStrId, indexes);
    if (!Object.keys(fields).length) continue;
    for (const src of Object.values(sources)) {
      sourceCounts[src] = (sourceCounts[src] || 0) + 1;
    }
    plans.push({
      recordId: rec.id,
      name: rec.fields[CENSUS_FIELDS.name],
      country: rec.fields[CENSUS_FIELDS.country],
      proposed: fields,
      sources,
    });
  }

  let updated = 0;
  let errors = 0;
  for (let i = 0; i < plans.length; i += BATCH) {
    const chunk = plans.slice(i, i + BATCH);
    if (dryRun) {
      updated += chunk.length;
      continue;
    }
    try {
      await base(HOTEL_CENSUS_TABLE).update(
        chunk.map((p) => ({ id: p.recordId, fields: p.proposed }))
      );
      updated += chunk.length;
      if (updated % 500 === 0) console.log(`  …${updated} updated`);
    } catch (err) {
      errors += chunk.length;
      console.error("Batch failed:", err?.message || err);
    }
  }

  const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const logPath = join(REPORTS, `hotel-census-market-submarket-fallback-${stamp}.csv`);
  const summaryPath = join(REPORTS, `hotel-census-market-submarket-fallback-${stamp}.json`);

  writeCsv(
    logPath,
    plans.map((p) => ({
      recordId: p.recordId,
      name: p.name,
      country: p.country,
      fieldsUpdated: Object.keys(p.proposed).join("; "),
      sources: JSON.stringify(p.sources),
      proposed: JSON.stringify(p.proposed),
    })),
    ["recordId", "name", "country", "fieldsUpdated", "sources", "proposed"]
  );

  writeJson(summaryPath, {
    generatedAt: new Date().toISOString(),
    dryRun,
    totalRecords: records.length,
    recordsUpdated: updated,
    errors,
    sourceCounts,
  });

  console.log(`=== Market/Submarket fallback (${dryRun ? "DRY RUN" : "LIVE"}) ===`);
  console.log("Records with proposals:", plans.length);
  console.log("By source:", sourceCounts);
  console.log("Updated:", updated, dryRun ? "(dry-run)" : "");
  console.log("Errors:", errors);
  console.log("Reports:", logPath, summaryPath);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
