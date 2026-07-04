#!/usr/bin/env node
/**
 * Audit and repair Hotel Census Market / Submarket mis-tags.
 *
 * Usage:
 *   node scripts/apply-hotel-census-market-submarket-consistency.mjs --dry-run
 *   node scripts/apply-hotel-census-market-submarket-consistency.mjs --apply
 *   node scripts/apply-hotel-census-market-submarket-consistency.mjs --dry-run --country=Colombia
 */
import "../load-env.js";
import { existsSync, readdirSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { getPlatformBase } from "../lib/hotel-census/platform-base.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { readStrExcelDirectory } from "../lib/str-census-import/excel-parse.mjs";
import { indexStrExcelById } from "../lib/hotel-census/geography-fallback-enrichment.js";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";
import {
  buildMarketSubmarketConsistencyIndexes,
  planMarketSubmarketConsistencyFixes,
} from "../lib/hotel-census/census-market-submarket-consistency.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const EXCEL_DIR = join(__dirname, "..", "data", "str-imports");
const BATCH = 10;

function parseArgs() {
  const apply = process.argv.includes("--apply");
  const dryRun = process.argv.includes("--dry-run") || !apply;
  const countryArg = process.argv.find((a) => a.startsWith("--country="));
  const country = countryArg ? countryArg.split("=").slice(1).join("=").trim() : "";
  const minConfidence = process.argv.find((a) => a.startsWith("--min-confidence="))?.split("=")[1] || "Medium";
  return { dryRun, apply, country, minConfidence };
}

async function main() {
  const { dryRun, apply, country, minConfidence } = parseArgs();
  const base = getPlatformBase();
  if (!base) {
    console.error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");
    process.exit(1);
  }

  console.log(dryRun ? "DRY RUN — no Airtable writes" : "APPLY — updating Hotel Census");

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

  const scoped = country
    ? records.filter((rec) => String(rec.fields[CENSUS_FIELDS.country] || "").trim() === country)
    : records;

  const excelFiles = existsSync(EXCEL_DIR)
    ? readdirSync(EXCEL_DIR).filter((f) => /\.(xls|xlsx)$/i.test(f))
    : [];
  const { allRows: excelRows } = readStrExcelDirectory(EXCEL_DIR, () => excelFiles);
  const excelByStrId = indexStrExcelById(excelRows);

  const indexes = buildMarketSubmarketConsistencyIndexes(records);
  const { plans, issueCounts, sourceCounts } = planMarketSubmarketConsistencyFixes(
    scoped,
    { indexes, excelByStrId },
    { minConfidence, trustStr: true }
  );

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
      if (updated % 100 === 0) console.log(`  …${updated} updated`);
    } catch (err) {
      errors += chunk.length;
      console.error("Batch failed:", err?.message || err);
    }
  }

  const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const scope = country ? `-${country.replace(/\s+/g, "-").toLowerCase()}` : "";
  const logPath = join(
    REPORTS,
    `hotel-census-market-submarket-consistency${scope}-${stamp}.csv`
  );
  const summaryPath = join(
    REPORTS,
    `hotel-census-market-submarket-consistency${scope}-${stamp}.json`
  );

  writeCsv(
    logPath,
    plans.map((p) => ({
      recordId: p.recordId,
      name: p.name,
      country: p.country,
      city: p.city,
      currentMarket: p.currentMarket,
      currentSubmarket: p.currentSubmarket,
      proposedMarket: p.proposed[CENSUS_FIELDS.market] || "",
      proposedSubmarket: p.proposed[CENSUS_FIELDS.submarket] || "",
      reason: p.reason,
      confidence: p.confidence,
      source: p.source,
      issues: (p.issues || []).join("; "),
    }))
  );

  const summary = {
    generatedAt: new Date().toISOString(),
    dryRun,
    applied: !dryRun,
    countryScope: country || null,
    minConfidence,
    totalCensusRecords: records.length,
    scopedRecords: scoped.length,
    issueCounts,
    sourceCounts,
    proposedUpdates: plans.length,
    updated,
    errors,
    logPath,
  };
  writeJson(summaryPath, summary);

  console.log(JSON.stringify(summary, null, 2));
  console.log(`Report: ${logPath}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
