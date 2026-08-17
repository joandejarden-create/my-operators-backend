#!/usr/bin/env node
/**
 * Brand census match expansion — Wyndham + Accor (dry-run + optional apply medium+).
 *
 *   node scripts/run-brand-census-match-expansion.mjs --brand wyndham
 *   node scripts/run-brand-census-match-expansion.mjs --brand accor
 *   node scripts/run-brand-census-match-expansion.mjs --brand all --apply
 */
import "../load-env.js";
import { readFileSync, existsSync, writeFileSync, mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import { CENSUS_FIELDS, HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";
import {
  planBrandCensusDirectoryMatch,
  writeBrandCensusMatchExpansionReports,
} from "../lib/hotel-census/plan-brand-census-directory-match.js";
import { loadWyndhamDirectoryRows } from "../lib/hotel-census/plan-wyndham-census-sitemap-match.js";
import { loadAccorDirectoryRows } from "../lib/hotel-census/plan-accor-census-sitemap-match.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");

const BRAND_CONFIG = {
  wyndham: {
    label: "Wyndham",
    parentFormula: `FIND("Wyndham", {${CENSUS_FIELDS.parentCompany}})`,
    loadDirectory: () => loadWyndhamDirectoryRows(),
    reportBasename: "wyndham-census-match-expansion",
  },
  accor: {
    label: "Accor",
    parentFormula: `FIND("Accor", {${CENSUS_FIELDS.parentCompany}})`,
    loadDirectory: () => loadAccorDirectoryRows(),
    reportBasename: "accor-census-match-expansion",
  },
};

function parseArgs() {
  const args = process.argv.slice(2);
  const brandArg = args.find((a) => a.startsWith("--brand="))?.split("=")[1] ||
    (args.includes("--brand") ? args[args.indexOf("--brand") + 1] : "all");
  return {
    brand: brandArg || "all",
    apply: args.includes("--apply"),
    minScore: Number(args.find((a) => a.startsWith("--min-score="))?.split("=")[1] || 50),
    minApplyConfidence: args.find((a) => a.startsWith("--min-apply-confidence="))?.split("=")[1] || "medium",
  };
}

async function runBrand(brandKey, opts) {
  const cfg = BRAND_CONFIG[brandKey];
  if (!cfg) throw new Error(`Unknown brand: ${brandKey}`);

  console.log(`\n=== ${cfg.label} census match expansion ===\n`);
  const directoryRows = cfg.loadDirectory();
  if (!directoryRows.length) {
    throw new Error(`No directory rows for ${cfg.label}. Run extract first.`);
  }
  console.log("Directory rows:", directoryRows.length);

  const plan = await planBrandCensusDirectoryMatch({
    parentFormula: cfg.parentFormula,
    directoryRows,
    minScore: opts.minScore,
    requireCountryMatch: true,
    minApplyConfidence: opts.minApplyConfidence,
    includeAmenitiesFromCache: true,
    scoringProfile: brandKey === "accor" ? "accor" : "",
  });

  const paths = writeBrandCensusMatchExpansionReports(plan, cfg.reportBasename, REPORTS);
  console.log("Census scanned:", plan.censusRowsScanned);
  console.log("Pair candidates (pre-dedup):", plan.pairCandidates);
  console.log("Assigned (1:1):", plan.assignedCount);
  console.log("Auto-apply ready (medium+):", plan.readyToApply);
  console.log("Steward review (low):", plan.stewardReviewCount);
  console.log("Skipped/unmatched:", plan.skippedCount);
  console.log("Plan:", paths.jsonPath);
  console.log("Steward CSV:", paths.stewardPath);

  if (!opts.apply || !plan.planRows.length) return { brand: brandKey, updated: 0, plan };

  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );
  let updated = 0;
  let batch = [];
  for (const row of plan.planRows) {
    if (!Object.keys(row.applyFields).length) continue;
    batch.push({ id: row.censusRecordId, fields: row.applyFields });
    if (batch.length >= 10) {
      await base(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
      updated += batch.length;
      batch = [];
    }
  }
  if (batch.length) {
    await base(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
    updated += batch.length;
  }

  console.log("Applied rows:", updated);
  mkdirSync(REPORTS, { recursive: true });
  writeFileSync(
    join(REPORTS, `${cfg.reportBasename}-apply-log.json`),
    JSON.stringify({ generatedAt: new Date().toISOString(), updated, brand: brandKey }, null, 2)
  );
  return { brand: brandKey, updated, plan };
}

const opts = parseArgs();
const brands = opts.brand === "all" ? ["wyndham", "accor"] : [opts.brand];

/** @type {object[]} */
const results = [];
for (const b of brands) {
  results.push(await runBrand(b, opts));
}

if (!opts.apply) {
  console.log("\nDry-run complete. Use --apply to write medium+ confidence matches only.");
}
