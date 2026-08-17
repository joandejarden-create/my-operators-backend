#!/usr/bin/env node
/**
 * Audit Hotel Census Amenities fill rate by Parent Company.
 *
 *   node scripts/audit-census-amenities-by-parent.mjs
 *   node scripts/audit-census-amenities-by-parent.mjs --csv
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { getPlatformBase } from "../lib/hotel-census/platform-base.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { CENSUS_AMENITIES_TEXT_FIELD } from "../lib/hilton-amenity-map.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const AMENITIES_FIELD = CENSUS_AMENITIES_TEXT_FIELD;

function isBlank(v) {
  return v == null || String(v).trim() === "";
}

function parentKey(raw) {
  const s = String(raw || "").trim();
  return s || "(blank parent)";
}

function parseArgs() {
  return { csv: process.argv.includes("--csv") };
}

async function main() {
  const opts = parseArgs();
  const base = getPlatformBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const fields = [
    CENSUS_FIELDS.name,
    CENSUS_FIELDS.parentCompany,
    CENSUS_FIELDS.status,
    CENSUS_FIELDS.country,
    AMENITIES_FIELD,
  ];

  const records = await base(HOTEL_CENSUS_TABLE).select({ fields, pageSize: 100 }).all();

  /** @type {Map<string, { total: number, filled: number, blank: number, open: number, openBlank: number }>} */
  const byParent = new Map();
  /** @type {object[]} */
  const blankRows = [];

  for (const rec of records) {
    const f = rec.fields || {};
    const parent = parentKey(f[CENSUS_FIELDS.parentCompany]);
    const filled = !isBlank(f[AMENITIES_FIELD]);
    const status = String(f[CENSUS_FIELDS.status] || "").trim().toLowerCase();
    const isOpen = status === "open";

    if (!byParent.has(parent)) {
      byParent.set(parent, { total: 0, filled: 0, blank: 0, open: 0, openBlank: 0 });
    }
    const bucket = byParent.get(parent);
    bucket.total++;
    if (filled) bucket.filled++;
    else {
      bucket.blank++;
      blankRows.push({
        recordId: rec.id,
        name: f[CENSUS_FIELDS.name] || "",
        parentCompany: parent,
        status: f[CENSUS_FIELDS.status] || "",
        country: f[CENSUS_FIELDS.country] || "",
      });
    }
    if (isOpen) {
      bucket.open++;
      if (!filled) bucket.openBlank++;
    }
  }

  const rows = [...byParent.entries()]
    .map(([parentCompany, stats]) => ({
      parentCompany,
      ...stats,
      fillPct: stats.total ? Math.round((100 * stats.filled) / stats.total) : 0,
      openFillPct: stats.open ? Math.round((100 * (stats.open - stats.openBlank)) / stats.open) : 0,
    }))
    .sort((a, b) => b.blank - a.blank);

  const summary = {
    generatedAt: new Date().toISOString(),
    totalRows: records.length,
    amenitiesFilled: records.length - blankRows.length,
    amenitiesBlank: blankRows.length,
    fillPct: records.length
      ? Math.round((100 * (records.length - blankRows.length)) / records.length)
      : 0,
    parentCompanyCount: rows.length,
    byParent: rows,
  };

  mkdirSync(REPORTS, { recursive: true });
  const jsonPath = join(REPORTS, "census-amenities-by-parent-audit.json");
  writeFileSync(jsonPath, JSON.stringify(summary, null, 2));

  console.log("=== Hotel Census Amenities by Parent Company ===\n");
  console.log("Total rows:", summary.totalRows);
  console.log("Amenities filled:", summary.amenitiesFilled, `(${summary.fillPct}%)`);
  console.log("Amenities blank:", summary.amenitiesBlank);
  console.log("\nBy parent (sorted by blank count):\n");
  console.log(
    "Parent".padEnd(42),
    "Total".padStart(6),
    "Filled".padStart(7),
    "Blank".padStart(6),
    "Fill%".padStart(6),
    "OpenBlank".padStart(10)
  );
  console.log("-".repeat(80));
  for (const r of rows) {
    console.log(
      r.parentCompany.slice(0, 41).padEnd(42),
      String(r.total).padStart(6),
      String(r.filled).padStart(7),
      String(r.blank).padStart(6),
      String(r.fillPct).padStart(5) + "%",
      String(r.openBlank).padStart(10)
    );
  }
  console.log("\nJSON:", jsonPath);

  if (opts.csv) {
    const csvPath = join(REPORTS, "census-amenities-blank-rows.csv");
    const header = "recordId,name,parentCompany,status,country\n";
    const body = blankRows
      .map((r) =>
        [r.recordId, `"${String(r.name).replace(/"/g, '""')}"`, `"${r.parentCompany}"`, r.status, r.country].join(
          ","
        )
      )
      .join("\n");
    writeFileSync(csvPath, header + body + "\n");
    console.log("Blank rows CSV:", csvPath);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
