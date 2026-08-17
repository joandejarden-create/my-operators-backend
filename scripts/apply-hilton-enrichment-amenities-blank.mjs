#!/usr/bin/env node
/** Apply amenities + website + open date from enrichment plan (fill-blank only). */
import "../load-env.js";
import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { CENSUS_AMENITIES_TEXT_FIELD, CENSUS_AMENITY_YN_COLUMNS } from "../lib/hilton-amenity-map.js";
import { yearFromDate, CENSUS_YEAR_AFFILIATED_FIELD } from "../lib/hotel-census/hilton-census-field-backfill-contract.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const BATCH = 10;

async function fieldExists(base, field) {
  try {
    await base(HOTEL_CENSUS_TABLE).select({ fields: [field], maxRecords: 1 }).firstPage();
    return true;
  } catch {
    return false;
  }
}

async function main() {
  const args = process.argv.slice(2);
  const inputIdx = args.indexOf("--input");
  const input =
    inputIdx >= 0 ? args[inputIdx + 1] : "reports/hilton-census-enrichment-plan-all-brands.json";
  const dryRun = args.includes("--dry-run");

  const plan = JSON.parse(readFileSync(join(__dirname, "..", input), "utf8"));
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );

  const present = new Set(["Website", "Open Date", CENSUS_YEAR_AFFILIATED_FIELD]);
  for (const f of [CENSUS_AMENITIES_TEXT_FIELD, ...CENSUS_AMENITY_YN_COLUMNS]) {
    if (await fieldExists(base, f)) present.add(f);
  }

  const censusById = new Map();
  const censusRecords = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: [...present, CENSUS_FIELDS?.name || "name"].filter(Boolean),
      filterByFormula: `FIND("Hilton", {Parent Company})`,
      pageSize: 100,
    })
    .all();
  for (const rec of censusRecords) censusById.set(rec.id, rec.fields || {});

  const patches = [];
  for (const row of plan.planRows || []) {
    if (!row.censusRecordId || row.matchConfidence === "none") continue;
    const f = censusById.get(row.censusRecordId);
    if (!f) continue;
    const patch = {};

    const website = row.applyFields?.Website || row.sourceUrl;
    if (present.has("Website") && isBlankCensusValue(f.Website) && website) patch.Website = website;

    const openFromActions = (row.fieldActions || []).find((a) => a.logicalKey === "openDate")?.proposed;
    const openDate = row.applyFields?.["Open Date"] || openFromActions;
    if (present.has("Open Date") && isBlankCensusValue(f["Open Date"]) && openDate) {
      patch["Open Date"] = openDate;
    }

    if (present.has(CENSUS_AMENITIES_TEXT_FIELD) && isBlankCensusValue(f[CENSUS_AMENITIES_TEXT_FIELD])) {
      const text = row.amenitiesTextSuggested || row.applyFields?.Amenities;
      if (text) patch[CENSUS_AMENITIES_TEXT_FIELD] = text;
    }

    for (const [col, val] of Object.entries(row.amenityFlagsSuggested || {})) {
      if (present.has(col) && isBlankCensusValue(f[col]) && val) patch[col] = val;
    }

    const od = patch["Open Date"] || f["Open Date"];
    const year = yearFromDate(od);
    if (present.has(CENSUS_YEAR_AFFILIATED_FIELD) && isBlankCensusValue(f[CENSUS_YEAR_AFFILIATED_FIELD]) && year) {
      patch[CENSUS_YEAR_AFFILIATED_FIELD] = year;
    }

    if (Object.keys(patch).length) {
      patches.push({ id: row.censusRecordId, name: row.censusName, fields: patch });
    }
  }

  console.log("Patches ready:", patches.length, dryRun ? "(dry-run)" : "");
  let updated = 0;
  let batch = [];
  for (const p of patches) {
    batch.push({ id: p.id, fields: p.fields });
    if (batch.length >= BATCH) {
      if (!dryRun) await base(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
      updated += batch.length;
      batch = [];
    }
  }
  if (batch.length) {
    if (!dryRun) await base(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
    updated += batch.length;
  }

  const logPath = join(__dirname, "..", "reports", "hilton-enrichment-amenities-apply-log.csv");
  mkdirSync(dirname(logPath), { recursive: true });
  writeFileSync(
    logPath,
    `recordId,name,fields\n${patches.map((p) => `${p.id},"${p.name}",${Object.keys(p.fields).join(";")}`).join("\n")}\n`
  );
  console.log("Updated:", updated);
  console.log("Log:", logPath);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
