#!/usr/bin/env node
/**
 * Apply Accor amenities from directory extract cache (no live re-fetch).
 * Use after extract when live fetch is rate-limited.
 *
 *   node scripts/apply-accor-amenities-from-directory-cache.mjs
 *   node scripts/apply-accor-amenities-from-directory-cache.mjs --apply
 */
import "../load-env.js";
import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { getPlatformBase } from "../lib/hotel-census/platform-base.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { DEFAULT_ACCOR_EXTRACT_JSON } from "../lib/hotel-census/plan-accor-census-sitemap-match.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const apply = process.argv.includes("--apply");

const extract = JSON.parse(readFileSync(join(__dirname, "..", DEFAULT_ACCOR_EXTRACT_JSON), "utf8"));
const byUrl = new Map(
  (extract.propertyRows || []).map((r) => [String(r.propertyUrl || "").toLowerCase(), r])
);

const base = getPlatformBase();
const records = await base(HOTEL_CENSUS_TABLE)
  .select({
    fields: ["name", "Website", "Amenities"],
    filterByFormula: `FIND("Accor", {${CENSUS_FIELDS.parentCompany}})`,
    pageSize: 100,
  })
  .all();

/** @type {object[]} */
const plan = [];
for (const rec of records) {
  if (!isBlankCensusValue(rec.fields?.Amenities)) continue;
  const url = String(rec.fields?.Website || "").trim().toLowerCase();
  if (!url) continue;
  const dir = byUrl.get(url);
  if (!dir?.amenitiesText) continue;
  plan.push({
    censusRecordId: rec.id,
    censusName: rec.fields?.name,
    propertyUrl: rec.fields?.Website,
    amenityCount: dir.amenities?.length || 0,
    amenitiesText: dir.amenitiesText,
    applyFields: { Amenities: dir.amenitiesText },
    source: "accor_directory_extract_cache",
  });
}

mkdirSync(REPORTS, { recursive: true });
writeFileSync(
  join(REPORTS, "accor-amenities-cache-apply-plan.json"),
  JSON.stringify({ generatedAt: new Date().toISOString(), ready: plan.length, plan }, null, 2)
);

console.log("Accor amenity cache ready:", plan.length, "of", records.length, "census rows");
if (!apply) {
  console.log("Dry-run. Use --apply to write Amenities (fill-blank only).");
  process.exit(0);
}

const airtable = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_ALT
);
let updated = 0;
for (let i = 0; i < plan.length; i += 10) {
  const batch = plan.slice(i, i + 10).map((p) => ({ id: p.censusRecordId, fields: p.applyFields }));
  await airtable(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
  updated += batch.length;
}
console.log("Applied amenities:", updated);
