#!/usr/bin/env node
/**
 * Re-fetch Accor amenities for census rows that already have Website (slow, rate-limit safe).
 */
import "../load-env.js";
import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import { fetchAccorHotelAmenities } from "../lib/accor-hotel-content-fetch.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { getPlatformBase } from "../lib/hotel-census/platform-base.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const apply = process.argv.includes("--apply");
const delayMs = Number(process.argv.find((a) => a.startsWith("--delay-ms="))?.split("=")[1] || 800);

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

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
let n = 0;
for (const rec of records) {
  if (!isBlankCensusValue(rec.fields?.Amenities)) continue;
  const url = String(rec.fields?.Website || "").trim();
  if (!url || !/accor\.com/i.test(url)) continue;
  n++;
  console.log(`[${n}] ${rec.fields?.name}`);
  const fetched = await fetchAccorHotelAmenities(url);
  await sleep(delayMs);
  if (!fetched.amenitiesText) continue;
  plan.push({
    censusRecordId: rec.id,
    censusName: rec.fields?.name,
    propertyUrl: url,
    amenityCount: fetched.amenities.length,
    applyFields: { Amenities: fetched.amenitiesText },
  });
}

const out = join(__dirname, "..", "reports", "accor-amenities-refetch-plan.json");
mkdirSync(join(__dirname, "..", "reports"), { recursive: true });
writeFileSync(out, JSON.stringify({ generatedAt: new Date().toISOString(), ready: plan.length, plan }, null, 2));
console.log("Ready:", plan.length);

if (!apply || !plan.length) process.exit(0);

const airtable = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_ALT
);
for (let i = 0; i < plan.length; i += 10) {
  const batch = plan.slice(i, i + 10).map((p) => ({ id: p.censusRecordId, fields: p.applyFields }));
  await airtable(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
}
console.log("Applied:", plan.length);
