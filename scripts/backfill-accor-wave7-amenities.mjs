#!/usr/bin/env node
/**
 * Wave 7: fill-blank Amenities for MGallery (and Handwritten if any) from Accor pages.
 *
 *   node scripts/backfill-accor-wave7-amenities.mjs
 *   node scripts/backfill-accor-wave7-amenities.mjs --apply
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { CENSUS_AMENITIES_TEXT_FIELD } from "../lib/hilton-amenity-map.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { isCalaCountry } from "../lib/design-hotels-census-enrichment.js";
import { fetchAccorHotelAmenities } from "../lib/accor-hotel-content-fetch.js";

const APPLY = process.argv.includes("--apply");
const AFFS = ["MGallery Collection", "Handwritten Collection"];
const DELAY = 300;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function main() {
  mkdirSync("reports", { recursive: true });
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );
  const formula = `OR(${AFFS.map((a) => `{${CENSUS_FIELDS.affiliation}}="${a}"`).join(",")})`;
  const records = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: [
        "name",
        CENSUS_FIELDS.affiliation,
        CENSUS_FIELDS.country,
        "Website",
        CENSUS_AMENITIES_TEXT_FIELD,
      ],
      filterByFormula: formula,
      pageSize: 100,
    })
    .all();

  const planRows = [];
  const skipped = [];
  let n = 0;
  for (const rec of records) {
    if (!isCalaCountry(rec.fields[CENSUS_FIELDS.country])) continue;
    if (!isBlankCensusValue(rec.fields[CENSUS_AMENITIES_TEXT_FIELD])) {
      skipped.push({ id: rec.id, reason: "present" });
      continue;
    }
    const website = String(rec.fields.Website || "").trim();
    if (!website || !/accor\.com/i.test(website)) {
      skipped.push({ id: rec.id, name: rec.fields.name, reason: "no_accor_website" });
      continue;
    }
    n++;
    console.log(` [${n}] ${rec.fields.name}`);
    const fetched = await fetchAccorHotelAmenities(website);
    await sleep(DELAY);
    if (!fetched.ok || !fetched.amenitiesText) {
      skipped.push({
        id: rec.id,
        name: rec.fields.name,
        reason: "empty",
        status: fetched.status,
      });
      continue;
    }
    planRows.push({
      censusRecordId: rec.id,
      censusName: rec.fields.name,
      affiliation: rec.fields[CENSUS_FIELDS.affiliation],
      amenityCount: fetched.amenities.length,
      applyFields: { [CENSUS_AMENITIES_TEXT_FIELD]: fetched.amenitiesText },
    });
  }

  writeFileSync(
    "reports/accor-wave7-amenities-plan.json",
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        mode: APPLY ? "apply" : "dry-run",
        readyToApply: planRows.length,
        planRows,
        skipped,
      },
      null,
      2
    )
  );
  console.log("Ready:", planRows.length);
  if (!APPLY) {
    console.log("DRY-RUN");
    return;
  }
  for (const row of planRows) {
    await base(HOTEL_CENSUS_TABLE).update([{ id: row.censusRecordId, fields: row.applyFields }], {
      typecast: true,
    });
  }
  console.log("Updated:", planRows.length);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
