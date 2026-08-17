#!/usr/bin/env node
/**
 * Wave 6: fill-blank Amenities for Dazzler + Trademark from wyndhamhotels.com
 * (services-amenities / overview JSON-LD — no invention).
 *
 *   node scripts/backfill-wyndham-wave6-amenities.mjs
 *   node scripts/backfill-wyndham-wave6-amenities.mjs --apply
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { CENSUS_AMENITIES_TEXT_FIELD } from "../lib/hilton-amenity-map.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { isCalaCountry } from "../lib/design-hotels-census-enrichment.js";
import { fetchWyndhamHotelAmenities } from "../lib/wyndham-hotel-content-fetch.js";

const APPLY = process.argv.includes("--apply");
const AFFS = ["Dazzler by Wyndham", "Trademark Collection by Wyndham"];
const DELAY = 400;

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
      skipped.push({ id: rec.id, reason: "amenities_present" });
      continue;
    }
    const website = String(rec.fields.Website || "").trim();
    if (!website || !/wyndhamhotels\.com/i.test(website)) {
      skipped.push({ id: rec.id, name: rec.fields.name, reason: "no_wyndham_website" });
      continue;
    }
    n++;
    console.log(` [${n}] ${rec.fields.name}`);
    try {
      const fetched = await fetchWyndhamHotelAmenities(website);
      await sleep(DELAY);
      if (!fetched.ok || !fetched.amenitiesText || !fetched.amenities?.length) {
        skipped.push({
          id: rec.id,
          name: rec.fields.name,
          reason: "empty_or_soft_blocked",
          parseErrors: fetched.parseErrors || [],
          status: fetched.status,
        });
        continue;
      }
      planRows.push({
        censusRecordId: rec.id,
        censusName: rec.fields.name,
        affiliation: rec.fields[CENSUS_FIELDS.affiliation],
        website,
        amenityCount: fetched.amenities.length,
        source: fetched.source,
        applyFields: { [CENSUS_AMENITIES_TEXT_FIELD]: fetched.amenitiesText },
      });
    } catch (err) {
      skipped.push({
        id: rec.id,
        name: rec.fields.name,
        reason: "fetch_error",
        error: String(err?.message || err),
      });
    }
  }

  writeFileSync(
    "reports/wyndham-wave6-amenities-plan.json",
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
  console.log("\nReady:", planRows.length, "Skipped soft-block/empty:", skipped.filter((s) => s.reason === "empty_or_soft_blocked").length);
  for (const r of planRows) console.log(`  ${r.affiliation} | ${r.censusName} | ${r.amenityCount}`);

  if (!APPLY) {
    console.log("DRY-RUN");
    return;
  }
  for (const row of planRows) {
    await base(HOTEL_CENSUS_TABLE).update([{ id: row.censusRecordId, fields: row.applyFields }], {
      typecast: true,
    });
  }
  writeFileSync(
    "reports/wyndham-wave6-amenities-apply-log.json",
    JSON.stringify({ generatedAt: new Date().toISOString(), updated: planRows.length, planRows }, null, 2)
  );
  console.log("Updated:", planRows.length);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
