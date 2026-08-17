#!/usr/bin/env node
/**
 * Wave 6: fill-blank Amenities for Kimpton / Hotel Indigo / Vignette from
 * official IHG property pages (amenity-title / JSON-LD only).
 *
 *   node scripts/backfill-ihg-wave6-amenities.mjs
 *   node scripts/backfill-ihg-wave6-amenities.mjs --apply
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { CENSUS_AMENITIES_TEXT_FIELD } from "../lib/hilton-amenity-map.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { isCalaCountry } from "../lib/design-hotels-census-enrichment.js";
import { IHG_FETCH_HEADERS } from "../lib/ihg-brand-directory-extract.js";
import {
  extractIhgAmenitiesFromHtml,
  formatIhgAmenitiesText,
  ihgHoteldetailLooksBlocked,
} from "../lib/ihg-hotel-amenities-extract.js";

const APPLY = process.argv.includes("--apply");
const AFFS = ["Kimpton Hotels", "Hotel Indigo", "Vignette Collection"];
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
      skipped.push({ id: rec.id, reason: "amenities_present" });
      continue;
    }
    const website = String(rec.fields.Website || "").trim();
    if (!website || !/ihg\.com/i.test(website)) {
      skipped.push({ id: rec.id, name: rec.fields.name, reason: "no_ihg_website" });
      continue;
    }
    n++;
    console.log(` [${n}] ${rec.fields.name}`);
    try {
      const res = await fetch(website, { headers: IHG_FETCH_HEADERS, redirect: "follow" });
      const html = await res.text();
      await sleep(DELAY);
      const blocked = ihgHoteldetailLooksBlocked(html, res.url);
      const amenities = blocked ? [] : extractIhgAmenitiesFromHtml(html);
      const amenitiesText = formatIhgAmenitiesText(amenities);
      if (!amenitiesText || amenities.length < 1) {
        skipped.push({
          id: rec.id,
          name: rec.fields.name,
          reason: blocked ? "blocked" : "no_amenities",
          status: res.status,
          htmlLen: html.length,
        });
        continue;
      }
      planRows.push({
        censusRecordId: rec.id,
        censusName: rec.fields.name,
        affiliation: rec.fields[CENSUS_FIELDS.affiliation],
        website,
        amenityCount: amenities.length,
        applyFields: { [CENSUS_AMENITIES_TEXT_FIELD]: amenitiesText },
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
    "reports/ihg-wave6-amenities-plan.json",
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
  console.log("\nReady:", planRows.length);
  for (const r of planRows) {
    console.log(`  ${r.affiliation} | ${r.censusName} | ${r.amenityCount} amenities`);
  }

  if (!APPLY) {
    console.log("DRY-RUN");
    return;
  }
  let updated = 0;
  let batch = [];
  async function flush() {
    if (!batch.length) return;
    await base(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
    updated += batch.length;
    batch = [];
  }
  for (const row of planRows) {
    batch.push({ id: row.censusRecordId, fields: row.applyFields });
    if (batch.length >= 10) await flush();
  }
  await flush();
  writeFileSync(
    "reports/ihg-wave6-amenities-apply-log.json",
    JSON.stringify({ generatedAt: new Date().toISOString(), updated, planRows }, null, 2)
  );
  console.log("Updated:", updated);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
