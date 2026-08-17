#!/usr/bin/env node
/**
 * Fill-blank Amenities for Vignette Collection rows with IHG Website.
 *
 *   node scripts/backfill-vignette-cala-amenities.mjs
 *   node scripts/backfill-vignette-cala-amenities.mjs --apply
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { IHG_FETCH_HEADERS } from "../lib/ihg-brand-directory-extract.js";
import {
  extractIhgAmenitiesFromHtml,
  formatIhgAmenitiesText,
  ihgHoteldetailLooksBlocked,
} from "../lib/ihg-hotel-amenities-extract.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";

const APPLY = process.argv.includes("--apply");
const DELAY = 250;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function main() {
  mkdirSync("reports", { recursive: true });
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );
  const rows = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: ["name", "Website", "Amenities"],
      filterByFormula: `{${CENSUS_FIELDS.affiliation}}="Vignette Collection"`,
    })
    .all();

  const plan = [];
  for (const rec of rows) {
    if (!isBlankCensusValue(rec.fields?.Amenities)) continue;
    const url = String(rec.fields?.Website || "").trim();
    if (!/^https:\/\/www\.ihg\.com\/vignettecollection\//i.test(url)) {
      plan.push({ id: rec.id, name: rec.fields?.name, status: "no_ihg_website" });
      continue;
    }
    const res = await fetch(url, { headers: IHG_FETCH_HEADERS, redirect: "follow" });
    const html = await res.text();
    await sleep(DELAY);
    const blocked = ihgHoteldetailLooksBlocked(html, res.url);
    const amenities = blocked ? [] : extractIhgAmenitiesFromHtml(html);
    const amenitiesText = formatIhgAmenitiesText(amenities);
    plan.push({
      id: rec.id,
      name: rec.fields?.name,
      url,
      status: amenities.length ? "ready" : blocked ? "blocked" : "empty",
      amenityCount: amenities.length,
      amenitiesText,
      applyFields: amenities.length ? { Amenities: amenitiesText } : null,
    });
    console.log(rec.fields?.name, plan[plan.length - 1].status, amenities.length);
  }

  writeFileSync(
    "reports/vignette-cala-amenities-plan.json",
    JSON.stringify({ generatedAt: new Date().toISOString(), plan }, null, 2)
  );

  const ready = plan.filter((p) => p.applyFields);
  if (!APPLY) {
    console.log("DRY-RUN ready:", ready.length);
    return;
  }
  for (const p of ready) {
    await base(HOTEL_CENSUS_TABLE).update([{ id: p.id, fields: p.applyFields }], {
      typecast: true,
    });
  }
  console.log("Applied amenities:", ready.length);
  writeFileSync(
    "reports/vignette-cala-amenities-apply-log.json",
    JSON.stringify({ generatedAt: new Date().toISOString(), applied: ready.length, ready }, null, 2)
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
