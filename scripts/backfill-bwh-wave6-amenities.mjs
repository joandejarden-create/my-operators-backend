#!/usr/bin/env node
/**
 * Wave 6: fill-blank Amenities (+ optional Hotel Description) for BW Premier /
 * BW Signature from BWH hotelDetails proxy when not captcha-blocked.
 *
 *   node scripts/backfill-bwh-wave6-amenities.mjs
 *   node scripts/backfill-bwh-wave6-amenities.mjs --apply
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { CENSUS_AMENITIES_TEXT_FIELD } from "../lib/hilton-amenity-map.js";
import { CENSUS_DESCRIPTION_FIELD } from "../lib/hotel-census/hilton-description-enrichment-contract.js";
import { CENSUS_PROPERTY_ID_FIELD } from "../lib/hotel-census/hilton-property-id-contract.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { isCalaCountry } from "../lib/design-hotels-census-enrichment.js";
import {
  fetchBwhHotelDetails,
  extractBwhAmenitiesFromHotelDetails,
} from "../lib/bwh-brand-directory-extract.js";

const APPLY = process.argv.includes("--apply");
const AFFS = ["BW Premier Collection", "BW Signature Collection"];
const DELAY = 400;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function extractBwhDescription(json) {
  const candidates = [
    json?.hotelDescription,
    json?.description,
    json?.propertyDescription,
    json?.overview,
    json?.hotel?.description,
    json?.hotel?.hotelDescription,
  ];
  for (const c of candidates) {
    const t = String(c || "")
      .replace(/<[^>]+>/g, " ")
      .replace(/\s+/g, " ")
      .trim();
    if (t.length >= 60) return t;
  }
  return "";
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
        CENSUS_PROPERTY_ID_FIELD,
        CENSUS_AMENITIES_TEXT_FIELD,
        CENSUS_DESCRIPTION_FIELD,
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
    const needAmen = isBlankCensusValue(rec.fields[CENSUS_AMENITIES_TEXT_FIELD]);
    const needDesc = isBlankCensusValue(rec.fields[CENSUS_DESCRIPTION_FIELD]);
    if (!needAmen && !needDesc) {
      skipped.push({ id: rec.id, reason: "fields_present" });
      continue;
    }
    const pid = String(rec.fields[CENSUS_PROPERTY_ID_FIELD] || "").trim();
    if (!pid) {
      skipped.push({ id: rec.id, name: rec.fields.name, reason: "no_property_id" });
      continue;
    }
    n++;
    console.log(` [${n}] ${rec.fields.name} ${pid}`);
    const fetched = await fetchBwhHotelDetails(pid);
    await sleep(DELAY);
    if (!fetched.ok) {
      skipped.push({
        id: rec.id,
        name: rec.fields.name,
        pid,
        reason: fetched.error || "fetch_failed",
        status: fetched.status,
      });
      continue;
    }
    const labels = extractBwhAmenitiesFromHotelDetails(fetched.json);
    const desc = extractBwhDescription(fetched.json);
    /** @type {Record<string, string>} */
    const applyFields = {};
    if (needAmen && labels.length) {
      applyFields[CENSUS_AMENITIES_TEXT_FIELD] = labels.join("; ");
    }
    if (needDesc && desc) {
      applyFields[CENSUS_DESCRIPTION_FIELD] = desc;
    }
    if (!Object.keys(applyFields).length) {
      skipped.push({
        id: rec.id,
        name: rec.fields.name,
        pid,
        reason: "no_extractable_content",
        amenityCount: labels.length,
        hasDesc: Boolean(desc),
      });
      continue;
    }
    planRows.push({
      censusRecordId: rec.id,
      censusName: rec.fields.name,
      affiliation: rec.fields[CENSUS_FIELDS.affiliation],
      propertyId: pid,
      amenityCount: labels.length,
      applyFields,
    });
  }

  writeFileSync(
    "reports/bwh-wave6-amenities-plan.json",
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
    console.log(" ", r.affiliation, "|", r.censusName, "|", Object.keys(r.applyFields).join("+"));
  }

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
    "reports/bwh-wave6-amenities-apply-log.json",
    JSON.stringify({ generatedAt: new Date().toISOString(), updated: planRows.length, planRows }, null, 2)
  );
  console.log("Updated:", planRows.length);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
