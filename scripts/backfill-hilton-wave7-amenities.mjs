#!/usr/bin/env node
/**
 * Wave 7: fill-blank Amenities for Curio/Tapestry rows that already have Property ID
 * (ctyhocn) via Hilton GraphQL amenities — catches any gaps directory sync missed.
 *
 *   node scripts/backfill-hilton-wave7-amenities.mjs
 *   node scripts/backfill-hilton-wave7-amenities.mjs --apply
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { CENSUS_PROPERTY_ID_FIELD } from "../lib/hotel-census/hilton-property-id-contract.js";
import { CENSUS_AMENITIES_TEXT_FIELD } from "../lib/hilton-amenity-map.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { isCalaCountry } from "../lib/design-hotels-census-enrichment.js";
import {
  HILTON_GRAPHQL_URL,
  HILTON_GRAPHQL_HEADERS,
} from "../lib/hilton-hotel-description-fetch.js";

const APPLY = process.argv.includes("--apply");
const AFFS = ["Curio Collection by Hilton", "Tapestry Collection by Hilton"];
const DELAY = 250;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function fetchAmenities(ctyhocn) {
  const query = `query q($ctyhocn: String!, $language: String!) {
    hotel(ctyhocn: $ctyhocn, language: $language) {
      ctyhocn name
      amenities { id name }
    }
  }`;
  const res = await fetch(HILTON_GRAPHQL_URL, {
    method: "POST",
    headers: {
      ...HILTON_GRAPHQL_HEADERS,
      Referer: `https://www.hilton.com/en/hotels/${ctyhocn.toLowerCase()}-hotel/`,
    },
    body: JSON.stringify({
      operationName: "q",
      query,
      variables: { ctyhocn, language: "en" },
    }),
  });
  const json = await res.json();
  const amenities = json?.data?.hotel?.amenities || [];
  const labels = amenities
    .map((a) => String(a?.name || "").trim())
    .filter((s) => s.length >= 2 && s.length <= 100);
  const uniq = [...new Set(labels)].sort((a, b) => a.localeCompare(b));
  return { ok: res.ok && !json.errors, labels: uniq, errors: json.errors || [] };
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
    const pid = String(rec.fields[CENSUS_PROPERTY_ID_FIELD] || "")
      .trim()
      .toUpperCase();
    if (!/^[A-Z0-9]{4,8}$/.test(pid)) {
      skipped.push({ id: rec.id, name: rec.fields.name, reason: "no_ctyhocn" });
      continue;
    }
    n++;
    console.log(` [${n}] ${rec.fields.name} ${pid}`);
    try {
      const got = await fetchAmenities(pid);
      await sleep(DELAY);
      if (!got.labels.length) {
        skipped.push({
          id: rec.id,
          name: rec.fields.name,
          pid,
          reason: "empty_amenities",
          errors: got.errors,
        });
        continue;
      }
      planRows.push({
        censusRecordId: rec.id,
        censusName: rec.fields.name,
        affiliation: rec.fields[CENSUS_FIELDS.affiliation],
        propertyId: pid,
        amenityCount: got.labels.length,
        applyFields: { [CENSUS_AMENITIES_TEXT_FIELD]: got.labels.join("; ") },
      });
    } catch (err) {
      skipped.push({
        id: rec.id,
        name: rec.fields.name,
        pid,
        reason: "error",
        error: String(err?.message || err),
      });
    }
  }

  writeFileSync(
    "reports/hilton-wave7-amenities-plan.json",
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
  for (const r of planRows) console.log(" ", r.propertyId, r.censusName, r.amenityCount);

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
