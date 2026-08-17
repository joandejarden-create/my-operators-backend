#!/usr/bin/env node
/**
 * Active Brand Setup → CALA Hotel Census enrichment coverage matrix.
 *
 *   node scripts/export-active-brand-cala-enrichment-coverage.mjs
 *   node scripts/export-active-brand-cala-enrichment-coverage.mjs --tag=baseline
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import Airtable from "airtable";
import { BRAND_STATUS_ACTIVE_FORMULA } from "../lib/brand-status-active.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { isCalaCountry } from "../lib/design-hotels-census-enrichment.js";
import { CENSUS_PROPERTY_ID_FIELD } from "../lib/hotel-census/hilton-property-id-contract.js";
import { CENSUS_AMENITIES_TEXT_FIELD } from "../lib/hilton-amenity-map.js";
import { CENSUS_DESCRIPTION_FIELD } from "../lib/hotel-census/hilton-description-enrichment-contract.js";
import { writeCsv } from "../lib/str-census-import/report-utils.mjs";

const tag =
  process.argv.find((a) => a.startsWith("--tag="))?.split("=")[1] || "baseline";
const OUT_CSV = join("reports", `active-brand-cala-enrichment-coverage-${tag}.csv`);
const OUT_JSON = join("reports", `active-brand-cala-enrichment-coverage-${tag}.json`);

function blank(v) {
  return v == null || String(v).trim() === "";
}

function pct(n, d) {
  if (!d) return null;
  return Math.round((1000 * n) / d) / 10;
}

async function main() {
  mkdirSync("reports", { recursive: true });
  const apiKey = process.env.AIRTABLE_API_KEY;
  const mvp = new Airtable({ apiKey }).base(process.env.AIRTABLE_BASE_ID);
  const plat = new Airtable({ apiKey }).base(process.env.AIRTABLE_BASE_ID_ALT);

  const brands = (
    await mvp("Brand Setup - Brand Basics")
      .select({
        filterByFormula: BRAND_STATUS_ACTIVE_FORMULA,
        fields: ["Brand Name", "Parent Company"],
      })
      .all()
  )
    .map((r) => ({
      brandName: String(r.fields["Brand Name"] || "").trim(),
      parentCompany: String(r.fields["Parent Company"] || "").trim(),
      recordId: r.id,
    }))
    .filter((b) => b.brandName)
    .sort((a, b) => a.brandName.localeCompare(b.brandName));

  const want = new Set(brands.map((b) => b.brandName));
  /** @type {Record<string, object>} */
  const by = {};
  for (const b of brands) {
    by[b.brandName] = {
      brandName: b.brandName,
      parentCompany: b.parentCompany,
      brandSetupRecordId: b.recordId,
      cala: 0,
      withWebsite: 0,
      withPropertyId: 0,
      withAmenities: 0,
      withDescription: 0,
    };
  }

  const records = await plat(HOTEL_CENSUS_TABLE)
    .select({
      fields: [
        CENSUS_FIELDS.name,
        CENSUS_FIELDS.affiliation,
        CENSUS_FIELDS.country,
        "Website",
        CENSUS_PROPERTY_ID_FIELD,
        CENSUS_AMENITIES_TEXT_FIELD,
        CENSUS_DESCRIPTION_FIELD,
      ],
      pageSize: 100,
    })
    .all();

  for (const r of records) {
    const aff = String(r.fields[CENSUS_FIELDS.affiliation] || "").trim();
    if (!want.has(aff)) continue;
    if (!isCalaCountry(r.fields[CENSUS_FIELDS.country])) continue;
    const row = by[aff];
    row.cala++;
    if (!blank(r.fields.Website)) row.withWebsite++;
    if (!blank(r.fields[CENSUS_PROPERTY_ID_FIELD])) row.withPropertyId++;
    if (!blank(r.fields[CENSUS_AMENITIES_TEXT_FIELD])) row.withAmenities++;
    if (!blank(r.fields[CENSUS_DESCRIPTION_FIELD])) row.withDescription++;
  }

  const rows = brands.map((b) => {
    const x = by[b.brandName];
    return {
      ...x,
      pctWebsite: pct(x.withWebsite, x.cala),
      pctPropertyId: pct(x.withPropertyId, x.cala),
      pctAmenities: pct(x.withAmenities, x.cala),
      pctDescription: pct(x.withDescription, x.cala),
      blankWebsite: x.cala - x.withWebsite,
      blankPropertyId: x.cala - x.withPropertyId,
      blankAmenities: x.cala - x.withAmenities,
      blankDescription: x.cala - x.withDescription,
    };
  });

  writeCsv(OUT_CSV, rows);
  writeFileSync(
    OUT_JSON,
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        tag,
        activeBrandCount: brands.length,
        fieldMapping: {
          Website: "Website",
          propertyId: CENSUS_PROPERTY_ID_FIELD,
          amenities: CENSUS_AMENITIES_TEXT_FIELD,
          description: CENSUS_DESCRIPTION_FIELD,
        },
        rows,
      },
      null,
      2
    )
  );

  console.log(`Wrote ${OUT_CSV}`);
  console.log("Brand|CALA|%Web|%PID|%Amen|%Desc");
  for (const r of rows) {
    if (!r.cala) {
      console.log(`${r.brandName}|0|—|—|—|—`);
      continue;
    }
    console.log(
      `${r.brandName}|${r.cala}|${r.pctWebsite}|${r.pctPropertyId}|${r.pctAmenities}|${r.pctDescription}`
    );
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
