#!/usr/bin/env node
import "../load-env.js";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { exactMatchKey, normalizeParentCompanyKey } from "../lib/hotel-census/brand-alias-resolve.js";

const BRAND_SETUP_TABLE = "Brand Setup - Brand Basics";
const censusBase = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_ALT
);
const setupBase = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID
);

const HILTON_PARENT_NORM = normalizeParentCompanyKey("Hilton Worldwide");

/** Hilton-brand affiliations from Brand Setup */
const hiltonBrandKeys = new Set();
const brandSetupParent = new Map();
const setupRows = await setupBase(BRAND_SETUP_TABLE)
  .select({ fields: ["Brand Name", "Parent Company"], pageSize: 100 })
  .all();
for (const r of setupRows) {
  const brand = exactMatchKey(r.get("Brand Name"));
  const parent = String(r.get("Parent Company") || "").trim();
  if (brand && parent) {
    brandSetupParent.set(brand, parent);
    if (normalizeParentCompanyKey(parent) === HILTON_PARENT_NORM) {
      hiltonBrandKeys.add(brand);
    }
  }
}

function isHiltonAffiliation(affiliation) {
  const aff = exactMatchKey(affiliation);
  if (!aff) return false;
  if (hiltonBrandKeys.has(aff)) return true;
  for (const bk of hiltonBrandKeys) {
    if (aff.includes(bk) || bk.includes(aff)) return true;
  }
  return false;
}

const censusRows = await censusBase(HOTEL_CENSUS_TABLE)
  .select({
    fields: [CENSUS_FIELDS.name, CENSUS_FIELDS.affiliation, CENSUS_FIELDS.parentCompany, CENSUS_FIELDS.country],
    filterByFormula: `FIND("Hilton", {${CENSUS_FIELDS.parentCompany}})`,
    pageSize: 100,
  })
  .all();

console.log("Rows with Hilton in Parent Company:", censusRows.length);
console.log("Hilton brands in Brand Setup:", hiltonBrandKeys.size);

const wrong = [];
for (const r of censusRows) {
  const affiliation = String(r.get(CENSUS_FIELDS.affiliation) || "").trim();
  const parent = String(r.get(CENSUS_FIELDS.parentCompany) || "").trim();
  if (isHiltonAffiliation(affiliation)) continue;
  wrong.push({
    id: r.id,
    name: r.get(CENSUS_FIELDS.name),
    affiliation,
    parent,
    country: r.get(CENSUS_FIELDS.country),
    expectedFromSetup: brandSetupParent.get(exactMatchKey(affiliation)) || "",
  });
}

console.log("\nNon-Hilton affiliation with Hilton parent:", wrong.length);
for (const row of wrong) {
  console.log(JSON.stringify(row));
}
