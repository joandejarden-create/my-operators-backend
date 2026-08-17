#!/usr/bin/env node
import "../load-env.js";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { exactMatchKey, normalizeParentCompanyKey } from "../lib/hotel-census/brand-alias-resolve.js";

const BRAND_SETUP_TABLE = "Brand Setup - Brand Basics";
const ALIAS_TABLE = "Brand Alias Mapping";

const censusBase = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_ALT
);
const setupBase = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID
);

/** @type {Map<string, string>} affiliationKey → parent company from Brand Setup */
const brandSetupParent = new Map();
const setupRows = await setupBase(BRAND_SETUP_TABLE)
  .select({ fields: ["Brand Name", "Parent Company"], pageSize: 100 })
  .all();
for (const r of setupRows) {
  const brand = exactMatchKey(r.get("Brand Name"));
  const parent = String(r.get("Parent Company") || "").trim();
  if (brand && parent) brandSetupParent.set(brand, parent);
}

/** @type {Map<string, string>} affiliation alias → parent from alias table */
const aliasParent = new Map();
try {
  const aliasRows = await censusBase(ALIAS_TABLE)
    .select({
      fields: ["Canonical Brand Name", "Alias / Source Brand Name", "Parent Company", "Active"],
      pageSize: 100,
    })
    .all();
  for (const r of aliasRows) {
    if (r.get("Active") === false) continue;
    const parent = String(r.get("Parent Company") || "").trim();
    if (!parent) continue;
    for (const f of ["Canonical Brand Name", "Alias / Source Brand Name"]) {
      const k = exactMatchKey(r.get(f));
      if (k) aliasParent.set(k, parent);
    }
  }
} catch (e) {
  console.warn("Alias table:", e.message);
}

function resolveExpectedParent(affiliation) {
  const aff = exactMatchKey(affiliation);
  if (!aff) return "";
  if (aliasParent.has(aff)) return aliasParent.get(aff);
  if (brandSetupParent.has(aff)) return brandSetupParent.get(aff);
  for (const [bk, parent] of brandSetupParent) {
    if (aff.includes(bk) || bk.includes(aff)) return parent;
  }
  for (const [bk, parent] of aliasParent) {
    if (aff.includes(bk) || bk.includes(aff)) return parent;
  }
  return "";
}

const HILTON_PARENT_NORM = normalizeParentCompanyKey("Hilton Worldwide");

const censusRows = await censusBase(HOTEL_CENSUS_TABLE)
  .select({
    fields: [CENSUS_FIELDS.name, CENSUS_FIELDS.affiliation, CENSUS_FIELDS.parentCompany, CENSUS_FIELDS.country],
    pageSize: 100,
  })
  .all();

const hiltonParentWrong = [];
const anyMismatch = [];
const noExpected = [];

for (const r of censusRows) {
  const affiliation = String(r.get(CENSUS_FIELDS.affiliation) || "").trim();
  const parent = String(r.get(CENSUS_FIELDS.parentCompany) || "").trim();
  const parentNorm = normalizeParentCompanyKey(parent);
  const expected = resolveExpectedParent(affiliation);

  if (!affiliation || affiliation === "Independent") continue;

  if (parentNorm === HILTON_PARENT_NORM && expected && normalizeParentCompanyKey(expected) !== HILTON_PARENT_NORM) {
    hiltonParentWrong.push({
      id: r.id,
      name: r.get(CENSUS_FIELDS.name),
      affiliation,
      currentParent: parent,
      expectedParent: expected,
      country: r.get(CENSUS_FIELDS.country),
    });
  }

  if (expected && normalizeParentCompanyKey(expected) !== parentNorm) {
    anyMismatch.push({
      id: r.id,
      name: r.get(CENSUS_FIELDS.name),
      affiliation,
      currentParent: parent || "(blank)",
      expectedParent: expected,
      country: r.get(CENSUS_FIELDS.country),
    });
  } else if (!expected && parentNorm === HILTON_PARENT_NORM) {
    noExpected.push({
      id: r.id,
      name: r.get(CENSUS_FIELDS.name),
      affiliation,
      currentParent: parent,
      country: r.get(CENSUS_FIELDS.country),
    });
  }
}

console.log("Brand Setup brands:", brandSetupParent.size);
console.log("Alias mappings:", aliasParent.size);
console.log("Census rows:", censusRows.length);
console.log("\nHilton parent but non-Hilton brand (has expected parent):", hiltonParentWrong.length);
for (const row of hiltonParentWrong.slice(0, 30)) {
  console.log(`  ${row.affiliation} | ${row.name} | ${row.currentParent} → ${row.expectedParent}`);
}
console.log("\nAll parent mismatches (expected from Brand Setup/Alias):", anyMismatch.length);
const byAff = new Map();
for (const row of anyMismatch) {
  const k = `${row.affiliation} | ${row.currentParent} → ${row.expectedParent}`;
  byAff.set(k, (byAff.get(k) || 0) + 1);
}
console.log("Top mismatch groups:");
[...byAff.entries()]
  .sort((a, b) => b[1] - a[1])
  .slice(0, 40)
  .forEach(([k, n]) => console.log(`  ${n}x ${k}`));

console.log("\nHilton parent, no Brand Setup match:", noExpected.length);
const noExpAff = new Map();
for (const row of noExpected) {
  noExpAff.set(row.affiliation, (noExpAff.get(row.affiliation) || 0) + 1);
}
[...noExpAff.entries()].sort((a, b) => b[1] - a[1]).slice(0, 20).forEach(([k, n]) => console.log(`  ${n}x ${k}`));
