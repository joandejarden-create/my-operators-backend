#!/usr/bin/env node
import "../load-env.js";
import { readFileSync, writeFileSync, mkdirSync } from "fs";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { BRAND_STATUS_ACTIVE_FORMULA } from "../lib/brand-status-active.js";

const APPLY = process.argv.includes("--apply");
mkdirSync("reports", { recursive: true });

const apiKey = process.env.AIRTABLE_API_KEY;
const mvp = new Airtable({ apiKey }).base(process.env.AIRTABLE_BASE_ID);
const plat = new Airtable({ apiKey }).base(process.env.AIRTABLE_BASE_ID_ALT);

const active = new Set(
  (
    await mvp("Brand Setup - Brand Basics")
      .select({ filterByFormula: BRAND_STATUS_ACTIVE_FORMULA, fields: ["Brand Name"] })
      .all()
  )
    .map((r) => String(r.fields["Brand Name"] || "").trim())
    .filter(Boolean)
);

const plan = JSON.parse(readFileSync("reports/choice-census-regional-enrichment-plan.json", "utf8"));
const applyPlan = (plan.applyPlan || []).filter((row) => {
  // Look up affiliation
  return true;
});

/** @type {object[]} */
const ready = [];
for (const row of plan.applyPlan || []) {
  const recs = await plat(HOTEL_CENSUS_TABLE)
    .select({
      filterByFormula: `RECORD_ID()="${row.censusRecordId}"`,
      fields: [CENSUS_FIELDS.affiliation, "Website", "Property ID"],
      maxRecords: 1,
    })
    .firstPage();
  const aff = String(recs[0]?.fields?.[CENSUS_FIELDS.affiliation] || "").trim();
  if (!active.has(aff)) {
    console.log("SKIP non-active", row.censusName, aff);
    continue;
  }
  ready.push({ ...row, affiliation: aff });
}

writeFileSync(
  "reports/choice-wave3-active-apply-plan.json",
  JSON.stringify({ generatedAt: new Date().toISOString(), ready }, null, 2)
);
console.log("Active Choice ready:", ready.length);
for (const r of ready) console.log(" ", r.affiliation, r.censusName, r.applyFields);

if (!APPLY) {
  console.log("DRY-RUN");
  process.exit(0);
}

for (const row of ready) {
  await plat(HOTEL_CENSUS_TABLE).update([{ id: row.censusRecordId, fields: row.applyFields }], {
    typecast: true,
  });
  console.log("UPDATED", row.censusRecordId, row.censusName);
}
writeFileSync(
  "reports/choice-wave3-active-apply-log.json",
  JSON.stringify({ generatedAt: new Date().toISOString(), updated: ready.length, ready }, null, 2)
);
