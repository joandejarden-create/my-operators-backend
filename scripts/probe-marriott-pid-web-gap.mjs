#!/usr/bin/env node
import "../load-env.js";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { MAP_DIRECTORY_ENRICHMENT } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { CENSUS_PROPERTY_ID_FIELD } from "../lib/hotel-census/hilton-property-id-contract.js";
import { planMarriottCensusEnrichment } from "../lib/hotel-census/plan-marriott-census-enrichment.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_ALT
);
const recs = await base(HOTEL_CENSUS_TABLE)
  .select({
    filterByFormula: `FIND("Marriott", {${CENSUS_FIELDS.parentCompany}})`,
    fields: [CENSUS_FIELDS.name, MAP_DIRECTORY_ENRICHMENT.website, CENSUS_PROPERTY_ID_FIELD],
    pageSize: 100,
  })
  .all();

const pidNoWeb = recs.filter(
  (r) =>
    !isBlankCensusValue(r.get(CENSUS_PROPERTY_ID_FIELD)) &&
    isBlankCensusValue(r.get(MAP_DIRECTORY_ENRICHMENT.website))
);
const webNoPid = recs.filter(
  (r) =>
    isBlankCensusValue(r.get(CENSUS_PROPERTY_ID_FIELD)) &&
    !isBlankCensusValue(r.get(MAP_DIRECTORY_ENRICHMENT.website))
);
const bothBlank = recs.filter(
  (r) =>
    isBlankCensusValue(r.get(CENSUS_PROPERTY_ID_FIELD)) &&
    isBlankCensusValue(r.get(MAP_DIRECTORY_ENRICHMENT.website))
);

console.log("pid set, website blank:", pidNoWeb.length);
console.log("website set, pid blank:", webNoPid.length);
console.log("both blank:", bothBlank.length);

const plan = await planMarriottCensusEnrichment({ minConfidence: "low" });
console.log("\nPlan ready:", plan.readyToApply);
console.log("Plan by field:");
const byField = {};
for (const r of plan.planRows) {
  for (const f of Object.keys(r.applyFields)) byField[f] = (byField[f] || 0) + 1;
}
console.log(byField);

const plannedIds = new Set(plan.planRows.map((r) => r.censusRecordId));
const pidNoWebNotPlanned = pidNoWeb.filter((r) => !plannedIds.has(r.id));
console.log("\nPID but no web, NOT in plan:", pidNoWebNotPlanned.length);
console.log("Sample:", pidNoWebNotPlanned.slice(0, 10).map((r) => ({
  id: r.id,
  name: r.get(CENSUS_FIELDS.name),
  pid: r.get(CENSUS_PROPERTY_ID_FIELD),
})));
