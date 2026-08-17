#!/usr/bin/env node
/**
 * Post-apply audit: CALA IHG Website / Property ID / Amenities blank counts.
 */
import "../load-env.js";
import { writeFileSync } from "node:fs";
import { CENSUS_FIELDS, HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";
import { getPlatformBase } from "../lib/hotel-census/platform-base.js";
import { CENSUS_PROPERTY_ID_FIELD } from "../lib/hotel-census/hilton-property-id-contract.js";
import { isCalaCountry } from "../lib/design-hotels-census-enrichment.js";
import { IHG_PARENT_FORMULA } from "../lib/hotel-census/plan-ihg-census-directory-match.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";

const base = getPlatformBase();
const recs = await base(HOTEL_CENSUS_TABLE)
  .select({
    fields: [
      "name",
      CENSUS_FIELDS.parentCompany,
      CENSUS_FIELDS.country,
      "Website",
      CENSUS_PROPERTY_ID_FIELD,
      "Amenities",
    ],
    filterByFormula: IHG_PARENT_FORMULA,
    pageSize: 100,
  })
  .all();

const cala = recs.filter((r) => isCalaCountry(r.fields[CENSUS_FIELDS.country]));
const stats = {
  generatedAt: new Date().toISOString(),
  totalIhg: recs.length,
  calaIhg: cala.length,
  withWebsite: 0,
  withPropertyId: 0,
  withAmenities: 0,
  blankWebsite: 0,
  blankPropertyId: 0,
  blankAmenities: 0,
  bothWebsiteAndPropertyId: 0,
};

for (const r of cala) {
  const web = !isBlankCensusValue(r.fields.Website);
  const pid = !isBlankCensusValue(r.fields[CENSUS_PROPERTY_ID_FIELD]);
  const amen = !isBlankCensusValue(r.fields.Amenities);
  if (web) stats.withWebsite++;
  else stats.blankWebsite++;
  if (pid) stats.withPropertyId++;
  else stats.blankPropertyId++;
  if (amen) stats.withAmenities++;
  else stats.blankAmenities++;
  if (web && pid) stats.bothWebsiteAndPropertyId++;
}

console.log(JSON.stringify(stats, null, 2));
writeFileSync("reports/ihg-census-post-apply-audit.json", JSON.stringify(stats, null, 2));
