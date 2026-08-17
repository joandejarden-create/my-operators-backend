#!/usr/bin/env node
import "../load-env.js";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { MAP_DIRECTORY_ENRICHMENT, isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { CENSUS_PROPERTY_ID_FIELD } from "../lib/hotel-census/hilton-property-id-contract.js";
import {
  planMarriottCensusEnrichment,
  pickMarriottDirectoryNameMatch,
} from "../lib/hotel-census/plan-marriott-census-enrichment.js";
import { crawlMarriottCountrySitemaps } from "../lib/marriott-brand-directory-extract.js";
import { crawlRitzCarltonOverviewSupplement, mergeMarriottDirectoryRows } from "../lib/marriott-brand-tld-sitemap-supplement.js";
import { deriveCountrySlugsFromCensusRows } from "../lib/hotel-census/plan-marriott-census-enrichment.js";
import { mapCensusRowForDirectoryMatch } from "../lib/hotel-census/match-brand-directory-to-census.js";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_ALT
);
const recs = await base(HOTEL_CENSUS_TABLE)
  .select({
    filterByFormula: `FIND("Marriott", {${CENSUS_FIELDS.parentCompany}})`,
    fields: [CENSUS_FIELDS.name, CENSUS_FIELDS.country, CENSUS_FIELDS.city, CENSUS_FIELDS.status, MAP_DIRECTORY_ENRICHMENT.website, CENSUS_PROPERTY_ID_FIELD],
    pageSize: 100,
  })
  .all();

function isOpen(r) {
  const s = r.get(CENSUS_FIELDS.status);
  if (Array.isArray(s)) return s.some((x) => /open/i.test(String(x)));
  return /open/i.test(String(s || ""));
}

const openBlank = recs.filter(
  (r) =>
    isOpen(r) &&
    isBlankCensusValue(r.get(CENSUS_PROPERTY_ID_FIELD)) &&
    isBlankCensusValue(r.get(MAP_DIRECTORY_ENRICHMENT.website))
);

const censusRows = openBlank.map(mapCensusRowForDirectoryMatch);
const slugs = deriveCountrySlugsFromCensusRows(censusRows);
const crawl = await crawlMarriottCountrySitemaps({ countrySlugs: slugs, delayMs: 80 });
const ritz = await crawlRitzCarltonOverviewSupplement({});
const directory = mergeMarriottDirectoryRows(crawl.hotels, ritz.hotels);

console.log("Open blank both:", openBlank.length);
console.log("Directory:", directory.length);

for (const row of censusRows) {
  const best = pickMarriottDirectoryNameMatch(row, directory);
  console.log(
    best
      ? `MATCH ${best.sim.toFixed(2)} ${best.directoryRow.marshaCode} | ${row.name}`
      : `MISS ${row.name}`
  );
}

const plan = await planMarriottCensusEnrichment({ skipGlobalRescue: true, crawlDelayMs: 80 });
const openIds = new Set(openBlank.map((r) => r.id));
const plannedOpen = plan.planRows.filter((r) => openIds.has(r.censusRecordId));
console.log("\nPlan without global rescue for open blanks:", plannedOpen.length);
