#!/usr/bin/env node
import "../load-env.js";
import { planMarriottCensusEnrichment } from "../lib/hotel-census/plan-marriott-census-enrichment.js";
import { scoreDirectoryAgainstCensus } from "../lib/hotel-census/match-brand-directory-to-census.js";
import { crawlMarriottCountrySitemaps } from "../lib/marriott-brand-directory-extract.js";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { MAP_DIRECTORY_ENRICHMENT } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { CENSUS_PROPERTY_ID_FIELD } from "../lib/hotel-census/hilton-property-id-contract.js";
import { mapCensusRowForDirectoryMatch } from "../lib/hotel-census/match-brand-directory-to-census.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_ALT
);
const rec = await base(HOTEL_CENSUS_TABLE).find("recaiJ11ElqigjE42");
const censusRow = mapCensusRowForDirectoryMatch(rec);

const crawl = await crawlMarriottCountrySitemaps({
  countrySlugs: ["dominican-republic"],
  delayMs: 100,
});
const sdqal = crawl.hotels.find((h) => h.marshaCode === "SDQAL");
console.log("Directory row:", sdqal?.name, sdqal?.country, sdqal?.marshaCode, sdqal?.brandPropertyCode);

const scored = scoreDirectoryAgainstCensus(sdqal, censusRow);
console.log("Score:", scored);

const plan = await planMarriottCensusEnrichment({
  countrySlugs: ["dominican-republic"],
  directoryRows: crawl.hotels,
});
const hit = plan.planRows.find((r) => r.marshaCode === "SDQAL");
console.log("In plan:", hit);
console.log("Unmatched census with SDQAL name:", plan.unmatchedCensusCount);
