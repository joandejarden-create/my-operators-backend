#!/usr/bin/env node
import "../load-env.js";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { MAP_DIRECTORY_ENRICHMENT, isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { CENSUS_PROPERTY_ID_FIELD } from "../lib/hotel-census/hilton-property-id-contract.js";
import { censusCountryToSitemapSlug } from "../lib/marriott-brand-directory-extract.js";
import { nameSimilarity } from "../lib/independent-census/match-current-census.js";
import { planMarriottCensusEnrichment } from "../lib/hotel-census/plan-marriott-census-enrichment.js";
import { crawlMarriottCountrySitemaps } from "../lib/marriott-brand-directory-extract.js";
import { crawlRitzCarltonOverviewSupplement, mergeMarriottDirectoryRows } from "../lib/marriott-brand-tld-sitemap-supplement.js";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_ALT
);
const recs = await base(HOTEL_CENSUS_TABLE)
  .select({
    filterByFormula: `FIND("Marriott", {${CENSUS_FIELDS.parentCompany}})`,
    fields: [
      CENSUS_FIELDS.name,
      CENSUS_FIELDS.country,
      CENSUS_FIELDS.city,
      CENSUS_FIELDS.affiliation,
      CENSUS_FIELDS.status,
      MAP_DIRECTORY_ENRICHMENT.website,
      CENSUS_PROPERTY_ID_FIELD,
    ],
    pageSize: 100,
  })
  .all();

function isOpen(r) {
  const s = r.get(CENSUS_FIELDS.status);
  if (Array.isArray(s)) return s.some((x) => /open/i.test(String(x)));
  return /open/i.test(String(s || ""));
}

const openBlankPid = recs.filter(
  (r) => isOpen(r) && isBlankCensusValue(r.get(CENSUS_PROPERTY_ID_FIELD))
);
const openBlankWeb = recs.filter(
  (r) => isOpen(r) && isBlankCensusValue(r.get(MAP_DIRECTORY_ENRICHMENT.website))
);
const openBlankBoth = recs.filter(
  (r) =>
    isOpen(r) &&
    isBlankCensusValue(r.get(CENSUS_PROPERTY_ID_FIELD)) &&
    isBlankCensusValue(r.get(MAP_DIRECTORY_ENRICHMENT.website))
);

console.log("Open + blank Property ID:", openBlankPid.length);
console.log("Open + blank Website:", openBlankWeb.length);
console.log("Open + blank both:", openBlankBoth.length);

const plan = await planMarriottCensusEnrichment({ minConfidence: "low", crawlDelayMs: 100 });
const plannedIds = new Set(plan.planRows.map((r) => r.censusRecordId));
const openNotInPlan = openBlankBoth.filter((r) => !plannedIds.has(r.id));
console.log("\nOpen both blank NOT in current plan:", openNotInPlan.length);

// Best sitemap name match for open rows not in plan
const slugs = [...new Set(openNotInPlan.map((r) => censusCountryToSitemapSlug(r.get(CENSUS_FIELDS.country))).filter(Boolean))];
const crawl = await crawlMarriottCountrySitemaps({ countrySlugs: slugs.length ? slugs : undefined, delayMs: 80 });
const ritz = await crawlRitzCarltonOverviewSupplement({});
const directory = mergeMarriottDirectoryRows(crawl.hotels, ritz.hotels);

console.log("\nOpen blanks not in plan — best directory match:");
for (const r of openNotInPlan.slice(0, 30)) {
  const name = r.get(CENSUS_FIELDS.name);
  const country = r.get(CENSUS_FIELDS.country);
  const slug = censusCountryToSitemapSlug(country);
  const pool = slug ? directory.filter((d) => d.countryPage === slug) : directory;
  let best = null;
  for (const d of pool) {
    const sim = nameSimilarity(d.name, name);
    if (!best || sim > best.sim) best = { sim, d };
  }
  console.log({
    name,
    country,
    slug: slug || "NONE",
    bestSim: best?.sim?.toFixed(2),
    bestName: best?.d?.name?.slice(0, 60),
    marsha: best?.d?.marshaCode,
  });
}
