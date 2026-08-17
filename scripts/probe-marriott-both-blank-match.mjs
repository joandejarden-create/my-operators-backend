#!/usr/bin/env node
import "../load-env.js";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { MAP_DIRECTORY_ENRICHMENT, isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { CENSUS_PROPERTY_ID_FIELD } from "../lib/hotel-census/hilton-property-id-contract.js";
import {
  crawlMarriottCountrySitemaps,
  CENSUS_COUNTRY_TO_SITEMAP_SLUG,
} from "../lib/marriott-brand-directory-extract.js";
import { nameSimilarity } from "../lib/independent-census/match-current-census.js";
import { mapCensusRowForDirectoryMatch } from "../lib/hotel-census/match-brand-directory-to-census.js";

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
      MAP_DIRECTORY_ENRICHMENT.website,
      CENSUS_PROPERTY_ID_FIELD,
    ],
    pageSize: 100,
  })
  .all();

const bothBlank = recs.filter(
  (r) =>
    isBlankCensusValue(r.get(CENSUS_PROPERTY_ID_FIELD)) &&
    isBlankCensusValue(r.get(MAP_DIRECTORY_ENRICHMENT.website))
);

const countryCounts = {};
for (const r of bothBlank) {
  const c = String(r.get(CENSUS_FIELDS.country) || "").toLowerCase();
  countryCounts[c] = (countryCounts[c] || 0) + 1;
}
console.log("Both blank:", bothBlank.length);
console.log("\nCountries (both blank):");
for (const [c, n] of Object.entries(countryCounts).sort((a, b) => b[1] - a[1])) {
  const mapped = CENSUS_COUNTRY_TO_SITEMAP_SLUG[c] ? "YES" : "NO";
  console.log(`  ${n}\t${c}\tsitemap:${mapped}`);
}

const slugs = [...new Set(Object.values(CENSUS_COUNTRY_TO_SITEMAP_SLUG))];
const crawl = await crawlMarriottCountrySitemaps({ countrySlugs: slugs, delayMs: 100 });
console.log("\nSitemap hotels:", crawl.hotels.length);

const censusRows = bothBlank.map(mapCensusRowForDirectoryMatch);
let foundInSitemap = 0;
let foundHighSim = 0;
const samples = [];

for (const cr of censusRows) {
  let best = null;
  for (const h of crawl.hotels) {
    const sim = nameSimilarity(cr.name, h.name);
    if (!best || sim > best.sim) best = { sim, hotel: h };
  }
  if (best && best.sim >= 0.55) {
    foundHighSim++;
    if (samples.length < 8) {
      samples.push({ census: cr.name, sitemap: best.hotel.name, sim: best.sim, marsha: best.hotel.marshaCode });
    }
  }
  const slug = CENSUS_COUNTRY_TO_SITEMAP_SLUG[cr.countryNorm || cr.country.toLowerCase()];
  if (slug) {
    const countryHotels = crawl.hotels.filter((h) => h.countrySlug === slug);
    let cb = null;
    for (const h of countryHotels) {
      const sim = nameSimilarity(cr.name, h.name);
      if (!cb || sim > cb.sim) cb = { sim, hotel: h };
    }
    if (cb && cb.sim >= 0.55) foundInSitemap++;
  }
}

console.log("\nHigh name sim (>=0.55) in ANY sitemap:", foundHighSim);
console.log("High name sim in country sitemap:", foundInSitemap);
console.log("\nSamples:");
for (const s of samples) console.log(s);
