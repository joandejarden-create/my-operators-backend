#!/usr/bin/env node
/**
 * Extract Mexico property IDs + URLs from Choice regional browse page.
 */
import "../load-env.js";
import { readFileSync } from "node:fs";
import { writeFileSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import { choicePropertyIdFromUrl } from "../lib/choice-hotel-content-fetch.js";
import { CENSUS_FIELDS, HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";
import { getPlatformBase } from "../lib/hotel-census/platform-base.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { deriveInferredHotelName } from "../lib/independent-census/match-brand-directory-properties.js";
import {
  mapCensusRowForDirectoryMatch,
  scoreDirectoryAgainstCensus,
} from "../lib/hotel-census/match-brand-directory-to-census.js";

const REGIONAL_URL =
  process.argv[2] ||
  "https://www.choicehotels.com/en-uk/mexico/regional-hotels?placeId=ChIJU1NoiDs6BIQREZgJa760ZO0";

const EXTRACT_PATH = "reports/independent-census-choice-property-url-extract-cala-2026-05-20.json";

const res = await fetch(REGIONAL_URL, {
  headers: {
    "User-Agent":
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    Accept: "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
  },
  redirect: "follow",
});

if (!res.ok) {
  console.error("HTTP", res.status);
  process.exit(1);
}

const html = await res.text();
const relPathRe = /\/mexico\/[a-z0-9-]+\/[a-z0-9-]+\/(mx\d{2,3})/gi;
/** @type {Map<string, string>} */
const urlById = new Map();
for (const m of html.matchAll(relPathRe)) {
  const id = m[1].toUpperCase();
  urlById.set(id, `https://www.choicehotels.com${m[0]}`);
}

const extract = JSON.parse(readFileSync(EXTRACT_PATH, "utf8"));
const allSitemap = Array.isArray(extract.propertyRows) ? extract.propertyRows : [];

for (const row of allSitemap) {
  const id = String(row.propertyId || "").toUpperCase();
  if (id.startsWith("MX") && row.propertyUrl) {
    urlById.set(id, row.propertyUrl);
  }
}

// Tokens on regional page may reference properties whose path is not in the 4 /mexico/... matches
const mxTokens = [...new Set([...html.matchAll(/\b(mx\d{2,3})\b/gi)].map((m) => m[1].toUpperCase()))];
for (const id of mxTokens) {
  if (!urlById.has(id)) {
    const row = allSitemap.find((r) => String(r.propertyId || "").toUpperCase() === id);
    if (row?.propertyUrl) urlById.set(id, row.propertyUrl);
  }
}

const sitemapMx = allSitemap.filter(
  (r) =>
    String(r.countryOrRegionSegment || "").toLowerCase() === "mexico" ||
    String(r.inferredCountry || "").toLowerCase() === "mexico"
);

console.log("Regional page HTML:", html.length, "bytes");
console.log("MX property URLs resolved:", urlById.size);
console.log("Sitemap Mexico rows:", sitemapMx.length);

const base = getPlatformBase();
const census = await base(HOTEL_CENSUS_TABLE)
  .select({
    fields: ["name", "Website", "Property ID", "Amenities", CENSUS_FIELDS.city, CENSUS_FIELDS.country],
    filterByFormula: `AND(FIND("Choice", {${CENSUS_FIELDS.parentCompany}}), {${CENSUS_FIELDS.country}}="Mexico")`,
  })
  .all();

const mexicoRows = census.map(mapCensusRowForDirectoryMatch);
console.log("Choice census Mexico:", mexicoRows.length);

/** @type {object[]} */
const matches = [];
for (const censusRow of mexicoRows) {
  let best = null;
  for (const [propertyId, propertyUrl] of urlById) {
    const sitemapRow = allSitemap.find((r) => String(r.propertyId).toUpperCase() === propertyId);
    const citySlug = sitemapRow?.citySlug?.replace(/-/g, " ") || "";
    const brandName = sitemapRow?.matchedBrandSetupBrand || sitemapRow?.inferredBrandName || "";
    const dir = {
      name: sitemapRow ? deriveInferredHotelName(sitemapRow) : propertyId,
      city: sitemapRow?.citySlug?.replace(/-/g, " ") || "",
      country: sitemapRow?.inferredCountry || "Mexico",
      website: propertyUrl,
      brandPropertyCode: propertyId,
      source: "choice_regional_page",
    };
    const scored = scoreDirectoryAgainstCensus(dir, censusRow);
    if (!best || scored.score > best.score) {
      best = { propertyId, propertyUrl, scored, sitemapRow, dir };
    }
  }
  if (best && best.score >= 45) {
    matches.push({
      censusRecordId: censusRow.recordId,
      censusName: censusRow.name,
      censusCity: censusRow.city,
      propertyId: best.propertyId,
      propertyUrl: best.propertyUrl,
      score: best.score,
      confidence: best.scored.confidence,
      hasWebsite: !isBlankCensusValue(censusRow.fields?.Website),
      blankAmenities: isBlankCensusValue(censusRow.fields?.Amenities),
    });
  }
}

matches.sort((a, b) => b.score - a.score);
console.log("\nMexico matches (score>=45):", matches.length);
for (const m of matches.slice(0, 20)) {
  console.log(
    `  ${m.score} ${m.confidence} | ${m.censusName} (${m.censusCity}) -> ${m.propertyId} ${m.hasWebsite ? "[has web]" : ""}`
  );
}

mkdirSync("reports", { recursive: true });
const out = join("reports", "choice-mexico-regional-extract.json");
writeFileSync(
  out,
  JSON.stringify(
    {
      generatedAt: new Date().toISOString(),
      regionalUrl: REGIONAL_URL,
      placeId: "ChIJU1NoiDs6BIQREZgJa760ZO0",
      propertyUrlCount: urlById.size,
      propertyUrls: Object.fromEntries(urlById),
      censusMexicoRows: mexicoRows.length,
      matches,
    },
    null,
    2
  )
);
console.log("\nWrote:", out);
