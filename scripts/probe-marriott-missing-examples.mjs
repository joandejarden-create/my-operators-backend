#!/usr/bin/env node
import "../load-env.js";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { crawlMarriottCountrySitemaps, marshaFromMarriottWebsite } from "../lib/marriott-brand-directory-extract.js";
import { MAP_DIRECTORY_ENRICHMENT } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { CENSUS_PROPERTY_ID_FIELD } from "../lib/hotel-census/hilton-property-id-contract.js";

const EXAMPLES = [
  { marsha: "SDQAL", url: "https://www.marriott.com/en-us/hotels/sdqal-aloft-santo-domingo-piantini/overview/" },
  { marsha: "PLSRR", url: "https://www.ritzcarlton.com/en/hotels/plsrr-the-ritz-carlton-residences-turks-and-caicos/overview/" },
  { marsha: "LIRWI", url: "https://www.marriott.com/en-us/hotels/lirwi-the-westin-reserva-conchal-an-all-inclusive-golf-resort-and-spa/overview/" },
];

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_ALT
);
const recs = await base(HOTEL_CENSUS_TABLE)
  .select({
    filterByFormula: `FIND("Marriott", {${CENSUS_FIELDS.parentCompany}})`,
    fields: [
      CENSUS_FIELDS.name,
      CENSUS_FIELDS.country,
      CENSUS_FIELDS.affiliation,
      MAP_DIRECTORY_ENRICHMENT.website,
      CENSUS_PROPERTY_ID_FIELD,
    ],
    pageSize: 100,
  })
  .all();

console.log("Marriott census rows:", recs.length);
console.log("Blank website:", recs.filter((r) => !String(r.get(MAP_DIRECTORY_ENRICHMENT.website) || "").trim()).length);
console.log("Blank property ID:", recs.filter((r) => !String(r.get(CENSUS_PROPERTY_ID_FIELD) || "").trim()).length);

const crawl = await crawlMarriottCountrySitemaps({
  countrySlugs: ["dominican-republic", "costa-rica"],
  delayMs: 150,
});

for (const ex of EXAMPLES) {
  console.log("\n===", ex.marsha, "===");
  const inSitemap = crawl.hotels.find((h) => h.marshaCode === ex.marsha);
  console.log("In marriott.com sitemap:", inSitemap ? inSitemap.name : "NO");
  if (inSitemap) console.log("  Sitemap URL:", inSitemap.website);

  const censusHits = recs.filter((r) => {
    const name = String(r.get(CENSUS_FIELDS.name) || "").toLowerCase();
    const pid = String(r.get(CENSUS_PROPERTY_ID_FIELD) || "").toUpperCase();
    if (pid === ex.marsha) return true;
    if (ex.marsha === "SDQAL" && name.includes("aloft") && name.includes("santo domingo")) return true;
    if (ex.marsha === "PLSRR" && name.includes("ritz") && name.includes("turks")) return true;
    if (ex.marsha === "LIRWI" && name.includes("westin") && name.includes("conchal")) return true;
    return false;
  });
  console.log("Census matches:", censusHits.length);
  for (const r of censusHits) {
    console.log(
      " ",
      r.id,
      r.get(CENSUS_FIELDS.name),
      "| country:",
      r.get(CENSUS_FIELDS.country),
      "| web:",
      r.get(MAP_DIRECTORY_ENRICHMENT.website) || "(blank)",
      "| pid:",
      r.get(CENSUS_PROPERTY_ID_FIELD) || "(blank)"
    );
  }
}

// Check sitemap index for turks-and-caicos
const indexRes = await fetch(
  "https://www.marriott.com/content/dam/marriott-seo/en/marriott-tng/sitemap-hotel-sitemaps.xml"
);
const indexXml = await indexRes.text();
const turks = indexXml.match(/turks[^<]*/gi);
console.log("\nTurks sitemap in index:", turks?.slice(0, 5) || "none");
