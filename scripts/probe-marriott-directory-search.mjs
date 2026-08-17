#!/usr/bin/env node
import { crawlMarriottCountrySitemaps } from "../lib/marriott-brand-directory-extract.js";
import { crawlRitzCarltonOverviewSupplement, mergeMarriottDirectoryRows } from "../lib/marriott-brand-tld-sitemap-supplement.js";
import { nameSimilarity } from "../lib/independent-census/match-current-census.js";

const QUERIES = [
  "Ritz-Carlton St. Thomas",
  "Ritz-Carlton Aruba",
  "Ritz-Carlton Grand Cayman",
  "Ritz-Carlton Santiago",
  "Ritz-Carlton Turks",
  "St. Regis Bahia Beach",
  "Dorado Beach",
  "Zadun",
  "Park Tower",
  "The Brown Guatape",
  "Punta Islita",
  "AC Kingston",
  "Courtyard Kingston",
  "Courtyard Bridgetown",
  "Treasure Beach",
  "Crystal Cove",
  "Hideaway Royalton Negril",
  "Aloft Tulum",
  "Planet Hollywood Cancun",
  "AC Guadalajara",
  "Ixtapan",
  "Sheraton Buganvilias",
  "La Concha",
  "City Express Santa Fe",
];

const crawl = await crawlMarriottCountrySitemaps({ delayMs: 80 });
const ritz = await crawlRitzCarltonOverviewSupplement({});
const dir = mergeMarriottDirectoryRows(crawl.hotels, ritz.hotels);
console.log("Directory size:", dir.length);

for (const q of QUERIES) {
  const hits = dir
    .map((d) => ({ d, sim: nameSimilarity(d.name, q) }))
    .filter((x) => x.sim >= 0.35 || new RegExp(q.split(/\s+/).slice(0, 2).join("|"), "i").test(x.d.name))
    .sort((a, b) => b.sim - a.sim)
    .slice(0, 3);
  console.log("\n===", q, "===");
  for (const h of hits) console.log(h.sim.toFixed(2), h.d.marshaCode, h.d.name);
}
