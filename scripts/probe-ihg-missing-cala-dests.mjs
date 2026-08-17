#!/usr/bin/env node
import { writeFileSync } from "node:fs";

const UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

function extractLocs(xml) {
  return [...String(xml || "").matchAll(/<loc>([^<]+)<\/loc>/gi)].map((m) => m[1].trim());
}

const r = await fetch("https://www.ihg.com/services/sitemaps/destinations.en.sitemap.xml", {
  headers: { "User-Agent": UA },
});
const locs = extractLocs(await r.text());
const keys = [
  "turks",
  "curacao",
  "curaçao",
  "belize",
  "saint-lucia",
  "st-lucia",
  "antigua",
  "kitts",
  "vincent",
  "cuba",
  "haiti",
  "virgin",
  "martinique",
  "guadeloupe",
  "bonaire",
  "sint-maarten",
];
const hits = locs.filter((u) => keys.some((k) => u.toLowerCase().includes(k)));
console.log("hits", hits.length);
for (const u of hits.slice(0, 80)) console.log(u);
writeFileSync(
  "reports/ihg-missing-cala-dest-locs.json",
  JSON.stringify({ count: hits.length, hits }, null, 2)
);
