#!/usr/bin/env node
import { writeFileSync } from "node:fs";
import { load } from "cheerio";

const url =
  process.argv[2] ||
  "https://www.wyndhamhotels.com/wyndham-alltra/playa-del-carmen-mexico/wyndham-alltra-playa-del-carmen-adults-only-all-inclusive/overview";
const html = await (await fetch(url, { headers: { "User-Agent": "Mozilla/5.0 Chrome/131" } })).text();
writeFileSync("reports/wyndham-page-full.html", html);
const $ = load(html);

/** @type {string[]} */
const labels = [];
$("[class*='AmenityCard'], [class*='amenity-card'], [class*='amenityCard']").each((_, el) => {
  const t = $(el).find("span, p, h3, h4").first().text().replace(/\s+/g, " ").trim();
  if (t) labels.push(t);
});
$("[class*='featured-amenities' i] li, [class*='FeaturedAmenities' i] li").each((_, el) => {
  const t = $(el).text().replace(/\s+/g, " ").trim();
  if (t) labels.push(t);
});
$("img[alt]").each((_, el) => {
  const alt = $(el).attr("alt")?.trim();
  if (alt && alt.length > 2 && alt.length < 80 && !/logo|icon|wyndham|hotel image/i.test(alt)) labels.push(alt);
});
console.log("labels", [...new Set(labels)].slice(0, 30));
console.log("has NEXT", /__NEXT_DATA__/.test(html));
console.log("featuredAmenities json", /featuredAmenities/i.test(html));
