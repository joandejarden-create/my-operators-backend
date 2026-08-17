#!/usr/bin/env node
/**
 * Discover Choice regional placeId from country browse pages.
 * Usage: node scripts/discover-choice-regional-placeids.mjs [country-slug ...]
 */
import "../load-env.js";
import { writeFileSync, mkdirSync } from "node:fs";
import { CHOICE_FETCH_HEADERS } from "../lib/choice-regional-directory-extract.js";

const SLUGS =
  process.argv.slice(2).length > 0
    ? process.argv.slice(2)
    : [
        "brazil",
        "colombia",
        "chile",
        "peru",
        "argentina",
        "ecuador",
        "costa-rica",
        "panama",
        "guatemala",
        "dominican-republic",
        "jamaica",
        "trinidad-and-tobago",
        "barbados",
        "bahamas",
        "aruba",
        "curacao",
        "puerto-rico",
        "canada",
        "united-states",
      ];

/** @type {object[]} */
const results = [];

for (const slug of SLUGS) {
  const urls = [
    `https://www.choicehotels.com/en-uk/${slug}/regional-hotels`,
    `https://www.choicehotels.com/en-uk/${slug}`,
  ];

  let found = null;
  for (const url of urls) {
    try {
      const res = await fetch(url, { redirect: "follow", headers: CHOICE_FETCH_HEADERS });
      const html = await res.text();
      const placeIds = [
        ...new Set(
          [...html.matchAll(/regional-hotels\?placeId=([A-Za-z0-9_-]+)/g)].map((m) => m[1])
        ),
      ];
      const hotelCount = (html.match(/"@type":"Hotel"/g) || []).length;
      if (placeIds.length || hotelCount) {
        found = {
          slug,
          url,
          status: res.status,
          htmlLength: html.length,
          placeIds,
          hotelJsonCount: hotelCount,
          blocked: /access denied/i.test(html),
        };
        break;
      }
    } catch (err) {
      found = { slug, error: String(err.message || err) };
    }
  }
  results.push(found || { slug, error: "no_placeId_found" });
  console.log(JSON.stringify(results[results.length - 1]));
  await new Promise((r) => setTimeout(r, 300));
}

mkdirSync("reports", { recursive: true });
writeFileSync(
  "reports/choice-regional-placeid-discovery.json",
  JSON.stringify({ generatedAt: new Date().toISOString(), results }, null, 2)
);
console.log("\nWrote reports/choice-regional-placeid-discovery.json");
