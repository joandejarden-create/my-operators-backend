#!/usr/bin/env node
/**
 * Pilot: fetch Choice property HTML via Puppeteer (regional warm-up) and save for amenity parse.
 *
 *   node scripts/fetch-choice-amenity-html-pilot.mjs mx043 mx092 mx228
 *   node scripts/fetch-choice-amenity-html-pilot.mjs --headed mx043
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import {
  fetchChoiceHotelHtmlPuppeteer,
  parseChoiceAmenitiesFromHtml,
  choicePropertyIdFromUrl,
} from "../lib/choice-hotel-content-fetch.js";
import { buildChoiceRegionalPageForCountry } from "../lib/choice-regional-directory-extract.js";

const HEADED = process.argv.includes("--headed");
const ids = process.argv.slice(2).filter((a) => !a.startsWith("--"));

const PILOT = ids.length
  ? ids
  : ["mx043", "mx092", "mx228", "mx077", "mx180", "br080", "br084", "br183"];

/** @type {{ id: string, url: string, country: string }[]} */
const URLS = {
  mx043: {
    url: "https://www.choicehotels.com/chihuahua/chihuahua/quality-inn-hotels/mx043",
    country: "Mexico",
  },
  mx092: {
    url: "https://www.choicehotels.com/guanajuato/irapuato/comfort-inn-hotels/mx092",
    country: "Mexico",
  },
  mx228: {
    url: "https://www.choicehotels.com/mexico/mexico-city/ascend-hotels/mx228",
    country: "Mexico",
  },
  mx077: {
    url: "https://www.choicehotels.com/chihuahua/chihuahua/comfort-inn-hotels/mx077",
    country: "Mexico",
  },
  mx180: {
    url: "https://www.choicehotels.com/sinaloa/mazatlan/park-inn-hotels/mx180",
    country: "Mexico",
  },
  br080: {
    url: "https://www.choicehotels.com/santa-catarina/joinville/comfort-inn-hotels/br080",
    country: "Brazil",
  },
  br084: {
    url: "https://www.choicehotels.com/rio-grande-do-sul/porto-alegre/quality-inn-hotels/br084",
    country: "Brazil",
  },
  br183: {
    url: "https://www.choicehotels.com/sao-paulo/sao-paulo/radisson-hotels/br183",
    country: "Brazil",
  },
};

const OUT_DIR = "reports/choice-amenity-html";
mkdirSync(OUT_DIR, { recursive: true });

/** @type {object[]} */
const results = [];

for (const pid of PILOT) {
  const key = pid.toLowerCase();
  const entry = URLS[key];
  if (!entry) {
    console.log("Skip unknown pilot id:", pid);
    continue;
  }

  const regionalWarmupUrl = buildChoiceRegionalPageForCountry(entry.country)?.url || "";
  console.log(`\nFetch ${key}…`);
  try {
    const fetched = await fetchChoiceHotelHtmlPuppeteer(entry.url, {
      regionalWarmupUrl,
      headless: HEADED ? false : "new",
      timeoutMs: 120000,
    });
    const outPath = join(OUT_DIR, `${key}.html`);
    writeFileSync(outPath, fetched.html, "utf8");

    const parsed = parseChoiceAmenitiesFromHtml(fetched.html);
    const row = {
      propertyId: key.toUpperCase(),
      url: entry.url,
      htmlLength: fetched.html.length,
      blocked: fetched.blocked,
      amenityCount: parsed.amenities.length,
      amenitiesPreview: parsed.amenities.slice(0, 8).join("; "),
      savedTo: outPath,
    };
    results.push(row);
    console.log(row);
  } catch (err) {
    console.log("Error:", err.message);
    results.push({ propertyId: key, error: err.message });
  }
}

console.log("\nSummary:", results.filter((r) => r.amenityCount > 0).length, "with amenities");
console.log("Next: node scripts/apply-choice-amenities-from-html.mjs --apply");
