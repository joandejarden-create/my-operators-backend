#!/usr/bin/env node
/**
 * Hydrate Accor directory metadata for priority property IDs (from expansion plan).
 * Faster than full 5813-hotel scan; restores lat/lng for geo matching.
 *
 *   node scripts/hydrate-accor-directory-priority.mjs
 *   node scripts/hydrate-accor-directory-priority.mjs --apply --delay-ms=100
 */
import "../load-env.js";
import { readFileSync, writeFileSync, existsSync, mkdirSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import {
  parseAccorHotelMetadataFromHtml,
  ACCOR_FETCH_HEADERS,
} from "../lib/accor-brand-directory-extract.js";
import { accorCanonicalPropertyUrl } from "../lib/hotel-census/accor-directory-name-normalize.js";
import { normalizeCountry } from "../lib/independent-census/match-current-census.js";
import { accorCountryCodeIsCala } from "../lib/brand-sitemap/cala-url-segments.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const PLAN_PATH = join(REPORTS, "accor-census-match-expansion-plan.json");
const OUT = join(REPORTS, "accor-property-directory-extract.json");
const delayMs = Number(process.argv.find((a) => a.startsWith("--delay-ms="))?.split("=")[1] || 100);

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

if (!existsSync(PLAN_PATH)) {
  throw new Error(`Missing ${PLAN_PATH}`);
}

const plan = JSON.parse(readFileSync(PLAN_PATH, "utf8"));
/** @type {Map<string, object>} */
const ids = new Map();
for (const row of [...(plan.planRows || []), ...(plan.stewardRows || [])]) {
  const id = String(row.propertyId || "").toUpperCase();
  if (id) ids.set(id, row);
}

console.log("=== Hydrate Accor priority directory ===\n");
console.log("Property IDs:", ids.size);

/** @type {object[]} */
const propertyRows = [];
let n = 0;
for (const [propertyId] of ids) {
  n++;
  const url = accorCanonicalPropertyUrl(propertyId);
  if (n % 25 === 0) console.log(` [${n}/${ids.size}]`);
  try {
    const res = await fetch(url, { headers: ACCOR_FETCH_HEADERS, redirect: "follow" });
    if (!res.ok) continue;
    const meta = parseAccorHotelMetadataFromHtml(await res.text());
    if (!meta?.name) continue;
    const isCala = accorCountryCodeIsCala(meta.countryCode);
    if (!isCala) continue;
    propertyRows.push({
      propertyUrl: url,
      propertyId,
      source: "accor_sitemap",
      sourceName: "Accor priority hydrate",
      sourceType: "brand_directory",
      inferredHotelName: meta.name,
      city: meta.city,
      country: meta.country,
      countryCode: meta.countryCode,
      countryNorm: normalizeCountry(meta.country),
      latitude: meta.latitude,
      longitude: meta.longitude,
      amenities: meta.amenities || [],
      amenitiesText: meta.amenitiesText || "",
      calaFilterStatus: "included",
    });
  } catch (err) {
    console.warn("skip", propertyId, err?.message || err);
  }
  if (delayMs > 0) await sleep(delayMs);
}

mkdirSync(REPORTS, { recursive: true });
writeFileSync(
  OUT,
  JSON.stringify(
    {
      generatedAt: new Date().toISOString(),
      ok: true,
      priorityHydrate: true,
      propertyIdsRequested: ids.size,
      propertyRows,
      summary: { totalPropertyUrls: propertyRows.length, calaIncluded: propertyRows.length },
    },
    null,
    2
  )
);

console.log("CALA rows hydrated:", propertyRows.length);
console.log("Written:", OUT);
