#!/usr/bin/env node
/**
 * Deep CALA scan for Accor brand SOU (Handwritten Collection) + known names.
 */
import { writeFileSync } from "node:fs";
import {
  fetchAccorCatalogHotels,
  fetchAccorCatalogByBbox,
  fetchAccorCatalogByIds,
  BRAZIL_CATALOG_BBOXES,
} from "../lib/accor-catalog-api.js";
import { COUNTRY_CONFIG_LIST } from "../lib/radar-buildout/country-configs.js";

const BRAND = "SOU";

/** @type {Map<string, object>} */
const byId = new Map();

function addHotels(hotels, meta = {}) {
  for (const h of hotels || []) {
    byId.set(h.propertyId, { ...h, ...meta });
  }
}

console.log("=== brand=SOU all CALA countries ===");
for (const country of COUNTRY_CONFIG_LIST) {
  const res = await fetchAccorCatalogHotels(country, {
    brand: BRAND,
    enlargementAllowed: true,
  });
  const n = res.ok ? (res.hotels || []).length : 0;
  if (n) console.log(`  ${country}: ${n}`);
  if (res.ok) addHotels(res.hotels, { via: `country:${country}` });
}

console.log("=== Brazil bboxes brand=SOU ===");
for (const box of BRAZIL_CATALOG_BBOXES) {
  const res = await fetchAccorCatalogByBbox(box.boxBottomLeft, box.boxTopRight, {
    brand: BRAND,
  });
  const n = res.ok ? (res.hotels || []).length : 0;
  console.log(`  ${box.label}: ${n}${res.ok ? "" : ` err=${res.error}`}`);
  if (res.ok) addHotels(res.hotels, { via: `bbox:${box.label}` });
}

console.log("=== city queries (Nui / João Pessoa / Handwritten / Marival) ===");
const queries = [
  "Joao Pessoa",
  "João Pessoa",
  "Nui",
  "Nui Handwritten",
  "Handwritten Collection",
  "Marival Distinct",
  "Nuevo Vallarta",
  "Tambaú",
  "Tambau",
];
for (const q of queries) {
  const res = await fetchAccorCatalogHotels(q, { enlargementAllowed: true });
  const hits = (res.hotels || []).filter(
    (h) =>
      h.brand === BRAND ||
      /handwritten/i.test(h.name) ||
      /nui\b/i.test(h.name) ||
      /marival distinct/i.test(h.name)
  );
  console.log(`  q=${q} total=${res.hotels?.length || 0} hits=${hits.length}`);
  for (const h of hits) {
    console.log(`    ${h.propertyId} ${h.brand} ${h.name} | ${h.city} ${h.country}`);
  }
  addHotels(hits, { via: `q:${q}` });
}

// Also check if Nui has a known Accor id pattern — search Brazil northeast without brand, filter name
console.log("=== Brazil Northeast all brands, filter Handwritten/Nui ===");
const ne = BRAZIL_CATALOG_BBOXES.find((b) => b.label === "Northeast");
const neRes = await fetchAccorCatalogByBbox(ne.boxBottomLeft, ne.boxTopRight, {});
const neHits = (neRes.hotels || []).filter(
  (h) => /handwritten/i.test(h.name) || /\bnui\b/i.test(h.name) || h.brand === BRAND
);
console.log(`  northeast total=${neRes.hotels?.length || 0} hits=${neHits.length}`);
for (const h of neHits) console.log(`    ${h.propertyId} ${h.brand} ${h.name}`);
addHotels(neHits, { via: "northeast_filter" });

// Probe C280 already known; try fetch brand page hotel list via ids if we find any
const hotels = [...byId.values()];
writeFileSync(
  "reports/handwritten-cala-sou-deep-scan.json",
  JSON.stringify(
    {
      generatedAt: new Date().toISOString(),
      brandCode: BRAND,
      count: hotels.length,
      hotels,
    },
    null,
    2
  )
);

console.log("\n=== UNIQUE SOU / Handwritten CALA ===");
console.log("count", hotels.length);
for (const h of hotels) {
  console.log(h.propertyId, h.brand, "|", h.name, "|", h.city, h.country, "|", h.via);
}

// Confirm C280 still resolves
const c280 = await fetchAccorCatalogByIds(["C280"]);
console.log("\nC280 confirm brand", c280.hotels?.[0]?.brand, c280.hotels?.[0]?.name);
