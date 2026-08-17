#!/usr/bin/env node
/**
 * Discover Handwritten Collection (Accor) hotels in CALA via catalog API + C280 probe.
 */
import { writeFileSync } from "node:fs";
import { fetchAccorCatalogHotels, fetchAccorCatalogByIds } from "../lib/accor-catalog-api.js";
import { COUNTRY_CONFIG_LIST } from "../lib/radar-buildout/country-configs.js";

const C280 = await fetchAccorCatalogByIds(["C280"]);
console.log("C280 fetch", JSON.stringify(C280, null, 2));

const brandFromC280 = C280.hotels?.[0]?.brand || "";
console.log("brand code from C280:", brandFromC280);

/** @type {Map<string, object>} */
const byId = new Map();

if (C280.hotels?.[0]) {
  byId.set(C280.hotels[0].propertyId, C280.hotels[0]);
}

// Try common Handwritten brand codes + whatever C280 reports
const brandCodes = [
  ...new Set(
    [brandFromC280, "HWC", "HND", "HAN", "HTW", "HWR", "SBH", "MSH"]
      .map((b) => String(b || "").trim().toUpperCase())
      .filter(Boolean)
  ),
];

for (const brand of brandCodes) {
  for (const country of COUNTRY_CONFIG_LIST) {
    const res = await fetchAccorCatalogHotels(country, {
      brand,
      enlargementAllowed: false,
    });
    if (!res.ok) {
      console.log(`skip ${country} brand=${brand} ok=false`, res.error || res.status);
      continue;
    }
    const n = (res.hotels || []).length;
    if (n) console.log(`HIT ${country} brand=${brand} count=${n}`);
    for (const h of res.hotels || []) {
      byId.set(h.propertyId, { ...h, queryBrand: brand, queryCountry: country });
    }
  }
}

// Also search Mexico / Brazil without brand filter then filter names containing Handwritten
for (const country of ["Mexico", "Brazil", "Colombia", "Argentina", "Chile", "Peru"]) {
  const res = await fetchAccorCatalogHotels(country, { enlargementAllowed: false });
  if (!res.ok) continue;
  const hits = (res.hotels || []).filter(
    (h) =>
      /handwritten/i.test(h.name) ||
      /handwritten/i.test(h.brand) ||
      String(h.brand || "").toUpperCase() === brandFromC280
  );
  console.log(`name-filter ${country}: ${hits.length} / ${res.hotels?.length || 0}`);
  for (const h of hits) byId.set(h.propertyId, { ...h, queryCountry: country, via: "name_or_brand_filter" });
}

const hotels = [...byId.values()];
writeFileSync(
  "reports/handwritten-cala-catalog-discovery.json",
  JSON.stringify(
    {
      generatedAt: new Date().toISOString(),
      brandFromC280,
      brandCodesTried: brandCodes,
      count: hotels.length,
      hotels,
    },
    null,
    2
  )
);
console.log("\nTOTAL unique", hotels.length);
for (const h of hotels) {
  console.log(
    h.propertyId,
    h.brand,
    "|",
    h.name,
    "|",
    h.city,
    h.country || h.countryCode,
    "|",
    h.propertyUrl
  );
}
