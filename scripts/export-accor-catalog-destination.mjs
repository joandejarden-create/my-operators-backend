#!/usr/bin/env node
/**
 * Export Accor catalog hotels for a destination (address, phone, GPS).
 * Use instead of scraping booking city search pages.
 *
 *   node scripts/export-accor-catalog-destination.mjs --destination bogota --country-code CO
 */
import "../load-env.js";
import { writeFileSync, mkdirSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { fetchAccorCatalogHotels } from "../lib/accor-catalog-api.js";
import { writeCsv } from "../lib/str-census-import/report-utils.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const dest = process.argv.find((a) => a.startsWith("--destination="))?.split("=")[1] ||
  (process.argv.includes("--destination") ? process.argv[process.argv.indexOf("--destination") + 1] : "");
const countryCode = process.argv.find((a) => a.startsWith("--country-code="))?.split("=")[1] || "";

if (!dest) {
  console.error("Usage: --destination bogota [--country-code CO]");
  process.exit(1);
}

const result = await fetchAccorCatalogHotels(dest, { countryCode: countryCode || undefined });
console.log(`Catalog: ${result.count} hotels for q="${dest}"`);

const rows = result.hotels.map((h) => ({
  propertyId: h.propertyId,
  name: h.name,
  brand: h.brand,
  city: h.city,
  country: h.country,
  address1: h.address1,
  postalCode: h.postalCode,
  telephone: h.telephone,
  email: h.email,
  latitude: h.latitude,
  longitude: h.longitude,
  propertyUrl: h.propertyUrl,
}));

const slug = dest.toLowerCase().replace(/[^a-z0-9]+/g, "-");
const outDir = join(__dirname, "..", "reports");
mkdirSync(outDir, { recursive: true });
const jsonPath = join(outDir, `accor-catalog-${slug}.json`);
const csvPath = join(outDir, `accor-catalog-${slug}.csv`);
writeFileSync(jsonPath, JSON.stringify({ generatedAt: new Date().toISOString(), query: dest, ...result, hotels: rows }, null, 2));
writeCsv(csvPath, rows, Object.keys(rows[0] || {}));
console.log("JSON:", jsonPath);
console.log("CSV:", csvPath);
