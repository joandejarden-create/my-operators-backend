#!/usr/bin/env node
/**
 * Add amenitiesText to existing Accor directory extract rows (re-fetch metadata).
 */
import "../load-env.js";
import { readFileSync, writeFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { ACCOR_FETCH_HEADERS, parseAccorHotelMetadataFromHtml } from "../lib/accor-brand-directory-extract.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const path = join(__dirname, "..", "reports", "accor-property-directory-extract.json");
const delayMs = Number(process.argv.find((a) => a.startsWith("--delay-ms="))?.split("=")[1] || 100);

const data = JSON.parse(readFileSync(path, "utf8"));
const rows = data.propertyRows || [];
let updated = 0;

for (let i = 0; i < rows.length; i++) {
  const row = rows[i];
  if (row.amenitiesText) continue;
  try {
    const res = await fetch(row.propertyUrl, { headers: ACCOR_FETCH_HEADERS });
    if (!res.ok) continue;
    const meta = parseAccorHotelMetadataFromHtml(await res.text());
    if (!meta) continue;
    row.amenities = meta.amenities;
    row.amenitiesText = meta.amenitiesText;
    updated++;
  } catch {
    /* skip */
  }
  if ((i + 1) % 50 === 0) console.log(`[${i + 1}/${rows.length}] hydrated: ${updated}`);
  await new Promise((r) => setTimeout(r, delayMs));
}

data.propertyRows = rows;
data.amenitiesHydratedAt = new Date().toISOString();
writeFileSync(path, JSON.stringify(data, null, 2));
console.log("Hydrated amenities for", updated, "rows");
