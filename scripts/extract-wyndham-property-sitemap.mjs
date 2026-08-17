#!/usr/bin/env node
/**
 * Extract Wyndham property URLs from sitemap (CALA-filtered by URL path).
 *
 *   node scripts/extract-wyndham-property-sitemap.mjs
 *   node scripts/extract-wyndham-property-sitemap.mjs --all-regions
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { extractWyndhamPropertyUrls } from "../lib/wyndham-brand-directory-extract.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUT = join(__dirname, "..", "reports", "wyndham-property-directory-extract.json");

const calaOnly = !process.argv.includes("--all-regions");

console.log("=== Wyndham sitemap extract ===\n");
console.log("CALA filter:", calaOnly);
console.log("Fetch metadata:", true);

const result = await extractWyndhamPropertyUrls({
  calaOnly,
  fetchMetadata: true,
  delayMs: 80,
});
if (!result.ok) {
  console.error(result.error);
  process.exit(1);
}

mkdirSync(join(__dirname, "..", "reports"), { recursive: true });
writeFileSync(
  OUT,
  JSON.stringify({ generatedAt: new Date().toISOString(), ...result }, null, 2)
);

console.log("Child sitemaps scanned:", result.childSitemapsScanned);
console.log("Candidate overview URLs:", result.candidateOverviewUrls);
console.log("Metadata fetched:", result.metadataFetched);
console.log("Property rows (CALA):", result.propertyRows.length);
console.log("Written:", OUT);
