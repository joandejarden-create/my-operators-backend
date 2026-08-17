#!/usr/bin/env node
/**
 * Extract Accor hotel URLs + JSON-LD metadata (CALA filter by country code).
 *
 *   node scripts/extract-accor-property-sitemap.mjs
 *   node scripts/extract-accor-property-sitemap.mjs --max-fetch=500
 *   node scripts/extract-accor-property-sitemap.mjs --sitemap-only
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { extractAccorPropertyUrls } from "../lib/accor-brand-directory-extract.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUT = join(__dirname, "..", "reports", "accor-property-directory-extract.json");

function parseArgs() {
  const args = process.argv.slice(2);
  const maxArg = args.find((a) => a.startsWith("--max-fetch="));
  return {
    sitemapOnly: args.includes("--sitemap-only"),
    maxFetch: maxArg ? Number(maxArg.split("=")[1]) : null,
    delayMs: Number(args.find((a) => a.startsWith("--delay-ms="))?.split("=")[1] || 120),
  };
}

const opts = parseArgs();

console.log("=== Accor sitemap extract ===\n");
console.log("Fetch metadata:", !opts.sitemapOnly);
if (opts.maxFetch) console.log("Max fetch:", opts.maxFetch);

const result = await extractAccorPropertyUrls({
  calaOnly: true,
  fetchMetadata: !opts.sitemapOnly,
  maxFetch: opts.maxFetch,
  delayMs: opts.delayMs,
});

if (!result.ok) {
  console.error(result.error);
  if (result.partial?.propertyRows?.length) {
    writeFileSync(OUT, JSON.stringify({ generatedAt: new Date().toISOString(), partial: true, ...result.partial }, null, 2));
    console.log("Partial rows saved:", result.partial.propertyRows.length);
  }
  process.exit(1);
}

mkdirSync(join(__dirname, "..", "reports"), { recursive: true });
const outPath = opts.sitemapOnly
  ? join(__dirname, "..", "reports", "accor-property-sitemap-locs.json")
  : OUT;
writeFileSync(
  outPath,
  JSON.stringify({ generatedAt: new Date().toISOString(), ...result }, null, 2)
);

console.log("Sitemap locs:", result.sitemapLocsTotal);
console.log("Metadata fetched:", result.metadataFetched);
console.log("CALA property rows:", result.propertyRows.length);
console.log("Written:", outPath);
