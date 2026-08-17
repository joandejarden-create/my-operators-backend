#!/usr/bin/env node
/**
 * Audit Demand Anchor deduplication for a country/market.
 *   node scripts/audit-demand-anchor-dedup.mjs --country Mexico --market "Cancún / Riviera Maya"
 */
import "../load-env.js";
import { writeFileSync, mkdirSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { fetchDemandAnchorRecords } from "../lib/demand-anchors/airtable-demand-anchors-io.js";
import { auditDemandAnchorDedup } from "../lib/radar-buildout/audit-demand-anchor-dedup.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

function getArg(name, fallback = "") {
  const idx = process.argv.indexOf(name);
  return idx >= 0 ? process.argv[idx + 1] : fallback;
}

const country = getArg("--country", "Mexico");
const market = getArg("--market", "Cancún / Riviera Maya");
const output = getArg("--output", "data/mexico-cancun-demand-anchor-dedup-audit.json");

const result = await fetchDemandAnchorRecords({ country, includeHidden: true });
if (result.error) {
  console.error("Failed to fetch Demand Anchors:", result.error);
  process.exit(1);
}

const records = result.allPoints || result.points || [];
const audit = auditDemandAnchorDedup(records, { country, market });

const outPath = join(root, output);
const outDir = dirname(outPath);
if (!existsSync(outDir)) mkdirSync(outDir, { recursive: true });
writeFileSync(outPath, JSON.stringify(audit, null, 2) + "\n");

console.log("Demand Anchor dedup audit:", country, "—", market);
console.log("Country DA total:", audit.summary.countryDemandAnchorsTotal);
console.log("Market-scoped records:", audit.summary.marketScopedRecords);
console.log("Definite duplicate pairs:", audit.summary.definiteDuplicatePairs);
console.log("Possible duplicate pairs:", audit.summary.possibleDuplicatePairs);
console.log("Safe to keep:", audit.summary.safeToKeepCount);
console.log("Written:", output);
