#!/usr/bin/env node
/**
 * Audit Travel Infrastructure for a country/market corridor.
 *   node scripts/audit-market-travel-infrastructure.mjs --country Mexico --market "Cancún / Riviera Maya"
 */
import "../load-env.js";
import { writeFileSync, mkdirSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { fetchTravelInfrastructureRecords } from "../lib/travel-infrastructure/airtable-travel-infrastructure-io.js";
import { auditMarketTravelInfrastructure } from "../lib/radar-buildout/audit-market-travel-infrastructure.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

function getArg(name, fallback = "") {
  const idx = process.argv.indexOf(name);
  return idx >= 0 ? process.argv[idx + 1] : fallback;
}

const country = getArg("--country", "Mexico");
const market = getArg("--market", "Cancún / Riviera Maya");
const output =
  getArg("--output") || "data/mexico-cancun-travel-infrastructure-audit.json";
const submarketsArg = getArg("--submarkets", "");
const submarkets = submarketsArg
  ? submarketsArg.split(",").map((s) => s.trim()).filter(Boolean)
  : [];

const result = await fetchTravelInfrastructureRecords({
  country,
  includeHidden: true,
});

if (result.error) {
  console.error("Failed to fetch Travel Infrastructure:", result.error);
  process.exit(1);
}

const records = result.allPoints || result.points || [];
const audit = auditMarketTravelInfrastructure(records, {
  country,
  market,
  submarkets,
});

const outPath = join(root, output);
const outDir = dirname(outPath);
if (!existsSync(outDir)) mkdirSync(outDir, { recursive: true });
writeFileSync(outPath, JSON.stringify(audit, null, 2) + "\n");

console.log("Travel Infrastructure audit:", country, "—", market);
console.log("Country TI total:", audit.summary.countryTravelInfrastructureTotal ?? audit.summary.mexicoTravelInfrastructureTotal);
console.log("Market-matched TI:", audit.summary.marketMatchedRecords);
console.log("Expected nodes found:", audit.summary.existingExpectedNodes);
console.log("Likely missing:", audit.likelyMissingRecords.length ? audit.likelyMissingRecords.join(", ") : "(none flagged)");
console.log("Weak submarket records:", audit.summary.weakSubmarketRecords);
console.log("Additional fixture recommended:", audit.summary.additionalFixtureRecommended ? "yes" : "no");
console.log("Written:", output);
