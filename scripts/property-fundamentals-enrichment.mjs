#!/usr/bin/env node
/**
 * Property Fundamentals Enrichment (write-through)
 *
 * Dry-run:  npm run census:property-fundamentals-enrichment -- --mode dry-run
 * Run:      …gates… npm run census:property-fundamentals-enrichment -- --mode run --enable-production-writes
 * Resume:   …gates… npm run census:property-fundamentals-enrichment -- --mode resume --enable-production-writes
 */
import "dotenv/config";
import { runPropertyFundamentalsEnrichmentV1 } from "../lib/research-engine-v2/property-fundamentals-enrichment-v1.js";

const argv = process.argv.slice(2);
function argValue(flag) {
  const i = argv.indexOf(flag);
  return i >= 0 ? argv[i + 1] || null : null;
}

const mode = String(argValue("--mode") || "dry-run").toLowerCase();
if (!["dry-run", "run", "resume"].includes(mode)) {
  console.error("Invalid --mode (dry-run|run|resume)");
  process.exit(2);
}

const wantWrites = argv.includes("--enable-production-writes");
const enableProductionWrites =
  wantWrites &&
  (mode === "run" || mode === "resume") &&
  String(process.env.ALLOW_CENSUS_AUTOPILOT_APPLY || "0") === "1" &&
  String(process.env.CONFIRM_WRITE_TO_PRODUCTION_CENSUS || "0") === "1" &&
  String(process.env.CONFIRM_NO_BRAND_EXPLORER_WRITES || "0") === "1" &&
  String(process.env.CONFIRM_NO_BRAND_SETUP_WRITES || "0") === "1" &&
  String(process.env.ENABLE_CURRENT_BRAND_WRITES || "0") === "0" &&
  String(process.env.ENABLE_BRAND_FAMILY_WRITES || "0") === "0" &&
  String(process.env.ENABLE_ROOMS_WRITES || "1") === "1";

if (wantWrites && !enableProductionWrites) {
  console.error("Production writes requested but gate env flags incomplete.");
  console.error(
    "Need ALLOW_CENSUS_AUTOPILOT_APPLY=1 CONFIRM_WRITE_TO_PRODUCTION_CENSUS=1 CONFIRM_NO_BRAND_EXPLORER_WRITES=1 CONFIRM_NO_BRAND_SETUP_WRITES=1 ENABLE_ROOMS_WRITES=1 (brand writes must stay 0)."
  );
  process.exit(2);
}

const report = await runPropertyFundamentalsEnrichmentV1({
  mode,
  enableProductionWrites,
  maxResearch: Number(argValue("--max-research") || 80),
  maxGeoOnly: Number(argValue("--max-geo") || 2500),
  maxRnt: Number(argValue("--max-rnt") || 600),
  rntMaxRows: Number(argValue("--rnt-max-rows") || 25000),
  log: (m) => console.log(m),
});

console.log(JSON.stringify(report, null, 2));
process.exitCode = report.ok === false ? 2 : 0;
