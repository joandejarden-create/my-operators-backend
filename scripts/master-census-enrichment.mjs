#!/usr/bin/env node
/**
 * Master Hotel Property Census Enrichment
 *
 * Dry-run:  npm run census:master-enrichment -- --mode dry-run
 * Run:      …gates… npm run census:master-enrichment -- --mode run --enable-production-writes
 * Resume:   …gates… npm run census:master-enrichment -- --mode resume --enable-production-writes
 */
import "dotenv/config";
import { runMasterCensusEnrichment } from "../lib/research-engine-v2/master-census-enrichment-v1.js";

const argv = process.argv.slice(2);
function argValue(flag) {
  const i = argv.indexOf(flag);
  return i >= 0 ? argv[i + 1] || null : null;
}
function has(flag) {
  return argv.includes(flag);
}

const mode = String(argValue("--mode") || "dry-run").toLowerCase();
if (!["dry-run", "run", "resume"].includes(mode)) {
  console.error("Invalid --mode (dry-run|run|resume)");
  process.exit(2);
}

const wantWrites = has("--enable-production-writes");
const enableProductionWrites =
  wantWrites &&
  (mode === "run" || mode === "resume") &&
  String(process.env.ALLOW_CENSUS_AUTOPILOT_APPLY || "0") === "1" &&
  String(process.env.CONFIRM_WRITE_TO_PRODUCTION_CENSUS || "0") === "1" &&
  String(process.env.CONFIRM_NO_BRAND_EXPLORER_WRITES || "0") === "1" &&
  String(process.env.CONFIRM_NO_BRAND_SETUP_WRITES || "0") === "1";

if (wantWrites && !enableProductionWrites) {
  console.error("Production writes requested but gate env flags incomplete.");
  console.error(
    "Need ALLOW_CENSUS_AUTOPILOT_APPLY=1 CONFIRM_WRITE_TO_PRODUCTION_CENSUS=1 CONFIRM_NO_BRAND_EXPLORER_WRITES=1 CONFIRM_NO_BRAND_SETUP_WRITES=1"
  );
  process.exit(2);
}

const report = await runMasterCensusEnrichment({
  mode,
  enableProductionWrites,
  maxPfResearch: Number(argValue("--max-pf-research") || 120),
  maxPfGeo: Number(argValue("--max-pf-geo") || 500),
  maxPfRnt: Number(argValue("--max-pf-rnt") || 200),
  maxBrandValidate: Number(argValue("--max-brand-validate") || 2000),
  maxCoordinateRequests: argValue("--max-coordinate-requests")
    ? Number(argValue("--max-coordinate-requests"))
    : undefined,
  coordinateSampleSize: Number(argValue("--coordinate-sample-size") || 25),
  maxRoomsPerSource: Number(argValue("--max-rooms-per-source") || 6000),
  skipRoomsRegistry: has("--skip-rooms-registry"),
  skipPropertyFundamentals: has("--skip-property-fundamentals"),
  skipCoordinates: has("--skip-coordinates"),
  forceReopenCoordinates: has("--force-reopen-coordinates"),
  skipCoordinateSample: has("--skip-coordinate-sample"),
  brandRoomsWave: has("--brand-rooms-wave") || !has("--continue-mapbox-wave"),
  forceBrandRoomsWave: has("--force-brand-rooms-wave"),
  continueMapboxWave: has("--continue-mapbox-wave"),
  skipBrandPortfolio: has("--skip-brand-portfolio"),
  log: (m) => console.log(m),
});

console.log(JSON.stringify(report, null, 2));
process.exitCode = report.ok === false ? 2 : 0;
