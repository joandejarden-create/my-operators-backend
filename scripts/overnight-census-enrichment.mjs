#!/usr/bin/env node
/**
 * Overnight Autonomous Census Enrichment
 *
 * Dry-run:  npm run census:overnight-enrichment -- --mode dry-run --max-runtime-minutes 1
 * Run:      …gates… npm run census:overnight-enrichment -- --mode run --enable-production-writes
 * Resume:   …gates… npm run census:overnight-enrichment -- --mode resume --enable-production-writes
 */
import "dotenv/config";
import { runOvernightCensusEnrichment } from "../lib/research-engine-v2/overnight-census-enrichment-v1.js";

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

const maxRuntimeMinutes = argValue("--max-runtime-minutes")
  ? Number(argValue("--max-runtime-minutes"))
  : undefined;
const maxExternalCostUsd = argValue("--max-external-cost-usd")
  ? Number(argValue("--max-external-cost-usd"))
  : undefined;

const report = await runOvernightCensusEnrichment({
  mode,
  enableProductionWrites,
  maxRuntimeMinutes,
  maxExternalCostUsd,
  log: (m) => console.log(m),
});

console.log(JSON.stringify(report, null, 2));
process.exitCode = report.ok === false ? 2 : 0;
