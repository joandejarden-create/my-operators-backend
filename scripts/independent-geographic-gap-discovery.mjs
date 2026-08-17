#!/usr/bin/env node
/**
 * Independent geographic gap discovery wave.
 *
 * Dry-run:  npm run census:independent-gap-discovery -- --mode dry-run
 * Run:      …gates… npm run census:independent-gap-discovery -- --mode run --enable-production-writes
 */
import "dotenv/config";
import { runIndependentGeographicGapDiscoveryWaveV1 } from "../lib/research-engine-v2/independent-geographic-gap-discovery-wave-v1.js";

const argv = process.argv.slice(2);
function argValue(flag) {
  const i = argv.indexOf(flag);
  return i >= 0 ? argv[i + 1] || null : null;
}

const mode = String(argValue("--mode") || "dry-run").toLowerCase();
if (!["dry-run", "run", "resume"].includes(mode)) {
  console.error("Invalid --mode");
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
  String(process.env.ENABLE_FULL_CALA_15K_CENSUS_SHELL || "0") === "1" &&
  String(process.env.ENABLE_CENSUS_SHELL_INSERTS || "0") === "1" &&
  String(process.env.ENABLE_CURRENT_BRAND_WRITES || "0") === "0" &&
  String(process.env.ENABLE_BRAND_FAMILY_WRITES || "0") === "0" &&
  String(process.env.ENABLE_ROOMS_WRITES || "0") === "0";

if (wantWrites && !enableProductionWrites) {
  console.error("Production writes requested but gate env flags incomplete.");
  process.exit(2);
}

const report = await runIndependentGeographicGapDiscoveryWaveV1({
  mode,
  enableProductionWrites,
  hbxBudget: Number(argValue("--hbx-budget") || 10),
  serpMax: Number(argValue("--serp-max") || 60),
  maxGeographies: Number(argValue("--max-geographies") || 17),
  log: (m) => console.log(m),
});

console.log(JSON.stringify(report, null, 2));
process.exitCode = report.ok === false ? 2 : 0;
