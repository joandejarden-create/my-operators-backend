#!/usr/bin/env node
/**
 * CALA Official Rooms Source Registry Wave
 *
 * Dry-run:  npm run census:official-rooms-source-registry -- --mode dry-run
 * Run:      …gates… npm run census:official-rooms-source-registry -- --mode run --enable-production-writes
 */
import "dotenv/config";
import { runOfficialRoomsSourceRegistryWave } from "../lib/research-engine-v2/official-rooms-source-registry-wave-v1.js";

const argv = process.argv.slice(2);
function argValue(flag) {
  const i = argv.indexOf(flag);
  return i >= 0 ? argv[i + 1] || null : null;
}

const mode = String(argValue("--mode") || "dry-run").toLowerCase();
if (!["dry-run", "run"].includes(mode)) {
  console.error("Invalid --mode (dry-run|run)");
  process.exit(2);
}

const wantWrites = argv.includes("--enable-production-writes");
const enableProductionWrites =
  wantWrites &&
  mode === "run" &&
  String(process.env.ALLOW_CENSUS_AUTOPILOT_APPLY || "0") === "1" &&
  String(process.env.CONFIRM_WRITE_TO_PRODUCTION_CENSUS || "0") === "1" &&
  String(process.env.CONFIRM_NO_BRAND_EXPLORER_WRITES || "0") === "1" &&
  String(process.env.CONFIRM_NO_BRAND_SETUP_WRITES || "0") === "1" &&
  String(process.env.ENABLE_ROOMS_WRITES || "1") === "1" &&
  String(process.env.ENABLE_CURRENT_BRAND_WRITES || "0") === "0" &&
  String(process.env.ENABLE_BRAND_FAMILY_WRITES || "0") === "0";

if (wantWrites && !enableProductionWrites) {
  console.error("Production writes requested but gate env flags incomplete.");
  process.exit(2);
}

const report = await runOfficialRoomsSourceRegistryWave({
  mode,
  enableProductionWrites,
  maxPerSource: Number(argValue("--max-per-source") || 5000),
  skipBarbados: argv.includes("--skip-barbados"),
  log: (m) => console.log(m),
});

console.log(JSON.stringify(report, null, 2));
process.exitCode = report.ok === false ? 2 : 0;
