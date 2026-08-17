#!/usr/bin/env node
/**
 * Full CALA Core Identity Census Orchestrator
 *
 * Dry-run:
 *   npm run census:full-cala-core-identity -- --mode dry-run
 *
 * Production:
 *   gate env… npm run census:full-cala-core-identity -- --mode run --enable-production-writes
 */
import "dotenv/config";
import { runFullCalaCoreIdentityCensusV1 } from "../lib/research-engine-v2/full-cala-core-identity-census-orchestrator-v1.js";

const argv = process.argv.slice(2);
function argValue(flag) {
  const i = argv.indexOf(flag);
  return i >= 0 ? argv[i + 1] || null : null;
}

const mode = String(argValue("--mode") || "dry-run").toLowerCase();
if (!["dry-run", "run", "resume"].includes(mode)) {
  console.error("Invalid --mode; use dry-run | run | resume");
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

const report = await runFullCalaCoreIdentityCensusV1({
  mode,
  enableProductionWrites,
  hbxRequestBudget: Number(argValue("--hbx-budget") || process.env.HBX_CORE_REQUEST_BUDGET || 40),
  hbxPageSize: Number(argValue("--hbx-page-size") || 1000),
  serpApiMaxSearches: Number(
    argValue("--serp-max") || process.env.SERPAPI_CORE_HOLD_UPGRADES || 40
  ),
  skipHbx: argv.includes("--skip-hbx"),
  skipSerpApi: argv.includes("--skip-serpapi"),
  skipShell: argv.includes("--skip-shell"),
  log: (msg) => console.log(msg),
});

console.log(JSON.stringify(report, null, 2));
process.exitCode = report.ok === false ? 2 : 0;
