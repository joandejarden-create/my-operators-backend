#!/usr/bin/env node
/**
 * Core Identity Foundation Closure
 *
 * Dry-run:  npm run census:core-identity-foundation -- --mode dry-run
 * Run:      …gates… npm run census:core-identity-foundation -- --mode run --enable-production-writes
 */
import "dotenv/config";
import { runCoreIdentityFoundationClosureV1 } from "../lib/research-engine-v2/full-cala-core-identity-foundation-closure-v1.js";

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

const report = await runCoreIdentityFoundationClosureV1({
  mode,
  enableProductionWrites,
  hbxBudget: Number(argValue("--hbx-budget") || 8),
  serpMax: Number(argValue("--serp-max") || 40),
  log: (m) => console.log(m),
});

console.log(JSON.stringify(report, null, 2));
process.exitCode = report.ok === false ? 2 : 0;
