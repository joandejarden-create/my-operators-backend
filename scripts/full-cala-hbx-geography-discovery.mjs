#!/usr/bin/env node
/**
 * Full CALA HBX Geography Discovery Wave
 *
 * Dry-run:
 *   npm run census:full-cala-hbx-geography-discovery -- --mode dry-run
 *
 * Production:
 *   …gate env… npm run census:full-cala-hbx-geography-discovery -- --mode run --enable-production-writes
 *
 * Resume:
 *   …gate env… --mode resume --enable-production-writes
 */
import "dotenv/config";
import { runFullCalaHbxGeographyDiscoveryWaveV1 } from "../lib/research-engine-v2/full-cala-hbx-geography-discovery-wave-v1.js";

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

const maxGeo = argValue("--max-geographies");
const report = await runFullCalaHbxGeographyDiscoveryWaveV1({
  mode,
  enableProductionWrites,
  maxGeographies: maxGeo != null ? Number(maxGeo) : Infinity,
  skipShell: argv.includes("--skip-shell"),
  skipPostAudit: argv.includes("--skip-post-audit"),
  log: (msg) => console.log(msg),
});

console.log(
  JSON.stringify(
    {
      DISCOVERY_STATUS: report.DISCOVERY_STATUS,
      CANONICAL_IN_SCOPE_GEOGRAPHIES: report.CANONICAL_IN_SCOPE_GEOGRAPHIES,
      HBX_GEOGRAPHIES_COMPLETE_BEFORE: report.HBX_GEOGRAPHIES_COMPLETE_BEFORE,
      HBX_GEOGRAPHIES_ATTEMPTED_THIS_RUN: report.HBX_GEOGRAPHIES_ATTEMPTED_THIS_RUN,
      HBX_GEOGRAPHIES_COMPLETE_AFTER: report.HBX_GEOGRAPHIES_COMPLETE_AFTER,
      HBX_UNSUPPORTED_GEOGRAPHIES: report.HBX_UNSUPPORTED_GEOGRAPHIES,
      HBX_ZERO_RESULT_GEOGRAPHIES: report.HBX_ZERO_RESULT_GEOGRAPHIES,
      NEW_HBX_SOURCE_ROWS: report.NEW_HBX_SOURCE_ROWS,
      NEW_UNIQUE_HBX_HOTEL_CODES: report.NEW_UNIQUE_HBX_HOTEL_CODES,
      NEW_CVENT_HBX_MATCHES: report.NEW_CVENT_HBX_MATCHES,
      EXISTING_HOLD_CANDIDATES_UPGRADED_TO_SAFE:
        report.EXISTING_HOLD_CANDIDATES_UPGRADED_TO_SAFE,
      NEW_SHELLS_INSERTED: report.NEW_SHELLS_INSERTED,
      CENSUS_BEFORE: report.CENSUS_BEFORE,
      CENSUS_AFTER: report.CENSUS_AFTER,
      TOTAL_HOLDS_AFTER: report.TOTAL_HOLDS_AFTER,
      GEOGRAPHIES_WITH_ZERO_CENSUS_AFTER:
        report.GEOGRAPHIES_WITH_ZERO_CENSUS_AFTER,
      GEOGRAPHIES_WITH_SOURCE_GAPS_AFTER:
        report.GEOGRAPHIES_WITH_SOURCE_GAPS_AFTER,
      TOP_REMAINING_SOURCE_GAPS: report.TOP_REMAINING_SOURCE_GAPS,
      FOUNDER_DECISION_REQUIRED: report.FOUNDER_DECISION_REQUIRED,
      FOUNDER_DECISION: report.FOUNDER_DECISION,
      ledger_path: report.ledger_path,
      STOP_REASON: report.STOP_REASON || null,
      queued: report.HBX_GEOGRAPHIES_QUEUED,
      bermuda_in_scope: report.bermuda_in_scope,
    },
    null,
    2
  )
);

process.exit(
  report.FOUNDER_DECISION_REQUIRED === "YES" || report.ok === false ? 1 : 0
);
