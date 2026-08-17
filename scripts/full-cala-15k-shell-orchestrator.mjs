#!/usr/bin/env node
/**
 * Full CALA shell orchestrator — SAFE HBX-backed auto-apply until exhausted.
 *
 * Dry-run (read-only):
 *   npm run census:full-cala-15k-shell-orchestrator -- --mode dry-run
 *
 * Production:
 *   ALLOW_CENSUS_AUTOPILOT_APPLY=1 CONFIRM_WRITE_TO_PRODUCTION_CENSUS=1 \
 *   CONFIRM_NO_BRAND_EXPLORER_WRITES=1 CONFIRM_NO_BRAND_SETUP_WRITES=1 \
 *   ENABLE_FULL_CALA_15K_CENSUS_SHELL=1 ENABLE_CENSUS_SHELL_INSERTS=1 \
 *   ENABLE_CURRENT_BRAND_WRITES=0 ENABLE_BRAND_FAMILY_WRITES=0 ENABLE_ROOMS_WRITES=0 \
 *   npm run census:full-cala-15k-shell-orchestrator -- --mode run --enable-production-writes
 *
 * Resume after interruption:
 *   …same env… --mode resume --enable-production-writes
 *
 * Resume after founder-stop (only when issue resolved):
 *   … --mode resume --enable-production-writes --acknowledge-founder-stop
 */
import "dotenv/config";
import { runFullCala15kShellOrchestratorV1 } from "../lib/research-engine-v2/full-cala-15k-shell-orchestrator-v1.js";

const argv = process.argv.slice(2);

function argValue(flag) {
  const i = argv.indexOf(flag);
  if (i < 0) return null;
  return argv[i + 1] || null;
}

const modeRaw = argValue("--mode") || "dry-run";
const mode = String(modeRaw).toLowerCase();
if (!["dry-run", "run", "resume"].includes(mode)) {
  console.error(`Invalid --mode ${modeRaw}; use dry-run | run | resume`);
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
  console.error(
    "Production writes requested but gate env flags are incomplete or brand/rooms writes are enabled. Aborting."
  );
  process.exit(2);
}

const maxBatchesArg = argValue("--max-batches");
const maxBatches =
  maxBatchesArg != null && Number.isFinite(Number(maxBatchesArg))
    ? Number(maxBatchesArg)
    : mode === "dry-run"
      ? 1
      : Infinity;

const report = await runFullCala15kShellOrchestratorV1({
  mode,
  enableProductionWrites,
  maxBatches,
  acknowledgeFounderStop: argv.includes("--acknowledge-founder-stop"),
  log: (msg) => console.log(msg),
});

console.log(
  JSON.stringify(
    {
      ORCHESTRATOR_STATUS: report.ORCHESTRATOR_STATUS,
      STOP_REASON: report.STOP_REASON,
      FINAL_CENSUS_COUNT: report.FINAL_CENSUS_COUNT,
      SHELLS_ADDED_THIS_RUN: report.SHELLS_ADDED_THIS_RUN,
      BATCHES_COMPLETED_THIS_RUN: report.BATCHES_COMPLETED_THIS_RUN,
      COUNTRIES_PROCESSED: report.COUNTRIES_PROCESSED,
      HOLDS_CREATED: report.HOLDS_CREATED,
      ERRORS: report.ERRORS,
      LAST_SUCCESSFUL_BATCH: report.LAST_SUCCESSFUL_BATCH,
      NEXT_SAFE_POOL: report.NEXT_SAFE_POOL,
      NEXT_UNRESOLVED_POOL: report.NEXT_UNRESOLVED_POOL,
      FOUNDER_DECISION_REQUIRED: report.FOUNDER_DECISION_REQUIRED,
      FOUNDER_DECISION: report.FOUNDER_DECISION,
      production_writes: report.production_writes,
      production_table_id: report.production_table_id,
      run_id: report.run_id,
      checkpoint_path: report.checkpoint_path,
      run_dir: report.run_dir,
      resume_command: report.resume_command,
    },
    null,
    2
  )
);

const founderStop =
  report.FOUNDER_DECISION_REQUIRED === "YES" ||
  String(report.ORCHESTRATOR_STATUS || "").includes("stop_for_founder");
process.exit(founderStop ? 1 : report.ok === false ? 1 : 0);
