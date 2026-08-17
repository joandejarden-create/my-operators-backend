#!/usr/bin/env node
/**
 * Census Autopilot V3 — Governed Airtable Migration Pilot
 *
 * Phase 1 (default): dry-run only — NEVER writes Airtable.
 * Phase 2: requires ENABLE_VERIFIED_CENSUS_WRITES=1 AND --phase2 after Joan authorization.
 * Bound to authorized Phase 1 run_id only (see phase2-executor AUTHORIZED_RUN_ID).
 *
 * npm run census:autopilot-v3-airtable-migration
 * npm run census:autopilot-v3-airtable-migration -- --phase2
 */

import path from "node:path";
import { fileURLToPath } from "node:url";
import "dotenv/config";
import {
  runCensusAutopilotV3Phase1,
  runCensusAutopilotV3Phase2,
  PHASE2_ENV_GATE,
} from "../lib/research-engine-v2/census-autopilot-v3/index.js";

const ROOT = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const wantPhase2 = process.argv.includes("--phase2");
const gateOn = String(process.env[PHASE2_ENV_GATE] || "").trim() === "1";

if (wantPhase2) {
  if (!gateOn) {
    console.error(
      JSON.stringify(
        {
          error: "PHASE2_BLOCKED",
          message: `Phase 2 requires ${PHASE2_ENV_GATE}=1 after Joan reviews Phase 1 dry-run`,
        },
        null,
        2
      )
    );
    process.exit(2);
  }

  const result = await runCensusAutopilotV3Phase2({
    root: ROOT,
    log: console.log,
  });

  console.log(
    JSON.stringify(
      {
        outDir: result.outDir,
        success: result.success,
        circuit: result.circuit,
        pilot_a: result.pilotASummary,
        pilot_b_executed: result.pilotBExecuted,
        pilot_b: result.pilotBSummary,
        a_match_rate_pct: result.aMatchRate,
        full: result.fullSummary,
        validation: {
          expected_vs_actual_match_rate_pct:
            result.validation.expected_vs_actual_match_rate_pct,
          duplicate_inserts: result.validation.duplicate_inserts,
          cvent_leakage: result.validation.cvent_leakage,
          legacy_leakage: result.validation.legacy_leakage,
          rooms_written: result.validation.rooms_written,
        },
        writes_executed: true,
        authorized_run_id: "cav3_2026-08-08T15-04-05-566Z",
      },
      null,
      2
    )
  );
  process.exit(result.success ? 0 : 1);
}

const result = await runCensusAutopilotV3Phase1({
  root: ROOT,
  log: console.log,
});

console.log(
  JSON.stringify(
    {
      outDir: result.outDir,
      runId: result.runId,
      hard_gates_pass: result.gates.all_pass,
      selected: result.selection.actual,
      inserts: result.dry.inserts.length,
      updates: result.dry.updates.length,
      rooms_pending: result.dry.roomsPending,
      serpapi_blocked_rows: result.dry.serpapiBlockedFields,
      writes_executed: false,
      phase2_gate: `${PHASE2_ENV_GATE}=1 + --phase2`,
    },
    null,
    2
  )
);
