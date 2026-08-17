#!/usr/bin/env node
/**
 * Census Autopilot V3.1 — 250-property autonomous production scale proof.
 *
 * npm run census:autopilot-v3-1-scale-proof
 * npm run census:autopilot-v3-1-scale-proof -- --apply
 * npm run census:autopilot-v3-1-scale-proof -- --apply --resume-research
 */
import path from "node:path";
import { fileURLToPath } from "node:url";
import "dotenv/config";
import {
  runCensusAutopilotV31,
  V31_OUT_REL,
} from "../lib/research-engine-v2/census-autopilot-v3/v31-scale-proof.js";
import { PHASE2_ENV_GATE } from "../lib/research-engine-v2/census-autopilot-v3/constants.js";

const ROOT = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const applyWrites = process.argv.includes("--apply");
const skipResearch = process.argv.includes("--skip-research");
const resumeResearch = process.argv.includes("--resume-research");

if (applyWrites && String(process.env[PHASE2_ENV_GATE] || "").trim() !== "1") {
  console.error(JSON.stringify({ error: "NEED_ENABLE_VERIFIED_CENSUS_WRITES" }));
  process.exit(2);
}

const result = await runCensusAutopilotV31({
  root: ROOT,
  log: console.log,
  applyWrites,
  skipResearch,
  resumeResearch,
});

console.log(
  JSON.stringify(
    {
      out: result.outDir || path.join(ROOT, V31_OUT_REL),
      run_id: result.runId,
      scale_verdict: result.scaleVerdict,
      staging_avg: result.stagingAvg,
      inserts: result.dry.inserts.length,
      updates: result.dry.updates.length,
      gates_pass: result.gates.all_pass,
      applyWrites,
      phase2_success: result.phase2?.success ?? null,
      circuit: result.phase2?.circuit ?? null,
      serpapi_calls: result.cost.serpapi_calls,
      elapsed_ms: result.elapsedMs,
    },
    null,
    2
  )
);

if (applyWrites && result.phase2 && !result.phase2.success) process.exit(1);
