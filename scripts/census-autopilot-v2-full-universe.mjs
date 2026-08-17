#!/usr/bin/env node
/**
 * Census Autopilot V2 — Full LATAM/Caribbean Census Factory
 *
 * npm run census:autopilot-v2-full-universe
 *
 * Phase A: full candidate classification (no mass paid crawl)
 * Phase B: bounded SerpApi wave (ceiling = min(500, 25% available))
 *
 * No Airtable. No Webhound. No StayingAPI. No Cvent production evidence.
 */

import path from "node:path";
import { fileURLToPath } from "node:url";
import "dotenv/config";
import { runCensusAutopilotV2 } from "../lib/research-engine-v2/census-autopilot-v2/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");

const phaseBMax = Number(process.env.SERPAPI_V2_PHASE_B_MAX || 40);
const skipPhaseB = process.argv.includes("--phase-a-only");

const result = await runCensusAutopilotV2({
  root: ROOT,
  phaseB: !skipPhaseB,
  phaseBMax,
  log: console.log,
});

console.log(
  JSON.stringify(
    {
      outDir: result.outDir,
      runId: result.runId,
      candidates: result.universe.total_candidates,
      unique_pids: result.dedupeSummary.estimated_unique_physical_hotels,
      phaseB: result.phaseB?.results_count || 0,
      runtime_ms: result.actualRuntimeMs,
    },
    null,
    2
  )
);
