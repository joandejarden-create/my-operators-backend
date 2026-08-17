#!/usr/bin/env node
/**
 * Phase 3A.11 — Wave-1 live OpenAI showcase execution entry.
 *
 *   node scripts/ai-visibility-phase3a11-live-env.mjs scripts/ai-visibility-phase3a11-wave1-execute.mjs --execute
 *   node scripts/ai-visibility-phase3a11-live-env.mjs scripts/ai-visibility-phase3a11-wave1-execute.mjs --execute --resume
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { executeWave1Showcase, preflightWave1LiveEnv } from "../lib/ai-visibility/wave1-showcase-orchestrator.js";
import { buildWave1PostWaveAudit } from "../lib/ai-visibility/wave1-post-wave-audit.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function parseArgs(argv) {
  const out = {
    execute: false,
    resume: false,
    wave1Id: null,
    stopAfterFirstSlot: false,
    preflightOnly: false,
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--execute") out.execute = true;
    else if (a === "--resume") out.resume = true;
    else if (a === "--wave1-id") out.wave1Id = argv[++i];
    else if (a === "--stop-after-first-slot") out.stopAfterFirstSlot = true;
    else if (a === "--preflight-only") out.preflightOnly = true;
  }
  return out;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const preflight = preflightWave1LiveEnv();
  if (args.preflightOnly || !args.execute) {
    console.log(JSON.stringify({ mode: "preflight", ...preflight }, null, 2));
    process.exit(preflight.LIVE_ENV_READY ? 0 : 2);
  }

  if (!preflight.LIVE_ENV_READY) {
    console.error(
      JSON.stringify(
        {
          ok: false,
          BUILD_STATUS: "BRAND_AI_VISIBILITY_PHASE_3A11_LIVE_OPENAI_SHOWCASE_WAVE_BLOCKED",
          reason: "preflight_failed",
          preflight,
          LIVE_PROVIDER_LOGICAL_CALLS: 0,
        },
        null,
        2
      )
    );
    process.exit(2);
  }

  const result = await executeWave1Showcase({
    execute: true,
    resume: args.resume,
    wave1Id: args.wave1Id || undefined,
    stopAfterFirstSlot: args.stopAfterFirstSlot,
  });

  if (result.mode === "blocked") {
    console.error(JSON.stringify(result, null, 2));
    process.exit(2);
  }

  const audit = await buildWave1PostWaveAudit({
    wave1Id: result.wave1Id,
    checkpoint: result.checkpoint,
    summary: result.summary,
  });

  const outDir = path.join(
    ROOT,
    "data",
    "ai-visibility",
    "runtime",
    "wave1-showcase",
    "waves",
    result.wave1Id
  );
  fs.mkdirSync(outDir, { recursive: true });
  const reportPath = path.join(outDir, "phase3a11-final-report.json");
  const report = {
    BUILD_MARKER: "BRAND_AI_VISIBILITY_PHASE_3A11_LIVE_OPENAI_SHOWCASE_WAVE_COMPLETE",
    ...audit,
    summary: result.summary,
  };
  fs.writeFileSync(reportPath, JSON.stringify(report, null, 2), "utf8");

  console.log(
    JSON.stringify(
      {
        ok: true,
        WAVE1_ID: result.wave1Id,
        STATUS: result.summary.status,
        ACTIVATION_GATE: result.summary.activationGate?.RESULT || null,
        LOGICAL: result.summary.logical,
        COST: result.summary.cost,
        DATASET_STATUS: audit.DATASET_STATUS,
        BUILD_STATUS: audit.BUILD_STATUS,
        REPORT_PATH: reportPath,
        AIRTABLE_WRITES: 0,
        ENTITLEMENT_WRITES: 0,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(
    JSON.stringify(
      {
        error: err.message,
        code: err.code || null,
        stack: process.env.AI_VISIBILITY_DEBUG === "1" ? err.stack : undefined,
      },
      null,
      2
    )
  );
  process.exit(1);
});
