#!/usr/bin/env node
/** Retry exhausted Gemini baseline fingerprints only (503 SERVER). No OpenAI/Perplexity/Claude. */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  executeProviderBaseline,
  deriveProviderHardCapFromValidation,
} from "../lib/ai-visibility/provider-baseline-orchestrator.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const WAVE_ID = "aiv_baseline_gemini_20260814_1105_9b7e19";
const MODEL = "gemini-3.6-flash";

process.env.AI_VISIBILITY_ENABLED = "true";
process.env.AI_VISIBILITY_LIVE_TEST = "true";
process.env.AI_VISIBILITY_GEMINI_MODEL = MODEL;

const val = JSON.parse(
  fs.readFileSync(
    path.join(
      ROOT,
      "data/ai-visibility/runtime/provider-validation/gemini/waves/aiv_validation_gemini_20260814_1100_41a5e2/validation-summary.json"
    ),
    "utf8"
  )
);

const result = await executeProviderBaseline({
  provider: "gemini",
  model: MODEL,
  waveId: WAVE_ID,
  resume: true,
  retryFailedFingerprints: true,
  force: true,
  hardCapUsd: deriveProviderHardCapFromValidation("gemini", val),
});

console.log(
  JSON.stringify(
    {
      waveId: WAVE_ID,
      SUCCEEDED: result.SUCCEEDED,
      FAILED: result.FAILED,
      status: result.status,
      costUsd: result.costLedger?.actualUsd,
    },
    null,
    2
  )
);

process.exit(result.SUCCEEDED === 84 && result.status === "completed" ? 0 : 1);
