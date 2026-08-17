#!/usr/bin/env node
/**
 * Phase 3B.4 baseline completion — resume Gemini + optional Claude.
 * LIVE_OPENAI_CALLS=0 · LIVE_PERPLEXITY_CALLS=0
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  executeProviderBaseline,
  deriveProviderHardCapFromValidation,
} from "../lib/ai-visibility/provider-baseline-orchestrator.js";
import {
  resolveProviderBaselineStoreRoot,
  resolveProviderValidationStoreRoot,
} from "../lib/ai-visibility/storage/resolve-store-root.js";
import { PROVIDER_BASELINE_HARD_CAPS } from "../lib/ai-visibility/provider-baseline-state.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const GEMINI_WAVE_ID = "aiv_baseline_gemini_20260814_1105_9b7e19";
const CLAUDE_WAVE_ID = "aiv_baseline_claude_20260814_1204_2a263a";
const GEMINI_MODEL = "gemini-3.6-flash";
const CLAUDE_MODEL = process.env.AI_VISIBILITY_CLAUDE_MODEL || "claude-sonnet-4-6";

function readJson(filePath) {
  if (!fs.existsSync(filePath)) return null;
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function readCheckpoint(provider, waveId) {
  const storeRoot = resolveProviderBaselineStoreRoot(provider);
  return readJson(path.join(storeRoot, "checkpoints", `${waveId}.json`));
}

function findLatestValidationSummary(provider) {
  const storeRoot = resolveProviderValidationStoreRoot(provider);
  const wavesDir = path.join(storeRoot, "waves");
  if (!fs.existsSync(wavesDir)) return null;

  let latest = null;
  let latestMtime = 0;
  for (const waveId of fs.readdirSync(wavesDir)) {
    const summaryPath = path.join(wavesDir, waveId, "validation-summary.json");
    if (!fs.existsSync(summaryPath)) continue;
    const mtime = fs.statSync(summaryPath).mtimeMs;
    if (mtime > latestMtime) {
      latestMtime = mtime;
      latest = readJson(summaryPath);
    }
  }
  return latest;
}

function resolveHardCap(provider) {
  const validation = findLatestValidationSummary(provider);
  if (validation) {
    return deriveProviderHardCapFromValidation(provider, validation);
  }
  return PROVIDER_BASELINE_HARD_CAPS[provider] ?? 50;
}

function loadBaselineFromDisk(provider, waveId) {
  const storeRoot = resolveProviderBaselineStoreRoot(provider);
  const summary = readJson(path.join(storeRoot, "waves", waveId, "baseline-summary.json"));
  if (summary) {
    return {
      SUCCEEDED: summary.succeeded ?? summary.logical?.succeeded ?? 0,
      FAILED: summary.failed ?? summary.logical?.failedFinal ?? 0,
      TOTAL_ATTEMPTS: summary.logical?.totalAttempts ?? 0,
      RETRIES: summary.logical?.retried ?? 0,
      status: summary.status,
      slotResults: summary.slots ?? summary.slotResults,
      costLedger: summary.costLedger ?? { actualUsd: summary.costUsd ?? null },
      ACTUAL_MODEL_RETURNED: summary.returnedModel ?? null,
      waveId,
      storeRoot,
    };
  }

  const cp = readCheckpoint(provider, waveId);
  if (!cp) return null;
  return {
    SUCCEEDED: cp.logical?.succeeded ?? 0,
    FAILED: cp.logical?.failedFinal ?? 0,
    TOTAL_ATTEMPTS: cp.logical?.totalAttempts ?? 0,
    RETRIES: cp.logical?.retried ?? 0,
    status: cp.status,
    slotResults: cp.slots,
    costLedger: cp.costLedger,
    ACTUAL_MODEL_RETURNED: cp.modelReturned?.[0] ?? null,
    waveId,
    storeRoot,
  };
}

console.log(
  JSON.stringify(
    {
      phase: "3B.4_COMPLETE_BASELINE",
      LIVE_OPENAI_CALLS: 0,
      LIVE_PERPLEXITY_CALLS: 0,
      geminiWaveId: GEMINI_WAVE_ID,
      claudeWaveId: CLAUDE_WAVE_ID,
      geminiModel: GEMINI_MODEL,
    },
    null,
    2
  )
);

process.env.AI_VISIBILITY_ENABLED = "true";
process.env.AI_VISIBILITY_LIVE_TEST = "true";
process.env.AI_VISIBILITY_GEMINI_MODEL = GEMINI_MODEL;

const results = {
  phase: "3B.4_COMPLETE_BASELINE",
  LIVE_OPENAI_CALLS: 0,
  LIVE_PERPLEXITY_CALLS: 0,
  openAiBaselineUntouched: true,
  perplexityBaselineUntouched: true,
  startedAt: new Date().toISOString(),
  gemini: {},
  claude: {},
};

results.gemini.baseline = await executeProviderBaseline({
  provider: "gemini",
  model: GEMINI_MODEL,
  waveId: GEMINI_WAVE_ID,
  resume: true,
  retryFailedFingerprints: true,
  force: true,
  hardCapUsd: resolveHardCap("gemini"),
});

const claudeCp = readCheckpoint("claude", CLAUDE_WAVE_ID);
if (claudeCp && claudeCp.status !== "completed") {
  results.claude.baseline = await executeProviderBaseline({
    provider: "claude",
    model: CLAUDE_MODEL,
    waveId: CLAUDE_WAVE_ID,
    resume: true,
    retryFailedFingerprints: true,
    force: true,
    hardCapUsd: resolveHardCap("claude"),
  });
} else {
  results.claude.skipped = claudeCp ? "already_completed" : "checkpoint_not_found";
  results.claude.baseline = loadBaselineFromDisk("claude", CLAUDE_WAVE_ID);
}

results.completedAt = new Date().toISOString();

const outDir = path.join(ROOT, "data", "ai-visibility", "runtime", "phase3b4-reports");
fs.mkdirSync(outDir, { recursive: true });
const outPath = path.join(outDir, `phase3b4_complete_baseline_${Date.now()}.json`);
fs.writeFileSync(outPath, JSON.stringify(results, null, 2), "utf8");

console.log("\n--- COMPLETE BASELINE RESULTS ---\n");
console.log(
  JSON.stringify(
    {
      gemini: {
        waveId: GEMINI_WAVE_ID,
        SUCCEEDED: results.gemini.baseline?.SUCCEEDED,
        FAILED: results.gemini.baseline?.FAILED,
        status: results.gemini.baseline?.status,
      },
      claude: {
        waveId: CLAUDE_WAVE_ID,
        skipped: results.claude.skipped ?? null,
        SUCCEEDED: results.claude.baseline?.SUCCEEDED,
        FAILED: results.claude.baseline?.FAILED,
        status: results.claude.baseline?.status,
      },
      reportPath: outPath,
    },
    null,
    2
  )
);

const geminiComplete = results.gemini.baseline?.SUCCEEDED === 84 && results.gemini.baseline?.status === "completed";
const claudeComplete =
  results.claude.skipped === "already_completed" ||
  (results.claude.baseline?.SUCCEEDED === 84 && results.claude.baseline?.status === "completed");

if (!geminiComplete || !claudeComplete) process.exit(1);
process.exit(0);
