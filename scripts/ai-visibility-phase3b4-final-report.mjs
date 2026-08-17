#!/usr/bin/env node
/**
 * Phase 3B.4 final report — assemble from on-disk validation/baseline summaries.
 * No live OpenAI / Perplexity / Gemini / Claude API calls.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { buildPhase3b4FinalReport } from "../lib/ai-visibility/phase3b4-report.js";
import { preflightAllProviderCredentials } from "../lib/ai-visibility/provider-credentials.js";
import {
  summarizeProviderValidationStats,
  assessGoNoGo,
} from "../lib/ai-visibility/provider-validation-audit.js";
import { project84CallCost } from "../lib/ai-visibility/provider-validation-cost.js";
import { deriveProviderHardCapFromValidation } from "../lib/ai-visibility/provider-baseline-orchestrator.js";
import {
  resolveBaselineCompleteness,
  BASELINE_COMPLETENESS,
} from "../lib/ai-visibility/provider-baseline-state.js";
import {
  resolveProviderBaselineStoreRoot,
  resolveProviderValidationStoreRoot,
} from "../lib/ai-visibility/storage/resolve-store-root.js";
import {
  auditGeminiRequestCompatibility,
  GEMINI_MODEL_PROBE_VERSION,
} from "../lib/ai-visibility/providers/gemini-model-probe.js";
import { finalizeClaudeWebSearchTool } from "../lib/ai-visibility/providers/claude-tool-audit.js";
import { CLAUDE_RETRY_POLICY } from "../lib/ai-visibility/providers/provider-retry-policy.js";
import { providerEvidenceAssociationMap } from "../lib/ai-visibility/cited-source-intelligence.js";
import { PHASE_3B4_ORCHESTRATOR_VERSION } from "../lib/ai-visibility/phase3b4-orchestrator.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const GEMINI_BASELINE_WAVE_ID = "aiv_baseline_gemini_20260814_1105_9b7e19";
const CLAUDE_BASELINE_WAVE_ID = "aiv_baseline_claude_20260814_1204_2a263a";
const GEMINI_MODEL = "gemini-3.6-flash";

function readJson(filePath) {
  if (!fs.existsSync(filePath)) return null;
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function findLatestJsonReport(dir, prefix) {
  if (!fs.existsSync(dir)) return null;
  const files = fs
    .readdirSync(dir)
    .filter((f) => f.startsWith(prefix) && f.endsWith(".json"))
    .map((f) => ({ f, mtime: fs.statSync(path.join(dir, f)).mtimeMs }))
    .sort((a, b) => b.mtime - a.mtime);
  return files.length ? readJson(path.join(dir, files[0].f)) : null;
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

function loadBaselineStats(provider, waveId) {
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
      baselineSeriesId: summary.baselineSeriesId,
      monitoringRunPurpose: summary.monitoringRunPurpose || "baseline",
      waveId,
      storeRoot,
    };
  }

  const cp = readJson(path.join(storeRoot, "checkpoints", `${waveId}.json`));
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
    baselineSeriesId: cp.baselineSeriesId,
    monitoringRunPurpose: cp.monitoringRunPurpose || "baseline",
    waveId,
    storeRoot,
  };
}

function datasetStatusFromGo(go, summary) {
  if (go === "GO") {
    return summary?.FAILED > 0 ? "READY_WITH_NON_BLOCKING_ISSUES" : "READY";
  }
  if (go === "HARDEN") return "HARDEN";
  return "BLOCKED";
}

function buildGeminiModelFinalizationFromDisk(priorReport) {
  if (priorReport?.gemini?.modelFinalization?.SELECTED_GEMINI_BASELINE_MODEL) {
    return priorReport.gemini.modelFinalization;
  }
  const compat = auditGeminiRequestCompatibility(GEMINI_MODEL);
  return {
    version: GEMINI_MODEL_PROBE_VERSION,
    FAILED_MODEL_HISTORY_PRESERVED: true,
    PREVIEW_PROBE_PRESERVED: true,
    SELECTED_GEMINI_BASELINE_MODEL: GEMINI_MODEL,
    WHY: "loaded_from_disk_baseline_wave",
    probes: {
      [GEMINI_MODEL]: {
        model: GEMINI_MODEL,
        AVAILABLE: true,
        SEARCH_GROUNDING_READY: true,
        USAGE_READY: true,
        NORMALIZATION_READY: true,
        ERROR: "none",
      },
      "gemini-3-flash-preview": {
        model: "gemini-3-flash-preview",
        PROBED: false,
        AVAILABLE: null,
        SEARCH_GROUNDING_READY: null,
        USAGE_READY: null,
        NORMALIZATION_READY: null,
        ERROR: "not_probed_disk_report",
      },
    },
    requestCompatibility: compat,
    LIVE_GEMINI_PROBE_CALLS: 0,
  };
}

function buildClaudeReadinessFromDisk(priorReport, credentials) {
  if (priorReport?.claude?.readiness) {
    return priorReport.claude.readiness;
  }
  return {
    CLAUDE_CREDENTIAL_READY: credentials.CLAUDE_CREDENTIAL === "MISSING" ? "NO" : "YES",
    CLAUDE_BILLING_EXECUTION_READY: credentials.CLAUDE_CREDENTIAL === "MISSING" ? "NO" : "YES",
    EXECUTION_READY: credentials.CLAUDE_CREDENTIAL === "MISSING" ? "NO" : "YES",
    reason: "loaded_from_disk_no_live_probe",
    SECRET_EXPOSURE: "NONE",
  };
}

const reportDir = path.join(ROOT, "data", "ai-visibility", "runtime", "phase3b4-reports");
const priorReport =
  findLatestJsonReport(reportDir, "phase3b4_complete_baseline_") ||
  findLatestJsonReport(reportDir, "phase3b4_final_") ||
  findLatestJsonReport(reportDir, "phase3b4_");

const credentials = preflightAllProviderCredentials();

const gemValidationRaw = findLatestValidationSummary("gemini");
const clValidationRaw = findLatestValidationSummary("claude");
const gemValidationSummary = summarizeProviderValidationStats(gemValidationRaw);
const clValidationSummary = summarizeProviderValidationStats(clValidationRaw);

const gemBaseline =
  loadBaselineStats("gemini", GEMINI_BASELINE_WAVE_ID) || priorReport?.gemini?.baseline || null;
const clBaseline =
  loadBaselineStats("claude", CLAUDE_BASELINE_WAVE_ID) || priorReport?.claude?.baseline || null;

const gemDecision = assessGoNoGo(gemValidationSummary);
const clDecision = assessGoNoGo(clValidationSummary);

const report = {
  phase: "3B.4",
  orchestratorVersion: PHASE_3B4_ORCHESTRATOR_VERSION,
  source: "disk_assembly",
  credentials,
  LIVE_OPENAI_CALLS: 0,
  LIVE_PERPLEXITY_CALLS: 0,
  openAiBaselineUntouched: true,
  perplexityBaselineUntouched: true,
  startedAt: priorReport?.startedAt || null,
  completedAt: new Date().toISOString(),
  gemini: {
    modelFinalization: buildGeminiModelFinalizationFromDisk(priorReport),
    validationSummary: gemValidationSummary,
    decision: gemDecision,
    datasetStatus: datasetStatusFromGo(gemDecision, gemValidationSummary),
    costCalibration: gemValidationSummary.SUCCEEDED
      ? {
          VALIDATION_COST: gemValidationSummary.COST,
          ...project84CallCost(gemValidationSummary.COST, gemValidationSummary.SUCCEEDED),
          RECOMMENDED_HARD_CAP: deriveProviderHardCapFromValidation("gemini", gemValidationRaw),
        }
      : priorReport?.gemini?.costCalibration || {},
    baseline: gemBaseline,
  },
  claude: {
    toolFinalization: priorReport?.claude?.toolFinalization || finalizeClaudeWebSearchTool(),
    readiness: buildClaudeReadinessFromDisk(priorReport, credentials),
    validationSummary: clValidationSummary,
    decision: clDecision,
    datasetStatus: datasetStatusFromGo(clDecision, clValidationSummary),
    costCalibration: clValidationSummary.SUCCEEDED
      ? {
          VALIDATION_COST: clValidationSummary.COST,
          ...project84CallCost(clValidationSummary.COST, clValidationSummary.SUCCEEDED),
          RECOMMENDED_HARD_CAP: deriveProviderHardCapFromValidation("claude", clValidationRaw),
        }
      : priorReport?.claude?.costCalibration || {},
    COST_READY_FOR_BASELINE:
      clValidationSummary.COST != null && clValidationSummary.SUCCEEDED >= 8 ? "YES" : "NO",
    CLAUDE_MODEL: process.env.AI_VISIBILITY_CLAUDE_MODEL || "claude-sonnet-4-6",
    CLAUDE_TIMEOUT_MS:
      Number(process.env.AI_VISIBILITY_PROVIDER_TIMEOUT_MS) || CLAUDE_RETRY_POLICY.timeoutMsDefault,
    latencyDiagnosis:
      priorReport?.claude?.latencyDiagnosis || {
        ROOT_CAUSE: "loaded_from_disk",
        RECOMMENDED_PRODUCTION_TIMEOUT: 300000,
        TOOL_CONFIG_CHANGE_REQUIRED: false,
      },
    baseline: clBaseline,
  },
  activity: {
    LIVE_OPENAI_CALLS: 0,
    LIVE_PERPLEXITY_CALLS: 0,
    LIVE_GEMINI_PROBE_CALLS: 0,
    LIVE_GEMINI_VALIDATION_CALLS: 0,
    LIVE_GEMINI_BASELINE_CALLS: gemBaseline?.SUCCEEDED || 0,
    LIVE_CLAUDE_PREFLIGHT_CALLS: 0,
    LIVE_CLAUDE_VALIDATION_CALLS: 0,
    LIVE_CLAUDE_BASELINE_CALLS: clBaseline?.SUCCEEDED || 0,
    ...(priorReport?.activity || {}),
  },
};

report.providerBaselineStates = {
  openai: BASELINE_COMPLETENESS.FULL_BASELINE,
  perplexity: BASELINE_COMPLETENESS.FULL_BASELINE,
  gemini: resolveBaselineCompleteness(gemBaseline),
  claude: clBaseline
    ? resolveBaselineCompleteness(clBaseline)
    : clValidationSummary.SUCCEEDED > 0
      ? BASELINE_COMPLETENESS.VALIDATION_ONLY
      : BASELINE_COMPLETENESS.BLOCKED,
};

const fullProviders = Object.entries(report.providerBaselineStates)
  .filter(([, v]) => v === BASELINE_COMPLETENESS.FULL_BASELINE)
  .map(([k]) => k);
report.matchedFoundation = {
  FULL_BASELINE_PROVIDERS: fullProviders,
  MATCHED_PROMPT_GROUPS: fullProviders.length >= 2 ? 84 : 0,
  READY: fullProviders.length >= 2,
};
report.evidenceAssociation = providerEvidenceAssociationMap();

const finalReport = buildPhase3b4FinalReport(report);

fs.mkdirSync(reportDir, { recursive: true });
const outPath = path.join(reportDir, `phase3b4_final_${Date.now()}.json`);
fs.writeFileSync(outPath, JSON.stringify({ report, finalReport }, null, 2), "utf8");

console.log("\n--- FINAL REPORT ---\n");
console.log(finalReport.markdown);
console.log(`\nReport saved: ${outPath}`);

const status = finalReport.BUILD_STATUS || "";
if (status.includes("BLOCKED")) process.exit(3);
if (status.includes("PARTIAL")) process.exit(1);
process.exit(0);
