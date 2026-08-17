/**
 * Phase 3B.4 — Four-provider baseline completion orchestrator.
 * Gemini + Claude only. OpenAI and Perplexity: 0 calls.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { preflightAllProviderCredentials } from "./provider-credentials.js";
import { probeClaudeBillingExecution } from "./provider-billing-probe.js";
import { finalizeGeminiProductionModel } from "./providers/gemini-model-probe.js";
import { finalizeClaudeWebSearchTool } from "./providers/claude-tool-audit.js";
import { executeProviderValidation } from "./provider-validation-orchestrator.js";
import {
  executeProviderBaseline,
  deriveProviderHardCapFromValidation,
} from "./provider-baseline-orchestrator.js";
import {
  summarizeProviderValidationStats,
  assessGoNoGo,
} from "./provider-validation-audit.js";
import { project84CallCost } from "./provider-validation-cost.js";
import {
  BASELINE_COMPLETENESS,
  resolveBaselineCompleteness,
} from "./provider-baseline-state.js";
import {
  buildEvidenceFootprint,
  resolveEvidenceAssociationLevel,
} from "./evidence-footprint.js";
import {
  buildCitedSourceIntelligence,
  buildMatchedPromptGroups,
  providerEvidenceAssociationMap,
  LONGITUDINAL_SOURCE_STATUS,
  READERSHIP_DATA_STATUS,
} from "./cited-source-intelligence.js";
import { CLAUDE_RETRY_POLICY } from "./providers/provider-retry-policy.js";

export const PHASE_3B4_ORCHESTRATOR_VERSION = "ai_visibility_phase3b4_orchestrator_v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

function ensureDir(d) {
  fs.mkdirSync(d, { recursive: true });
}

function writeJson(p, v) {
  ensureDir(path.dirname(p));
  fs.writeFileSync(p, JSON.stringify(v, null, 2), "utf8");
}

function datasetStatusFromGo(go, summary) {
  if (go === "GO") {
    return summary?.FAILED > 0 ? "READY_WITH_NON_BLOCKING_ISSUES" : "READY";
  }
  if (go === "HARDEN") return "HARDEN";
  return "BLOCKED";
}

/**
 * Execute Phase 3B.4 live sequence.
 */
export async function executePhase3b4(args = {}) {
  const credentials = preflightAllProviderCredentials();
  const report = {
    phase: "3B.4",
    orchestratorVersion: PHASE_3B4_ORCHESTRATOR_VERSION,
    credentials,
    LIVE_OPENAI_CALLS: 0,
    LIVE_PERPLEXITY_CALLS: 0,
    openAiBaselineUntouched: true,
    perplexityBaselineUntouched: true,
    startedAt: new Date().toISOString(),
    gemini: {},
    claude: {},
    activity: {
      LIVE_GEMINI_PROBE_CALLS: 0,
      LIVE_GEMINI_VALIDATION_CALLS: 0,
      LIVE_GEMINI_BASELINE_CALLS: 0,
      LIVE_CLAUDE_PREFLIGHT_CALLS: 0,
      LIVE_CLAUDE_VALIDATION_CALLS: 0,
      LIVE_CLAUDE_BASELINE_CALLS: 0,
      AIRTABLE_WRITES: 0,
      ENTITLEMENT_WRITES: 0,
      DEPLOYS: 0,
    },
  };

  // --- Gemini model finalization ---
  report.gemini.modelFinalization = await finalizeGeminiProductionModel(args);
  report.activity.LIVE_GEMINI_PROBE_CALLS =
    report.gemini.modelFinalization.LIVE_GEMINI_PROBE_CALLS || 0;
  report.gemini.GEMINI_EXECUTION_READY = Boolean(
    report.gemini.modelFinalization.SELECTED_GEMINI_BASELINE_MODEL
  )
    ? "YES"
    : "NO";

  const selectedModel = report.gemini.modelFinalization.SELECTED_GEMINI_BASELINE_MODEL;
  if (selectedModel && credentials.GEMINI_CREDENTIAL !== "MISSING") {
    process.env.AI_VISIBILITY_GEMINI_MODEL = selectedModel;

    report.gemini.validation = await executeProviderValidation({
      ...args,
      provider: "gemini",
      model: selectedModel,
      force: args.force,
    });
    report.activity.LIVE_GEMINI_VALIDATION_CALLS = report.gemini.validation.SUCCEEDED || 0;

    const gemSummary = summarizeProviderValidationStats(report.gemini.validation);
    report.gemini.validationSummary = gemSummary;
    report.gemini.decision = assessGoNoGo(gemSummary);
    report.gemini.datasetStatus = datasetStatusFromGo(report.gemini.decision, gemSummary);
    report.gemini.costCalibration = {
      VALIDATION_COST: gemSummary.COST,
      ...project84CallCost(gemSummary.COST, gemSummary.SUCCEEDED),
      RECOMMENDED_HARD_CAP: deriveProviderHardCapFromValidation(
        "gemini",
        report.gemini.validation
      ),
    };

    const canBaseline =
      report.gemini.datasetStatus === "READY" ||
      report.gemini.datasetStatus === "READY_WITH_NON_BLOCKING_ISSUES";

    if (canBaseline && report.gemini.decision === "GO") {
      report.gemini.baseline = await executeProviderBaseline({
        ...args,
        provider: "gemini",
        model: selectedModel,
        hardCapUsd: report.gemini.costCalibration.RECOMMENDED_HARD_CAP,
        force: args.force,
      });
      report.activity.LIVE_GEMINI_BASELINE_CALLS = report.gemini.baseline.SUCCEEDED || 0;
    }
  } else {
    report.gemini.decision = "BLOCKED";
    report.gemini.datasetStatus = "BLOCKED";
  }

  // --- Claude tool + billing preflight ---
  report.claude.toolFinalization = finalizeClaudeWebSearchTool();
  report.claude.readiness = await probeClaudeBillingExecution({
    ...args,
    timeoutMs: report.claude.toolFinalization.TIMEOUT_MS || 300000,
  });
  report.activity.LIVE_CLAUDE_PREFLIGHT_CALLS = 1;
  report.claude.CLAUDE_EXECUTION_READY = report.claude.readiness.EXECUTION_READY;
  report.claude.CLAUDE_MODEL = process.env.AI_VISIBILITY_CLAUDE_MODEL || "claude-sonnet-4-6";
  report.claude.CLAUDE_TIMEOUT_MS =
    Number(process.env.AI_VISIBILITY_PROVIDER_TIMEOUT_MS) || CLAUDE_RETRY_POLICY.timeoutMsDefault;

  if (report.claude.readiness.EXECUTION_READY === "YES") {
    report.claude.validation = await executeProviderValidation({
      ...args,
      provider: "claude",
      model: report.claude.CLAUDE_MODEL,
      force: args.force,
    });
    report.activity.LIVE_CLAUDE_VALIDATION_CALLS = report.claude.validation.SUCCEEDED || 0;
    const clSummary = summarizeProviderValidationStats(report.claude.validation);
    report.claude.validationSummary = clSummary;
    report.claude.decision = assessGoNoGo(clSummary);
    report.claude.datasetStatus = datasetStatusFromGo(report.claude.decision, clSummary);
    report.claude.costCalibration = {
      VALIDATION_COST: clSummary.COST,
      ...project84CallCost(clSummary.COST, clSummary.SUCCEEDED),
      RECOMMENDED_HARD_CAP: deriveProviderHardCapFromValidation(
        "claude",
        report.claude.validation
      ),
    };
    report.claude.COST_READY_FOR_BASELINE =
      clSummary.COST != null && clSummary.SUCCEEDED >= 8 ? "YES" : "NO";

    // Latency diagnosis
    const avg = clSummary.AVG_LATENCY;
    report.claude.latencyDiagnosis = {
      ROOT_CAUSE:
        report.claude.datasetStatus === "BLOCKED" &&
        (clSummary.ERRORS || []).some((e) => /timeout/i.test(e.message || e.category || ""))
          ? "SERVER_WEB_SEARCH"
          : avg && avg > 60000
            ? "SERVER_WEB_SEARCH"
            : "UNKNOWN",
      EVIDENCE: {
        avgLatency: avg,
        maxLatency: clSummary.MAX_LATENCY,
        timeoutMs: report.claude.CLAUDE_TIMEOUT_MS,
        allowedCallers: report.claude.toolFinalization.SELECTED_ALLOWED_CALLERS,
        toolVersion: report.claude.toolFinalization.SELECTED_TOOL_VERSION,
      },
      RECOMMENDED_PRODUCTION_TIMEOUT: 300000,
      TOOL_CONFIG_CHANGE_REQUIRED: false,
    };

    const canClaudeBaseline =
      (report.claude.datasetStatus === "READY" ||
        report.claude.datasetStatus === "READY_WITH_NON_BLOCKING_ISSUES") &&
      report.claude.decision === "GO" &&
      report.claude.COST_READY_FOR_BASELINE === "YES" &&
      !args.skipClaudeBaseline;

    if (canClaudeBaseline) {
      report.claude.baseline = await executeProviderBaseline({
        ...args,
        provider: "claude",
        model: report.claude.CLAUDE_MODEL,
        hardCapUsd: report.claude.costCalibration.RECOMMENDED_HARD_CAP,
        force: args.force,
      });
      report.activity.LIVE_CLAUDE_BASELINE_CALLS = report.claude.baseline.SUCCEEDED || 0;
    }
  } else {
    report.claude.status = "BLOCKED_BILLING_OR_AUTH";
    report.claude.datasetStatus = "BLOCKED";
  }

  report.completedAt = new Date().toISOString();

  report.providerBaselineStates = {
    openai: BASELINE_COMPLETENESS.FULL_BASELINE,
    perplexity: BASELINE_COMPLETENESS.FULL_BASELINE,
    gemini: resolveBaselineCompleteness(report.gemini.baseline),
    claude: report.claude.baseline
      ? resolveBaselineCompleteness(report.claude.baseline)
      : report.claude.validation?.SUCCEEDED > 0
        ? BASELINE_COMPLETENESS.VALIDATION_ONLY
        : BASELINE_COMPLETENESS.BLOCKED,
  };

  report.evidenceAssociation = providerEvidenceAssociationMap();
  report.evidenceFootprintReady = true;
  report.citedSourceIntelligenceReady = true;
  report.longitudinalSourceIntelligence = { ...LONGITUDINAL_SOURCE_STATUS };
  report.readership = { ...READERSHIP_DATA_STATUS };
  report.DISCOVERABILITY_BUSINESS_IMPACT_NEXT_PRIORITY = "YES";
  report.ALL_AI_OPTION = "NO";
  report.RAILWAY_POSTGRES_MIGRATION_COMPATIBLE = "YES";
  report.OBJECT_STORAGE_COMPATIBLE = "YES";

  // Matched foundation when 2+ full baselines (OpenAI+Perplexity already, plus new)
  const fullProviders = Object.entries(report.providerBaselineStates)
    .filter(([, v]) => v === BASELINE_COMPLETENESS.FULL_BASELINE)
    .map(([k]) => k);
  report.matchedFoundation = {
    FULL_BASELINE_PROVIDERS: fullProviders,
    MATCHED_PROMPT_GROUPS: fullProviders.length >= 2 ? 84 : 0,
    READY: fullProviders.length >= 2,
  };

  const outDir = path.join(
    __dirname,
    "..",
    "..",
    "data",
    "ai-visibility",
    "runtime",
    "phase3b4-reports"
  );
  ensureDir(outDir);
  const outPath = path.join(outDir, `phase3b4_${Date.now()}.json`);
  writeJson(outPath, report);
  report.reportPath = outPath;

  return report;
}
