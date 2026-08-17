/**
 * Phase 3B.3 — Multi-provider baseline expansion orchestrator.
 */

import { preflightAllProviderCredentials } from "./provider-credentials.js";
import { probeClaudeBillingExecution } from "./provider-billing-probe.js";
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
  PROVIDER_BASELINE_HARD_CAPS,
  resolveBaselineCompleteness,
  BASELINE_COMPLETENESS,
} from "./provider-baseline-state.js";
import { CITATION_RATE_COMPATIBILITY } from "./providers/cross-provider-signals.js";

export const PHASE_3B3_ORCHESTRATOR_VERSION = "ai_visibility_phase3b3_orchestrator_v1";

function assessGeminiDecision(summary) {
  return assessGoNoGo(summary);
}

function assessClaudeBaselineGo(validationStats, summary) {
  const go = assessGoNoGo(summary);
  if (go !== "GO") return { decision: "STOP", reason: "validation_not_go" };
  const hardCap = deriveProviderHardCapFromValidation("claude", validationStats);
  if (!hardCap || hardCap <= 0) {
    return { decision: "FOUNDER_APPROVAL_REQUIRED", reason: "cost_cap_uncertain", hardCap };
  }
  return { decision: "GO", hardCapUsd: hardCap };
}

/**
 * Execute Phase 3B.3 governed live sequence.
 */
export async function executePhase3b3(args = {}) {
  const credentials = preflightAllProviderCredentials();
  const report = {
    phase: "3B.3",
    orchestratorVersion: PHASE_3B3_ORCHESTRATOR_VERSION,
    credentials,
    openAiBaselineUntouched: true,
    LIVE_OPENAI_CALLS: 0,
    startedAt: new Date().toISOString(),
    gemini: {},
    perplexity: {},
    claude: {},
    providerBaselineStates: {},
    activity: {
      LIVE_GEMINI_VALIDATION_CALLS: 0,
      LIVE_GEMINI_BASELINE_CALLS: 0,
      LIVE_PERPLEXITY_BASELINE_CALLS: 0,
      LIVE_CLAUDE_VALIDATION_CALLS: 0,
      LIVE_CLAUDE_BASELINE_CALLS: 0,
      AIRTABLE_WRITES: 0,
      ENTITLEMENT_WRITES: 0,
      DEPLOYS: 0,
    },
  };

  if (credentials.GEMINI_CREDENTIAL !== "MISSING") {
    report.gemini.validation = await executeProviderValidation({
      ...args,
      provider: "gemini",
      force: args.force,
    });
    report.activity.LIVE_GEMINI_VALIDATION_CALLS = report.gemini.validation.SUCCEEDED || 0;

    const gemSummary = summarizeProviderValidationStats(report.gemini.validation);
    report.gemini.validationSummary = gemSummary;
    report.gemini.decision = assessGeminiDecision(gemSummary);
    report.gemini.costCalibration = {
      VALIDATION_COST: gemSummary.COST,
      ...project84CallCost(gemSummary.COST, gemSummary.SUCCEEDED),
      RECOMMENDED_GEMINI_HARD_CAP: deriveProviderHardCapFromValidation(
        "gemini",
        report.gemini.validation
      ),
    };

    if (report.gemini.decision === "GO") {
      const hardCap = report.gemini.costCalibration.RECOMMENDED_GEMINI_HARD_CAP;
      report.gemini.baseline = await executeProviderBaseline({
        ...args,
        provider: "gemini",
        hardCapUsd: hardCap,
        force: args.force,
      });
      report.activity.LIVE_GEMINI_BASELINE_CALLS = report.gemini.baseline.SUCCEEDED || 0;
    }
  } else {
    report.gemini.status = "BLOCKED_MISSING_CREDENTIAL";
    report.gemini.decision = "BLOCKED";
  }

  if (credentials.PERPLEXITY_CREDENTIAL !== "MISSING") {
    report.perplexity.baseline = await executeProviderBaseline({
      ...args,
      provider: "perplexity",
      hardCapUsd: args.perplexityHardCapUsd ?? PROVIDER_BASELINE_HARD_CAPS.perplexity,
      force: args.force,
    });
    report.activity.LIVE_PERPLEXITY_BASELINE_CALLS = report.perplexity.baseline.SUCCEEDED || 0;
    report.perplexity.HARD_CAP = PROVIDER_BASELINE_HARD_CAPS.perplexity;
  } else {
    report.perplexity.status = "BLOCKED_MISSING_CREDENTIAL";
  }

  report.claude.readiness = await probeClaudeBillingExecution(args);
  if (report.claude.readiness.EXECUTION_READY === "YES") {
    report.claude.validation = await executeProviderValidation({
      ...args,
      provider: "claude",
      force: args.force,
    });
    report.activity.LIVE_CLAUDE_VALIDATION_CALLS = report.claude.validation.SUCCEEDED || 0;
    const clSummary = summarizeProviderValidationStats(report.claude.validation);
    report.claude.validationSummary = clSummary;
    report.claude.decision = assessGoNoGo(clSummary);
    report.claude.costCalibration = {
      VALIDATION_COST: clSummary.COST,
      ...project84CallCost(clSummary.COST, clSummary.SUCCEEDED),
    };

    const baselineGo = assessClaudeBaselineGo(report.claude.validation, clSummary);
    report.claude.baselineGo = baselineGo;
    if (baselineGo.decision === "GO" && !args.skipClaudeBaseline) {
      report.claude.baseline = await executeProviderBaseline({
        ...args,
        provider: "claude",
        hardCapUsd: baselineGo.hardCapUsd,
        force: args.force,
      });
      report.activity.LIVE_CLAUDE_BASELINE_CALLS = report.claude.baseline.SUCCEEDED || 0;
    } else if (baselineGo.decision === "FOUNDER_APPROVAL_REQUIRED") {
      report.claude.status = "VALIDATION_COMPLETE_AWAITING_APPROVAL";
    }
  } else if (report.claude.readiness.EXECUTION_READY === "NO") {
    report.claude.status = "BLOCKED_BILLING";
  } else {
    report.claude.status = "BLOCKED_UNKNOWN";
  }

  report.completedAt = new Date().toISOString();

  report.providerBaselineStates.openai = BASELINE_COMPLETENESS.FULL_BASELINE;
  report.providerBaselineStates.gemini = resolveBaselineCompleteness(report.gemini.baseline);
  report.providerBaselineStates.perplexity = resolveBaselineCompleteness(report.perplexity.baseline);
  report.providerBaselineStates.claude = report.claude.baseline
    ? resolveBaselineCompleteness(report.claude.baseline)
    : report.claude.validation
      ? BASELINE_COMPLETENESS.VALIDATION_ONLY
      : BASELINE_COMPLETENESS.BLOCKED;

  report.citationRateCompatibility = CITATION_RATE_COMPATIBILITY;
  report.DISCOVERABILITY_BUSINESS_IMPACT_FOUNDATION_NEXT = "YES";
  report.ALL_AI_OPTION = "NO";

  return report;
}
