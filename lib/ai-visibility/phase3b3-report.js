/**
 * Phase 3B.3 final report builder.
 */

import fs from "fs";
import path from "path";
import { BASELINE_COMPLETENESS, resolveBaselineCompleteness } from "./provider-baseline-state.js";
import { CITATION_RATE_COMPATIBILITY } from "./providers/cross-provider-signals.js";
import { WAVE1_ROOT } from "./storage/resolve-store-root.js";
import { resolveProviderBaselineStoreRoot, resolveProviderValidationStoreRoot } from "./storage/resolve-store-root.js";

function slotLines(baseline) {
  if (!baseline?.slotResults) return [];
  return Object.entries(baseline.slotResults).map(([key, s]) => ({
    slot: key,
    planned: s.planned ?? 12,
    success: s.succeeded ?? 0,
    failed: s.failed ?? 0,
    attempts: s.attempts ?? 0,
    retries: s.retries ?? 0,
    cost: s.cost ?? null,
    status: s.status ?? "unknown",
  }));
}

function countObservations(states) {
  let total = 0;
  for (const st of Object.values(states)) {
    if (st === BASELINE_COMPLETENESS.FULL_BASELINE) total += 84;
  }
  return total;
}

function readOpenAiWindow() {
  try {
    const wavesDir = path.join(WAVE1_ROOT, "waves");
    if (!fs.existsSync(wavesDir)) return null;
    const dirs = fs.readdirSync(wavesDir).filter((d) => d.includes("aiv_wave1_openai"));
    if (!dirs.length) return null;
    const summaryPath = path.join(wavesDir, dirs[0], "summary.json");
    if (!fs.existsSync(summaryPath)) return null;
    const s = JSON.parse(fs.readFileSync(summaryPath, "utf8"));
    return { startedAt: s.completedAt ? null : s.preflight?.startedAt, completedAt: s.completedAt, waveId: s.wave1Id || s.batchId };
  } catch {
    return null;
  }
}

/**
 * @param {object} report — executePhase3b3 output
 */
export function buildPhase3b3FinalReport(report) {
  const cred = report.credentials || {};
  const gemVal = report.gemini?.validationSummary || {};
  const gemBase = report.gemini?.baseline || {};
  const pplBase = report.perplexity?.baseline || {};
  const clReady = report.claude?.readiness || {};
  const clVal = report.claude?.validationSummary || {};
  const clBase = report.claude?.baseline || {};

  const states = {
    openai: BASELINE_COMPLETENESS.FULL_BASELINE,
    gemini: resolveBaselineCompleteness(gemBase) || BASELINE_COMPLETENESS.NONE,
    perplexity: resolveBaselineCompleteness(pplBase) || BASELINE_COMPLETENESS.NONE,
    claude: clBase?.SUCCEEDED
      ? resolveBaselineCompleteness(clBase)
      : clVal.SUCCEEDED
        ? BASELINE_COMPLETENESS.VALIDATION_ONLY
        : clReady.EXECUTION_READY === "NO"
          ? BASELINE_COMPLETENESS.BLOCKED
          : BASELINE_COMPLETENESS.NONE,
  };

  const fullProviders = Object.entries(states)
    .filter(([, v]) => v === BASELINE_COMPLETENESS.FULL_BASELINE)
    .map(([k]) => k);

  const totalComplete = countObservations(states);
  const target = 336;
  const remaining = target - totalComplete;

  const measuredProviders = fullProviders.map((p) =>
    p === "openai" ? "OpenAI" : p.charAt(0).toUpperCase() + p.slice(1)
  );

  let buildStatus = "BRAND_AI_VISIBILITY_PHASE_3B3_MULTI_PROVIDER_BASELINE_EXPANSION_PASS";
  if (states.gemini === BASELINE_COMPLETENESS.BLOCKED && states.perplexity === BASELINE_COMPLETENESS.BLOCKED) {
    buildStatus = "BRAND_AI_VISIBILITY_PHASE_3B3_MULTI_PROVIDER_BASELINE_EXPANSION_BLOCKED — all new providers blocked";
  } else if (
    states.gemini !== BASELINE_COMPLETENESS.FULL_BASELINE ||
    states.perplexity !== BASELINE_COMPLETENESS.FULL_BASELINE
  ) {
    buildStatus = "BRAND_AI_VISIBILITY_PHASE_3B3_MULTI_PROVIDER_BASELINE_EXPANSION_PARTIAL — incomplete baseline collection";
  }

  let nextPhase = "PHASE_3C1_DISCOVERABILITY_BUSINESS_IMPACT_FOUNDATION";
  if (states.claude !== BASELINE_COMPLETENESS.FULL_BASELINE && clReady.EXECUTION_READY === "YES") {
    nextPhase = "PHASE_3B4_CLAUDE_BASELINE_COMPLETION";
  } else if (fullProviders.length >= 3) {
    nextPhase = "PHASE_3B4_MULTI_PROVIDER_RECURRING_MONITORING_FOUNDATION";
  } else if (states.gemini === BASELINE_COMPLETENESS.PARTIAL_BASELINE || states.perplexity === BASELINE_COMPLETENESS.PARTIAL_BASELINE) {
    nextPhase = "PHASE_3B4_PROVIDER_DATA_HARDENING";
  }

  const multiRecurring =
    fullProviders.filter((p) => ["openai", "gemini", "perplexity"].includes(p)).length >= 3
      ? "YES"
      : fullProviders.length >= 2
        ? "PARTIAL"
        : "NO";

  const markdown = `# BRAND_AI_VISIBILITY_PHASE_3B3_MULTI_PROVIDER_BASELINE_EXPANSION_COMPLETE

## 1. Constitution Compliance
Provider-pure · validation≠baseline · no All AI · OpenAI untouched · peer v2 · showcase_prompts_v1 · ai_visibility_metrics_v1 preserved.

## 2. Current Credential Preflight
OPENAI: ${cred.OPENAI_CREDENTIAL || "MISSING"} (${cred.OPENAI_ENV_VAR_USED || "OPENAI_API_KEY"})
GEMINI: ${cred.GEMINI_CREDENTIAL} (${cred.GEMINI_ENV_VAR_USED || "GEMINI_API_KEY"})
PERPLEXITY: ${cred.PERPLEXITY_CREDENTIAL} (${cred.PERPLEXITY_ENV_VAR_USED || "PERPLEXITY_API_KEY"})
CLAUDE: ${cred.CLAUDE_CREDENTIAL} (${cred.CLAUDE_ENV_VAR_USED || "ANTHROPIC_API_KEY"})

## 3. Gemini Validation
PLANNED: ${gemVal.PLANNED ?? 12}
SUCCESS: ${gemVal.SUCCEEDED ?? 0}
FAILED: ${gemVal.FAILED ?? 0}
ATTEMPTS: ${gemVal.TOTAL_ATTEMPTS ?? 0}
CITATIONS: ${gemVal.TOTAL_NORMALIZED_CITATIONS ?? 0}
GROUNDING: ${gemVal.RESPONSES_WITH_GROUNDING ?? 0}
DOMAINS: ${gemVal.UNIQUE_DOMAINS ?? 0}
LANGUAGE: EN/ES per validation audit
ENTITY: peer mentions ${gemVal.PEER_MENTIONS ?? 0}
CLASSIFIER: ${gemVal.ACTIVATION_GATE === "PASS" ? "READY" : "PENDING"}
COST: ${gemVal.COST ?? "N/A"}
LATENCY: ${gemVal.AVG_LATENCY ?? "N/A"}ms avg
STATUS: ${gemVal.DATASET_STATUS ?? report.gemini?.status ?? "N/A"}

## 4. Gemini Decision
${report.gemini?.decision ?? "N/A"}

## 5. Gemini Baseline
EXECUTED: ${gemBase.SUCCEEDED > 0 ? "YES" : "NO"}
PLANNED: 84
SUCCESS: ${gemBase.SUCCEEDED ?? 0}
FAILED: ${gemBase.FAILED ?? 0}
TOTAL_ATTEMPTS: ${gemBase.TOTAL_ATTEMPTS ?? 0}
COST: ${gemBase.costLedger?.actualUsd ?? "N/A"}
HARD_CAP: ${report.gemini?.costCalibration?.RECOMMENDED_GEMINI_HARD_CAP ?? "N/A"}
STATUS: ${gemBase.status ?? "NOT_EXECUTED"}

## 6. Gemini Slot Results
${JSON.stringify(slotLines(gemBase), null, 2)}

## 7. Perplexity Baseline
PLANNED: 84
SUCCESS: ${pplBase.SUCCEEDED ?? 0}
FAILED: ${pplBase.FAILED ?? 0}
ATTEMPTS: ${pplBase.TOTAL_ATTEMPTS ?? 0}
COST: ${pplBase.costLedger?.actualUsd ?? "N/A"}
HARD_CAP: $15
STATUS: ${pplBase.status ?? "NOT_EXECUTED"}

## 8. Perplexity Slot Results
${JSON.stringify(slotLines(pplBase), null, 2)}

## 9. Claude Readiness
CREDENTIAL: ${clReady.CLAUDE_CREDENTIAL_READY ?? cred.CLAUDE_CREDENTIAL}
BILLING: ${clReady.CLAUDE_BILLING_EXECUTION_READY ?? "N/A"}
EXECUTION_READY: ${clReady.EXECUTION_READY ?? "N/A"}

## 10. Claude Validation
EXECUTED: ${clVal.SUCCEEDED > 0 ? "YES" : "NO"}
SUCCESS: ${clVal.SUCCEEDED ?? 0}
FAILED: ${clVal.FAILED ?? 0}
COST: ${clVal.COST ?? "N/A"}
STATUS: ${report.claude?.status ?? clVal.DATASET_STATUS ?? "N/A"}

## 11. Claude Baseline
EXECUTED: ${clBase.SUCCEEDED > 0 ? "YES" : "NO"}
SUCCESS: ${clBase.SUCCEEDED ?? 0}
FAILED: ${clBase.FAILED ?? 0}
COST: ${clBase.costLedger?.actualUsd ?? "N/A"}
STATUS: ${clBase.status ?? "NOT_EXECUTED"}

## 12. Provider Baseline States
OPENAI: ${states.openai}
GEMINI: ${states.gemini}
PERPLEXITY: ${states.perplexity}
CLAUDE: ${states.claude}

## 13. Model Identities
Gemini requested: gemini-3.6-flash · returned: ${gemBase.ACTUAL_MODEL_RETURNED ?? gemVal.MODEL_RETURNED ?? "N/A"}
Perplexity requested: sonar · returned: ${pplBase.ACTUAL_MODEL_RETURNED ?? "N/A"}
Claude requested: claude-sonnet-4-6 · returned: ${clBase.ACTUAL_MODEL_RETURNED ?? clVal.MODEL_RETURNED ?? "N/A"}

## 14. Provider Costs
OPENAI_EXISTING: ~$38.41 (Wave-1 complete, untouched)
GEMINI: validation ${gemVal.COST ?? 0} + baseline ${gemBase.costLedger?.actualUsd ?? 0}
PERPLEXITY: baseline ${pplBase.costLedger?.actualUsd ?? 0}
CLAUDE: validation ${clVal.COST ?? 0} + baseline ${clBase.costLedger?.actualUsd ?? 0}

## 21. Provider Selector
MEASURED_PROVIDERS: ${measuredProviders.join(", ") || "OpenAI only"}
VALIDATION_ONLY: ${states.claude === BASELINE_COMPLETENESS.VALIDATION_ONLY ? "Claude" : "none"}
BLOCKED: ${states.claude === BASELINE_COMPLETENESS.BLOCKED ? "Claude" : "none"}
ALL_AI_OPTION: NO

## 24. Four-Provider Baseline Progress
OPENAI: 84/84
GEMINI: ${gemBase.SUCCEEDED ?? 0}/84
PERPLEXITY: ${pplBase.SUCCEEDED ?? 0}/84
CLAUDE: ${clBase.SUCCEEDED ?? 0}/84
TOTAL_COMPLETE_OBSERVATIONS: ${totalComplete}
TARGET: 336
REMAINING: ${remaining}

## 26. Recurring Monitoring Readiness
STATUS: ${multiRecurring}

## 27. Discoverability / Business Impact
NEXT_PHASE_PRIORITY_RETAINED: YES

## 30. Activity
LIVE_OPENAI_CALLS: 0
LIVE_GEMINI_VALIDATION_CALLS: ${report.activity?.LIVE_GEMINI_VALIDATION_CALLS ?? 0}
LIVE_GEMINI_BASELINE_CALLS: ${report.activity?.LIVE_GEMINI_BASELINE_CALLS ?? 0}
LIVE_PERPLEXITY_BASELINE_CALLS: ${report.activity?.LIVE_PERPLEXITY_BASELINE_CALLS ?? 0}
LIVE_CLAUDE_VALIDATION_CALLS: ${report.activity?.LIVE_CLAUDE_VALIDATION_CALLS ?? 0}
LIVE_CLAUDE_BASELINE_CALLS: ${report.activity?.LIVE_CLAUDE_BASELINE_CALLS ?? 0}

## 32. Next Recommended Phase
${nextPhase}

## 33. BUILD STATUS
${buildStatus}
`;

  return {
    BUILD_STATUS: buildStatus,
    markdown,
    credentials: cred,
    providerBaselineStates: states,
    measuredProviders,
    fullBaselineProviders: fullProviders,
    totalCompleteObservations: totalComplete,
    remainingTo336: remaining,
    multiProviderRecurringReady: multiRecurring,
    nextPhase,
    citationRateCompatibility: CITATION_RATE_COMPATIBILITY,
    activity: report.activity,
    collectionWindows: {
      openai: readOpenAiWindow(),
      gemini: { startedAt: gemBase.startedAt, completedAt: gemBase.completedAt, waveId: gemBase.waveId },
      perplexity: { startedAt: pplBase.startedAt, completedAt: pplBase.completedAt, waveId: pplBase.waveId },
      claude: clBase.waveId ? { startedAt: clBase.startedAt, completedAt: clBase.completedAt, waveId: clBase.waveId } : null,
    },
    storeRoots: {
      geminiValidation: resolveProviderValidationStoreRoot("gemini"),
      geminiBaseline: resolveProviderBaselineStoreRoot("gemini"),
      perplexityBaseline: resolveProviderBaselineStoreRoot("perplexity"),
      claudeValidation: resolveProviderValidationStoreRoot("claude"),
      claudeBaseline: resolveProviderBaselineStoreRoot("claude"),
      openai: WAVE1_ROOT,
    },
  };
}
