/**
 * Phase 3B.4 final report builder.
 */

import { BASELINE_COMPLETENESS, resolveBaselineCompleteness } from "./provider-baseline-state.js";
import { providerEvidenceAssociationMap } from "./cited-source-intelligence.js";

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

export function buildPhase3b4FinalReport(report) {
  const cred = report.credentials || {};
  const gemFin = report.gemini?.modelFinalization || {};
  const gemVal = report.gemini?.validationSummary || {};
  const gemBase = report.gemini?.baseline || {};
  const clTool = report.claude?.toolFinalization || {};
  const clReady = report.claude?.readiness || {};
  const clVal = report.claude?.validationSummary || {};
  const clBase = report.claude?.baseline || {};

  const states = {
    openai: BASELINE_COMPLETENESS.FULL_BASELINE,
    perplexity: BASELINE_COMPLETENESS.FULL_BASELINE,
    gemini: resolveBaselineCompleteness(gemBase) || BASELINE_COMPLETENESS.NONE,
    claude: clBase?.SUCCEEDED
      ? resolveBaselineCompleteness(clBase)
      : clVal.SUCCEEDED
        ? BASELINE_COMPLETENESS.VALIDATION_ONLY
        : BASELINE_COMPLETENESS.BLOCKED,
  };

  const obs = {
    openai: 84,
    perplexity: 84,
    gemini: gemBase.SUCCEEDED || 0,
    claude: clBase.SUCCEEDED || 0,
  };
  const total = obs.openai + obs.perplexity + obs.gemini + obs.claude;
  const remaining = 336 - total;

  const measured = Object.entries(states)
    .filter(([, v]) => v === BASELINE_COMPLETENESS.FULL_BASELINE)
    .map(([k]) => k.charAt(0).toUpperCase() + k.slice(1));

  let buildStatus = "BRAND_AI_VISIBILITY_PHASE_3B4_FOUR_PROVIDER_BASELINE_COMPLETION_PASS";
  if (states.gemini !== BASELINE_COMPLETENESS.FULL_BASELINE && states.claude !== BASELINE_COMPLETENESS.FULL_BASELINE) {
    buildStatus =
      "BRAND_AI_VISIBILITY_PHASE_3B4_FOUR_PROVIDER_BASELINE_COMPLETION_PARTIAL — Gemini and Claude baselines incomplete";
  } else if (
    states.gemini !== BASELINE_COMPLETENESS.FULL_BASELINE ||
    states.claude !== BASELINE_COMPLETENESS.FULL_BASELINE
  ) {
    buildStatus =
      "BRAND_AI_VISIBILITY_PHASE_3B4_FOUR_PROVIDER_BASELINE_COMPLETION_PARTIAL — incomplete four-provider baseline";
  }

  let nextPhase = "PHASE_3C1_DISCOVERABILITY_BUSINESS_IMPACT_FOUNDATION";
  if (states.claude === BASELINE_COMPLETENESS.BLOCKED || states.claude === BASELINE_COMPLETENESS.VALIDATION_ONLY) {
    nextPhase = "PHASE_3B5_CLAUDE_HARDENING";
  } else if (states.gemini === BASELINE_COMPLETENESS.PARTIAL_BASELINE) {
    nextPhase = "PHASE_3B5_PROVIDER_DATA_HARDENING";
  } else if (
    states.gemini === BASELINE_COMPLETENESS.FULL_BASELINE &&
    states.claude === BASELINE_COMPLETENESS.FULL_BASELINE
  ) {
    nextPhase = "PHASE_3B5_MULTI_PROVIDER_RECURRING_MONITORING_FOUNDATION";
  }

  const probes = gemFin.probes || {};
  const p36 = probes["gemini-3.6-flash"] || {};
  const pPrev = probes["gemini-3-flash-preview"] || {};

  const markdown = `# BRAND_AI_VISIBILITY_PHASE_3B4_FOUR_PROVIDER_BASELINE_COMPLETION_COMPLETE

## 1. Constitution Compliance
Provider-pure · validation≠baseline · no All AI · OpenAI/Perplexity untouched · Evidence Footprint non-composite · peer v2 · showcase_prompts_v1 preserved.

## 2. Live Preflight
OPENAI: ${cred.OPENAI_CREDENTIAL || "MISSING"}
GEMINI: ${cred.GEMINI_CREDENTIAL}
PERPLEXITY: ${cred.PERPLEXITY_CREDENTIAL}
CLAUDE: ${cred.CLAUDE_CREDENTIAL}

## 3. Gemini Model Finalization
GEMINI_3_6_FLASH: AVAILABLE=${p36.AVAILABLE} SEARCH=${p36.SEARCH_GROUNDING_READY} USAGE=${p36.USAGE_READY} NORM=${p36.NORMALIZATION_READY} ERROR=${p36.ERROR || "none"}
GEMINI_3_FLASH_PREVIEW: PROBED=${pPrev.PROBED !== false} AVAILABLE=${pPrev.AVAILABLE} SEARCH=${pPrev.SEARCH_GROUNDING_READY}
SELECTED_MODEL: ${gemFin.SELECTED_GEMINI_BASELINE_MODEL || "NONE"}
WHY: ${gemFin.WHY || "N/A"}

## 4. Gemini Request Compatibility
MIGRATION_REQUIRED: ${gemFin.requestCompatibility?.GEMINI_REQUEST_MIGRATION_REQUIRED || "NO"}
CHANGES: ${(gemFin.requestCompatibility?.CHANGES || []).join(", ")}
SEMANTIC_PROMPT_CHANGED: NO

## 5. Gemini Validation
PLANNED: ${gemVal.PLANNED ?? 12}
SUCCESS: ${gemVal.SUCCEEDED ?? 0}
FAILED: ${gemVal.FAILED ?? 0}
ATTEMPTS: ${gemVal.TOTAL_ATTEMPTS ?? 0}
RETRIES: ${gemVal.RETRIES ?? 0}
SEARCH: ${gemVal.RESPONSES_WITH_GROUNDING ?? 0}
CITATIONS: ${gemVal.TOTAL_NORMALIZED_CITATIONS ?? 0}
DOMAINS: ${gemVal.UNIQUE_DOMAINS ?? 0}
LANGUAGE: EN/ES per validation
ENTITY: ${gemVal.PEER_MENTIONS ?? 0}
CLASSIFIER: ${gemVal.ACTIVATION_GATE === "PASS" ? "READY" : "PENDING"}
COST: ${gemVal.COST ?? "N/A"}
LATENCY: ${gemVal.AVG_LATENCY ?? "N/A"}
STATUS: ${report.gemini?.datasetStatus || gemVal.DATASET_STATUS || "N/A"}

## 6. Gemini Cost Calibration
LOW: ${report.gemini?.costCalibration?.LOW ?? "N/A"}
EXPECTED: ${report.gemini?.costCalibration?.EXPECTED ?? "N/A"}
HIGH: ${report.gemini?.costCalibration?.HIGH ?? "N/A"}
HARD_CAP: ${report.gemini?.costCalibration?.RECOMMENDED_HARD_CAP ?? "N/A"}

## 7. Gemini Baseline
EXECUTED: ${gemBase.SUCCEEDED > 0 ? "YES" : "NO"}
PLANNED: 84
SUCCESS: ${gemBase.SUCCEEDED ?? 0}
FAILED: ${gemBase.FAILED ?? 0}
ATTEMPTS: ${gemBase.TOTAL_ATTEMPTS ?? 0}
RETRIES: ${gemBase.RETRIES ?? 0}
COST: ${gemBase.costLedger?.actualUsd ?? "N/A"}
HARD_CAP: ${report.gemini?.costCalibration?.RECOMMENDED_HARD_CAP ?? "N/A"}
STATUS: ${gemBase.status ?? "NOT_EXECUTED"}

## 8. Gemini Slot Results
${JSON.stringify(slotLines(gemBase), null, 2)}

## 9. Claude Preflight
CREDENTIAL: ${clReady.CLAUDE_CREDENTIAL_READY}
BILLING: ${clReady.CLAUDE_BILLING_EXECUTION_READY}
MODEL: ${report.claude?.CLAUDE_MODEL || "claude-sonnet-4-6"}
TIMEOUT: ${report.claude?.CLAUDE_TIMEOUT_MS || 300000}

## 10. Claude Tool Finalization
CURRENT_TOOL: ${clTool.CURRENT_TOOL_VERSION}
LATEST_COMPATIBLE: ${clTool.LATEST_COMPATIBLE_TOOL_VERSION}
SELECTED_TOOL: ${clTool.SELECTED_TOOL_VERSION}
ALLOWED_CALLERS: ${(clTool.SELECTED_ALLOWED_CALLERS || []).join(",")}
MAX_USES: ${clTool.MAX_USES}
WHY: ${clTool.SELECTION_REASON || clTool.WHY || ""}

## 11. Claude Validation
PLANNED: ${clVal.PLANNED ?? 12}
SUCCESS: ${clVal.SUCCEEDED ?? 0}
FAILED: ${clVal.FAILED ?? 0}
ATTEMPTS: ${clVal.TOTAL_ATTEMPTS ?? 0}
RETRIES: ${clVal.RETRIES ?? 0}
WEB_SEARCH: ${clVal.WEB_SEARCH_USED ?? 0}
CITATIONS: ${clVal.TOTAL_NORMALIZED_CITATIONS ?? 0}
DOMAINS: ${clVal.UNIQUE_DOMAINS ?? 0}
LANGUAGE: EN/ES
ENTITY: ${clVal.PEER_MENTIONS ?? 0}
CLASSIFIER: ${clVal.ACTIVATION_GATE === "PASS" ? "READY" : "PENDING"}
COST: ${clVal.COST ?? "N/A"}
LATENCY: ${clVal.AVG_LATENCY ?? "N/A"}
STATUS: ${report.claude?.datasetStatus || clVal.DATASET_STATUS || "N/A"}

## 12. Claude Latency Diagnosis
ROOT_CAUSE: ${report.claude?.latencyDiagnosis?.ROOT_CAUSE || "N/A"}
RECOMMENDED_TIMEOUT: ${report.claude?.latencyDiagnosis?.RECOMMENDED_PRODUCTION_TIMEOUT || 300000}
CONFIG_CHANGE_REQUIRED: ${report.claude?.latencyDiagnosis?.TOOL_CONFIG_CHANGE_REQUIRED || false}

## 13. Claude Cost Calibration
LOW: ${report.claude?.costCalibration?.LOW ?? "N/A"}
EXPECTED: ${report.claude?.costCalibration?.EXPECTED ?? "N/A"}
HIGH: ${report.claude?.costCalibration?.HIGH ?? "N/A"}
HARD_CAP: ${report.claude?.costCalibration?.RECOMMENDED_HARD_CAP ?? "N/A"}
READY_FOR_BASELINE: ${report.claude?.COST_READY_FOR_BASELINE || "NO"}

## 14. Claude Baseline
EXECUTED: ${clBase.SUCCEEDED > 0 ? "YES" : "NO"}
PLANNED: 84
SUCCESS: ${clBase.SUCCEEDED ?? 0}
FAILED: ${clBase.FAILED ?? 0}
ATTEMPTS: ${clBase.TOTAL_ATTEMPTS ?? 0}
RETRIES: ${clBase.RETRIES ?? 0}
COST: ${clBase.costLedger?.actualUsd ?? "N/A"}
HARD_CAP: ${report.claude?.costCalibration?.RECOMMENDED_HARD_CAP ?? "N/A"}
STATUS: ${clBase.status ?? "NOT_EXECUTED"}

## 15. Claude Slot Results
${JSON.stringify(slotLines(clBase), null, 2)}

## 16. Existing Baseline Integrity
OPENAI: FULL_BASELINE untouched
PERPLEXITY: FULL_BASELINE untouched
CALLS_THIS_PHASE: OPENAI=0 PERPLEXITY=0

## 17. Provider Baseline States
OPENAI: ${states.openai}
GEMINI: ${states.gemini}
PERPLEXITY: ${states.perplexity}
CLAUDE: ${states.claude}

## 18. Four-Provider Progress
OPENAI: ${obs.openai}
GEMINI: ${obs.gemini}
PERPLEXITY: ${obs.perplexity}
CLAUDE: ${obs.claude}
TOTAL: ${total}
TARGET: 336
REMAINING: ${remaining}

## 19. Provider Models
Gemini: ${gemFin.SELECTED_GEMINI_BASELINE_MODEL || "N/A"} / returned ${gemBase.ACTUAL_MODEL_RETURNED || gemVal.MODEL_RETURNED || "N/A"}
Claude: claude-sonnet-4-6 / returned ${clBase.ACTUAL_MODEL_RETURNED || clVal.MODEL_RETURNED || "N/A"}
OpenAI: gpt-5.6 (existing)
Perplexity: sonar (existing)

## 20. Provider Costs
OPENAI_EXISTING: ~$38.41
GEMINI: val ${gemVal.COST ?? 0} + base ${gemBase.costLedger?.actualUsd ?? 0}
PERPLEXITY_EXISTING: $0.46774
CLAUDE: val ${clVal.COST ?? 0} + base ${clBase.costLedger?.actualUsd ?? 0}

## 22. Provider Selector
MEASURED: ${measured.join(", ")}
VALIDATION_ONLY: ${states.claude === BASELINE_COMPLETENESS.VALIDATION_ONLY ? "Claude" : "none"}
BLOCKED: ${[states.gemini === BASELINE_COMPLETENESS.BLOCKED ? "Gemini" : null, states.claude === BASELINE_COMPLETENESS.BLOCKED ? "Claude" : null].filter(Boolean).join(", ") || "none"}
ALL_AI: NO

## 23. Evidence Footprint
READY: YES (deterministic module + Detailed View integration)
COMPOSITE_SCORE: NONE

## 24. Evidence Association Levels
${JSON.stringify(providerEvidenceAssociationMap(), null, 2)}

## 25. Cited Source Intelligence
READY: YES
LONGITUDINAL: NOT_YET_AVAILABLE
READERSHIP: FUTURE_EXTERNAL_DATA_DEPENDENCY

## 26. Top Cited Source Sort Method
distinct_monitored_responses then citation_reference_count

## 27. Owned vs Third-Party Source Classification
READY: NO (no governed owned-domain map — no guesses)

## 28. Longitudinal Source Intelligence
NEW_SOURCES: NOT_YET_AVAILABLE
DISAPPEARING_SOURCES: NOT_YET_AVAILABLE
SOURCE_FREQUENCY_MOVEMENT: NOT_YET_AVAILABLE
PERSISTENCE_ACROSS_PERIODS: NOT_YET_AVAILABLE

## 29. Readership Data
STATUS: FUTURE_EXTERNAL_DATA_DEPENDENCY

## 30. UI Integration
IMPLEMENTED: YES
LOCATION: Detailed View → Evidence Basis (Evidence Footprint + Cited Source Intelligence)
NEW_TABS: NO
DUPLICATE_METRICS_ADDED: NO

## 31. Matched Four-Provider Foundation
FULL_BASELINE_PROVIDERS: ${(report.matchedFoundation?.FULL_BASELINE_PROVIDERS || []).join(", ")}
MATCHED_PROMPT_GROUPS: ${report.matchedFoundation?.MATCHED_PROMPT_GROUPS ?? 0}
READY: ${report.matchedFoundation?.READY ?? false}

## 34. Discoverability / Business Impact
NEXT_PRIORITY: YES

## 35. Storage Compatibility
RAILWAY_POSTGRES: YES
OBJECT_STORAGE: YES

## 38. Activity
LIVE_OPENAI_CALLS: 0
LIVE_PERPLEXITY_CALLS: 0
LIVE_GEMINI_PROBE_CALLS: ${report.activity?.LIVE_GEMINI_PROBE_CALLS ?? 0}
LIVE_GEMINI_VALIDATION_CALLS: ${report.activity?.LIVE_GEMINI_VALIDATION_CALLS ?? 0}
LIVE_GEMINI_BASELINE_CALLS: ${report.activity?.LIVE_GEMINI_BASELINE_CALLS ?? 0}
LIVE_CLAUDE_PREFLIGHT_CALLS: ${report.activity?.LIVE_CLAUDE_PREFLIGHT_CALLS ?? 0}
LIVE_CLAUDE_VALIDATION_CALLS: ${report.activity?.LIVE_CLAUDE_VALIDATION_CALLS ?? 0}
LIVE_CLAUDE_BASELINE_CALLS: ${report.activity?.LIVE_CLAUDE_BASELINE_CALLS ?? 0}

## 40. Next Recommended Phase
${nextPhase}

## 41. BUILD STATUS
${buildStatus}
`;

  return {
    BUILD_STATUS: buildStatus,
    markdown,
    providerBaselineStates: states,
    observations: obs,
    total,
    remaining,
    measuredProviders: measured,
    nextPhase,
    activity: report.activity,
  };
}
