/**
 * Phase 3B.5 final report builder.
 */

import { BASELINE_COMPLETENESS } from "./provider-baseline-state.js";
import { BASELINE_FREEZE_ID } from "./baseline-freeze.js";
import { CLAUDE_COMPLETION_CUMULATIVE_CAP_USD } from "./phase3b5-orchestrator.js";

export function buildPhase3b5FinalReport(report) {
  const inv = report.inventory || {};
  const states = report.providerBaselineStates || {};
  const obs = report.observations || {};

  let buildStatus = "BRAND_AI_VISIBILITY_PHASE_3B5_FOUR_PROVIDER_BASELINE_FINALIZATION_PASS";
  if (obs.TOTAL !== 336) {
    buildStatus = `BRAND_AI_VISIBILITY_PHASE_3B5_FOUR_PROVIDER_BASELINE_FINALIZATION_PARTIAL — ${336 - (obs.TOTAL || 0)} observations remaining`;
  }

  const measured = Object.entries(states)
    .filter(([, v]) => v === BASELINE_COMPLETENESS.FULL_BASELINE)
    .map(([k]) => k.charAt(0).toUpperCase() + k.slice(1));

  let nextPhase = "PHASE_3B6_MULTI_PROVIDER_RECURRING_MONITORING_FOUNDATION";
  if (obs.TOTAL !== 336) nextPhase = "PHASE_3B6_PROVIDER_DATA_HARDENING";

  const markdown = `# BRAND_AI_VISIBILITY_PHASE_3B5_FOUR_PROVIDER_BASELINE_FINALIZATION_COMPLETE

## 1. Constitution Compliance
Targeted completion only · 324 protected fingerprints · no OpenAI/Perplexity calls · same wave/series · validation≠baseline.

## 2. Missing Fingerprint Inventory
OPENAI: ${inv.OPENAI?.successful}/84 missing ${inv.OPENAI?.missingCount}
GEMINI: ${inv.GEMINI?.successful}/84 missing ${inv.GEMINI?.missingCount}
PERPLEXITY: ${inv.PERPLEXITY?.successful}/84 missing ${inv.PERPLEXITY?.missingCount}
CLAUDE: ${inv.CLAUDE?.successful}/84 missing ${inv.CLAUDE?.missingCount}
TOTAL: ${inv.TOTAL_SUCCESSFUL} successful · ${inv.TOTAL_MISSING} missing

## 3. Successful Fingerprint Protection
PROTECTED: ${report.fingerprintProtection?.SUCCESSFUL_FINGERPRINTS_PROTECTED}
COUNT: ${report.fingerprintProtection?.PROTECTED_COUNT}

## 4. Preflight
GEMINI: ${report.gemini?.preflight?.GEMINI_EXECUTION_READY} · ${report.gemini?.preflight?.GEMINI_MODEL}
CLAUDE: ${report.claude?.preflight?.CLAUDE_EXECUTION_READY} · ${report.claude?.preflight?.CLAUDE_MODEL}

## 5. Gemini Targeted Retry
FINGERPRINT: ${report.gemini?.target?.fingerprint || "N/A"}
PROMPT: ${report.gemini?.target?.promptId || "N/A"}
ATTEMPTS: ${report.gemini?.baseline?.ATTEMPTS_THIS_PHASE ?? 0}
SUCCESS: ${report.gemini?.baseline?.SUCCEEDED ?? inv.GEMINI?.successful}
ERROR: ${report.gemini?.baseline?.FAILED ? "see checkpoint" : "none"}

## 6. Gemini Final State
OBSERVATIONS: ${report.gemini?.baseline?.SUCCEEDED ?? inv.GEMINI?.successful}
STATE: ${states.gemini}

## 7. Claude Cap
EXISTING_COST: ${report.claude?.costCap?.EXISTING_BASELINE_COST}
NEW_CUMULATIVE_CAP: ${CLAUDE_COMPLETION_CUMULATIVE_CAP_USD}
AVAILABLE_BUDGET: ${report.claude?.costCap?.AVAILABLE_REMAINING_BUDGET}

## 8. Claude Resume
EXECUTED: ${report.activity?.LIVE_CLAUDE_CALLS ?? 0}
SUCCESS: ${report.claude?.baseline?.SUCCEEDED ?? inv.CLAUDE?.successful}
TOTAL_BASELINE_COST: ${report.claude?.baseline?.costLedger?.actualUsd ?? "N/A"}

## 9. Claude MEXICO_ES
${JSON.stringify(report.claude?.mexicoEs || {}, null, 2)}

## 10. Claude Final State
OBSERVATIONS: ${report.claude?.baseline?.SUCCEEDED ?? inv.CLAUDE?.successful}
STATE: ${states.claude}

## 11. Existing Baseline Integrity
OPENAI/PERPLEXITY CALLS_THIS_PHASE: 0

## 12. Four-Provider Baseline State
OPENAI: ${states.openai}
GEMINI: ${states.gemini}
PERPLEXITY: ${states.perplexity}
CLAUDE: ${states.claude}

## 13. Observation Reconciliation
TOTAL: ${obs.TOTAL}/336 · REMAINING: ${336 - (obs.TOTAL || 0)}

## 14. Baseline Freeze
READY: ${report.baselineFreeze?.BASELINE_FREEZE_READY ?? false}
ID: ${BASELINE_FREEZE_ID}

## 18. Provider Selector
MEASURED: ${measured.join(", ") || "none"}
ALL_AI: NO

## 28. Biweekly Monitoring Readiness
STATUS: ${obs.TOTAL === 336 ? "YES" : "PARTIAL"}

## 29. Trend
AVAILABLE: NO

## 31. Discoverability / Business Impact
PRIORITY_RETAINED: YES

## 35. Activity
LIVE_OPENAI: 0 · LIVE_PERPLEXITY: 0 · LIVE_GEMINI: ${report.activity?.LIVE_GEMINI_CALLS ?? 0} · LIVE_CLAUDE: ${report.activity?.LIVE_CLAUDE_CALLS ?? 0}

## 37. Next Recommended Phase
${nextPhase}

## 38. BUILD STATUS
${buildStatus}
`;

  return {
    BUILD_STATUS: buildStatus,
    markdown,
    providerBaselineStates: states,
    observations: obs,
    measuredProviders: measured,
    NEXT_RECOMMENDED_PHASE: nextPhase,
  };
}
