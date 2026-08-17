#!/usr/bin/env node
/**
 * Phase 3B.2 — Execute controlled multi-provider validation (live).
 * Gemini / Perplexity / Claude only — no OpenAI.
 */
import { executeMultiProviderValidation } from "../lib/ai-visibility/provider-validation-orchestrator.js";
import { buildControlledValidationPlan } from "../lib/ai-visibility/providers/validation-plan.js";

const plan = buildControlledValidationPlan();
console.log(
  JSON.stringify(
    {
      phase: "3B.2_EXECUTE",
      CALLS_PER_PROVIDER: plan.CALLS_PER_PROVIDER,
      TOTAL_PLANNED: plan.TOTAL_CALLS,
      PROMPT_IDS: plan.PROMPT_IDS,
      LIVE_OPENAI_CALLS: 0,
    },
    null,
    2
  )
);

const report = await executeMultiProviderValidation({ force: false });

const summary = {};
for (const [provider, r] of Object.entries(report.results || {})) {
  summary[provider] = {
    status: r.status || "executed",
    waveId: r.waveId,
    PLANNED: r.PLANNED,
    SUCCEEDED: r.SUCCEEDED,
    FAILED: r.FAILED,
    RETRIES: r.RETRIES,
    TOTAL_ATTEMPTS: r.TOTAL_ATTEMPTS,
    activationGate: r.activationGate?.RESULT,
    storeRoot: r.storeRoot,
    costUsd: r.costLedger?.actualUsd,
  };
}

console.log(
  JSON.stringify(
    {
      parentValidationId: report.parentValidationId,
      waveIds: report.waveIds,
      summary,
      openAiBaselineUntouched: report.openAiBaselineUntouched,
    },
    null,
    2
  )
);

const anyExecuted = Object.values(report.results || {}).some(
  (r) => r.SUCCEEDED > 0 || r.ATTEMPTED > 0
);
if (!anyExecuted) process.exit(2);
